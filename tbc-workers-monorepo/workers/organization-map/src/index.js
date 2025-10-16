// organizations-map worker
// Routes you attach to this Worker:
//   GET  /orgs               -> list available GeoJSON files
//   GET  /orgs/<geojson-file>.geojson
//   POST /orgs/admin/regenerate
//   GET  /orgs/ok
//
// Env vars (Settings → Variables/Secrets):
//   AIRTABLE_TOKEN, AIRTABLE_BASE_ID, REGEN_TOKEN
//   ORG_TABLE_NAME (default "Master List")
//   ORG_GEOJSON_FILE (default "organization_map.geojson")
//   FIELD_LAT (default "Latitude"), FIELD_LON (default "Longitude")
//   (optional) AIRTABLE_VIEW_NAME
//   (optional) AIRTABLE_PAGE_LIMIT (default 10 pages per request)
//
// R2 binding (Settings → Bindings → R2):
//   ORG_MAP_BUCKET  -> your bucket (object key: configurable via ORG_GEOJSON_FILE)

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (request.method === "OPTIONS") return withCORS(new Response(null, { status: 204 }));

    try {
      const objectKey = getGeoJsonKey(env);

      // List available GeoJSON files so other services (e.g. network_map.api)
      // can discover what has already been generated.
      if (request.method === "GET" && (pathname === "/orgs" || pathname === "/orgs/")) {
        const files = await listAllGeoJson(env.ORG_MAP_BUCKET);
        return withCORS(json({ files }));
      }

      // Serve the latest generated GeoJSON from R2
      if (
        request.method === "GET" &&
        pathname.startsWith("/orgs/") &&
        pathname.endsWith(".geojson") &&
        pathname !== "/orgs/admin/regenerate"
      ) {
        const requestedKey = pathname.replace(/^\/orgs\//, "");
        if (!isSafeKey(requestedKey)) return withCORS(json({ error: "Invalid GeoJSON file name" }, 400));
        const obj = await env.ORG_MAP_BUCKET.get(requestedKey);
        if (!obj) return withCORS(json({ error: "Organization GeoJSON not generated yet", key: requestedKey }, 404));
        return withCORS(new Response(obj.body, {
          headers: {
            "Content-Type": "application/geo+json; charset=utf-8",
            "Cache-Control": "public, max-age=300",
            "X-Served-From": "R2"
          }
        }));
      }

      // Regenerate from Airtable -> write to R2
      if (
          request.method === "POST" &&
          (pathname === "/orgs/admin/regenerate" || pathname === "/orgs/admin/regenerate/")
          ) {
        const token = (request.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
        if (!token || token !== env.REGEN_TOKEN) return withCORS(json({ error: "Unauthorized" }, 401));

        const payload = await readJsonBody(request);
        const waitForCompletion = payload.waitForCompletion !== false; // default true
        const isInternalCall = payload._internal === true;

        // If this is an external call that wants completion, start the chain
        if (waitForCompletion && !isInternalCall) {
          return await handleAutoComplete(request, env, token);
        }

        // Otherwise process a single chunk (original behavior)
        return await processSingleChunk(request, env, objectKey, payload);
      }

      if (request.method === "GET" && pathname === "/orgs/ok") {
        return withCORS(textResponse("OK"));
      }

      return withCORS(json({ error: "Not found" }, 404));
    } catch (err) {
      return withCORS(json({ error: String(err?.message || err) }, 500));
    }
  }
};

/* ---------------- Auto-complete handler ---------------- */

async function handleAutoComplete(request, env, authToken) {
  const origin = new URL(request.url).origin;
  const internalUrl = `${origin}/orgs/admin/regenerate`;
  let cursor = null;
  let totalFeatures = 0;
  let iterations = 0;
  const maxIterations = 100; // Safety limit (6000 rows / 100 per page / 10 pages = 6 iterations)

  while (iterations < maxIterations) {
    iterations++;
    
    // Call ourselves internally
    const response = await fetch(internalUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${authToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        cursor,
        _internal: true, // Flag to prevent infinite recursion
        waitForCompletion: false
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Internal call failed: ${response.status} - ${errorText}`);
    }

    const result = await response.json();

    if (result.status === 'completed') {
      // Done! Return the final result
      return withCORS(json({
        ok: true,
        status: "completed",
        jobId: result.jobId,
        features: result.features,
        iterations,
        updatedAt: result.updatedAt,
        objectKey: result.objectKey
      }));
    }

    if (result.status === 'in_progress') {
      cursor = result.nextCursor;
      totalFeatures = result.totalFeatures || 0;
      
      // Continue to next iteration
      if (!cursor) {
        throw new Error("Received in_progress but no cursor");
      }
    } else {
      throw new Error(`Unexpected status: ${result.status}`);
    }
  }

  throw new Error(`Max iterations (${maxIterations}) reached. Data may be too large.`);
}

/* ---------------- Single chunk processor (original logic) ---------------- */

async function processSingleChunk(request, env, objectKey, payload) {
  const tableName = env.ORG_TABLE_NAME || "Master List";
  const fieldLat  = env.FIELD_LAT || "Latitude";
  const fieldLon  = env.FIELD_LON || "Longitude";

  if (!isSafeKey(objectKey)) throw new Error("Invalid GeoJSON file name");

  const MAX_PAGES = clampPages(env.AIRTABLE_PAGE_LIMIT);

  let jobId = payload.jobId || null;
  if (!jobId) jobId = (typeof crypto?.randomUUID === "function") ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const stateKey = `orgs/tmp/${jobId}.json`;
  const existingState = await readState(env.ORG_MAP_BUCKET, stateKey);

  const state = existingState ?? {
    jobId,
    objectKey,
    tableName,
    fieldLat,
    fieldLon,
    createdAt: new Date().toISOString(),
    cursor: null,
    chunkKeys: [],
    chunkCount: 0,
    totalFeatures: 0
  };

  const targetKey = state.objectKey || objectKey;
  state.objectKey = targetKey;
  if (!Array.isArray(state.chunkKeys)) state.chunkKeys = [];
  if (!Number.isFinite(Number(state.chunkCount))) state.chunkCount = state.chunkKeys.length;
  if (state.cursor === undefined) state.cursor = null;
  if (!Number.isFinite(Number(state.totalFeatures))) state.totalFeatures = 0;

  // If the request includes a cursor use that, otherwise resume from saved state.
  const startCursor = payload.cursor != null ? payload.cursor : state.cursor;

  const { records, nextCursor, pagesUsed } = await fetchAirtableChunk(env, tableName, [
    fieldLat, fieldLon,
    "Org Name",
    "Website",
    "Category",
    "Denomination",
    "Org Type",
    "Address",
    "County",
    "Network Name"
  ], { offset: startCursor || undefined, maxPages: MAX_PAGES });

  const features = recordsToFeatures(records, { fieldLat, fieldLon });
  const processed = features.length;

  // If there's still more to pull, persist this chunk and return progress.
  if (nextCursor) {
    const chunkKey = `orgs/tmp/${jobId}/chunk-${String(state.chunkCount + 1).padStart(4, "0")}.json`;
    await env.ORG_MAP_BUCKET.put(chunkKey, JSON.stringify(features), {
      httpMetadata: { contentType: "application/json; charset=utf-8", cacheControl: "no-store" }
    });

    state.cursor = nextCursor;
    state.chunkCount += 1;
    state.chunkKeys.push(chunkKey);
    state.totalFeatures = (Number(state.totalFeatures) || 0) + processed;
    state.updatedAt = new Date().toISOString();

    await env.ORG_MAP_BUCKET.put(stateKey, JSON.stringify(state), {
      httpMetadata: { contentType: "application/json; charset=utf-8", cacheControl: "no-store" }
    });

    return withCORS(json({
      ok: true,
      status: "in_progress",
      jobId,
      nextCursor,
      processed,
      totalFeatures: state.totalFeatures,
      pagesUsed,
      objectKey: targetKey
    }));
  }

  // No more Airtable pages to fetch -> gather any stored chunks and finalize.
  const allFeatures = [];

  if (state.chunkKeys.length) {
    for (const key of state.chunkKeys) {
      const chunkObj = await env.ORG_MAP_BUCKET.get(key);
      if (chunkObj) {
        const arr = JSON.parse(await chunkObj.text());
        if (Array.isArray(arr)) allFeatures.push(...arr);
        await env.ORG_MAP_BUCKET.delete(key).catch(() => {});
      }
    }
  }

  allFeatures.push(...features);

  const priorCount = Number(state.totalFeatures) || 0;
  const totalCount = priorCount + processed;

  const fc = JSON.stringify({ type: "FeatureCollection", features: allFeatures });

  await env.ORG_MAP_BUCKET.put(targetKey, fc, {
    httpMetadata: {
      contentType: "application/geo+json; charset=utf-8",
      cacheControl: "public, max-age=60"
    }
  });

  await env.ORG_MAP_BUCKET.delete(stateKey).catch(() => {});

  return withCORS(json({
    ok: true,
    status: "completed",
    jobId,
    features: totalCount,
    updatedAt: new Date().toISOString(),
    objectKey: targetKey
  }));
}

/* ---------------- Airtable helpers ---------------- */

async function fetchAirtableChunk(env, tableName, fields = [], { offset, maxPages }) {
  const base = env.AIRTABLE_BASE_ID;
  const key  = env.AIRTABLE_TOKEN;
  const api  = new URL(`https://api.airtable.com/v0/${base}/${encodeURIComponent(tableName)}`);

  api.searchParams.set("pageSize", "100");
  if (env.AIRTABLE_VIEW_NAME) api.searchParams.set("view", env.AIRTABLE_VIEW_NAME);
  for (const f of fields) if (f) api.searchParams.append("fields[]", f);

  const records = [];
  let nextOffset = offset;
  let pagesUsed = 0;

  while (pagesUsed < maxPages) {
    const url = new URL(api);
    if (nextOffset) url.searchParams.set("offset", nextOffset);

    const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
    pagesUsed += 1;
    if (!resp.ok) throw new Error(`Airtable error ${resp.status}: ${await resp.text()}`);

    const data = await resp.json();
    if (Array.isArray(data.records) && data.records.length) records.push(...data.records);

    if (data.offset) {
      nextOffset = data.offset;
    } else {
      nextOffset = null;
      break;
    }
  }

  return { records, nextCursor: nextOffset, pagesUsed };
}

/* ---------------- small utils ---------------- */

function toNum(v) { if (v == null) return NaN; return Number(typeof v === "string" ? v.trim() : v); }

function normalizeValue(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return [...new Set(v.map(normalizeValue))].filter(Boolean).join(", ");
  if (typeof v === "object") {
    const cand = v.email ?? v.name ?? v.text ?? v.value ?? null;
    if (cand != null) return String(cand).trim();
    return Object.values(v).map(normalizeValue).filter(Boolean).join(", ");
  }
  return String(v).trim();
}

function withCORS(res) {
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  h.set("Access-Control-Allow-Headers", "Content-Type,Authorization");
  h.set("Vary", "Origin");
  return new Response(res.body, { status: res.status, headers: h });
}
function textResponse(s, status = 200) {
  return new Response(s, { status, headers: { "Content-Type": "text/plain; charset=utf-8" } });
}
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "Content-Type": "application/json; charset=utf-8" } });
}

function getGeoJsonKey(env) {
  const raw = env.ORG_GEOJSON_FILE;
  const key = typeof raw === "string" && raw.trim() ? raw.trim() : "organization_map.geojson";
  return key;
}

function isSafeKey(key) {
  return typeof key === "string" && !key.includes("../") && !key.startsWith("/");
}

async function listAllGeoJson(bucket) {
  const files = [];
  let cursor;
  do {
    const page = await bucket.list({ cursor });
    for (const obj of page.objects || []) {
      if (obj?.key && obj.key.endsWith(".geojson") && isSafeKey(obj.key)) {
        files.push({
          key: obj.key,
          size: obj.size ?? null,
          uploaded: obj.uploaded ? new Date(obj.uploaded).toISOString() : null
        });
      }
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return files;
}

async function readJsonBody(request) {
  try {
    const contentType = request.headers.get("Content-Type") || "";
    if (!contentType.includes("application/json")) return {};
    const text = await request.text();
    if (!text) return {};
    return JSON.parse(text);
  } catch {
    return {};
  }
}

async function readState(bucket, key) {
  const obj = await bucket.get(key);
  if (!obj) return null;
  try {
    return JSON.parse(await obj.text());
  } catch {
    return null;
  }
}

function clampPages(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return 10;
  return Math.min(20, Math.max(1, Math.floor(n)));
}

function recordsToFeatures(records, { fieldLat, fieldLon }) {
  const features = [];
  for (const r of records) {
    const f = r?.fields || {};
    const lat = toNum(f[fieldLat]);
    const lon = toNum(f[fieldLon]);
    if (!isFinite(lat) || !isFinite(lon)) continue;

    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [lon, lat] },
      properties: {
        id: r.id,
        organization_name: normalizeValue(f["Org Name"]),
        website:           normalizeValue(f["Website"]),
        category:          normalizeValue(f["Category"]),
        denomination:      normalizeValue(f["Denomination"]),
        organization_type: normalizeValue(f["Org Type"]),
        full_address:      normalizeValue(f["Address"]),
        county:            normalizeValue(f["County"]),
        network_name:      normalizeValue(f["Network Name"])
      }
    });
  }
  return features;
}
