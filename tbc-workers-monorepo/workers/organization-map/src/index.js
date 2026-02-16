// organizations-map worker
// Routes:
//   GET  /orgs                           -> list available GeoJSON files
//   GET  /orgs/<file>.geojson            -> serve specific file
//   GET  /orgs/img/<id>/<index>          -> serve logo image (from R2 or redirect)
//   POST /orgs/admin/regenerate          -> (Bearer REGEN_TOKEN) rebuild data + prewarm images
//   POST /orgs/admin/delete              -> delete file
//   GET  /orgs/ok                        -> health check

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (request.method === "OPTIONS") return withCORS(new Response(null, { status: 204 }));

    try {
      const objectKey = getGeoJsonKey(env);

      // 1. Image Serving Route
      if (request.method === "GET" && pathname.startsWith("/orgs/img/")) {
        const { recordId, index } = parseAttachmentPath(pathname, "/orgs/img");
        if (!recordId) return withCORS(json({ error: "Invalid image path" }, 400));

        // Try R2 cache first
        if (env.ORG_IMAGES_BUCKET) {
          const key = `org-logos/${recordId}/${index}/w400-webp`;
          const obj = await env.ORG_IMAGES_BUCKET.get(key);
          if (obj) {
            return withCORS(new Response(obj.body, {
              headers: { 
                "Content-Type": "image/webp", 
                "Cache-Control": "public, max-age=604800, immutable" 
              }
            }));
          }
        }
        // Fallback to Airtable redirect
        return withCORS(await handleAttachmentRedirect(env, recordId, index, "Org Logo"));
      }

      // 2. List GeoJSON files
      if (request.method === "GET" && (pathname === "/orgs" || pathname === "/orgs/")) {
        const files = await listAllGeoJson(env.ORG_MAP_BUCKET);
        return withCORS(json({ files }));
      }

      // 3. Serve GeoJSON
      if (
        request.method === "GET" &&
        pathname.startsWith("/orgs/") &&
        pathname.endsWith(".geojson") &&
        pathname !== "/orgs/admin/regenerate"
      ) {
        const requestedKey = pathname.replace(/^\/orgs\//, "");
        if (!isSafeKey(requestedKey)) return withCORS(json({ error: "Invalid GeoJSON file name" }, 400));
        const obj = await env.ORG_MAP_BUCKET.get(requestedKey);
        if (!obj) return withCORS(json({ error: "File not found", key: requestedKey }, 404));
        return withCORS(new Response(obj.body, {
          headers: {
            "Content-Type": "application/geo+json; charset=utf-8",
            "Cache-Control": "public, max-age=300",
            "X-Served-From": "R2"
          }
        }));
      }

      // 4. Regenerate (Admin)
      if (request.method === "POST" && pathname === "/orgs/admin/regenerate") {
        const token = (request.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
        if (!token || token !== env.REGEN_TOKEN) return withCORS(json({ error: "Unauthorized" }, 401));

        const tableName = env.ORG_TABLE_NAME || "Master List";
        const fieldLat  = env.FIELD_LAT || "Latitude";
        const fieldLon  = env.FIELD_LON || "Longitude";
        const origin    = new URL(request.url).origin;

        if (!isSafeKey(objectKey)) throw new Error("Invalid GeoJSON file name");

        const MAX_PAGES = clampPages(env.AIRTABLE_PAGE_LIMIT);
        const payload = await readJsonBody(request);

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

        const startCursor = payload.cursor != null ? payload.cursor : state.cursor;

        // Fetch chunk from Airtable
        // NOTE: "Latest Prayer Request" removed from fields array
        const { records, nextCursor, pagesUsed } = await fetchAirtableChunk(env, tableName, [
          fieldLat, fieldLon,
          "Org Name",
          "Website",
          "Category",
          "Denomination",
          "Org Type",
          "Address",
          "County",
          "Network Name",
          "Org Logo",
          "Logo Background"
        ], { offset: startCursor || undefined, maxPages: MAX_PAGES });

        // Prewarm images for this chunk
        if (records.length > 0 && env.ORG_IMAGES_BUCKET) {
          await prewarmChunkImages(env, records, "Org Logo");
        }

        const features = recordsToFeatures(records, { fieldLat, fieldLon, origin });
        const processed = features.length;

        if (nextCursor) {
          // Save chunk
          const chunkKey = `orgs/tmp/${jobId}/chunk-${String(state.chunkCount + 1).padStart(4, "0")}.json`;
          await env.ORG_MAP_BUCKET.put(chunkKey, JSON.stringify(features), {
            httpMetadata: { contentType: "application/json; charset=utf-8", cacheControl: "no-store" }
          });

          // Update state
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

        // Finalize
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
          features: (Number(state.totalFeatures) || 0) + processed,
          updatedAt: new Date().toISOString(),
          objectKey: targetKey
        }));
      }

      // 5. Delete
      if (request.method === "POST" && pathname === "/orgs/admin/delete") {
        const token = (request.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
        if (!token || token !== env.REGEN_TOKEN) return withCORS(json({ error: "Unauthorized" }, 401));

        const body = await readJsonBody(request);
        const rawKey = typeof body.key === "string" ? body.key.trim() : "";
        if (!rawKey) return withCORS(json({ error: "Missing key" }, 400));
        if (!isSafeKey(rawKey) || !rawKey.endsWith(".geojson")) {
          return withCORS(json({ error: "Invalid GeoJSON file name" }, 400));
        }

        const exists = await env.ORG_MAP_BUCKET.head(rawKey);
        if (!exists) return withCORS(json({ error: "GeoJSON file not found", key: rawKey }, 404));

        await env.ORG_MAP_BUCKET.delete(rawKey);
        return withCORS(json({ ok: true, deleted: rawKey }));
      }

      // 6. Health check
      if (request.method === "GET" && pathname === "/orgs/ok") {
        return withCORS(textResponse("OK"));
      }

      return withCORS(json({ error: "Not found" }, 404));
    } catch (err) {
      return withCORS(json({ error: String(err?.message || err) }, 500));
    }
  }
};

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

async function fetchRecordById(env, recordId, tableName) {
  const table = env.ORG_TABLE_NAME || "Master List";
  const url = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` } });
  if (!res.ok) throw new Error(`Airtable error ${res.status}: ${await res.text()}`);
  return res.json();
}

/* ---------------- Image Prewarming & Handling ---------------- */

async function prewarmChunkImages(env, records, fieldName) {
  const tasks = [];
  for (const r of records) {
    const attachments = r.fields[fieldName];
    if (Array.isArray(attachments) && attachments.length > 0) {
      const att = attachments[0];
      tasks.push({ recordId: r.id, index: 0, att });
    }
  }

  await withConcurrency(tasks, 4, async ({ recordId, index, att }) => {
    const srcUrl = pickAttachmentUrl(att);
    if (!srcUrl) return;

    const key = `org-logos/${recordId}/${index}/w400-webp`;
    
    const response = await fetch(srcUrl, {
      cf: {
        cacheEverything: true,
        image: { width: 400, format: "webp", quality: 80 }
      }
    });

    if (response.ok) {
      await env.ORG_IMAGES_BUCKET.put(key, response.body, {
        httpMetadata: { 
          contentType: "image/webp", 
          cacheControl: "public, max-age=604800, immutable" 
        }
      });
    }
  });
}

async function withConcurrency(items, limit, fn) {
  const results = [];
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const cur = i++;
      try { results[cur] = await fn(items[cur]); } catch (e) { console.error(e); }
    }
  });
  await Promise.all(workers);
  return results;
}

function pickAttachmentUrl(att) {
  if (!att) return null;
  if (typeof att === 'string') return /^https?:\/\//i.test(att) ? att : null;
  
  // Prefer original URL to avoid bad thumbnail quality from Airtable
  return att.url || att?.thumbnails?.large?.url || att?.thumbnails?.full?.url || null;
}

function parseAttachmentPath(pathname, prefix) {
  const relative = pathname.replace(prefix, ""); 
  const parts = relative.split("/").filter(Boolean);
  return { recordId: parts[0], index: Number(parts[1] || 0) };
}

const urlCache = new Map();
async function handleAttachmentRedirect(env, recordId, index, fieldName) {
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0) return textResponse("Bad index", 400);

  const cacheKey = `${fieldName}:${recordId}:${idx}`;
  const cached = urlCache.get(cacheKey);
  if (cached && Date.now() < cached.expires) {
    return Response.redirect(cached.url, 302);
  }

  try {
    const rec = await fetchRecordById(env, recordId);
    const attachments = rec.fields?.[fieldName];
    if (!Array.isArray(attachments) || !attachments[idx]) {
      return textResponse("Image not found", 404);
    }
    const url = pickAttachmentUrl(attachments[idx]);
    if (!url) return textResponse("Invalid URL", 404);

    urlCache.set(cacheKey, { url, expires: Date.now() + 600000 }); 
    return Response.redirect(url, 302);
  } catch (e) {
    return textResponse("Error fetching record", 500);
  }
}

/* ---------------- JSON / GeoJSON Builders ---------------- */

function recordsToFeatures(records, { fieldLat, fieldLon, origin }) {
  const features = [];
  for (const r of records) {
    const f = r?.fields || {};
    const lat = toNum(f[fieldLat]);
    const lon = toNum(f[fieldLon]);
    if (!isFinite(lat) || !isFinite(lon)) continue;

    const props = {
      id: r.id,
      organization_name: normalizeValue(f["Org Name"]),
      website:           normalizeValue(f["Website"]),
      category:          normalizeValue(f["Category"]),
      // Use cleanDenomination to strip parenthetical text
      denomination:      cleanDenomination(f["Denomination"]),
      organization_type: normalizeValue(f["Org Type"]),
      full_address:      normalizeValue(f["Address"]),
      county:            normalizeValue(f["County"]),
      network_name:      normalizeValue(f["Network Name"]),
      // REMOVED: prayer_request mapping
      logo_background:   normalizeValue(f["Logo Background"])
    };

    if (Array.isArray(f["Org Logo"]) && f["Org Logo"].length > 0) {
      props.logo = `${origin}/orgs/img/${r.id}/0`;
    }

    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [lon, lat] },
      properties: props
    });
  }
  return features;
}

/* ---------------- Utils ---------------- */

// Helper to strip parenthetical text from denomination
function cleanDenomination(v) {
  const s = normalizeValue(v);
  // Removes " (Any Text)" including the parentheses
  return s.replace(/\s*\([^)]*\)/g, "").trim();
}

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
  return (typeof raw === "string" && raw.trim()) ? raw.trim() : "organization_map.geojson";
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
  } catch { return {}; }
}

async function readState(bucket, key) {
  const obj = await bucket.get(key);
  if (!obj) return null;
  try { return JSON.parse(await obj.text()); } catch { return null; }
}

function clampPages(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return 10;
  return Math.min(20, Math.max(1, Math.floor(n)));
}
