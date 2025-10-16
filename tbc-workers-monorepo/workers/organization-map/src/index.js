// organizations-map worker
// Routes you attach to this Worker:
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
//
// R2 binding (Settings → Bindings → R2):
//   ORG_MAP_BUCKET  -> your bucket (object key: configurable via ORG_GEOJSON_FILE)

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (request.method === "OPTIONS") return withCORS(new Response(null, { status: 204 }));

    try {
      const objectKey = getGeoJsonKey(env);

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
      if (request.method === "POST" && pathname === "/orgs/admin/regenerate") {
        const token = (request.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
        if (!token || token !== env.REGEN_TOKEN) return withCORS(json({ error: "Unauthorized" }, 401));

        const tableName = env.ORG_TABLE_NAME || "Master List";
        const fieldLat  = env.FIELD_LAT || "Latitude";
        const fieldLon  = env.FIELD_LON || "Longitude";

        // Pull only the columns we need (plus lat/lon)
        const records = await fetchAllAirtableRecords(env, tableName, [
          fieldLat, fieldLon,
          "Org Name",
          "Website",
          "Category",
          "Denomination",
          "Org Type",
          "Address",
          "County",
          "Network Name"
        ]);

        const features = [];
        for (const r of records) {
          const f = r.fields || {};
          const lat = toNum(f[fieldLat]);
          const lon = toNum(f[fieldLon]);
          if (!isFinite(lat) || !isFinite(lon)) continue;

          features.push({
            type: "Feature",
            geometry: { type: "Point", coordinates: [lon, lat] }, // [lon, lat]
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

        const fc = JSON.stringify({ type: "FeatureCollection", features });

        if (!isSafeKey(objectKey)) throw new Error("Invalid GeoJSON file name");

        await env.ORG_MAP_BUCKET.put(objectKey, fc, {
          httpMetadata: {
            contentType: "application/geo+json; charset=utf-8",
            cacheControl: "public, max-age=60"
          }
        });

        return withCORS(json({
          ok: true,
          features: features.length,
          updatedAt: new Date().toISOString(),
          objectKey
        }));
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

/* ---------------- Airtable helpers ---------------- */

async function fetchAllAirtableRecords(env, tableName, fields = []) {
  const base = env.AIRTABLE_BASE_ID;
  const key  = env.AIRTABLE_TOKEN;
  const api  = new URL(`https://api.airtable.com/v0/${base}/${encodeURIComponent(tableName)}`);

  api.searchParams.set("pageSize", "100");
  if (env.AIRTABLE_VIEW_NAME) api.searchParams.set("view", env.AIRTABLE_VIEW_NAME);
  for (const f of fields) if (f) api.searchParams.append("fields[]", f);

  const out = [];
  let offset;
  while (true) {
    const url = new URL(api);
    if (offset) url.searchParams.set("offset", offset);

    const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
    if (!resp.ok) throw new Error(`Airtable error ${resp.status}: ${await resp.text()}`);
    const data = await resp.json();

    if (data.records?.length) out.push(...data.records);
    if (data.offset) offset = data.offset; else break;
  }
  return out;
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
