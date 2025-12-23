// disaster-data worker (Dual Mode: Org Points & Resource Data)
//
// Routes:
//   GET  /disaster/org_points.geojson       (Existing)
//   GET  /disaster/resource_data.geojson    (New)
//   POST /disaster/admin/regenerate         (Existing - updates Org Points)
//   POST /disaster/admin/regenerate-resources (New - updates Resource Data)
//   GET  /disaster/ok
//
// Env vars (Settings -> Variables/Secrets):
//   AIRTABLE_TOKEN, AIRTABLE_BASE_ID, REGEN_TOKEN
//   ORG_TABLE_NAME      (default "Master List")
//   RESOURCE_TABLE_NAME (default "Resource Data")
//   FIELD_LAT (default "Latitude"), FIELD_LON (default "Longitude")
//
// R2 binding (Settings -> Bindings -> R2):
//   ORG_PINS_BUCKET  -> your bucket

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (request.method === "OPTIONS") return withCORS(new Response(null, { status: 204 }));

    try {
      // ---------------------------------------------------------
      // 1. GET: Serve the Files
      // ---------------------------------------------------------

      // Existing: Serve Org Points
      if (request.method === "GET" && pathname === "/disaster/org_points.geojson") {
        const obj = await env.ORG_PINS_BUCKET.get("org_points.geojson");
        if (!obj) return withCORS(json({ error: "Points GeoJSON not generated yet" }, 404));
        return withCORS(new Response(obj.body, {
          headers: {
            "Content-Type": "application/geo+json; charset=utf-8",
            "Cache-Control": "public, max-age=300",
            "X-Served-From": "R2"
          }
        }));
      }

      // NEW: Serve Resource Data
      if (request.method === "GET" && pathname === "/disaster/resource_data.geojson") {
        const obj = await env.ORG_PINS_BUCKET.get("resource_data.geojson");
        if (!obj) return withCORS(json({ error: "Resource GeoJSON not generated yet" }, 404));
        return withCORS(new Response(obj.body, {
          headers: {
            "Content-Type": "application/geo+json; charset=utf-8",
            "Cache-Control": "public, max-age=300",
            "X-Served-From": "R2"
          }
        }));
      }

      // ---------------------------------------------------------
      // 2. POST: Regenerate Data (Admin Only)
      // ---------------------------------------------------------

      // Common Auth Check for Admin Routes
      if (pathname.startsWith("/disaster/admin/")) {
        const token = (request.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
        if (!token || token !== env.REGEN_TOKEN) return withCORS(json({ error: "Unauthorized" }, 401));
      }

      // Existing: Regenerate Master List
      if (request.method === "POST" && pathname === "/disaster/admin/regenerate") {
        const tableName = env.ORG_TABLE_NAME || "Master List";
        const fieldLat  = env.FIELD_LAT || "Latitude";
        const fieldLon  = env.FIELD_LON || "Longitude";

        const records = await fetchAllAirtableRecords(env, tableName, [
          fieldLat, fieldLon,
          "Organization Name", "Website", "Organization Type", "Full Address",
          "Disaster Contact", "Disaster Email", "Disaster Phone",
          "Regular Services", "Disaster Services", "Physical Resources"
        ]);

        const features = records.map(r => {
          const f = r.fields || {};
          const lat = toNum(f[fieldLat]);
          const lon = toNum(f[fieldLon]);
          if (!isFinite(lat) || !isFinite(lon)) return null;

          return {
            type: "Feature",
            geometry: { type: "Point", coordinates: [lon, lat] },
            properties: {
              id: r.id,
              organization_name: normalizeValue(f["Organization Name"]),
              website:           normalizeValue(f["Website"]),
              organization_type: normalizeValue(f["Organization Type"]),
              full_address:      normalizeValue(f["Full Address"]),
              disaster_contact:  normalizeValue(f["Disaster Contact"]),
              disaster_email:    normalizeValue(f["Disaster Email"]),
              disaster_phone:    normalizeValue(f["Disaster Phone"]),
              regular_services:  normalizeValue(f["Regular Services"]),
              disaster_services: normalizeValue(f["Disaster Services"]),
              physical_resources:normalizeValue(f["Physical Resources"])
            }
          };
        }).filter(Boolean); // Remove nulls (missing lat/lon)

        await saveToR2(env, "org_points.geojson", features);
        return withCORS(json({ ok: true, features: features.length, updatedAt: new Date().toISOString() }));
      }

      // NEW: Regenerate Resource Data
      if (request.method === "POST" && pathname === "/disaster/admin/regenerate-resources") {
        const tableName = env.RESOURCE_TABLE_NAME || "Resource Data"; // Default to "Resource Data"
        const fieldLat  = env.FIELD_LAT || "Latitude";
        const fieldLon  = env.FIELD_LON || "Longitude";

        // Fetch fields specific to the Resource Data table
        const records = await fetchAllAirtableRecords(env, tableName, [
          fieldLat, fieldLon,
          "Organization Name", "Website Address", "Organization Type",
          "Organization Address", "Size", "Organization Leader",
          "Org Leader Email", "Org Leader Phone", "Disaster Contact",
          "Disaster Email", "Disaster Phone", "Compassion Leader",
          "Compassion Email", "Compassion Phone", "Regular Services",
          "Disaster Services", "Physical Resources", "Other Resources",
          "Timestamp"
        ]);

        const features = records.map(r => {
          const f = r.fields || {};
          const lat = toNum(f[fieldLat]);
          const lon = toNum(f[fieldLon]);
          if (!isFinite(lat) || !isFinite(lon)) return null;

          return {
            type: "Feature",
            geometry: { type: "Point", coordinates: [lon, lat] },
            properties: {
              id: r.id,
              organization_name:       normalizeValue(f["Organization Name"]),
              website:                 normalizeValue(f["Website Address"]),
              organization_type:       normalizeValue(f["Organization Type"]),
              full_address:            normalizeValue(f["Organization Address"]),
              org_size:                normalizeValue(f["Size"]),
              org_leader:              normalizeValue(f["Organization Leader"]),
              org_leader_email:        normalizeValue(f["Org Leader Email"]),
              org_leader_phone:        normalizeValue(f["Org Leader Phone"]),
              disaster_contact:        normalizeValue(f["Disaster Contact"]),
              disaster_email:          normalizeValue(f["Disaster Email"]),
              disaster_phone:          normalizeValue(f["Disaster Phone"]),
              compassion_leader:       normalizeValue(f["Compassion Leader"]),
              compassion_leader_email: normalizeValue(f["Compassion Email"]),
              compassion_leader_phone: normalizeValue(f["Compassion Phone"]),
              regular_services:        normalizeValue(f["Regular Services"]),
              disaster_services:       normalizeValue(f["Disaster Services"]),
              physical_resources:      normalizeValue(f["Physical Resources"]),
              other_resources:         normalizeValue(f["Other Resources"]),
              last_update:             normalizeValue(f["Timestamp"])
            }
          };
        }).filter(Boolean);

        await saveToR2(env, "resource_data.geojson", features);
        return withCORS(json({ ok: true, features: features.length, file: "resource_data.geojson", updatedAt: new Date().toISOString() }));
      }

      // Check Status
      if (request.method === "GET" && pathname === "/disaster/ok") {
        return withCORS(textResponse("OK"));
      }

      return withCORS(json({ error: "Not found" }, 404));
    } catch (err) {
      return withCORS(json({ error: String(err?.message || err) }, 500));
    }
  }
};

/* ---------------- Airtable & R2 helpers ---------------- */

async function fetchAllAirtableRecords(env, tableName, fields = []) {
  const base = env.AIRTABLE_BASE_ID;
  const key  = env.AIRTABLE_TOKEN;
  const api  = new URL(`https://api.airtable.com/v0/${base}/${encodeURIComponent(tableName)}`);

  api.searchParams.set("pageSize", "100");
  api.searchParams.set("cellFormat", "string"); // <--- ADD THIS LINE
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

async function saveToR2(env, filename, features) {
  const fc = JSON.stringify({ type: "FeatureCollection", features });
  await env.ORG_PINS_BUCKET.put(filename, fc, {
    httpMetadata: {
      contentType: "application/geo+json; charset=utf-8",
      cacheControl: "public, max-age=60"
    }
  });
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
