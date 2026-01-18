// Static publishing model for your network map
// Routes:
//   GET  /networks.geojson         -> serve latest from R2 (404 until first publish)
//   POST /admin/regenerate         -> (Bearer REGEN_TOKEN) rebuild from Airtable and publish to R2
//
// Requires (Settings → Variables/Secrets):
//   AIRTABLE_TOKEN
//   AIRTABLE_BASE_ID
//   NETWORKS_TABLE_NAME
//   (optional) AIRTABLE_VIEW_NAME
//   REGEN_TOKEN 
//
// Requires (Settings → Bindings → R2 bucket):
//   Binding name: NETWORK_MAP_BUCKET  (points to your R2 bucket)
//   The object key used: latest.geojson

export default {
  async fetch(request, env, ctx) {
    const { pathname } = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return withCORS(new Response(null, { status: 204 }));
    }

    try {
      if (request.method === 'GET' && (pathname === '/networks.geojson' || pathname === '/networks/polygons.geojson')) {
        const obj = await env.NETWORK_MAP_BUCKET.get('latest.geojson');
        if (!obj) return withCORS(json({ error: 'GeoJSON not generated yet' }, 404));
        return withCORS(new Response(obj.body, {
          headers: {
            'Content-Type': 'application/geo+json; charset=utf-8',
            'Cache-Control': 'public, max-age=300'
          }
        }));
      }

      if (request.method === 'POST' && (pathname === '/admin/regenerate' || pathname === '/networks/admin/regenerate')) {
        const token = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
        if (!token || token !== env.REGEN_TOKEN) {
          return withCORS(json({ error: 'Unauthorized' }, 401));
        }

        const records = await fetchAllRecords(env);
        
        // This will now actually work because the binding exists!
        await prewarmAll(env, records);

        const origin = new URL(request.url).origin;

        const features = [];
        for (const r of records) {
          const f = r.fields || {};
          const geometry = parseGeometry(f['Polygon']);
          if (!geometry) continue;

          const leaders = normalizeLeaders(f['Network Leaders Names']) || '';
          const allPrayerRequests = normalizeTextField(
            f['All Prayer Requests'] ?? f['All Prayer Request'] ?? f['Prayer Requests']
          );
          const latestPrayerRequest = normalizeTextField(f['Latest Prayer Request']);
          const canonicalPrayerRequest = allPrayerRequests || latestPrayerRequest;

          let photoUrls = [];
          const photoField = f['Photo'];
          const isPhotoAttachmentArray =
            Array.isArray(photoField) && photoField.length > 0 &&
            typeof photoField[0] === 'object' && (photoField[0]?.url || photoField[0]?.thumbnails);

          if (isPhotoAttachmentArray) {
            photoUrls = photoField.slice(0, 6).map((_, idx) => `${origin}/img/${r.id}/${idx}`);
          } else {
            photoUrls = [...new Set(collectPhotoUrls(photoField).map(normalizeUrl))].slice(0, 6);
          }

          let imageUrls = [];
          const imageField = f['Image'];
          const isImageAttachmentArray =
            Array.isArray(imageField) && imageField.length > 0 &&
            typeof imageField[0] === 'object' && (imageField[0]?.url || imageField[0]?.thumbnails);

          if (isImageAttachmentArray) {
            imageUrls = imageField.slice(0, 6).map((_, idx) => `${origin}/image/${r.id}/${idx}`);
          } else {
            imageUrls = [...new Set(collectPhotoUrls(imageField).map(normalizeUrl))].slice(0, 6);
          }

          const [photo1 = '', photo2 = '', photo3 = '', photo4 = '', photo5 = '', photo6 = ''] = photoUrls;
          const [image1 = '', image2 = '', image3 = '', image4 = '', image5 = '', image6 = ''] = imageUrls;
          const photo_count = photoUrls.filter(Boolean).length;
          const image_count = imageUrls.filter(Boolean).length;

          features.push({
            type: 'Feature',
            geometry,
            properties: {
              id: r.id,
              name: f['Network Name'] ?? '',
              leaders,
              contact_email: normalizeTextField(f['contact email'] ?? f['Contact Email'] ?? f['Contact email']),
              status: normalizeTextField(f['Status']),
              county: normalizeTextField(f['County']),
              tags: normalizeTextField(f['Tags']),
              number_of_churches: f['Number of Churches'] ?? '',
              unify_lead: normalizeTextField(f['Unify Lead']),
              latest_prayer_request: latestPrayerRequest,
              all_prayer_requests: allPrayerRequests,
              prayer_request: canonicalPrayerRequest,
              prayer_requests: canonicalPrayerRequest,
              photo1, photo2, photo3, photo4, photo5, photo6,
              photo_count,
              image1, image2, image3, image4, image5, image6,
              image_count,
            },
          });
        }

        const fc = JSON.stringify({ type: 'FeatureCollection', features });

        await env.NETWORK_MAP_BUCKET.put('latest.geojson', fc, {
          httpMetadata: {
            contentType: 'application/geo+json; charset=utf-8',
            cacheControl: 'public, max-age=60'
          }
        });

        return withCORS(json({
          ok: true,
          features: features.length,
          updatedAt: new Date().toISOString()
        }));
      }

      // --- UPDATED IMAGE HANDLERS ---
      if (request.method === 'GET' && pathname.startsWith('/img/')) {
        const { recordId, index } = parseAttachmentPath(pathname, 'img');
        if (!recordId) return withCORS(text('Bad index', 400));
        
        // 1. Try serving from R2 cache first
        if (env.NETWORK_IMAGES_BUCKET) {
            const key = `images/${recordId}/${index}/w400-webp`;
            const obj = await env.NETWORK_IMAGES_BUCKET.get(key);
            if (obj) {
                return withCORS(new Response(obj.body, {
                    headers: { 
                        'Content-Type': 'image/webp', 
                        'Cache-Control': 'public, max-age=604800, immutable' 
                    }
                }));
            }
        }
        // 2. Fallback to Airtable redirect
        return withCORS(await handleAttachmentRedirect(env, recordId, index, 'Photo'));
      }

      if (request.method === 'GET' && pathname.startsWith('/image/')) {
        const { recordId, index } = parseAttachmentPath(pathname, 'image');
        if (!recordId) return withCORS(text('Bad index', 400));

        // 1. Try serving from R2 cache first
        if (env.NETWORK_IMAGES_BUCKET) {
            const key = `images/${recordId}/${index}/w400-webp`;
            const obj = await env.NETWORK_IMAGES_BUCKET.get(key);
            if (obj) {
                return withCORS(new Response(obj.body, {
                    headers: { 
                        'Content-Type': 'image/webp', 
                        'Cache-Control': 'public, max-age=604800, immutable' 
                    }
                }));
            }
        }
        // 2. Fallback to Airtable redirect
        return withCORS(await handleAttachmentRedirect(env, recordId, index, 'Image'));
      }

      if (request.method === 'GET' && pathname === '/') {
        return withCORS(text('OK'));
      }

      return withCORS(json({ error: 'Not found' }, 404));
    } catch (err) {
      return withCORS(json({ error: String(err?.message || err) }, 500));
    }
  }
};

/* ---------------- Airtable fetch ---------------- */

async function fetchAllRecords(env) {
  const all = [];
  let offset;
  const baseUrl = new URL(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.NETWORKS_TABLE_NAME)}`);
  if (env.AIRTABLE_VIEW_NAME) baseUrl.searchParams.set('view', env.AIRTABLE_VIEW_NAME);
  baseUrl.searchParams.set('pageSize', '100');

  while (true) {
    const url = new URL(baseUrl);
    if (offset) url.searchParams.set('offset', offset);

    const res = await fetch(url, { headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` } });
    if (!res.ok) throw new Error(`Airtable error ${res.status}: ${await res.text()}`);

    const data = await res.json();
    if (data.records?.length) all.push(...data.records);
    if (data.offset) offset = data.offset; else break;
  }
  return all;
}

async function fetchRecordById(env, recordId) {
  const url = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.NETWORKS_TABLE_NAME)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` } });
  if (!res.ok) throw new Error(`Airtable error ${res.status}: ${await res.text()}`);
  return res.json();
}

/* ---------------- R2 prewarm helpers ---------------- */

function r2KeyForImage(recordId, index, variant = 'w400-webp') {
  return `images/${recordId}/${index}/${variant}`;
}

async function withConcurrency(items, limit, fn) {
  const results = [];
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const cur = i++;
      try { results[cur] = await fn(items[cur]); } catch (e) {}
    }
  });
  await Promise.all(workers);
  return results;
}

async function prewarmAttachments(env, recordId, fieldArray, maxCount = 6) {
  if (!env.NETWORK_IMAGES_BUCKET) return; 
  if (!Array.isArray(fieldArray) || fieldArray.length === 0) return;

  const tasks = fieldArray.slice(0, maxCount).map((att, idx) => ({ att, idx }));

  await withConcurrency(tasks, 4, async ({ att, idx }) => {
    const srcUrl = pickAttachmentUrl(att);
    if (!srcUrl) return;

    // Resize at edge to 400px webp
    const response = await fetch(srcUrl, {
      cf: {
        cacheEverything: true,
        image: { width: 400, format: 'webp', quality: 80 }
      }
    });
    if (!response.ok) return;

    const key = r2KeyForImage(recordId, idx, 'w400-webp');
    await env.NETWORK_IMAGES_BUCKET.put(key, response.body, {
      httpMetadata: { contentType: 'image/webp', cacheControl: 'public, max-age=604800, immutable' }
    });
  });
}

async function prewarmAll(env, records) {
  for (const r of records) {
    const f = r.fields || {};
    const photoField = f['Photo'];
    const imageField = f['Image'];

    const isPhotoAttachmentArray =
      Array.isArray(photoField) && photoField.length > 0 &&
      typeof photoField[0] === 'object' && (photoField[0]?.url || photoField[0]?.thumbnails);

    const isImageAttachmentArray =
      Array.isArray(imageField) && imageField.length > 0 &&
      typeof imageField[0] === 'object' && (imageField[0]?.url || imageField[0]?.thumbnails);

    if (isPhotoAttachmentArray) {
      await prewarmAttachments(env, r.id, photoField, 6);
    }
    if (isImageAttachmentArray) {
      await prewarmAttachments(env, r.id, imageField, 6);
    }
  }
}

/* ---------------- Image proxy ---------------- */

const urlCache = new Map();
const CACHE_TTL_MS = 8 * 60 * 1000;
function getCached(key) { const v = urlCache.get(key); if (!v) return null; if (Date.now() > v.expiresAt) { urlCache.delete(key); return null; } return v.url; }
function setCached(key, url) { urlCache.set(key, { url, expiresAt: Date.now() + CACHE_TTL_MS }); }

async function handleAttachmentRedirect(env, recordId, index, fieldName) {
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0) return text('Bad index', 400);

  const cacheKey = `${fieldName}:${recordId}:${idx}`;
  const cached = getCached(cacheKey);
  if (cached) return redirect(cached, 302, { 'Cache-Control': 'public, max-age=300' });

  const rec = await fetchRecordById(env, recordId);
  const attachments = Array.isArray(rec.fields?.[fieldName]) ? rec.fields[fieldName] : [];
  const att = attachments[idx];
  if (!att) return text(`${fieldName} not found`, 404);

  const freshUrl = pickAttachmentUrl(att);
  if (!freshUrl) return text(`${fieldName} URL missing`, 404);

  setCached(cacheKey, freshUrl);
  return redirect(freshUrl, 302, { 'Cache-Control': 'public, max-age=300' });
}

function parseAttachmentPath(pathname, prefix) {
  const parts = pathname.split('/');
  const recordId = parts[2];
  const index = Number(parts[3]);
  return { recordId, index };
}

/* ---------------- Normalizers ---------------- */

function parseGeometry(raw) {
  if (!raw) return null;
  try { const g = typeof raw === 'string' ? JSON.parse(raw) : raw; return g?.type ? g : null; }
  catch { return null; }
}
function pickAttachmentUrl(att) {
  if (!att) return null;
  if (typeof att === 'string') return /^https?:\/\//i.test(att) ? att : null;
  return att?.thumbnails?.large?.url || att?.thumbnails?.full?.url || att?.url || null;
}
function normalizeUrl(u) {
  let s = String(u || '').trim();
  s = s.replace(/^%20+/i, '').replace(/^\s+/, '');
  s = s.replace(/^(https?:)\/{2,}/i, (_, p1) => `${p1}//`);
  s = s.replace(/([^:])\/{2,}/g, '$1/');
  return s;
}
function collectPhotoUrls(value) {
  const urls = new Set();
  const pushAny = (v) => {
    if (v == null) return;
    if (Array.isArray(v)) { v.forEach(pushAny); return; }
    if (typeof v === 'string') {
      const s = v.trim();
      if ((s.startsWith('[') && s.endsWith(']')) || (s.startsWith('{') && s.endsWith('}'))) {
        try { pushAny(JSON.parse(s)); return; } catch {}
      }
      (s.includes(',') ? s.split(',') : [s]).forEach((part) => {
        const maybe = pickAttachmentUrl(part);
        if (maybe) urls.add(normalizeUrl(maybe));
      });
      return;
    }
    if (typeof v === 'object') {
      if (v.url || v.thumbnails) {
        const maybe = pickAttachmentUrl(v);
        if (maybe) urls.add(normalizeUrl(maybe));
        return;
      }
      Object.values(v).forEach(pushAny);
    }
  };
  pushAny(value);
  return Array.from(urls);
}
function normalizeLeaders(value) {
  const parts = [];
  const pushClean = (s) => {
    if (s == null) return;
    let t = String(s).trim();
    t = t.replace(/^(\[|\]+|"+|'+)|(\[|\]+|"+|'+)$/g, '');
    t = t.replace(/\s+/g, ' ').trim();
    if (/^rec[a-zA-Z0-9]{14}$/.test(t)) return;
    if (t) parts.push(t);
  };
  if (Array.isArray(value)) {
    value.forEach((v) => {
      if (typeof v === 'object' && v && 'name' in v) pushClean(v.name);
      else if (typeof v === 'string' && v.includes('","')) {
        v.split('","').forEach((x) => pushClean(x.replace(/^"+|"+$/g, '')));
      } else pushClean(v);
    });
  } else if (typeof value === 'string') {
    const text = value.trim();
    try {
      if (text.startsWith('[') && text.endsWith(']')) {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) parsed.forEach(pushClean); else pushClean(parsed);
      } else text.split(/[;,]/).forEach(s => pushClean(s));
    } catch { text.split(/[;,]/).forEach(s => pushClean(s)); }
  } else if (value != null) pushClean(value);
  return [...new Set(parts)].join(', ');
}
function normalizeTextField(value) {
  if (value == null) return '';
  const out = [];
  const pushAny = (v) => {
    if (v == null) return;
    if (Array.isArray(v)) { v.forEach(pushAny); return; }
    if (typeof v === 'object') {
      const cand = v.email ?? v.text ?? v.name ?? v.value ?? null;
      if (cand != null) {
        const t = String(cand).trim();
        if (t) out.push(t);
      } else Object.values(v).forEach(pushAny);
      return;
    }
    const t = String(v).trim();
    if (t) out.push(t);
  };
  pushAny(value);
  return [...new Set(out)].join(', ');
}

/* ---------------- tiny response helpers ---------------- */

function withCORS(res) {
  const h = new Headers(res.headers);
  h.set('Access-Control-Allow-Origin', '*');
  h.set('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  h.set('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  h.set('Vary', 'Origin');
  return new Response(res.body, { status: res.status, headers: h });
}
function text(s, status = 200) { return new Response(s, { status, headers: { 'Content-Type': 'text/plain; charset=utf-8' } }); }
function json(obj, status = 200) { return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json; charset=utf-8' } }); }
function redirect(location, status = 302, headers = {}) { return new Response(null, { status, headers: { Location: location, ...headers } }); }
