// Static publishing model for your network map
// Routes:
//   GET  /networks.geojson         -> serve latest from R2
//   POST /admin/regenerate         -> (Bearer REGEN_TOKEN) rebuild from Airtable and publish to R2
//   GET  /networks/img/...         -> Proxy for 'Photo' field
//   GET  /networks/image/...       -> Proxy for 'Image' field
//
// Requires (Settings → Variables/Secrets):
//   AIRTABLE_TOKEN, AIRTABLE_BASE_ID, NETWORKS_TABLE_NAME, REGEN_TOKEN
//
// Requires (Settings → Bindings → R2 bucket):
//   NETWORK_MAP_BUCKET (latest.geojson)
//   NETWORK_IMAGES_BUCKET (images/...)

export default {
  async fetch(request, env, ctx) {
    const { pathname } = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return withCORS(new Response(null, { status: 204 }));
    }

    try {
      // 1. GET GeoJSON
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

      // 2. REGENERATE (Admin)
      if (request.method === 'POST' && (pathname === '/admin/regenerate' || pathname === '/networks/admin/regenerate')) {
        const token = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
        if (!token || token !== env.REGEN_TOKEN) {
          return withCORS(json({ error: 'Unauthorized' }, 401));
        }

        // Fetch data
        const records = await fetchAllRecords(env);
        
        // --- OPTIMIZATION: Background Image Processing ---
        if (env.NETWORK_IMAGES_BUCKET) {
            ctx.waitUntil(prewarmAll(env, records));
        }
        // -------------------------------------------------

        const origin = new URL(request.url).origin;
        const features = [];

        for (const r of records) {
          const f = r.fields || {};
          const geometry = parseGeometry(f['Polygon']);
          if (!geometry) continue;

          const leaders = normalizeLeaders(f['Network Leaders Names']) || '';
          
          let photoUrls = [];
          const extractedPhotos = collectPhotoUrls(f['Photo']);
          if (extractedPhotos.length > 0) {
            photoUrls = extractedPhotos.slice(0, 6).map((_, idx) => `${origin}/networks/img/${r.id}/${idx}`);
          }

          let imageUrls = [];
          const extractedImages = collectPhotoUrls(f['Image']);
          if (extractedImages.length > 0) {
            imageUrls = extractedImages.slice(0, 6).map((_, idx) => `${origin}/networks/image/${r.id}/${idx}`);
          }

          const [photo1='', photo2='', photo3='', photo4='', photo5='', photo6=''] = photoUrls;
          const [image1='', image2='', image3='', image4='', image5='', image6=''] = imageUrls;

          features.push({
            type: 'Feature',
            geometry,
            properties: {
              id: r.id,
              name: f['Network Name'] ?? '',
              leaders,
              contact_email: normalizeTextField(f['contact email'] ?? f['Contact Email']),
              status: normalizeTextField(f['Status']),
              county: normalizeTextField(f['County']),
              tags: normalizeTextField(f['Tags']),
              number_of_churches: f['Number of Churches'] ?? '',
              unify_lead: normalizeTextField(f['Unify Lead']),
              photo1, photo2, photo3, photo4, photo5, photo6,
              photo_count: photoUrls.filter(Boolean).length,
              image1, image2, image3, image4, image5, image6,
              image_count: imageUrls.filter(Boolean).length,
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
          updatedAt: new Date().toISOString(),
          note: "Image optimization running in background"
        }));
      }

      // 3. IMAGE PROXY: /networks/img/
      if (request.method === 'GET' && pathname.startsWith('/networks/img/')) {
        const { recordId, index } = parseAttachmentPath(pathname, '/networks/img/');
        if (!recordId) return withCORS(text('Bad index', 400));
        
        // Try serving from R2 cache first
        if (env.NETWORK_IMAGES_BUCKET) {
            const key = `images/${recordId}/${index}/w400-webp`;
            const obj = await env.NETWORK_IMAGES_BUCKET.get(key);
            if (obj) {
                return withCORS(new Response(obj.body, {
                    headers: { 
                        'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg', 
                        'Cache-Control': 'public, max-age=604800, immutable' 
                    }
                }));
            }
        }
        // Fallback to Pull-Through Proxy
        return withCORS(await handleAttachmentProxy(env, ctx, recordId, index, 'Photo'));
      }

      // 4. IMAGE PROXY: /networks/image/
      if (request.method === 'GET' && pathname.startsWith('/networks/image/')) {
        const { recordId, index } = parseAttachmentPath(pathname, '/networks/image/');
        if (!recordId) return withCORS(text('Bad index', 400));

        // Try serving from R2 cache first
        if (env.NETWORK_IMAGES_BUCKET) {
            const key = `images/${recordId}/${index}/w400-webp`;
            const obj = await env.NETWORK_IMAGES_BUCKET.get(key);
            if (obj) {
                return withCORS(new Response(obj.body, {
                    headers: { 
                        'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg', 
                        'Cache-Control': 'public, max-age=604800, immutable' 
                    }
                }));
            }
        }
        // Fallback to Pull-Through Proxy
        return withCORS(await handleAttachmentProxy(env, ctx, recordId, index, 'Image'));
      }

      // Root check
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
      try { 
        results[cur] = await fn(items[cur]); 
      } catch (e) {
        console.error(`Concurrency error processing item at index ${cur}:`, e);
      }
    }
  });
  await Promise.all(workers);
  return results;
}

async function prewarmAttachments(env, recordId, fieldArray, maxCount = 6) {
  if (!env.NETWORK_IMAGES_BUCKET) return; 
  if (!Array.isArray(fieldArray) || fieldArray.length === 0) return;

  const tasks = fieldArray.slice(0, maxCount).map((urlStr, idx) => ({ urlStr, idx }));

  // Process up to 4 images concurrently per record
  await withConcurrency(tasks, 4, async ({ urlStr, idx }) => {
    const srcUrl = pickAttachmentUrl(urlStr);
    if (!srcUrl) return;

    const response = await fetch(srcUrl);
    
    if (!response.ok) {
      console.error(`Failed to fetch image for prewarm: ${srcUrl} - Status: ${response.status}`);
      return;
    }

    const key = r2KeyForImage(recordId, idx, 'w400-webp');
    const contentType = response.headers.get('content-type') || 'image/jpeg';
    
    await env.NETWORK_IMAGES_BUCKET.put(key, response.body, {
      httpMetadata: { contentType: contentType, cacheControl: 'public, max-age=604800, immutable' }
    });
  });
}

async function prewarmAll(env, records) {
  // Process records in parallel batches of 10
  await withConcurrency(records, 10, async (r) => {
    const f = r.fields || {};
    
    const photoUrls = collectPhotoUrls(f['Photo']);
    const imageUrls = collectPhotoUrls(f['Image']);

    if (photoUrls.length > 0) {
      await prewarmAttachments(env, r.id, photoUrls, 6);
    }
    if (imageUrls.length > 0) {
      await prewarmAttachments(env, r.id, imageUrls, 6);
    }
  });
}

/* ---------------- Image Proxy (Pull-Through Cache) ---------------- */

async function handleAttachmentProxy(env, ctx, recordId, index, fieldName) {
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0) return text('Bad index', 400);

  // 1. Ask Airtable for the latest fresh URL
  const rec = await fetchRecordById(env, recordId);
  const allUrls = collectPhotoUrls(rec.fields?.[fieldName]);
  const freshUrl = allUrls[idx];
  if (!freshUrl) return text(`${fieldName} URL missing`, 404);

  // 2. Fetch the actual image bytes from Airtable
  const response = await fetch(freshUrl);
  if (!response.ok) return text('Failed to fetch image from Airtable', response.status);

  const contentType = response.headers.get('content-type') || 'image/jpeg';

  // 3. Save a copy of it to R2 in the background so the NEXT user gets it instantly
  if (env.NETWORK_IMAGES_BUCKET) {
    const cacheResponse = response.clone();
    const key = `images/${recordId}/${idx}/w400-webp`;
    ctx.waitUntil(env.NETWORK_IMAGES_BUCKET.put(key, cacheResponse.body, {
      httpMetadata: { contentType, cacheControl: 'public, max-age=604800, immutable' }
    }));
  }

  // 4. Return the bytes directly to the browser (Keeping the api.tbc.city URL intact)
  return new Response(response.body, {
    status: 200,
    headers: {
      'Content-Type': contentType,
      'Cache-Control': 'public, max-age=604800, immutable'
    }
  });
}

function parseAttachmentPath(pathname, prefix) {
  const relative = pathname.replace(prefix, ""); 
  const parts = relative.split("/").filter(Boolean);
  return { recordId: parts[0], index: Number(parts[1] || 0) };
}

/* ---------------- Normalizers ---------------- */

function parseGeometry(raw) {
  if (!raw) return null;
  try { const g = typeof raw === 'string' ? JSON.parse(raw) : raw; return g?.type ? g : null; }
  catch { return null; }
}
function pickAttachmentUrl(att) {
  if (!att) return null;
  if (typeof att === 'string') {
    const trimmed = att.trim(); 
    return /^https?:\/\//i.test(trimmed) ? trimmed : null;
  }
  // Fallback priority: Thumbnails first (saves bandwidth), then original URL
  return att?.thumbnails?.large?.url || att?.url || att?.thumbnails?.full?.url || null;
}
function normalizeUrl(u) {
  let s = String(u || '').trim();
  s = s.replace(/^%20+/i, '').replace(/^\s+/, '');
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
        const maybe = pickAttachmentUrl(part.trim());
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
