// Static publishing model for your network map
// Routes:
//   GET  /networks.geojson                   -> serve latest from R2
//   POST /networks/admin/regenerate          -> (Bearer REGEN_TOKEN) rebuild + flush image cache
//   POST /networks/admin/regenerate-noflush  -> (Bearer REGEN_TOKEN) rebuild, skip already-cached images
//   GET  /networks/img/<id>/<index>          -> Proxy for 'Photo' field
//   GET  /networks/image/<id>/<index>        -> Proxy for 'Image' field
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

      // 2. REGENERATE — flush mode (default)
      // Overwrites all existing R2 image cache. Use this when images have
      // changed in Airtable and you need them to update.
      if (request.method === 'POST' && (pathname === '/admin/regenerate' || pathname === '/networks/admin/regenerate')) {
        const token = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
        if (!token || token !== env.REGEN_TOKEN) {
          return withCORS(json({ error: 'Unauthorized' }, 401));
        }
        return withCORS(await handleRegenerate(request, env, ctx, { flush: true }));
      }

      // 3. REGENERATE — no-flush mode
      // Skips images already in R2. Faster and cheaper when only GeoJSON
      // data has changed and images haven't.
      if (request.method === 'POST' && (pathname === '/admin/regenerate-noflush' || pathname === '/networks/admin/regenerate-noflush')) {
        const token = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
        if (!token || token !== env.REGEN_TOKEN) {
          return withCORS(json({ error: 'Unauthorized' }, 401));
        }
        return withCORS(await handleRegenerate(request, env, ctx, { flush: false }));
      }

      // 4. IMAGE PROXY: /networks/img/
      if (request.method === 'GET' && pathname.startsWith('/networks/img/')) {
        const { recordId, index } = parseAttachmentPath(pathname, '/networks/img/');
        if (!isValidRecordId(recordId) || !isValidIndex(index)) {
          return withCORS(text('Bad request', 400));
        }
        if (env.NETWORK_IMAGES_BUCKET) {
          const obj = await env.NETWORK_IMAGES_BUCKET.get(r2Key(recordId, index));
          if (obj) {
            return withCORS(new Response(obj.body, {
              headers: {
                'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg',
                'Cache-Control': 'public, max-age=604800, immutable'
              }
            }));
          }
        }
        return withCORS(await handleAttachmentProxy(env, ctx, recordId, index, 'Photo'));
      }

      // 5. IMAGE PROXY: /networks/image/
      if (request.method === 'GET' && pathname.startsWith('/networks/image/')) {
        const { recordId, index } = parseAttachmentPath(pathname, '/networks/image/');
        if (!isValidRecordId(recordId) || !isValidIndex(index)) {
          return withCORS(text('Bad request', 400));
        }
        if (env.NETWORK_IMAGES_BUCKET) {
          const obj = await env.NETWORK_IMAGES_BUCKET.get(r2Key(recordId, index));
          if (obj) {
            return withCORS(new Response(obj.body, {
              headers: {
                'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg',
                'Cache-Control': 'public, max-age=604800, immutable'
              }
            }));
          }
        }
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

/* ---------------- Regenerate handler ---------------- */

async function handleRegenerate(request, env, ctx, { flush }) {
  const records = await fetchAllRecords(env);

  if (env.NETWORK_IMAGES_BUCKET) {
    ctx.waitUntil(prewarmAll(env, records, { flush }));
  }

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
        number_of_churches: normalizeTextField(f['Number of Churches']),
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

  return json({
    ok: true,
    features: features.length,
    updatedAt: new Date().toISOString(),
    note: flush
      ? 'Image cache flush running in background'
      : 'Image cache prewarm running in background (existing images skipped)'
  });
}

/* ---------------- R2 key helper ---------------- */

function r2Key(recordId, index) {
  return `images/${recordId}/${index}/original`;
}

/* ---------------- Validation helpers ---------------- */

function isValidRecordId(id) {
  return typeof id === 'string' && /^rec[a-zA-Z0-9]{14}$/.test(id);
}

function isValidIndex(index) {
  return Number.isInteger(index) && index >= 0 && index <= 5;
}

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
  // recordId is validated by callers before reaching here
  const url = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.NETWORKS_TABLE_NAME)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` } });
  if (!res.ok) throw new Error(`Airtable error ${res.status}: ${await res.text()}`);
  return res.json();
}

/* ---------------- Image proxy (pull-through cache) ---------------- */

// On an R2 miss: fetches a fresh record from Airtable (guarantees a non-expired
// URL), fetches the image bytes, serves them to the browser, and writes to R2
// in the background. Every image reaches R2 after its first request regardless
// of whether prewarm succeeded.
async function handleAttachmentProxy(env, ctx, recordId, index, fieldName) {
  const rec = await fetchRecordById(env, recordId);
  const allUrls = collectPhotoUrls(rec.fields?.[fieldName]);
  const freshUrl = allUrls[index];
  if (!freshUrl) return text(`${fieldName} URL missing`, 404);

  const response = await fetch(freshUrl);
  if (!response.ok) return text('Failed to fetch image from Airtable', response.status);

  const contentType = response.headers.get('content-type') || 'image/jpeg';

  // Write to R2 in the background; browser doesn't wait for it
  if (env.NETWORK_IMAGES_BUCKET) {
    const key = r2Key(recordId, index);
    ctx.waitUntil(
      env.NETWORK_IMAGES_BUCKET.put(key, response.clone().body, {
        httpMetadata: { contentType, cacheControl: 'public, max-age=604800, immutable' }
      })
    );
  }

  return new Response(response.body, {
    status: 200,
    headers: {
      'Content-Type': contentType,
      'Cache-Control': 'public, max-age=604800, immutable'
    }
  });
}

/* ---------------- R2 prewarm (best-effort) ---------------- */

// Runs in the background after regenerate. Not the critical path —
// the pull-through cache above handles any misses. Failures are logged
// but not fatal.
//
// flush: true  — overwrites existing R2 keys (default regenerate)
// flush: false — skips existing R2 keys (regenerate-noflush)

async function withConcurrency(items, limit, fn) {
  const results = [];
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const cur = i++;
      try {
        results[cur] = await fn(items[cur]);
      } catch (e) {
        console.error(`Prewarm error at index ${cur}:`, e);
      }
    }
  });
  await Promise.all(workers);
  return results;
}

async function prewarmAttachments(env, recordId, urlStrings, maxCount = 6, { flush }) {
  if (!env.NETWORK_IMAGES_BUCKET) return;
  if (!Array.isArray(urlStrings) || urlStrings.length === 0) return;

  const tasks = urlStrings.slice(0, maxCount).map((urlStr, idx) => ({ urlStr, idx }));

  await withConcurrency(tasks, 4, async ({ urlStr, idx }) => {
    const srcUrl = pickAttachmentUrl(urlStr);
    if (!srcUrl) return;

    const key = r2Key(recordId, idx);

    if (!flush) {
      // No-flush mode: skip images already cached
      const existing = await env.NETWORK_IMAGES_BUCKET.head(key);
      if (existing) return;
    }

    const response = await fetch(srcUrl);
    if (!response.ok) {
      console.error(`Prewarm fetch failed for record ${recordId}[${idx}]: ${response.status}`);
      return;
    }

    const contentType = response.headers.get('content-type') || 'image/jpeg';
    await env.NETWORK_IMAGES_BUCKET.put(key, response.body, {
      httpMetadata: { contentType, cacheControl: 'public, max-age=604800, immutable' }
    });
  });
}

async function prewarmAll(env, records, { flush }) {
  await withConcurrency(records, 10, async (r) => {
    const f = r.fields || {};
    const photoUrls = collectPhotoUrls(f['Photo']);
    const imageUrls = collectPhotoUrls(f['Image']);
    if (photoUrls.length > 0) await prewarmAttachments(env, r.id, photoUrls, 6, { flush });
    if (imageUrls.length > 0) await prewarmAttachments(env, r.id, imageUrls, 6, { flush });
  });
}

/* ---------------- Path parser ---------------- */

function parseAttachmentPath(pathname, prefix) {
  const relative = pathname.replace(prefix, '');
  const parts = relative.split('/').filter(Boolean);
  return { recordId: parts[0], index: Number(parts[1] ?? NaN) };
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
  // Thumbnails preferred over original to save bandwidth (no image resizing available)
  return att?.thumbnails?.large?.url || att?.url || att?.thumbnails?.full?.url || null;
}

function normalizeUrl(u) {
  return String(u || '').trim().replace(/^%20+/i, '');
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
    const t = value.trim();
    try {
      if (t.startsWith('[') && t.endsWith(']')) {
        const parsed = JSON.parse(t);
        if (Array.isArray(parsed)) parsed.forEach(pushClean); else pushClean(parsed);
      } else t.split(/[;,]/).forEach(s => pushClean(s));
    } catch { t.split(/[;,]/).forEach(s => pushClean(s)); }
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

/* ---------------- Response helpers ---------------- */

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
