export default {
  async fetch(request, env) {
    // Pull ALL variables from the environment
    const { 
      AIRTABLE_API_KEY, 
      AIRTABLE_BASE_ID, 
      LEADERS_TABLE_NAME, 
      PRAYER_REQUESTS_TABLE, 
      PRAYER_LOGS_TABLE 
    } = env;
    
    const url = new URL(request.url);
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers });

    try {
      // 1. Get ALL Leaders (Uses variable)
      if (url.pathname === "/get-leaders") {
        let allLeaders = [];
        let offset = "";
        do {
          const fetchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(LEADERS_TABLE_NAME)}?fields%5B%5D=Leader%20Name${offset ? `&offset=${offset}` : ""}`;
          const res = await fetch(fetchUrl, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
          const data = await res.json();
          if (data.error) throw new Error(`Airtable Error: ${data.error.message}`);
          if (data.records) {
            allLeaders = allLeaders.concat(data.records.map(r => ({ id: r.id, name: r.fields["Leader Name"] || "Unknown" })));
          }
          offset = data.offset;
        } while (offset);
        return new Response(JSON.stringify(allLeaders), { headers });
      }

      // 2. Submit New Prayer (Uses variable)
      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_REQUESTS_TABLE)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({
            records: [{ fields: { "Leader": [body.leaderId], "Request Text": body.text, "Status": "Active" } }]
          })
        });
        if (!res.ok) throw new Error(`Airtable save error: ${res.statusText}`);
        return new Response(JSON.stringify({ status: "Saved" }), { headers });
      }

      // 3. Get a Random Active Prayer (Uses variable)
      if (url.pathname === "/get-prayer") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_REQUESTS_TABLE)}?filterByFormula=AND({Status}='Active')`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        if (!data.records || !data.records.length) return new Response(JSON.stringify({ empty: true }), { headers });
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({
          id: randomRecord.id,
          text: randomRecord.fields["Request Text"],
          name: randomRecord.fields["Leader Name"] 
        }), { headers });
      }

    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), { status: 500, headers });
    }
    return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers });
  }
};
