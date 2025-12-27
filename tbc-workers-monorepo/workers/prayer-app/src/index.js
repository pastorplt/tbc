export default {
  async fetch(request, env) {
    const { 
      AIRTABLE_API_KEY, 
      AIRTABLE_BASE_ID, 
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
      // 1. Get ALL Leaders (OMITTED for brevity, keep your existing logic)

      // 2. Submit New Prayer Request
      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const tableName = PRAYER_REQUESTS_TABLE || "Personal Prayer Requests";
        console.log(`Submitting to table: ${tableName}`);

        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({
            records: [{ fields: { "Leader": [body.leaderId], "Request Text": body.text, "Status": "Active" } }]
          })
        });
        if (!res.ok) throw new Error(`Airtable save error: ${res.statusText}`);
        return new Response(JSON.stringify({ status: "Saved" }), { headers });
      }

      // 3. Get a Random Active Prayer
      if (url.pathname === "/get-prayer") {
        const tableName = PRAYER_REQUESTS_TABLE || "Personal Prayer Requests";
        const filter = encodeURIComponent("AND({Status}='Active')");
        const fetchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}?filterByFormula=${filter}`;
        
        console.log(`Fetching from: ${fetchUrl}`);

        const res = await fetch(fetchUrl, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        
        console.log(`Found ${data.records?.length || 0} active records.`);

        if (!data.records || !data.records.length) {
          return new Response(JSON.stringify({ empty: true, debugTable: tableName }), { headers });
        }
        
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({
          id: randomRecord.id,
          text: randomRecord.fields["Request Text"],
          name: randomRecord.fields["Leader Name"] 
        }), { headers });
      }

      // 4. Log a Prayer
      if (url.pathname === "/log-prayer" && request.method === "POST") {
        const body = await request.json();
        const tableName = PRAYER_LOGS_TABLE || "Prayer Logs";
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({
            records: [{ fields: { "Prayer Request": [body.prayerRequestId] } }]
          })
        });
        return new Response(JSON.stringify({ success: true }), { headers });
      }

    } catch (err) {
      console.error(`Worker Error: ${err.message}`);
      return new Response(JSON.stringify({ error: err.message }), { status: 500, headers });
    }
    
    return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers });
  }
};
