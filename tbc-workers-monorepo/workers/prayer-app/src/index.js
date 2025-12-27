export default {
  async fetch(request, env) {
    const { AIRTABLE_API_KEY, AIRTABLE_BASE_ID } = env;
    const url = new URL(request.url);
    
    // Standard headers for all responses to fix CORS issues
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    // 1. Handle Browser Pre-flight (CORS fix)
    if (request.method === "OPTIONS") {
      return new Response(null, { headers });
    }

    try {
      // Get ALL Leaders for Autocomplete
if (url.pathname === "/get-leaders") {
  let allLeaders = [];
  let offset = "";
  
  // Loop until Airtable stops providing an offset
  do {
    const fetchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Leaders?fields%5B%5D=Leader%20Name${offset ? `&offset=${offset}` : ""}`;
    const res = await fetch(fetchUrl, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });
    
    const data = await res.json();
    const pageLeaders = data.records.map(r => ({ id: r.id, name: r.fields["Leader Name"] }));
    allLeaders = allLeaders.concat(pageLeaders);
    
    // Check if there is another page of data
    offset = data.offset;
  } while (offset);

  return new Response(JSON.stringify(allLeaders), { headers });
}

      // 3. Submit New Prayer Request
      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Requests`, {
          method: "POST",
          headers: { 
            Authorization: `Bearer ${AIRTABLE_API_KEY}`, 
            "Content-Type": "application/json" 
          },
          body: JSON.stringify({
            records: [{ 
              fields: { 
                "Leader": [body.leaderId], 
                "Request Text": body.text, 
                "Status": "Active" 
              } 
            }]
          })
        });
        if (!res.ok) throw new Error(`Airtable Save error: ${res.statusText}`);
        
        return new Response(JSON.stringify({ success: true }), { headers });
      }

      // 4. Get a Random Active Prayer
      if (url.pathname === "/get-prayer") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Requests?filterByFormula=AND({Status}='Active')`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        if (!res.ok) throw new Error(`Airtable Prayer Fetch error: ${res.statusText}`);
        
        const data = await res.json();
        if (!data.records.length) return new Response(JSON.stringify({ empty: true }), { headers });
        
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({
          id: randomRecord.id,
          text: randomRecord.fields["Request Text"],
          name: randomRecord.fields["Leader Name"] // Assumes Lookup field exists in Airtable
        }), { headers });
      }

      // 5. Log a completed prayer
      if (url.pathname === "/log-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Logs`, {
          method: "POST",
          headers: { 
            Authorization: `Bearer ${AIRTABLE_API_KEY}`, 
            "Content-Type": "application/json" 
          },
          body: JSON.stringify({
            records: [{ fields: { "Prayer Request": [body.prayerRequestId] } }]
          })
        });
        if (!res.ok) throw new Error(`Airtable Log error: ${res.statusText}`);
        
        return new Response(JSON.stringify({ success: true }), { headers });
      }

      // 6. Get Prayer History
      if (url.pathname === "/get-history") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Logs?maxRecords=20&sort%5B0%5D%5Bfield%5D=Date&sort%5B0%5D%5Bdirection%5D=desc`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        if (!res.ok) throw new Error(`Airtable History error: ${res.statusText}`);
        
        const data = await res.json();
        const history = data.records.map(r => ({
          date: new Date(r.fields["Date"]).toLocaleString(),
          leader: r.fields["Leader Name"] ? r.fields["Leader Name"][0] : "Unknown",
          request: r.fields["Request Text"] ? r.fields["Request Text"][0] : "No text provided"
        }));
        
        return new Response(JSON.stringify(history), { headers });
      }

    } catch (err) {
      // CORS fix: Return errors with headers so the browser can read the message
      return new Response(JSON.stringify({ error: err.message }), { status: 500, headers });
    }

    return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers });
  }
};
