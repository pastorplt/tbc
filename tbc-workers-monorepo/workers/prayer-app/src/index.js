export default {
  async fetch(request, env) {
    const { AIRTABLE_API_KEY, AIRTABLE_BASE_ID } = env;
    const url = new URL(request.url);
    
    // Standard headers for all responses to fix CORS and display issues
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    // Handle Browser Pre-flight (CORS fix)
    if (request.method === "OPTIONS") return new Response(null, { headers });

    try {
      // 1. Get ALL Leaders for Autocomplete (with Pagination)
      if (url.pathname === "/get-leaders") {
        let allLeaders = [];
        let offset = "";
        
        do {
          const fetchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Leaders?fields%5B%5D=Leader%20Name${offset ? `&offset=${offset}` : ""}`;
          const res = await fetch(fetchUrl, {
            headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
          });
          
          const data = await res.json();
          
          // Safety check: if Airtable returns an error (e.g., 401 or 404), throw it to the catch block
          if (data.error) {
            throw new Error(`Airtable Error: ${data.error.message || data.error}`);
          }

          // Safely map records if they exist; prevents the "map of undefined" error
          if (data.records) {
            const pageLeaders = data.records.map(r => ({ 
              id: r.id, 
              name: r.fields["Leader Name"] || "Unknown" 
            }));
            allLeaders = allLeaders.concat(pageLeaders);
          }
          
          offset = data.offset; // Airtable provides an offset if there are more than 100 records
        } while (offset);

        return new Response(JSON.stringify(allLeaders), { headers });
      }

      // 2. Submit New Prayer Request
      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Requests`, {
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
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Requests?filterByFormula=AND({Status}='Active')`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        
        if (!data.records || !data.records.length) {
          return new Response(JSON.stringify({ empty: true }), { headers });
        }
        
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({
          id: randomRecord.id,
          text: randomRecord.fields["Request Text"],
          name: randomRecord.fields["Leader Name"] // Assumes a lookup field exists in Airtable
        }), { headers });
      }

    } catch (err) {
      // Returns a JSON error message to the browser console for debugging
      return new Response(JSON.stringify({ error: err.message }), { status: 500, headers });
    }
    
    return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers });
  }
};
