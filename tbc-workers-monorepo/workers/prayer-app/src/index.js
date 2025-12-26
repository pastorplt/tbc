export default {
  async fetch(request, env) {
    const { AIRTABLE_API_KEY, AIRTABLE_BASE_ID } = env;
    const url = new URL(request.url);
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers });

    try {
      // 1. Get Leaders for Autocomplete
      if (url.pathname === "/get-leaders") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Leaders?fields%5B%5D=Leader%20Name`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        const leaders = data.records.map(r => ({ id: r.id, name: r.fields["Leader Name"] }));
        return new Response(JSON.stringify(leaders), { headers });
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
        return new Response("Saved", { headers });
      }

      // 3. Get a Random Active Prayer
      if (url.pathname === "/get-prayer") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Prayer%20Requests?filterByFormula=AND({Status}='Active')`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        if (!data.records.length) return new Response(JSON.stringify({ empty: true }), { headers });
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({
          id: randomRecord.id,
          text: randomRecord.fields["Request Text"],
          name: randomRecord.fields["Leader Name"] // Assuming a lookup field exists
        }), { headers });
      }

    } catch (err) {
      return new Response(err.message, { status: 500, headers });
    }
    return new Response("Not Found", { status: 404 });
  }
};
