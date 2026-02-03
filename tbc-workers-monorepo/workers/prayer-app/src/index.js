export default {
  async fetch(request, env) {
    const { 
      AIRTABLE_API_KEY, 
      AIRTABLE_BASE_ID, 
      LEADERS_TABLE_NAME, 
      PRAYER_REQUESTS_TABLE, 
      PRAYER_LOGS_TABLE,
      NETWORKS_TABLE_NAME,
      ORGANIZATIONS_TABLE_NAME,
      IN_PRAYER_REQUESTS_TABLE_NAME
    } = env;
    
    const url = new URL(request.url);
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers });

    // --- Helpers ---

    // 1. Generic function to create a record in any table
    async function createRecord(tableName, fields) {
      const endpoint = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { 
          Authorization: `Bearer ${AIRTABLE_API_KEY}`, 
          "Content-Type": "application/json" 
        },
        body: JSON.stringify({ records: [{ fields }] })
      });
      
      const data = await res.json();
      if (!res.ok || !data.records || !data.records.length) {
        throw new Error(`Airtable Create Error (${tableName}): ${JSON.stringify(data.error || data)}`);
      }
      return data.records[0];
    }

    // 2. Fetch all records (Updated to support Link Fields)
    // Now accepts an optional 'linkField' argument (e.g. "Network") to get the parent ID
    async function fetchAllRecords(tableName, nameField, linkField = null) {
      let allRecords = [];
      let offset = "";
      const baseUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}?fields%5B%5D=${encodeURIComponent(nameField)}`;

      do {
        let fetchUrl = baseUrl;
        // If we need the link (e.g. to filter churches by network), fetch that field too
        if (linkField) fetchUrl += `&fields%5B%5D=${encodeURIComponent(linkField)}`;
        if (offset) fetchUrl += `&offset=${offset}`;

        const res = await fetch(fetchUrl, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
        const data = await res.json();
        
        if (!res.ok || data.error) {
          throw new Error(`Airtable Error (${tableName}): ${data.error?.message || res.statusText}`);
        }
        
        if (data.records) {
          allRecords = allRecords.concat(data.records.map(r => {
            const item = { id: r.id, name: r.fields[nameField] || "" };
            // If a link field was requested, attach the first ID found (Airtable returns arrays)
            if (linkField && r.fields[linkField] && r.fields[linkField].length > 0) {
              item.parentId = r.fields[linkField][0]; 
            }
            return item;
          }));
        }
        offset = data.offset;
      } while (offset);
      return allRecords;
    }

    // 3. Find or Create Record (Used by legacy /submit-church-prayer)
    async function findOrCreate(tableName, nameField, nameValue) {
      const filterFormula = `{${nameField}} = '${nameValue.replace(/'/g, "\\'")}'`;
      const searchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}?filterByFormula=${encodeURIComponent(filterFormula)}&maxRecords=1`;
      const searchRes = await fetch(searchUrl, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
      const searchData = await searchRes.json();

      if (searchData.records && searchData.records.length > 0) {
        return searchData.records[0].id;
      }

      const createRes = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`, {
        method: "POST",
        headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          records: [{ fields: { [nameField]: nameValue } }]
        })
      });
      const createData = await createRes.json();
      if (createData.records && createData.records.length > 0) {
        return createData.records[0].id;
      }
      throw new Error(`Failed to create new ${tableName}: ${nameValue}`);
    }

    try {
      // ========================================================================
      // NEW ROUTE: /app-submit-prayer (Complex Relational Insert)
      // ========================================================================
      if (url.pathname === "/app-submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const { networkId, church, leader, request: requestText } = body;

        if (!networkId || !church || !leader || !requestText) {
          return new Response(JSON.stringify({ error: "Missing required fields" }), { status: 400, headers });
        }

        // 1. Resolve Church (Organization)
        let finalChurchId = null;
        if (!church.isNew) {
          finalChurchId = church.id;
        } else {
          const newOrgRecord = await createRecord(ORGANIZATIONS_TABLE_NAME, {
            "Org Name": church.name,
            "Address": church.address, 
            "Org Type": church.type, 
            "Network": [networkId]
          });
          finalChurchId = newOrgRecord.id;
        }

        // 2. Resolve Leader
        let finalLeaderId = null;
        if (!leader.isNew) {
          finalLeaderId = leader.id;
        } else {
          const newLeaderRecord = await createRecord(LEADERS_TABLE_NAME, {
            "Leader Name": leader.name,
            "Email": leader.email,
            "Phone": leader.phone,
            "Leads Church": [finalChurchId] 
          });
          finalLeaderId = newLeaderRecord.id;
        }

        // 3. Create Prayer Request
        await createRecord(IN_PRAYER_REQUESTS_TABLE_NAME, {
          "Network": [networkId],
          "Organization": [finalChurchId],
          "Leader": [finalLeaderId],
          "Request": requestText
        });

        return new Response(JSON.stringify({ 
          success: true, 
          churchId: finalChurchId, 
          leaderId: finalLeaderId 
        }), { headers });
      }

      // ========================================================================
      // Reference Routes (Updated for Filtering)
      // ========================================================================

      // 1. Get Leaders (No Parent Link requested, so NO filtering on frontend)
      if (url.pathname === "/get-leaders") {
        const leaders = await fetchAllRecords(LEADERS_TABLE_NAME, "Leader Name");
        return new Response(JSON.stringify(leaders), { headers });
      }

      // 2. Get Networks
      if (url.pathname === "/get-networks") {
        const networks = await fetchAllRecords(NETWORKS_TABLE_NAME, "Network Name");
        return new Response(JSON.stringify(networks), { headers });
      }

      // 3. Get Organizations (Requests "Network" field for filtering)
      if (url.pathname === "/get-organizations") {
        const orgs = await fetchAllRecords(ORGANIZATIONS_TABLE_NAME, "Org Name", "Network");
        return new Response(JSON.stringify(orgs), { headers });
      }

      // ========================================================================
      // Legacy / Personal Prayer Routes
      // ========================================================================

      if (url.pathname === "/submit-church-prayer" && request.method === "POST") {
        const body = await request.json();
        const { networkName, organizationName, leaderName, prayerRequest } = body;

        if (!networkName || !prayerRequest) {
          return new Response(JSON.stringify({ error: "Network and Request are required" }), { status: 400, headers });
        }

        const netFilter = `{Network Name} = '${networkName.replace(/'/g, "\\'")}'`;
        const netRes = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(NETWORKS_TABLE_NAME)}?filterByFormula=${encodeURIComponent(netFilter)}&maxRecords=1`, { 
            headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } 
        });
        const netData = await netRes.json();
        if (!netData.records || netData.records.length === 0) {
             return new Response(JSON.stringify({ error: "Network not found." }), { status: 400, headers });
        }
        const finalNetworkId = netData.records[0].id;

        let finalOrgId = null;
        if (organizationName) {
          finalOrgId = await findOrCreate(ORGANIZATIONS_TABLE_NAME, "Org Name", organizationName);
        }

        let finalLeaderId = null;
        if (leaderName) {
          finalLeaderId = await findOrCreate(LEADERS_TABLE_NAME, "Leader Name", leaderName);
        }

        const fields = { "Network": [finalNetworkId], "Request": prayerRequest };
        if (finalOrgId) fields["Organization"] = [finalOrgId];
        if (finalLeaderId) fields["Leader"] = [finalLeaderId];

        const saveRes = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(IN_PRAYER_REQUESTS_TABLE_NAME)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ records: [{ fields }] })
        });

        if (!saveRes.ok) throw new Error(`Airtable Save Error: ${saveRes.status}`);
        return new Response(JSON.stringify({ status: "Saved" }), { headers });
      }

      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_REQUESTS_TABLE)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ records: [{ fields: { "Leader": [body.leaderId], "Request Text": body.text, "Status": "Active" } }] })
        });
        if (!res.ok) throw new Error(`Airtable Save error: ${res.statusText}`);
        return new Response(JSON.stringify({ status: "Saved" }), { headers });
      }

      if (url.pathname === "/get-prayer") {
        const filterType = url.searchParams.get('filter') || 'unprayed-today';
        let formula = "AND({Status}='Active')";
        if (filterType === 'unprayed-today') formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 1))`;
        else if (filterType === 'unprayed-week') formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 7))`;
        else if (filterType === 'past-month') formula = `AND({Status}='Active', DATETIME_DIFF(NOW(), CREATED_TIME(), 'days') <= 30)`;

        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_REQUESTS_TABLE)}?filterByFormula=${encodeURIComponent(formula)}`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        if (!data.records || !data.records.length) return new Response(JSON.stringify({ empty: true }), { headers });
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return new Response(JSON.stringify({ id: randomRecord.id, text: randomRecord.fields["Request Text"], name: randomRecord.fields["Leader Name"], createdTime: randomRecord.createdTime }), { headers });
      }

      if (url.pathname === "/log-prayer" && request.method === "POST") {
        const body = await request.json();
        await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_LOGS_TABLE)}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ records: [{ fields: { "Prayer Request": [body.prayerRequestId] } }] })
        });
        return new Response(JSON.stringify({ success: true }), { headers });
      }

      if (url.pathname === "/get-history") {
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_LOGS_TABLE)}?maxRecords=20&sort%5B0%5D%5Bfield%5D=Date&sort%5B0%5D%5Bdirection%5D=desc`, {
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
        });
        const data = await res.json();
        const history = data.records.map(r => ({
          date: new Date(r.fields["Date"]).toLocaleString(),
          leader: r.fields["Leader Name"] ? r.fields["Leader Name"][0] : "Unknown",
          request: r.fields["Request Text"] ? r.fields["Request Text"][0] : "No text"
        }));
        return new Response(JSON.stringify(history), { headers });
      }

      if (url.pathname === "/archive-prayer" && request.method === "POST") {
        const body = await request.json();
        const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(PRAYER_REQUESTS_TABLE)}/${body.prayerRequestId}`, {
          method: "PATCH",
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: { "Status": "Archived" } })
        });
        if (!res.ok) throw new Error(`Airtable Archive error: ${res.statusText}`);
        return new Response(JSON.stringify({ success: true }), { headers });
      }

    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), { status: 500, headers });
    }
    
    return new Response(JSON.stringify({ error: "Route Not Found" }), { status: 404, headers });
  }
};
