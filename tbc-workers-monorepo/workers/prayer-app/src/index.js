export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // Combine headers for CORS
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers });

    const jsonResponse = (data, status = 200) => 
      new Response(JSON.stringify(data), { status, headers });

    // --- Helpers (Merged) ---

    // Generic Airtable Create
    async function createRecord(tableName, fields) {
      const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { 
          Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}`, 
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

    // Generic Airtable Find (Single Record)
    async function findAirtableRecord(table, formulaField, value) {
      const filter = `${formulaField} = '${value.replace(/'/g, "\\'")}'`;
      const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}?filterByFormula=${encodeURIComponent(filter)}&maxRecords=1`;
      const res = await fetch(endpoint, { 
        headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } 
      });
      const data = await res.json();
      return data.records?.[0] || null;
    }

    // Fetch All Records (Supports link fields for filtering)
    async function fetchAllRecords(tableName, nameField, linkField = null) {
      let allRecords = [];
      let offset = "";
      const baseUrl = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}?fields%5B%5D=${encodeURIComponent(nameField)}`;

      do {
        let fetchUrl = baseUrl;
        if (linkField) fetchUrl += `&fields%5B%5D=${encodeURIComponent(linkField)}`;
        if (offset) fetchUrl += `&offset=${offset}`;

        const res = await fetch(fetchUrl, { headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } });
        const data = await res.json();
        
        if (!res.ok || data.error) {
          throw new Error(`Airtable Error (${tableName}): ${data.error?.message || res.statusText}`);
        }
        
        if (data.records) {
          allRecords = allRecords.concat(data.records.map(r => {
            const item = { id: r.id, name: r.fields[nameField] || "" };
            // Capture parent link ID if requested
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

    // Send OTP (Twilio)
    async function sendOtp(phone) {
      const otp = Math.floor(100000 + Math.random() * 900000).toString();
      // Store in KV (Expires in 10 mins)
      if (env.AUTH_STORE) {
        await env.AUTH_STORE.put(`otp:${phone}`, otp, { expirationTtl: 600 });
      } else {
        console.warn("KV 'AUTH_STORE' not bound!");
      }

      // Fire and forget Twilio
      if (env.TWILIO_ACCOUNT_SID) {
        const endpoint = `https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Messages.json`;
        const formData = new URLSearchParams();
        formData.append("To", phone);
        formData.append("From", env.TWILIO_PHONE_NUMBER);
        formData.append("Body", `TBC Login Code: ${otp}`);
        
        fetch(endpoint, {
          method: "POST",
          headers: { 
            "Authorization": "Basic " + btoa(`${env.TWILIO_ACCOUNT_SID}:${env.TWILIO_AUTH_TOKEN}`),
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: formData
        }).catch(e => console.log("Twilio Error:", e)); 
      }
    }

    // Legacy Find/Create for old routes
    async function findOrCreate(tableName, nameField, nameValue) {
      const existing = await findAirtableRecord(tableName, nameField, nameValue);
      if (existing) return existing.id;
      const newRec = await createRecord(tableName, { [nameField]: nameValue });
      return newRec.id;
    }

    try {
      // ========================================================================
      // 🚦 1. AUTH ROUTES (New Logic)
      // ========================================================================

      // Step 1: Traffic Light Check
      if (url.pathname === "/auth/start" && request.method === "POST") {
        const { phone } = await request.json();
        const cleanPhone = phone.trim();

        // Check A: Already an App User?
        let appUser = await findAirtableRecord(env.USERS_TABLE_NAME, "Phone", cleanPhone);

        if (appUser) {
          if (appUser.fields["App Approved"] === true) {
            await sendOtp(cleanPhone);
            return jsonResponse({ status: "otp_sent", message: "Code sent." });
          } else {
            return jsonResponse({ status: "pending", message: "Account under review." });
          }
        }

        // Check B: Is it a Leader in Directory?
        const leader = await findAirtableRecord(env.LEADERS_TABLE_NAME, "Phone", cleanPhone);

        if (leader) {
          // Auto-promote to App User
          await createRecord(env.USERS_TABLE_NAME, {
            "Phone": cleanPhone,
            "App Approved": true,
            "Linked Leader": [leader.id],
            "SMS Opt-in": true
          });
          
          await sendOtp(cleanPhone);
          return jsonResponse({ status: "otp_sent", message: "Profile found. Code sent." });
        }

        // Check C: Stranger -> Registration
        return jsonResponse({ status: "needs_registration" });
      }

      // Step 2: Register New User
      if (url.pathname === "/auth/register" && request.method === "POST") {
        const { name, phone, networkId, orgId, reason } = await request.json();

        // Create PENDING Leader
        const leaderFields = {
          "Leader Name": name,
          "Phone": phone,
          "App User Status": "Pending",
        };
        if (networkId) leaderFields["Network Membership"] = [networkId];
        if (orgId) leaderFields["Leads Church"] = [orgId];

        const newLeader = await createRecord(env.LEADERS_TABLE_NAME, leaderFields);

        // Create Unapproved App User
        await createRecord(env.USERS_TABLE_NAME, {
          "Phone": phone,
          "App Approved": false,
          "Linked Leader": [newLeader.id],
          "Linked Network": networkId ? [networkId] : [],
          "Linked Organization": orgId ? [orgId] : [],
          "Notes": reason || "New registration request",
          "SMS Opt-in": true
        });

        return jsonResponse({ status: "created_pending", message: "Registration submitted." });
      }

      // Step 3: Verify OTP
      if (url.pathname === "/auth/verify-otp" && request.method === "POST") {
        const { phone, code } = await request.json();
        let isValid = false;

        // Check KV
        if (env.AUTH_STORE) {
          const storedOtp = await env.AUTH_STORE.get(`otp:${phone}`);
          if (storedOtp && storedOtp === code) {
            isValid = true;
            await env.AUTH_STORE.delete(`otp:${phone}`);
          }
        }

        // Backdoor for testing/admins (remove in prod if desired)
        if (!isValid && code === "123456") {
          const user = await findAirtableRecord(env.USERS_TABLE_NAME, "Phone", phone);
          if (user && user.fields["App Approved"] === true) {
            isValid = true;
          }
        }

        if (!isValid) return jsonResponse({ error: "Invalid code" }, 401);

        // Success -> Return User Token/Info
        const userRec = await findAirtableRecord(env.USERS_TABLE_NAME, "Phone", phone);
        const token = `session_${userRec.id}_${Date.now()}`; 

        return jsonResponse({ 
          token, 
          user: { 
            id: userRec.id, 
            leaderId: userRec.fields["Linked Leader"]?.[0] 
          } 
        });
      }

      // ========================================================================
      // 📖 2. DATA ROUTES (Used by Auth & Main App)
      // ========================================================================

      if (url.pathname === "/get-leaders") {
        const leaders = await fetchAllRecords(env.LEADERS_TABLE_NAME, "Leader Name");
        return jsonResponse(leaders);
      }

      if (url.pathname === "/get-networks") {
        const networks = await fetchAllRecords(env.NETWORKS_TABLE_NAME, "Network Name");
        return jsonResponse(networks);
      }

      if (url.pathname === "/get-organizations") {
        // Fetch with Network link for filtering
        const orgs = await fetchAllRecords(env.ORGANIZATIONS_TABLE_NAME, "Org Name", "Network");
        return jsonResponse(orgs);
      }

      // ========================================================================
      // 🙏 3. PRAYER APP ROUTES (Restored!)
      // ========================================================================

      if (url.pathname === "/app-submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const { networkId, church, leader, request: requestText } = body;

        if (!networkId || !church || !leader || !requestText) {
          return jsonResponse({ error: "Missing required fields" }, 400);
        }

        let finalChurchId = church.isNew 
          ? (await createRecord(env.ORGANIZATIONS_TABLE_NAME, {
              "Org Name": church.name,
              "Address": church.address, 
              "Org Type": church.type, 
              "Network": [networkId]
            })).id
          : church.id;

        let finalLeaderId = leader.isNew 
          ? (await createRecord(env.LEADERS_TABLE_NAME, {
              "Leader Name": leader.name,
              "Email": leader.email,
              "Phone": leader.phone,
              "Leads Church": [finalChurchId] 
            })).id
          : leader.id;

        await createRecord(env.IN_PRAYER_REQUESTS_TABLE_NAME, {
          "Network": [networkId],
          "Organization": [finalChurchId],
          "Leader": [finalLeaderId],
          "Request": requestText
        });

        return jsonResponse({ success: true, churchId: finalChurchId, leaderId: finalLeaderId });
      }

      // Legacy Personal Prayer Routes
      if (url.pathname === "/submit-church-prayer" && request.method === "POST") {
        // ... (Keep existing logic if still needed, or redirect to new flow)
        // For safety, preserving original logic:
        const body = await request.json();
        const { networkName, organizationName, leaderName, prayerRequest } = body;
        
        // Find Network ID
        const netRec = await findAirtableRecord(env.NETWORKS_TABLE_NAME, "Network Name", networkName);
        if (!netRec) return jsonResponse({ error: "Network not found." }, 400);
        
        const finalNetworkId = netRec.id;
        let finalOrgId = organizationName ? await findOrCreate(env.ORGANIZATIONS_TABLE_NAME, "Org Name", organizationName) : null;
        let finalLeaderId = leaderName ? await findOrCreate(env.LEADERS_TABLE_NAME, "Leader Name", leaderName) : null;

        const fields = { "Network": [finalNetworkId], "Request": prayerRequest };
        if (finalOrgId) fields["Organization"] = [finalOrgId];
        if (finalLeaderId) fields["Leader"] = [finalLeaderId];

        await createRecord(env.IN_PRAYER_REQUESTS_TABLE_NAME, fields);
        return jsonResponse({ status: "Saved" });
      }

      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        await createRecord(env.PRAYER_REQUESTS_TABLE, { 
          "Leader": [body.leaderId], 
          "Request Text": body.text, 
          "Status": "Active" 
        });
        return jsonResponse({ status: "Saved" });
      }

      if (url.pathname === "/get-prayer") {
        const filterType = url.searchParams.get('filter') || 'unprayed-today';
        let formula = "AND({Status}='Active')";
        if (filterType === 'unprayed-today') formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 1))`;
        else if (filterType === 'unprayed-week') formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 7))`;
        else if (filterType === 'past-month') formula = `AND({Status}='Active', DATETIME_DIFF(NOW(), CREATED_TIME(), 'days') <= 30)`;

        const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.PRAYER_REQUESTS_TABLE)}?filterByFormula=${encodeURIComponent(formula)}`;
        const res = await fetch(endpoint, { headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } });
        const data = await res.json();
        
        if (!data.records || !data.records.length) return jsonResponse({ empty: true });
        const randomRecord = data.records[Math.floor(Math.random() * data.records.length)];
        return jsonResponse({ 
          id: randomRecord.id, 
          text: randomRecord.fields["Request Text"], 
          name: randomRecord.fields["Leader Name"], 
          createdTime: randomRecord.createdTime 
        });
      }

      if (url.pathname === "/log-prayer" && request.method === "POST") {
        const body = await request.json();
        await createRecord(env.PRAYER_LOGS_TABLE, { "Prayer Request": [body.prayerRequestId] });
        return jsonResponse({ success: true });
      }

      if (url.pathname === "/get-history") {
        const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.PRAYER_LOGS_TABLE)}?maxRecords=20&sort%5B0%5D%5Bfield%5D=Date&sort%5B0%5D%5Bdirection%5D=desc`;
        const res = await fetch(endpoint, { headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } });
        const data = await res.json();
        const history = data.records.map(r => ({
          date: new Date(r.fields["Date"]).toLocaleString(),
          leader: r.fields["Leader Name"] ? r.fields["Leader Name"][0] : "Unknown",
          request: r.fields["Request Text"] ? r.fields["Request Text"][0] : "No text"
        }));
        return jsonResponse(history);
      }

      if (url.pathname === "/archive-prayer" && request.method === "POST") {
        const body = await request.json();
        const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.PRAYER_REQUESTS_TABLE)}/${body.prayerRequestId}`;
        const res = await fetch(endpoint, {
          method: "PATCH",
          headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}`, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: { "Status": "Archived" } })
        });
        if (!res.ok) throw new Error(`Archive Error: ${res.statusText}`);
        return jsonResponse({ success: true });
      }

    } catch (err) {
      return jsonResponse({ error: err.message }, 500);
    }
    
    return jsonResponse({ error: "Route Not Found" }, 404);
  }
};
