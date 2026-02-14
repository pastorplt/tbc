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

    // Phone Normalization - Extract last 10 digits only
    function normalizePhone(phone) {
      if (!phone) return "";
      // Remove all non-digit characters
      const digits = phone.replace(/\D/g, "");
      // Take last 10 digits (handles +1 country code)
      return digits.slice(-10);
    }

    async function findRecordByPhone(tableName, phoneValue) {
  const normalizedInput = normalizePhone(phoneValue);
  console.log(`[findRecordByPhone] Searching ${tableName} for normalized phone: ${normalizedInput}`);
  
  if (!normalizedInput || normalizedInput.length !== 10) {
    console.log(`[findRecordByPhone] Invalid normalized phone length: ${normalizedInput.length}`);
    return null;
  }

    // Fetch all records from the table - DON'T limit fields, we need everything
  let allRecords = [];
  let offset = "";
  const baseUrl = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;

  do {
    let fetchUrl = baseUrl;
    if (offset) fetchUrl += `&offset=${offset}`;

    const res = await fetch(fetchUrl, { 
      headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } 
    });
    const data = await res.json();
    
    if (!res.ok || data.error) {
      console.log(`[findRecordByPhone] Airtable error:`, data.error);
      throw new Error(`Airtable Error (${tableName}): ${data.error?.message || res.statusText}`);
    }
    
    if (data.records) {
      allRecords = allRecords.concat(data.records);
    }
    offset = data.offset;
  } while (offset);

  console.log(`[findRecordByPhone] Fetched ${allRecords.length} total records`);

  // Find matching record by normalized phone
  for (const record of allRecords) {
    const recordPhone = record.fields.Phone || record.fields.phone;
    const normalizedRecord = normalizePhone(recordPhone);
    
    console.log(`[findRecordByPhone] Record ${record.id}: Phone="${recordPhone}" -> normalized="${normalizedRecord}"`);
    
    if (recordPhone && normalizedRecord === normalizedInput) {
      console.log(`[findRecordByPhone] ✅ MATCH FOUND!`);
      return record;
    }
  }

  console.log(`[findRecordByPhone] ❌ No match found`);
  return null;
}

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
      const normalizedPhone = normalizePhone(phone);
      
      // Store in KV (Expires in 10 mins)
      if (env.AUTH_STORE) {
        await env.AUTH_STORE.put(`otp:${normalizedPhone}`, otp, { expirationTtl: 600 });
      } else {
        console.warn("KV 'AUTH_STORE' not bound!");
      }

      // Fire and forget Twilio
      if (env.TWILIO_ACCOUNT_SID) {
        const endpoint = `https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Messages.json`;
        const formData = new URLSearchParams();
        // Twilio needs +1 format for US numbers
        formData.append("To", `+1${normalizedPhone}`);
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
      if (url.pathname === "/api/auth/start" && request.method === "POST") {
        const { phone } = await request.json();
        const cleanPhone = normalizePhone(phone);

        console.log(`[Auth] Normalized phone: ${cleanPhone}`);

        // Validate phone number
        if (!cleanPhone || cleanPhone.length !== 10) {
          return jsonResponse({ error: "Invalid phone number format" }, 400);
        }

        // Check A: Already an App User?
        let appUser = await findRecordByPhone(env.USERS_TABLE_NAME, cleanPhone);

        if (appUser) {
          console.log(`[Auth] Found app user: ${appUser.id}`);
          if (appUser.fields["App Approved"] === true) {
            await sendOtp(cleanPhone);
            return jsonResponse({ status: "otp_sent", message: "Code sent." });
          } else {
            return jsonResponse({ status: "pending", message: "Account under review." });
          }
        }

        // Check B: Is it a Leader in Directory?
        const leader = await findRecordByPhone(env.LEADERS_TABLE_NAME, cleanPhone);

        if (leader) {
          console.log(`[Auth] Found leader: ${leader.id}`);
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
        console.log(`[Auth] No user or leader found, needs registration`);
        return jsonResponse({ status: "needs_registration" });
      }

      // Step 2: Register New User
      if (url.pathname === "/api/auth/register" && request.method === "POST") {
        const { name, phone, networkId, orgId, reason } = await request.json();
        const normalizedPhone = normalizePhone(phone);

        // Create PENDING Leader
        const leaderFields = {
          "Leader Name": name,
          "Phone": normalizedPhone,
          "App User Status": "Pending",
        };
        if (networkId) leaderFields["Network Membership"] = [networkId];
        if (orgId) leaderFields["Leads Church"] = [orgId];

        const newLeader = await createRecord(env.LEADERS_TABLE_NAME, leaderFields);

        // Create Unapproved App User
        await createRecord(env.USERS_TABLE_NAME, {
          "Phone": normalizedPhone,
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
      if (url.pathname === "/api/auth/verify-otp" && request.method === "POST") {
        const { phone, code } = await request.json();
        const normalizedPhone = normalizePhone(phone);
        let isValid = false;

        // Check KV
        if (env.AUTH_STORE) {
          const storedOtp = await env.AUTH_STORE.get(`otp:${normalizedPhone}`);
          if (storedOtp && storedOtp === code) {
            isValid = true;
            await env.AUTH_STORE.delete(`otp:${normalizedPhone}`);
          }
        }

        // Backdoor for testing/admins (remove in prod if desired)
        if (!isValid && code === "123456") {
          const user = await findRecordByPhone(env.USERS_TABLE_NAME, normalizedPhone);
          if (user) {
            isValid = true;
          }
        }

        if (!isValid) return jsonResponse({ error: "Invalid code" }, 401);

        // Success -> Return User Token/Info
        const userRec = await findRecordByPhone(env.USERS_TABLE_NAME, normalizedPhone);
        const token = `session_${userRec.id}_${Date.now()}`; 

        return jsonResponse({ 
          token, 
          user: { 
            id: userRec.id, 
            leaderId: userRec.fields["Linked Leader"]?.[0] 
          } 
        });
      }

      // DEBUG ENDPOINT - Shows what env vars the worker sees
if (url.pathname === "/api/debug-env" && request.method === "GET") {
  return jsonResponse({
    hasApiKey: !!env.AIRTABLE_API_KEY,
    hasToken: !!env.AIRTABLE_TOKEN,
    apiKeyPrefix: env.AIRTABLE_API_KEY ? env.AIRTABLE_API_KEY.substring(0, 10) : "missing",
    baseId: env.AIRTABLE_BASE_ID,
    leadersTable: env.LEADERS_TABLE_NAME,
    usersTable: env.USERS_TABLE_NAME
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
              "Phone": leader.phone ? normalizePhone(leader.phone) : "",
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
