export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // -------------------------------------------------------------------------
    // CORS & HEADERS
    // -------------------------------------------------------------------------
    const headers = { 
      "Content-Type": "application/json", 
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, PATCH",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers });

    const jsonResponse = (data, status = 200) => 
      new Response(JSON.stringify(data), { status, headers });

    // -------------------------------------------------------------------------
    // HELPER FUNCTIONS
    // -------------------------------------------------------------------------

    function normalizePhone(phone) {
      if (!phone) return "";
      const digits = phone.replace(/\D/g, "");
      return digits.slice(-10);
    }

    // AUTH HELPER: Extract Record ID from "Bearer session_RECID_TIMESTAMP"
    function getUserIdFromHeader(req) {
      const auth = req.headers.get("Authorization");
      if (!auth) return null;
      const token = auth.replace(/^Bearer\s+/i, "");
      const parts = token.split("_");
      // Format: session_recXYZ_12345678
      if (parts.length >= 2 && parts[0] === "session" && parts[1].startsWith("rec")) {
        return parts[1];
      }
      return null;
    }

    // AIRTABLE: Create Record
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

    // AIRTABLE: Update Record
    async function updateRecord(tableName, recordId, fields) {
      const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}/${recordId}`;
      const res = await fetch(endpoint, {
        method: "PATCH",
        headers: { 
          Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}`, 
          "Content-Type": "application/json" 
        },
        body: JSON.stringify({ fields })
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(`Airtable Update Error: ${JSON.stringify(data.error)}`);
      }
      return await res.json();
    }

    // AIRTABLE: Find Single Record
    async function findAirtableRecord(table, formulaField, value) {
      const filter = `${formulaField} = '${value.replace(/'/g, "\\'")}'`;
      const endpoint = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}?filterByFormula=${encodeURIComponent(filter)}&maxRecords=1`;
      const res = await fetch(endpoint, { 
        headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } 
      });
      const data = await res.json();
      return data.records?.[0] || null;
    }

    // AIRTABLE: Fetch All Records (Pagination)
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

    // AIRTABLE: Generic Query (Formula, Sort, Limit)
    async function fetchRecords(tableName, options = {}) {
      const { formula, sort, maxRecords, fields } = options;
      let url = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}?`;
      if (formula) url += `filterByFormula=${encodeURIComponent(formula)}&`;
      if (maxRecords) url += `maxRecords=${maxRecords}&`;
      if (sort) {
        sort.forEach((s, i) => {
          url += `sort[${i}][field]=${s.field}&sort[${i}][direction]=${s.direction}&`;
        });
      }
      if (fields) {
        fields.forEach(f => url += `fields[]=${encodeURIComponent(f)}&`);
      }

      const res = await fetch(url, { 
        headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } 
      });
      const data = await res.json();
      return data.records || [];
    }

    // AIRTABLE: Phone Search (Scan all records)
    async function findRecordByPhone(tableName, phoneValue) {
      const normalizedInput = normalizePhone(phoneValue);
      let allRecords = [];
      let offset = "";
      const baseUrl = `https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;

      do {
        let fetchUrl = baseUrl;
        if (offset) fetchUrl += `?offset=${offset}`;
        const res = await fetch(fetchUrl, { 
          headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` } 
        });
        const data = await res.json();
        if (!res.ok) throw new Error(`Airtable search failed: ${res.statusText}`);
        if (data.records) allRecords = allRecords.concat(data.records);
        offset = data.offset;
      } while (offset);

      for (const record of allRecords) {
        const p = record.fields.Phone || record.fields.phone;
        if (p && normalizePhone(p) === normalizedInput) return record;
      }
      return null;
    }

    // HELPER: Find or Create Logic
    async function findOrCreate(tableName, nameField, nameValue) {
      const existing = await findAirtableRecord(tableName, nameField, nameValue);
      if (existing) return existing.id;
      const newRec = await createRecord(tableName, { [nameField]: nameValue });
      return newRec.id;
    }

    // -------------------------------------------------------------------------
    // ROUTES
    // -------------------------------------------------------------------------

    try {
      // 0. UTILITY ROUTES (Restored)
      if (url.pathname === "/health" || url.pathname === "/") {
        return jsonResponse({ status: "ok", version: "2.0.0" });
      }

      if (url.pathname === "/api/sample/") {
        return jsonResponse({ message: "Hello from the sample endpoint!" });
      }

      // 1. AUTH ROUTES

      // Start Auth Flow
      if (url.pathname === "/api/auth/start" && request.method === "POST") {
        const { phone } = await request.json();
        const cleanPhone = normalizePhone(phone);
        if (!cleanPhone || cleanPhone.length !== 10) return jsonResponse({ error: "Invalid phone" }, 400);

        let appUser = await findRecordByPhone(env.USERS_TABLE_NAME, cleanPhone);
        if (appUser) {
           if (appUser.fields["App Approved"] === true) {
             // Logic: Send OTP via Twilio/etc (omitted for dev)
             return jsonResponse({ status: "otp_sent", message: "Code sent." });
           }
           return jsonResponse({ status: "pending", message: "Account under review." });
        }

        const leader = await findRecordByPhone(env.LEADERS_TABLE_NAME, cleanPhone);
        if (leader) {
          // Auto-migrate Leader to App User
          await createRecord(env.USERS_TABLE_NAME, {
            "Name": leader.fields["Leader Name"],
            "Phone": cleanPhone,
            "App Approved": true,
            "Linked Leader": [leader.id],
            "SMS Opt-in": true
          });
          return jsonResponse({ status: "otp_sent", message: "Profile found. Code sent." });
        }
        return jsonResponse({ status: "needs_registration" });
      }

      // Register New User
      if (url.pathname === "/api/auth/register" && request.method === "POST") {
        const { name, phone, networkId, orgId, reason } = await request.json();
        const normalizedPhone = normalizePhone(phone);

        // Create Leader record first (Potentially temporary)
        const leaderFields = {
          "Leader Name": name,
          "Phone": normalizedPhone,
          "App User Status": "Pending",
        };
        if (networkId) leaderFields["Network Membership"] = [networkId];
        if (orgId) leaderFields["Leads Church"] = [orgId];

        const newLeader = await createRecord(env.LEADERS_TABLE_NAME, leaderFields);

        // Create App User
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

      // 3. AUTH ROUTES (Verify OTP - RESTORED PROFILE DATA)
      if (url.pathname === "/api/auth/verify-otp" && request.method === "POST") {
        const { phone, code } = await request.json();
        const normalizedPhone = normalizePhone(phone);
        let isValid = false;

        // Check KV (if bound)
        if (env.AUTH_STORE) {
            const storedOtp = await env.AUTH_STORE.get(`otp:${normalizedPhone}`);
            if (storedOtp && storedOtp === code) {
                isValid = true;
                await env.AUTH_STORE.delete(`otp:${normalizedPhone}`);
            }
        }

        // DEV BACKDOOR (Keep for testing)
        if (!isValid && code === "123456") isValid = true;

        if (!isValid) return jsonResponse({ error: "Invalid code" }, 401);

        // Fetch User Record
        const userRec = await findRecordByPhone(env.USERS_TABLE_NAME, normalizedPhone);
        if (!userRec) return jsonResponse({ error: "User not found" }, 404);

        const token = `session_${userRec.id}_${Date.now()}`; 

        // 1. Initialize Profile with ID & Phone
        const userProfile = {
            id: userRec.id,
            phone: normalizedPhone,
            leaderId: userRec.fields["Linked Leader"]?.[0],
            networkId: userRec.fields["Linked Network"]?.[0],
            organizationId: userRec.fields["Linked Organization"]?.[0]
        };

        // 2. Fetch Leader Details (Name, Email)
        if (userProfile.leaderId) {
            try {
                // We use direct ID fetch for speed
                const leaderRes = await fetch(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.LEADERS_TABLE_NAME)}/${userProfile.leaderId}`, {
                    headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` }
                });
                if (leaderRes.ok) {
                    const leaderData = await leaderRes.json();
                    userProfile.name = leaderData.fields["Leader Name"];
                    userProfile.email = leaderData.fields["Email"];
                    
                    // Fallback: If User table didn't have links, grab them from Leader
                    if (!userProfile.networkId && leaderData.fields["Network Membership"]?.[0]) {
                        userProfile.networkId = leaderData.fields["Network Membership"][0];
                    }
                    if (!userProfile.organizationId && leaderData.fields["Leads Church"]?.[0]) {
                        userProfile.organizationId = leaderData.fields["Leads Church"][0];
                    }
                }
            } catch (e) { console.log("Leader fetch failed", e); }
        }

        // 3. Fetch Network Name
        if (userProfile.networkId) {
            try {
                const netRes = await fetch(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.NETWORKS_TABLE_NAME)}/${userProfile.networkId}`, {
                    headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` }
                });
                if (netRes.ok) {
                    const netData = await netRes.json();
                    userProfile.network = netData.fields["Network Name"];
                }
            } catch (e) { console.log("Network fetch failed", e); }
        }

        // 4. Fetch Organization (Church) Name
        if (userProfile.organizationId) {
            try {
                const orgRes = await fetch(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.ORGANIZATIONS_TABLE_NAME)}/${userProfile.organizationId}`, {
                    headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` }
                });
                if (orgRes.ok) {
                    const orgData = await orgRes.json();
                    userProfile.church = orgData.fields["Org Name"];
                }
            } catch (e) { console.log("Org fetch failed", e); }
        }

        return jsonResponse({ token, user: userProfile });
      }
      // DEV: Manually Approve User (Restored)
      if (url.pathname === "/api/auth/dev/approve" && request.method === "POST") {
        const { phone } = await request.json();
        const clean = normalizePhone(phone);
        const userRec = await findRecordByPhone(env.USERS_TABLE_NAME, clean);
        
        if (!userRec) return jsonResponse({ error: "User not found" }, 404);
        
        await updateRecord(env.USERS_TABLE_NAME, userRec.id, { "App Approved": true });
        return jsonResponse({ success: true, message: `User ${clean} approved.` });
      }

      // 2. DATA LIST ROUTES (Cached/Static-like)
      if (url.pathname === "/get-leaders") {
        const leaders = await fetchAllRecords(env.LEADERS_TABLE_NAME, "Leader Name");
        return jsonResponse(leaders);
      }

      if (url.pathname === "/get-networks") {
        const networks = await fetchAllRecords(env.NETWORKS_TABLE_NAME, "Network Name");
        return jsonResponse(networks);
      }

      if (url.pathname === "/get-organizations") {
        const orgs = await fetchAllRecords(env.ORGANIZATIONS_TABLE_NAME, "Org Name", "Network");
        return jsonResponse(orgs);
      }

      // 3. COMMUNITY PRAYER ROUTES (NEW ARCHITECTURE)

      // Submit Prayer Request (Authenticated)
      if (url.pathname === "/prayers" && request.method === "POST") {
        const userId = getUserIdFromHeader(request);
        if (!userId) return jsonResponse({ error: "Unauthorized" }, 401);

        const body = await request.json();
        const { request: reqText, visibility, networkId, organizationId, leaderId } = body;

        if (!reqText) return jsonResponse({ error: "Request text required" }, 400);

        const fields = {
            "Request": reqText,
            "Status": "Active",
            "Visibility": visibility || "Public",
            "Submitted By": [userId]
        };

        if (networkId) fields["Network"] = [networkId];
        else if (organizationId) fields["Organization"] = [organizationId];
        else if (leaderId) fields["Leader"] = [leaderId];
        else return jsonResponse({ error: "Must link to Network, Org, or Leader" }, 400);

        const newRec = await createRecord(env.PRAYER_REQUESTS_TABLE_NAME, fields);
        return jsonResponse({ success: true, id: newRec.id });
      }

      // Log Prayer Activity (Authenticated)
      // Route: /prayers/:id/pray
      if (url.pathname.match(/^\/prayers\/rec[\w]+\/pray$/) && request.method === "POST") {
        const userId = getUserIdFromHeader(request);
        if (!userId) return jsonResponse({ error: "Unauthorized" }, 401);

        const requestId = url.pathname.split("/")[2];

        const fields = {
            "App User": [userId],
            "Request": [requestId],
            "Action Type": "Prayed",
            "Status": "Active"
        };

        await createRecord(env.PRAYER_ACTIVITY_TABLE_NAME, fields);
        return jsonResponse({ success: true });
      }

// 4. ACTIVITY WALL (Direct Link Fetch - Scalable)
      if (url.pathname === "/users/me/activity" && request.method === "GET") {
        const userId = getUserIdFromHeader(request);
        if (!userId) return jsonResponse({ error: "Unauthorized" }, 401);

        // 1. Fetch the User Record first to get the list of IDs
        // This guarantees we only get *this* user's data, no matter how old it is.
        const userRes = await fetch(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${encodeURIComponent(env.USERS_TABLE_NAME)}/${userId}`, {
            headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` }
        });
        
        if (!userRes.ok) return jsonResponse({ timeline: [] }); // User not found?
        
        const userData = await userRes.json();
        const f = userData.fields;

        // 2. Extract IDs (Limit to recent 50 to prevent URL overflow)
        // Note: Field names must match what Airtable auto-created. 
        // Usually "Prayer Requests" and "Prayer Activity".
        const requestIds = (f["Prayer Requests"] || []).slice(-50); 
        const activityIds = (f["Prayer Activity"] || []).slice(-50);

        const timeline = [];

        // 3. Fetch Specific Prayer Requests (if any)
        if (requestIds.length > 0) {
            // Formula: OR(RECORD_ID()='rec1', RECORD_ID()='rec2')
            const formula = "OR(" + requestIds.map(id => `RECORD_ID()='${id}'`).join(",") + ")";
            
            const myRequests = await fetchRecords(env.PRAYER_REQUESTS_TABLE_NAME, {
                formula: formula,
                fields: ["Request", "Created", "Network", "Organization", "Leader", "Status"]
            });

            myRequests.forEach(r => {
                // Filter out archived if desired
                if (r.fields["Status"] === "Archived") return;

                let subjectType = "General";
                if (r.fields["Network"]) subjectType = "Network";
                else if (r.fields["Organization"]) subjectType = "Organization";
                else if (r.fields["Leader"]) subjectType = "Leader";

                timeline.push({
                    type: "submitted",
                    id: r.id,
                    title: "You asked for prayer",
                    subtitle: r.fields["Request"] || "No text",
                    date: r.createdTime,
                    timestamp: new Date(r.createdTime).getTime(),
                    subjectType: subjectType
                });
            });
        }

        // 4. Fetch Specific Prayer Activity (if any)
        if (activityIds.length > 0) {
            const formula = "OR(" + activityIds.map(id => `RECORD_ID()='${id}'`).join(",") + ")";
            
            const myActivity = await fetchRecords(env.PRAYER_ACTIVITY_TABLE_NAME, {
                formula: formula,
                // We fetch the Lookups you added
                fields: ["Created", "Request", "Status", "Request Snapshot", "Network", "Organization", "Leader"]
            });

            myActivity.forEach(a => {
                // Determine Entity Name from Lookups
                const af = a.fields;
                let entityName = "Prayer Request";
                let entityType = "Request";

                if (af["Network"] && af["Network"].length) { entityName = af["Network"][0]; entityType = "Network"; }
                else if (af["Organization"] && af["Organization"].length) { entityName = af["Organization"][0]; entityType = "Organization"; }
                else if (af["Leader"] && af["Leader"].length) { entityName = af["Leader"][0]; entityType = "Leader"; }

                const textSnippet = af["Request Snapshot"] ? af["Request Snapshot"][0] : "Prayer request";

                timeline.push({
                    type: "prayed",
                    id: a.id,
                    requestId: af["Request"] ? af["Request"][0] : null,
                    title: `You prayed for ${entityName}`,
                    subtitle: textSnippet,
                    date: a.createdTime,
                    timestamp: new Date(a.createdTime).getTime(),
                    entityType: entityType
                });
            });
        }

        // 5. Sort & Return
        timeline.sort((a, b) => b.timestamp - a.timestamp);
        return jsonResponse({ timeline });
      }

      
      // 3.5 PUBLIC PRAYER REQUESTS (Dynamic Fetch for Maps)
      // GET /public/requests?networkId=... (or organizationId, leaderId)
      if (url.pathname === "/public/requests" && request.method === "GET") {
        const networkId = url.searchParams.get("networkId");
        const orgId = url.searchParams.get("organizationId") || url.searchParams.get("orgId");
        const leaderId = url.searchParams.get("leaderId");

        // 1. Fetch all ACTIVE + PUBLIC requests
        // We fetch the recent active list and filter in memory to ensure we match the Linked Record ID correctly
        const activeRequests = await fetchRecords(env.PRAYER_REQUESTS_TABLE_NAME, {
          formula: "AND({Status}='Active', {Visibility}='Public')",
          sort: [{ field: "Created", direction: "desc" }],
          maxRecords: 100 
        });

        // 2. Filter for the specific target
        const matches = activeRequests.filter(r => {
           const f = r.fields;
           // Check if the target ID exists in the linked array
           if (networkId && f["Network"] && f["Network"].includes(networkId)) return true;
           if (orgId && f["Organization"] && f["Organization"].includes(orgId)) return true;
           if (leaderId && f["Leader"] && f["Leader"].includes(leaderId)) return true;
           return false;
        });

        // 3. Return simplified response
        return jsonResponse({
          requests: matches.map(r => ({
              id: r.id,
              text: r.fields["Request"],
              created: r.createdTime
          }))
        });
      }

      // 3.5 LOG ENTITY PRAYER (The "Standing Request" Pattern)
      // POST /prayers/log-entity
      // Body: { networkId, organizationId, leaderId }
      if (url.pathname === "/prayers/log-entity" && request.method === "POST") {
        const userId = getUserIdFromHeader(request);
        if (!userId) return jsonResponse({ error: "Unauthorized" }, 401);

        const body = await request.json();
        const { networkId, organizationId, leaderId } = body;

        if (!networkId && !organizationId && !leaderId) {
             return jsonResponse({ error: "Target ID required" }, 400);
        }

        // 1. Determine Target Table & Field
        let targetTable, targetField, targetId, nameField;
        if (networkId) { 
            targetTable = env.NETWORKS_TABLE_NAME; targetField = "Network"; targetId = networkId; nameField = "Network Name";
        } else if (organizationId) { 
            targetTable = env.ORGANIZATIONS_TABLE_NAME; targetField = "Organization"; targetId = organizationId; nameField = "Org Name";
        } else { 
            targetTable = env.LEADERS_TABLE_NAME; targetField = "Leader"; targetId = leaderId; nameField = "Leader Name";
        }

        const REQUESTS_TABLE = env.PRAYER_REQUESTS_TABLE_NAME || "Prayer Requests";

        // 2. Find Existing "Standing" Request
        // Formula: AND({LinkedField}='ID', {Type}='Standing')
        const existing = await fetchRecords(REQUESTS_TABLE, {
            formula: `AND({${targetField}}='${targetId}', {Type}='Standing')`,
            maxRecords: 1
        });

        let requestId;

        if (existing.length > 0) {
            requestId = existing[0].id;
        } else {
            // 3. Fetch Entity Name (for the request text)
            const entity = await findAirtableRecord(targetTable, "RECORD_ID()", targetId); // Helper needed or just fetch
            // Alternatively, just fetch the specific record directly:
            const entRes = await fetch(`https://api.airtable.com/v0/${env.AIRTABLE_BASE_ID}/${targetTable}/${targetId}`, {
                 headers: { Authorization: `Bearer ${env.AIRTABLE_API_KEY || env.AIRTABLE_TOKEN}` }
            });
            const entData = await entRes.json();
            const entityName = entData.fields?.[nameField] || "Unspecified";

            // 4. Create Standing Request
            const newReq = await createRecord(REQUESTS_TABLE, {
                [targetField]: [targetId],
                "Request": `Prayer for ${entityName}`,
                "Type": "Standing",
                "Status": "Active",
                "Visibility": "Public"
            });
            requestId = newReq.id;
        }

        // 5. Log Activity
        await createRecord(env.PRAYER_ACTIVITY_TABLE_NAME, {
            "App User": [userId],
            "Request": [requestId],
            "Action Type": "Prayed",
            "Status": "Active"
        });

        return jsonResponse({ success: true, requestId });
      }
      
      // 4. LEGACY / HYBRID SUBMISSION ROUTES

     // 2. APP SUBMIT PRAYER (Flexible: Network, Org, or Leader)
      if (url.pathname === "/app-submit-prayer" && request.method === "POST") {
        const body = await request.json();
        const { networkId, church, leader, request: reqText, userId } = body;
        
        // Extract User ID from Header or Body
        let authUserId = userId;
        const authHeader = request.headers.get("Authorization");
        if (authHeader) {
             const token = authHeader.replace(/^Bearer\s+/i, "");
             const parts = token.split("_");
             if (parts.length >= 2 && parts[0] === "session" && parts[1].startsWith("rec")) {
                authUserId = parts[1];
             }
        }

        if (!reqText) return jsonResponse({ error: "Missing request text" }, 400);

        // Validation: At least one target is required
        const hasNetwork = !!networkId;
        const hasChurch  = church && (church.id || church.isNew);
        const hasLeader  = leader && (leader.id || leader.isNew);

        if (!hasNetwork && !hasChurch && !hasLeader) {
             return jsonResponse({ error: "Must target at least one: Network, Church, or Leader" }, 400);
        }

        // Handle Church Logic (Create if New)
        let finalChurchId = null;
        if (church) {
            if (church.isNew && church.name) {
                const newOrg = await createRecord(env.ORGANIZATIONS_TABLE_NAME, {
                    "Org Name": church.name,
                    "Address": church.address, 
                    "Org Type": church.type, 
                    "Network": networkId ? [networkId] : []
                });
                finalChurchId = newOrg.id;
            } else {
                finalChurchId = church.id;
            }
        }

        // Handle Leader Logic (Create if New)
        let finalLeaderId = null;
        if (leader) {
            if (leader.isNew && leader.name) {
                const newLeader = await createRecord(env.LEADERS_TABLE_NAME, {
                    "Leader Name": leader.name,
                    "Email": leader.email,
                    "Phone": leader.phone ? normalizePhone(leader.phone) : "",
                    "Leads Church": finalChurchId ? [finalChurchId] : [] 
                });
                finalLeaderId = newLeader.id;
            } else {
                finalLeaderId = leader.id;
            }
        }

        // Build Record Fields
        const fields = {
            "Request": reqText,
            "Status": "Pending",
            "Visibility": "Public"
        };

        if (networkId) fields["Network"] = [networkId];
        if (finalChurchId) fields["Organization"] = [finalChurchId];
        if (finalLeaderId) fields["Leader"] = [finalLeaderId];
        if (authUserId) fields["Submitted By"] = [authUserId];

        // Uses the table name variable present in your file (falls back to string if var missing)
        const targetTable = env.PRAYER_REQUESTS_TABLE_NAME || env.IN_PRAYER_REQUESTS_TABLE_NAME || "Prayer Requests";

        await createRecord(targetTable, fields);
        return jsonResponse({ success: true, churchId: finalChurchId, leaderId: finalLeaderId });
      }

      // Public Web Form Submit
      if (url.pathname === "/submit-church-prayer" && request.method === "POST") {
        const body = await request.json();
        const { networkName, organizationName, leaderName, prayerRequest } = body;
        
        const netRec = await findAirtableRecord(env.NETWORKS_TABLE_NAME, "Network Name", networkName);
        if (!netRec) return jsonResponse({ error: "Network not found." }, 400);
        
        const finalNetworkId = netRec.id;
        let finalOrgId = organizationName ? await findOrCreate(env.ORGANIZATIONS_TABLE_NAME, "Org Name", organizationName) : null;
        let finalLeaderId = leaderName ? await findOrCreate(env.LEADERS_TABLE_NAME, "Leader Name", leaderName) : null;

        const fields = { 
            "Network": [finalNetworkId], 
            "Request": prayerRequest,
            "Status": "Pending", // Web submissions require moderation
            "Visibility": "Public"
        };
        if (finalOrgId) fields["Organization"] = [finalOrgId];
        if (finalLeaderId) fields["Leader"] = [finalLeaderId];

        await createRecord(env.PRAYER_REQUESTS_TABLE_NAME, fields);
        return jsonResponse({ status: "Saved" });
      }

      // 5. PERSONAL PRAYER APP ROUTES (LEGACY)
      
      if (url.pathname === "/submit-prayer" && request.method === "POST") {
        const body = await request.json();
        // Maps to "Personal Prayer Requests" table via env var
        await createRecord(env.PRAYER_REQUESTS_TABLE, { 
          "Leader": [body.leaderId], 
          "Request Text": body.text, 
          "Status": "Active" 
        });
        return jsonResponse({ status: "Saved" });
      }

      // Random Prayer Fetcher
      if (url.pathname === "/get-prayer") {
        const filterType = url.searchParams.get('filter') || 'unprayed-today';
        let formula = "AND({Status}='Active')";
        
        if (filterType === 'unprayed-today') {
            formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 1))`;
        } else if (filterType === 'unprayed-week') {
            formula = `AND({Status}='Active', OR({Last Prayed}=BLANK(), DATETIME_DIFF(NOW(), {Last Prayed}, 'days') >= 7))`;
        } else if (filterType === 'past-month') {
            formula = `AND({Status}='Active', DATETIME_DIFF(NOW(), CREATED_TIME(), 'days') <= 30)`;
        }

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
        // Uses legacy "Prayer Logs" table
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
