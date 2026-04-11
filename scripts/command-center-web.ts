#!/usr/bin/env tsx
/**
 * MXRE Command Center — Live Web Dashboard
 *
 * Runs a local web server that auto-refreshes with real-time data coverage stats.
 * Open http://localhost:3333 in your browser.
 */

import "dotenv/config";
import { createServer } from "node:http";
import { createClient } from "@supabase/supabase-js";

const PORT = parseInt(process.env.PORT || "3334", 10);
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Set SUPABASE_URL and SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const KNOWN_TOTALS: Record<string, number> = {
  // Oklahoma
  "Comanche_OK": 57_081, "Oklahoma_OK": 350_000, "Tulsa_OK": 280_000,
  // Texas
  "Dallas_TX": 1_200_000, "Tarrant_TX": 700_000, "Denton_TX": 400_000,
  "Austin_TX": 18_000, "Fannin_TX": 20_000, "Galveston_TX": 170_000, "Kerr_TX": 35_000, "Panola_TX": 15_000,
  // Ohio
  "Fairfield_OH": 75_000, "Geauga_OH": 51_000, "Paulding_OH": 12_000, "Wyandot_OH": 14_000,
  "Franklin_OH": 493_000, "Cuyahoga_OH": 521_000, "Hamilton_OH": 420_000,
  "Montgomery_OH": 288_000, "Summit_OH": 261_000, "Stark_OH": 202_000,
  // Michigan
  "Oakland_MI": 490_000, "Antrim_MI": 20_000,
  // Iowa
  "Black Hawk_IA": 70_000, "Boone_IA": 15_000, "Calhoun_IA": 8_000, "Clayton_IA": 12_000,
  "Jasper_IA": 22_000, "Linn_IA": 120_000, "Scott_IA": 85_000,
  // New Hampshire
  "Belknap_NH": 35_000, "Carroll_NH": 30_000, "Cheshire_NH": 40_000, "Grafton_NH": 50_000,
  "Hillsborough_NH": 210_000, "Rockingham_NH": 150_000, "Strafford_NH": 65_000, "Sullivan_NH": 25_000,
  // Arkansas
  "Saline_AR": 60_000,
  // Washington
  "Yakima_WA": 120_000,
  "Alachua_FL": 108_000, "Baker_FL": 14_000, "Bay_FL": 115_000, "Bradford_FL": 15_000,
  "Brevard_FL": 330_000, "Broward_FL": 750_000, "Calhoun_FL": 8_000, "Charlotte_FL": 120_000,
  "Citrus_FL": 95_000, "Clay_FL": 110_000, "Collier_FL": 220_000, "Columbia_FL": 40_000,
  "Dade_FL": 950_000, "Desoto_FL": 22_000, "Dixie_FL": 10_000, "Duval_FL": 450_000,
  "Escambia_FL": 165_000, "Flagler_FL": 65_000, "Franklin_FL": 18_000, "Gadsden_FL": 28_000,
  "Gilchrist_FL": 10_000, "Glades_FL": 10_000, "Gulf_FL": 14_000, "Hamilton_FL": 9_000,
  "Hardee_FL": 16_000, "Hendry_FL": 22_000, "Hernando_FL": 115_000, "Highlands_FL": 75_000,
  "Hillsborough_FL": 550_000, "Holmes_FL": 12_000, "Indian River_FL": 95_000, "Jackson_FL": 30_000,
  "Jefferson_FL": 10_000, "Lafayette_FL": 5_000, "Lake_FL": 210_000, "Lee_FL": 400_000,
  "Leon_FL": 135_000, "Levy_FL": 30_000, "Liberty_FL": 5_000, "Madison_FL": 12_000,
  "Manatee_FL": 210_000, "Marion_FL": 210_000, "Martin_FL": 80_000, "Monroe_FL": 55_000,
  "Nassau_FL": 55_000, "Okaloosa_FL": 115_000, "Okeechobee_FL": 25_000, "Orange_FL": 500_000,
  "Osceola_FL": 180_000, "Palm Beach_FL": 650_000, "Pasco_FL": 280_000, "Pinellas_FL": 400_000,
  "Polk_FL": 370_000, "Putnam_FL": 50_000, "Saint Johns_FL": 135_000, "Saint Lucie_FL": 165_000,
  "Santa Rosa_FL": 100_000, "Sarasota_FL": 230_000, "Seminole_FL": 195_000, "Sumter_FL": 75_000,
  "Suwannee_FL": 25_000, "Taylor_FL": 14_000, "Union_FL": 6_000, "Volusia_FL": 310_000,
  "Wakulla_FL": 20_000, "Walton_FL": 55_000, "Washington_FL": 15_000,
};

interface CountyStats {
  county_name: string;
  state_code: string;
  total: number;
  estimated: number;
  with_address: number;
  with_assessed: number;
  with_tax: number;
  with_rent: number;
  with_mortgage: number;
}

// Async stats cache - updated every 5 minutes in background, served instantly
let cachedStats: any = null;
let cacheTime = 0;
let lastIngestTime = 0;
let statsRefreshInProgress = false;

// Async background refresh - doesn't block dashboard response
async function refreshStatsCache() {
  if (statsRefreshInProgress) return; // Skip if already refreshing
  statsRefreshInProgress = true;

  try {
    // Query counts - use .limit(1) trick since RLS blocks aggregate COUNT()
    const [propResult, rentResult, mortResult, mortAmountResult] = await Promise.all([
      db.from("properties").select("id", { count: "exact", head: true }),
      db.from("rent_snapshots").select("id", { count: "exact", head: true }),
      db.from("mortgage_records").select("id", { count: "exact", head: true }),
      db.from("mortgage_records").select("id", { count: "exact", head: true }).not("loan_amount", "is", null).gt("loan_amount", 0),
    ]);

    const totalProps = propResult.count ?? 0;
    const totalRent = rentResult.count ?? 0;
    const totalMort = mortResult.count ?? 0;
    const totalMortAmounts = mortAmountResult.count ?? 0;

    // County breakdown (from county_stats_mv if available)
    let counties: CountyStats[] = [];
    try {
      const { data: countyData } = await db.from("county_stats_mv").select("*");
      if (countyData) {
        counties = countyData.map((r: any) => ({
          county_name: r.county_name,
          state_code: r.state_code,
          total: Number(r.total_props),
          estimated: KNOWN_TOTALS[`${r.county_name}_${r.state_code}`] || 0,
          with_address: Number(r.with_address),
          with_assessed: Number(r.with_assessed),
          with_tax: Number(r.with_tax),
          with_rent: 0,
          with_mortgage: 0,
        }));
      }
    } catch (mvErr) {
      // MV failed, just use totals
    }

    cachedStats = {
      counties,
      totals: {
        properties: totalProps,
        rent: totalRent,
        mortgage: totalMort,
        mortgage_total: totalMort,
        mortgage_with_amounts: totalMortAmounts,
      },
    };
    cacheTime = Date.now();
    console.log(`✓ Stats cache updated: ${totalProps.toLocaleString()} properties, ${totalRent.toLocaleString()} rents, ${totalMort.toLocaleString()} mortgages`);
  } catch (err) {
    console.error('Stats cache refresh error:', (err as Error).message);
  } finally {
    statsRefreshInProgress = false;
  }
}

// Return cached stats immediately (no waiting for queries)
function getStats() {
  return {
    ...cachedStats || { counties: [], totals: { properties: 0, rent: 0, mortgage: 0, mortgage_total: 0, mortgage_with_amounts: 0 } },
    lastUpdated: cacheTime,
    cacheAge: Date.now() - cacheTime,
  };
}

function pct(n: number, d: number): number {
  return d > 0 ? Math.round((n / d) * 100) : 0;
}

const STATE_NAMES: Record<string, string> = {
  FL: "Florida", OK: "Oklahoma", TX: "Texas", IL: "Illinois", OH: "Ohio",
  MI: "Michigan", IA: "Iowa", NH: "New Hampshire", AR: "Arkansas", WA: "Washington",
  NC: "North Carolina", IN: "Indiana", WI: "Wisconsin", MN: "Minnesota",
  CO: "Colorado", NJ: "New Jersey", PA: "Pennsylvania", MD: "Maryland",
  NY: "New York", OR: "Oregon",
};

const ALL_STATES = Object.keys(STATE_NAMES).sort();

function propertiesPageHTML(): string {
  const stateOptions = ALL_STATES.map(s => `<option value="${s}">${STATE_NAMES[s] || s}</option>`).join("");
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>MXRE Properties Explorer</title>
  <meta http-equiv="refresh" content="10">
  <style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#0f172a; color:#e2e8f0; padding:20px; }
    .header { text-align:center; padding:20px 0 10px; }
    .header h1 { font-size:28px; color:#f8fafc; letter-spacing:2px; }
    .tab-nav { display:flex; gap:0; justify-content:center; margin:20px 0 30px; border-bottom:1px solid #334155; }
    .tab-link { padding:10px 30px; color:#94a3b8; text-decoration:none; font-size:14px; font-weight:600; text-transform:uppercase; border-bottom:2px solid transparent; transition:all 0.2s; }
    .tab-link.active { color:#38bdf8; border-bottom-color:#38bdf8; }
    .container { max-width:1600px; margin:0 auto; }
    .filters { background:#1e293b; padding:20px; border-radius:12px; margin-bottom:20px; display:grid; grid-template-columns:1fr 1fr 1fr 1fr auto; gap:12px; align-items:end; }
    select, input { background:#0f172a; border:1px solid #334155; color:#e2e8f0; padding:8px 12px; border-radius:6px; font-size:13px; }
    select:focus, input:focus { outline:none; border-color:#38bdf8; }
    button { background:#38bdf8; color:#0f172a; border:none; padding:8px 16px; border-radius:6px; font-weight:600; cursor:pointer; font-size:13px; }
    button:hover { background:#0ea5e9; }
    .list { background:#1e293b; border-radius:12px; overflow:hidden; }
    .list-item { padding:10px 12px; border-bottom:1px solid #334155; cursor:pointer; transition:all 0.2s; }
    .list-item:hover { background:#334155; }
    .list-item .addr { font-weight:600; font-size:12px; }
    .list-item .meta { font-size:10px; color:#94a3b8; margin-top:2px; }
    .list-item .completeness { font-size:10px; color:#4ade80; margin-top:2px; }
    .details { background:#1e293b; border-radius:12px; padding:20px; max-height:800px; overflow-y:auto; }
    .details-header { font-size:18px; font-weight:600; margin-bottom:10px; color:#38bdf8; }
    .completeness-bar { background:#334155; height:6px; border-radius:3px; margin-bottom:20px; overflow:hidden; }
    .completeness-fill { background:#4ade80; height:100%; transition:width 0.3s; }
    .detail-group { margin-bottom:20px; }
    .detail-group-title { font-size:11px; text-transform:uppercase; color:#64748b; letter-spacing:0.5px; margin-bottom:8px; padding-bottom:8px; border-bottom:1px solid #334155; }
    .detail-row { display:grid; grid-template-columns:150px 1fr; margin-bottom:10px; font-size:13px; }
    .detail-label { color:#94a3b8; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }
    .detail-value { color:#e2e8f0; word-break:break-all; }
    .detail-value.empty { color:#64748b; font-style:italic; }
    .detail-value.populated { color:#4ade80; }
    .loading { text-align:center; color:#64748b; padding:20px; }
    .modal { display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.7); z-index:1000; align-items:center; justify-content:center; }
    .modal.active { display:flex; }
    .modal-content { background:#1e293b; border-radius:12px; padding:30px; max-width:90%; max-height:90%; overflow-y:auto; }
    .modal-close { position:absolute; top:20px; right:20px; background:#334155; border:none; color:#e2e8f0; width:30px; height:30px; border-radius:50%; cursor:pointer; font-size:18px; }
    .analyze-btn { background:#8b5cf6; margin-bottom:15px; padding:10px 16px; }
    .analyze-btn:hover { background:#7c3aed; }
    .analyze-btn:disabled { background:#64748b; cursor:not-allowed; }
    .analysis-result { background:#0f172a; padding:15px; border-radius:8px; margin-top:15px; border-left:3px solid #8b5cf6; }
    .analysis-result .loading { color:#94a3b8; font-size:12px; }
  </style>
</head>
<body>
  <div class="header"><h1>MXRE Properties Explorer</h1></div>
  ${navBar("properties")}

  <div class="container">
    <div class="filters">
      <select id="stateFilter" onchange="currentPage=1; loadProperties()">
        <option value="">All States</option>
        ${stateOptions}
      </select>
      <select id="countyFilter" onchange="currentPage=1; loadProperties()">
        <option value="">All Counties</option>
      </select>
      <select id="completenessFilter" onchange="loadProperties()">
        <option value="">All Properties</option>
        <option value="75">75%+ Complete</option>
        <option value="50">50%+ Complete</option>
        <option value="25">25%+ Complete</option>
      </select>
      <input type="number" id="limitInput" value="50" min="1" max="1000" placeholder="Page size">
      <button onclick="currentPage=1; loadProperties()">Load</button>
    </div>

    <div class="list" id="propertyList" style="max-height:600px; overflow-y:auto;">
      <div class="loading">Select filters and click Load</div>
    </div>

    <div id="pagination" style="display:flex; gap:10px; margin-top:15px; align-items:center; padding:10px; background:#0f172a; border-radius:8px;">
      <button id="prevBtn" onclick="previousPage()" style="padding:8px 16px; background:#334155; color:#e2e8f0; border:none; border-radius:6px; cursor:pointer;">← Previous</button>
      <div id="pageInfo" style="color:#94a3b8; flex:1;">Page 1</div>
      <button id="nextBtn" onclick="nextPage()" style="padding:8px 16px; background:#334155; color:#e2e8f0; border:none; border-radius:6px; cursor:pointer;">Next →</button>
    </div>
  </div>

  <div id="detailModal" class="modal" onclick="if(event.target===this)closeModal()">
    <div class="modal-content">
      <button class="modal-close" onclick="closeModal()">×</button>
      <div id="modalDetails"></div>
    </div>
  </div>

  <script>
    let properties = [];
    let propertyMap = {};
    let currentPage = 1;
    let pageSize = 50;
    let totalCount = 0;
    let hasMorePages = false;

    async function loadProperties() {
      const state = document.getElementById("stateFilter").value;
      const county = document.getElementById("countyFilter").value;
      pageSize = parseInt(document.getElementById("limitInput").value) || 50;
      const offset = (currentPage - 1) * pageSize;

      const listEl = document.getElementById("propertyList");
      listEl.innerHTML = '<div class="loading">Loading...</div>';

      try {
        const params = new URLSearchParams();
        if (state) params.append("state", state);
        if (county) params.append("county", county);
        params.append("limit", pageSize.toString());
        params.append("offset", offset.toString());

        const res = await fetch("/api/properties?" + params.toString());
        const data = await res.json();
        properties = data.properties || [];
        if (!Array.isArray(properties)) properties = [];

        totalCount = data.total || 0;
        hasMorePages = data.hasMore || false;

        // Store properties in map for quick lookup
        propertyMap = {};
        properties.forEach(p => { propertyMap[p.id] = p; });

        // Filter by completeness if needed
        const completenessMin = parseInt(document.getElementById("completenessFilter").value) || 0;
        let filtered = properties;
        if (completenessMin > 0) {
          filtered = properties.filter(p => {
            const completeness = calculateCompleteness(p);
            return completeness >= completenessMin;
          });
        }

        if (filtered.length === 0) {
          listEl.innerHTML = '<div class="loading">No properties found</div>';
        } else {
          listEl.innerHTML = filtered.map((p, i) => {
            const completeness = calculateCompleteness(p);
            return '<div class="list-item" data-id="' + p.id + '" style="cursor:pointer;">' +
              '<div class="addr">' + (p.address || "Unknown") + '</div>' +
              '<div class="meta">' + (p.county_name || "") + ', ' + (p.state_code || "") + ' • ' + (p.assessed_value || "N/A") + '</div>' +
              '<div class="completeness">' + completeness + '% complete</div>' +
            '</div>';
          }).join("");

          // Attach event listeners to property items
          document.querySelectorAll('.list-item').forEach(el => {
            el.addEventListener('click', function() {
              const id = this.getAttribute('data-id');
              const property = propertyMap[id];
              if (property) showDetails(property);
            });
          });
        }

        updatePaginationControls();
      } catch (err) {
        listEl.innerHTML = '<div class="loading">Error: ' + err.message + '</div>';
      }
    }

    function updatePaginationControls() {
      const paginationEl = document.getElementById("pagination");
      const prevBtn = document.getElementById("prevBtn");
      const nextBtn = document.getElementById("nextBtn");
      const pageInfo = document.getElementById("pageInfo");

      prevBtn.disabled = currentPage === 1;
      nextBtn.disabled = !hasMorePages;
      pageInfo.textContent = 'Page ' + currentPage + ' (showing ' + Math.min(pageSize, properties.length) + ' of ' + (totalCount || '?') + ')';
    }

    function previousPage() {
      if (currentPage > 1) {
        currentPage--;
        loadProperties();
      }
    }

    function nextPage() {
      if (hasMorePages) {
        currentPage++;
        loadProperties();
      }
    }

    function calculateCompleteness(obj) {
      if (!obj) return 0;
      const entries = Object.entries(obj);
      const populated = entries.filter(([k, v]) => v !== null && v !== undefined && v !== "").length;
      return Math.round((populated / entries.length) * 100);
    }

    function showDetails(property) {
      const modal = document.getElementById("detailModal");
      const modalDetails = document.getElementById("modalDetails");
      const completeness = calculateCompleteness(property);

      let html = '<div class="details-header">' + (property.address || "Property Details") + '</div>';
      html += '<div class="completeness-bar"><div class="completeness-fill" style="width:' + completeness + '%"></div></div>';
      html += '<div style="margin-bottom:15px; color:#94a3b8; font-size:12px;">' + completeness + '% data complete • ' + Object.keys(property).length + ' fields</div>';
      html += '<button class="analyze-btn" onclick="analyzeProperty(' + JSON.stringify(property).replace(/"/g, '&quot;') + ')">🧠 Analyze with AI</button>';
      html += '<div id="analysisResults"></div>';

      const fieldGroups = {
        'Location': ['address', 'city', 'county_name', 'state_code', 'zip', 'mailing_address', 'mailing_city', 'mailing_state', 'mailing_zip', 'legal_description', 'parcel_id'],
        'Valuation': ['assessed_value', 'market_value', 'appraised_building', 'appraised_land', 'land_value', 'property_tax', 'annual_tax', 'tax_year'],
        'Property Details': ['property_type', 'property_class', 'total_sqft', 'land_sqft', 'lot_sqft', 'lot_acres', 'year_built', 'bedrooms', 'bathrooms', 'bathrooms_full', 'total_rooms', 'total_units', 'stories', 'style', 'condition_code', 'neighborhood_code'],
        'Building Features': ['basement_type', 'exterior_wall', 'heating_type', 'fuel_type', 'fireplace', 'deck', 'porch', 'pool', 'hoa', 'has_attic', 'mobile_home'],
        'Owner Info': ['owner_name', 'owner_occupied', 'absentee_owner', 'in_state_absentee', 'corporate_owned'],
        'Flood & Risk': ['flood_zone', 'lien_status'],
        'Affordability': ['is_affordable', 'is_sfr', 'is_apartment', 'is_condo', 'is_btr', 'is_senior', 'is_student'],
        'Other': []
      };

      const used = new Set();

      for (const [groupName, fields] of Object.entries(fieldGroups)) {
        if (groupName === 'Other') continue;
        const groupFields = fields.filter(f => property.hasOwnProperty(f));
        if (groupFields.length === 0) continue;

        html += '<div class="detail-group"><div class="detail-group-title">' + groupName + '</div>';
        for (const field of groupFields) {
          const value = property[field];
          const hasValue = value !== null && value !== undefined && value !== "";
          const displayValue = hasValue ? value : '—';
          const valueClass = hasValue ? 'populated' : 'empty';
          const label = field.replace(/_/g, " ").replace(/\\b(\\w)/g, (m) => m.toUpperCase());
          html += '<div class="detail-row"><div class="detail-label">' + label + '</div><div class="detail-value ' + valueClass + '">' + displayValue + '</div></div>';
          used.add(field);
        }
        html += '</div>';
      }

      // Add remaining fields
      const otherFields = Object.keys(property).filter(k => !used.has(k) && k !== 'id');
      if (otherFields.length > 0) {
        html += '<div class="detail-group"><div class="detail-group-title">Other Fields</div>';
        for (const field of otherFields) {
          const value = property[field];
          const hasValue = value !== null && value !== undefined && value !== "";
          const displayValue = hasValue ? value : '—';
          const valueClass = hasValue ? 'populated' : 'empty';
          const label = field.replace(/_/g, " ").replace(/\\b(\\w)/g, (m) => m.toUpperCase());
          html += '<div class="detail-row"><div class="detail-label">' + label + '</div><div class="detail-value ' + valueClass + '">' + displayValue + '</div></div>';
        }
        html += '</div>';
      }

      modalDetails.innerHTML = html;
      modal.classList.add("active");
    }

    function closeModal() {
      document.getElementById("detailModal").classList.remove("active");
    }

    async function analyzeProperty(property) {
      const resultsDiv = document.getElementById("analysisResults");
      resultsDiv.innerHTML = '<div class="analysis-result"><div class="loading">🔄 Analyzing property...</div></div>';

      try {
        // Create a summary of the property data
        const propSummary = [
          property.address || "Unknown address",
          property.county_name && property.state_code ? property.county_name + ", " + property.state_code : "",
          property.property_type ? "Type: " + property.property_type : "",
          property.assessed_value ? "Assessed: $" + Number(property.assessed_value).toLocaleString() : "",
          property.bedrooms ? property.bedrooms + " bed" : "",
          property.bathrooms ? property.bathrooms + " bath" : "",
          property.year_built ? "Built: " + property.year_built : "",
          property.lien_status ? "Lien status: " + property.lien_status : ""
        ].filter(x => x).join(" • ");

        const prompt = "User: Analyze this property and provide investment insights: " + propSummary + ". Assistant:";

        const res = await fetch("http://localhost:18789/api/inference", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": "Bearer " + (window.gatewayToken || "") },
          body: JSON.stringify({ prompt: prompt, max_tokens: 200 })
        });

        if (!res.ok) {
          throw new Error("Inference service unavailable (start: C:\\\\Users\\\\msanc\\\\start_inference_service.ps1)");
        }

        const data = await res.json();
        const analysis = data.response || "";

        resultsDiv.innerHTML = '<div class="analysis-result"><strong>AI Analysis:</strong><div style="margin-top:10px; font-size:12px; line-height:1.6;">' + analysis.replace(/</g, "&lt;").replace(/>/g, "&gt;") + '</div></div>';
      } catch (err) {
        resultsDiv.innerHTML = '<div class="analysis-result" style="border-left-color:#ef4444;"><strong>Analysis Error:</strong><div style="margin-top:10px; font-size:12px; color:#fca5a5;">' + err.message + '</div></div>';
      }
    }

    // Load counties when state changes
    document.getElementById("stateFilter").addEventListener("change", async (e) => {
      const state = e.target.value;
      const countySelect = document.getElementById("countyFilter");
      countySelect.innerHTML = '<option value="">All Counties</option>';

      if (!state) return;

      try {
        const res = await fetch("/api/stats");
        const stats = await res.json();
        const counties = stats.counties
          .filter(c => c.state_code === state)
          .map(c => c.county_name)
          .sort();

        countySelect.innerHTML = '<option value="">All Counties</option>' +
          counties.map(c => '<option value="' + c + '">' + c + '</option>').join("");
      } catch (err) {
        console.error("Error loading counties:", err);
      }
    });
  </script>
</body>
</html>`;
}

function navBar(activeTab: "coverage" | "properties" | "published"): string {
  return `<nav class="tab-nav">
    <a href="/" class="tab-link ${activeTab === "coverage" ? "active" : ""}">Coverage</a>
    <a href="/published" class="tab-link ${activeTab === "published" ? "active" : ""}">Published Coverage</a>
    <a href="/properties" class="tab-link ${activeTab === "properties" ? "active" : ""}">Properties</a>
  </nav>`;
}

const SHARED_STYLES = `
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#0f172a; color:#e2e8f0; padding:20px; }
  .header { text-align:center; padding:20px 0 10px; }
  .header h1 { font-size:28px; color:#f8fafc; letter-spacing:2px; }
  .header .subtitle { color:#94a3b8; margin-top:5px; }
  .tab-nav { display:flex; gap:0; justify-content:center; margin:20px 0 30px; border-bottom:1px solid #334155; }
  .tab-link { padding:10px 30px; color:#94a3b8; text-decoration:none; font-size:14px; font-weight:600; letter-spacing:1px; text-transform:uppercase; border-bottom:2px solid transparent; transition:all 0.2s; }
  .tab-link:hover { color:#e2e8f0; }
  .tab-link.active { color:#38bdf8; border-bottom-color:#38bdf8; }
  .state-block { margin-bottom:30px; background:#1e293b; border-radius:12px; overflow:hidden; }
  .state-header { padding:15px 20px; background:#334155; display:flex; justify-content:space-between; align-items:center; }
  .state-header h2 { font-size:18px; }
  .state-summary { color:#94a3b8; font-size:13px; }
  .state-summary .big-num { color:#38bdf8; font-weight:700; font-size:15px; }
  table { width:100%; border-collapse:collapse; }
  th { text-align:left; padding:10px 15px; font-size:11px; text-transform:uppercase; letter-spacing:1px; color:#64748b; border-bottom:1px solid #334155; }
  td { padding:8px 15px; border-bottom:1px solid #1e293b; font-size:13px; }
  tr:hover { background:#334155; }
  .bar-bg { display:inline-block; width:80px; height:8px; background:#334155; border-radius:4px; vertical-align:middle; margin-right:8px; }
  .bar { height:100%; border-radius:4px; transition: width 0.5s; }
  .pct { font-weight:600; font-size:12px; }
  .pct.good { color:#4ade80; }
  .pct.warn { color:#facc15; }
  .updated { text-align:center; color:#475569; font-size:11px; margin-top:20px; }
  .badge { display:inline-block; padding:2px 8px; border-radius:9999px; font-size:11px; font-weight:600; margin-right:4px; }
  .badge-rent { background:#0e7490; color:#cffafe; }
  .badge-mort { background:#9333ea; color:#f3e8ff; }
  .badge-listing { background:#059669; color:#d1fae5; }
`;

function renderHTML(stats: any): string {
  const cacheAge = Math.round((stats.cacheAge || 0) / 1000);
  const lastUpdatedTime = stats.lastUpdated ? new Date(stats.lastUpdated).toLocaleTimeString() : "never";
  const byState = new Map<string, CountyStats[]>();
  for (const s of stats.counties) {
    if (!byState.has(s.state_code)) byState.set(s.state_code, []);
    byState.get(s.state_code)!.push(s);
  }

  let stateBlocks = "";
  for (const [state, counties] of byState) {
    const stateTotal = counties.reduce((a, c) => a + c.total, 0);
    const stateEst = counties.reduce((a, c) => a + (c.estimated || c.total), 0);
    const stateRent = counties.reduce((a, c) => a + c.with_rent, 0);
    const stateMort = counties.reduce((a, c) => a + c.with_mortgage, 0);
    const stateAssessed = counties.reduce((a, c) => a + c.with_assessed, 0);
    const stateTax = counties.reduce((a, c) => a + c.with_tax, 0);
    const countiesWithData = counties.filter(c => c.total > 0).length;

    stateBlocks += `
      <div class="state-block">
        <div class="state-header">
          <h2>${STATE_NAMES[state] || state}</h2>
          <div class="state-summary">
            <span class="big-num">${stateTotal.toLocaleString()}</span> properties |
            <span>${countiesWithData}</span>/${counties.length} counties |
            <span>${stateRent.toLocaleString()}</span> rent est |
            <span>${stateMort.toLocaleString()}</span> mortgage est
          </div>
        </div>
        <table>
          <thead>
            <tr>
              <th>County</th>
              <th>Properties</th>
              <th>Data Readiness</th>
              <th>Addresses</th>
              <th>Valuation</th>
              <th>Taxes</th>
              <th>Rent</th>
              <th>Liens</th>
              <th>MLS</th>
            </tr>
          </thead>
          <tbody>
            ${counties.sort((a, b) => b.total - a.total).map(c => {
              const est = c.estimated || c.total;
              const propPct = pct(c.total, est);
              const addrPct = pct(c.with_address, c.total);
              const taxPct = pct(c.with_tax, c.total);
              const assessedPct = pct(c.with_assessed, c.total);
              const rentPct = Math.min(100, pct(c.with_rent, c.total));
              const mortPct = Math.min(100, pct(c.with_mortgage, c.total));
              // Weighted readiness score — liens at 10% weight (realistic from actual filings)
              const MLS_SCANNED = new Set(["Fairfield_OH", "Denton_TX", "Tarrant_TX"]);
              const mlsScanned = MLS_SCANNED.has(`${c.county_name}_${c.state_code}`);
              const mlsPct = mlsScanned ? 100 : 0;
              const score = Math.round((addrPct * 0.20) + (assessedPct * 0.15) + (taxPct * 0.15) + (rentPct * 0.20) + (mortPct * 0.10) + (mlsPct * 0.20));
              // Floor check — liens floor at 25% (realistic for actual county recorder data)
              const meetsFloors = addrPct >= 80 && assessedPct >= 80 && taxPct >= 80 && rentPct >= 80 && mortPct >= 25 && mlsScanned;
              const readiness = meetsFloors ? score : Math.min(score, 79);
              const barColor = readiness >= 80 ? "#4ade80" : readiness >= 40 ? "#facc15" : "#f87171";
              const readyLabel = readiness >= 80 ? "API READY" : readiness >= 40 ? "PARTIAL" : "INCOMPLETE";
              const readyColor = readiness >= 80 ? "#4ade80" : readiness >= 40 ? "#facc15" : "#f87171";
              return `<tr>
                <td><strong>${c.county_name}</strong></td>
                <td>${c.total.toLocaleString()}</td>
                <td>
                  <div class="bar-bg"><div class="bar" style="width:${Math.min(readiness, 100)}%;background:${barColor}"></div></div>
                  <span class="pct" style="color:${readyColor}">${readiness}% ${readyLabel}</span>
                </td>
                <td><span class="pct ${addrPct >= 80 ? "good" : addrPct > 0 ? "warn" : ""}">${addrPct}%</span></td>
                <td><span class="pct ${assessedPct >= 80 ? "good" : assessedPct > 0 ? "warn" : ""}">${assessedPct}%</span></td>
                <td><span class="pct ${taxPct >= 80 ? "good" : taxPct > 0 ? "warn" : ""}">${taxPct}%</span></td>
                <td><span class="pct ${rentPct >= 80 ? "good" : rentPct > 0 ? "warn" : ""}">${rentPct}%</span></td>
                <td><span class="pct ${mortPct >= 50 ? "good" : mortPct > 0 ? "warn" : ""}">${mortPct}%</span></td>
                <td><span class="pct ${mlsPct >= 50 ? "good" : mlsPct > 0 ? "warn" : ""}">${mlsPct}%</span></td>
              </tr>`;
            }).join("")}
          </tbody>
        </table>
      </div>`;
  }

  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>MXRE Command Center</title>
<meta http-equiv="refresh" content="15">
<style>${SHARED_STYLES}
  .header .totals { margin-top:15px; display:flex; gap:40px; justify-content:center; }
  .header .totals .stat { text-align:center; }
  .header .totals .stat .num { font-size:32px; font-weight:700; color:#38bdf8; }
  .header .totals .stat .label { font-size:12px; color:#64748b; text-transform:uppercase; letter-spacing:1px; }
</style></head><body>
  <div class="header">
    <h1>MXRE COMMAND CENTER</h1>
    <div class="subtitle">Real-Time Data Coverage Dashboard</div>
    <div class="totals">
      <div class="stat"><div class="num">${stats.totals.properties.toLocaleString()}</div><div class="label">Total Properties</div></div>
      <div class="stat"><div class="num">${stats.totals.rent.toLocaleString()}</div><div class="label">Rent Estimates</div></div>
      <div class="stat"><div class="num">${stats.totals.mortgage_total.toLocaleString()}</div><div class="label">Lien Records</div></div>
      <div class="stat"><div class="num">${stats.totals.mortgage_with_amounts.toLocaleString()}</div><div class="label">With Amounts</div></div>
      <div class="stat"><div class="num">${stats.totals.mortgage.toLocaleString()}</div><div class="label">Linked to Properties</div></div>
      <div class="stat"><div class="num">${stats.counties.filter(c => c.total > 0).length}</div><div class="label">Active Counties</div></div>
    </div>
  </div>
  ${navBar("coverage")}
  ${stateBlocks}
  <div class="updated">Stats cache: ${cacheAge}s old (refreshes every 5 min) | Auto-refreshes page every 15s | DB: ${SUPABASE_URL}</div>
</body></html>`;
}

function renderPublishedHTML(stats: any): string {
  const cacheAge = Math.round((stats.cacheAge || 0) / 1000);

  // Compute readiness for each county — 6 dimensions
  // Counties where we've swept all ZIPs for MLS listings (on/off market is known)
  const MLS_SCANNED_COUNTIES = new Set([
    "Fairfield_OH", "Denton_TX", "Tarrant_TX", "Dallas_TX", "Harris_TX", "Wake_NC", "Columbia_FL",
    "Geauga_OH", "Paulding_OH", "Wyandot_OH", "Linn_IA", "Scott_IA", "Saline_AR",
    "Belknap_NH", "Carroll_NH", "Cheshire_NH", "Grafton_NH", "Hillsborough_NH", "Rockingham_NH", "Strafford_NH", "Sullivan_NH",
  ]);

  // Minimum floors for API Ready
  // Liens floor is 25% (realistic: ~65% of homes have mortgages, many properties are exempt/commercial/paid-off)
  // MLS scanned flag confirms we've checked listing status for the county
  const FLOORS = { addr: 80, value: 80, tax: 80, rent: 80, liens: 25, mls: true };

  function computeReadiness(c: CountyStats) {
    const addrPct = pct(c.with_address, c.total);
    const assessedPct = pct(c.with_assessed, c.total);
    const taxPct = pct(c.with_tax, c.total);
    const rentPct = Math.min(100, pct(c.with_rent, c.total));
    const mortPct = Math.min(100, pct(c.with_mortgage, c.total));
    const mlsScanned = MLS_SCANNED_COUNTIES.has(`${c.county_name}_${c.state_code}`);
    const mlsPct = mlsScanned ? 100 : 0;

    // Weighted score — liens weight reduced since 100% is unrealistic from actual filings
    // (many properties are cash purchases, paid-off, exempt, or commercial with no mortgage)
    const score = Math.round((addrPct * 0.20) + (assessedPct * 0.15) + (taxPct * 0.15) + (rentPct * 0.20) + (mortPct * 0.10) + (mlsPct * 0.20));

    // Floor check — all dimensions must meet minimums for API Ready
    const meetsFloors = addrPct >= FLOORS.addr && assessedPct >= FLOORS.value && taxPct >= FLOORS.tax && rentPct >= FLOORS.rent && mortPct >= FLOORS.liens && mlsScanned;

    // Readiness = score, but capped at 79 if floors not met
    const readiness = meetsFloors ? score : Math.min(score, 79);

    return { ...c, readiness, addrPct, assessedPct, taxPct, rentPct, mortPct, mlsPct, meetsFloors };
  }

  const allScored = stats.counties.map(computeReadiness);
  const readyCounties = allScored.filter(c => c.readiness >= 80).sort((a, b) => b.readiness - a.readiness);
  const nearReady = allScored.filter(c => c.readiness >= 60 && c.readiness < 80).sort((a, b) => b.readiness - a.readiness);
  const pipeline = allScored.filter(c => c.readiness >= 30 && c.readiness < 60).sort((a, b) => b.readiness - a.readiness);
  const earlyStage = allScored.filter(c => c.readiness >= 10 && c.readiness < 30 && c.total > 5000).sort((a, b) => b.readiness - a.readiness);

  const totalReady = readyCounties.reduce((a, c) => a + c.total, 0);
  const totalPipeline = nearReady.reduce((a, c) => a + c.total, 0) + pipeline.reduce((a, c) => a + c.total, 0);

  function gapTags(c: any): string {
    const gaps: string[] = [];
    if (c.addrPct < 80) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">ADDR</span>');
    if (c.assessedPct < 80) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">VALUE</span>');
    if (c.taxPct < 50) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">TAX</span>');
    if (c.rentPct < 50) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">RENT</span>');
    if (c.mortPct < 10) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">LIENS</span>');
    if (c.mlsPct < 10) gaps.push('<span style="background:#7f1d1d;padding:2px 6px;border-radius:4px;font-size:10px">MLS</span>');
    return gaps.length > 0 ? gaps.join(" ") : '<span style="color:#4ade80;font-size:11px">COMPLETE</span>';
  }

  function tierRow(c: any, highlight: boolean = false): string {
    const barColor = c.readiness >= 80 ? "#4ade80" : c.readiness >= 60 ? "#38bdf8" : c.readiness >= 30 ? "#facc15" : "#f87171";
    return `<tr${highlight ? ' style="background:#1a2332"' : ""}>
      <td><strong>${c.county_name}</strong></td>
      <td>${c.state_code}</td>
      <td>${c.total.toLocaleString()}</td>
      <td>
        <div class="bar-bg" style="width:100px"><div class="bar" style="width:${c.readiness}%;background:${barColor}"></div></div>
        <span class="pct" style="color:${barColor}">${c.readiness}%</span>
      </td>
      <td><span class="pct ${c.addrPct >= 80 ? "good" : c.addrPct > 0 ? "warn" : ""}">${c.addrPct}%</span></td>
      <td><span class="pct ${c.assessedPct >= 80 ? "good" : c.assessedPct > 0 ? "warn" : ""}">${c.assessedPct}%</span></td>
      <td><span class="pct ${c.taxPct >= 80 ? "good" : c.taxPct > 0 ? "warn" : ""}">${c.taxPct}%</span></td>
      <td><span class="pct ${c.rentPct >= 80 ? "good" : c.rentPct > 0 ? "warn" : ""}">${c.rentPct}%</span></td>
      <td><span class="pct ${c.mortPct >= 50 ? "good" : c.mortPct > 0 ? "warn" : ""}">${c.mortPct}%</span></td>
      <td><span class="pct ${c.mlsPct >= 50 ? "good" : c.mlsPct > 0 ? "warn" : ""}">${c.mlsPct}%</span></td>
      <td>${gapTags(c)}</td>
    </tr>`;
  }

  const thdr = `<thead><tr><th>County</th><th>State</th><th>Properties</th><th>Readiness</th><th>Addr</th><th>Value</th><th>Tax</th><th>Rent</th><th>Liens</th><th>MLS</th><th>Gaps</th></tr></thead>`;

  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>MXRE — Published Coverage</title>
<meta http-equiv="refresh" content="15">
<style>${SHARED_STYLES}
  .header .totals { margin-top:15px; display:flex; gap:30px; justify-content:center; flex-wrap:wrap; }
  .header .totals .stat { text-align:center; }
  .header .totals .stat .num { font-size:28px; font-weight:700; }
  .header .totals .stat .label { font-size:11px; color:#64748b; text-transform:uppercase; letter-spacing:1px; }
  .section { margin:20px 0; background:#1e293b; border-radius:12px; overflow:hidden; }
  .section-header { padding:12px 20px; background:#334155; display:flex; justify-content:space-between; align-items:center; }
  .section-header h2 { font-size:16px; }
  .section-header .count { color:#94a3b8; font-size:13px; }
  .empty-state { padding:30px; text-align:center; color:#64748b; }
  .empty-state h3 { color:#f87171; font-size:18px; margin-bottom:8px; }
  .bar-bg { display:inline-block; width:100px; height:8px; background:#334155; border-radius:4px; vertical-align:middle; margin-right:6px; }
  .bar { height:100%; border-radius:4px; transition: width 0.5s; }
</style></head><body>
  <div class="header">
    <h1>MXRE COVERAGE PIPELINE</h1>
    <div class="subtitle">Real-time progress toward API-ready counties</div>
    <div class="totals">
      <div class="stat"><div class="num" style="color:#4ade80">${readyCounties.length}</div><div class="label">API Ready</div></div>
      <div class="stat"><div class="num" style="color:#38bdf8">${nearReady.length}</div><div class="label">Near Ready 60-79%</div></div>
      <div class="stat"><div class="num" style="color:#facc15">${pipeline.length}</div><div class="label">Pipeline 30-59%</div></div>
      <div class="stat"><div class="num" style="color:#f87171">${earlyStage.length}</div><div class="label">Early Stage</div></div>
      <div class="stat"><div class="num" style="color:#4ade80">${totalReady.toLocaleString()}</div><div class="label">Ready Properties</div></div>
      <div class="stat"><div class="num" style="color:#38bdf8">${totalPipeline.toLocaleString()}</div><div class="label">In Pipeline</div></div>
    </div>
  </div>
  ${navBar("published")}

  <div class="section">
    <div class="section-header">
      <h2 style="color:#4ade80">API-Ready (80%+)</h2>
      <span class="count">${readyCounties.length} counties | ${totalReady.toLocaleString()} properties</span>
    </div>
    ${readyCounties.length > 0 ? `<table>${thdr}<tbody>${readyCounties.map(c => tierRow(c, true)).join("")}</tbody></table>` : `<div class="empty-state">
      <h3>No Counties Ready Yet</h3>
      <p>Driving Fairfield OH, Tarrant TX, Denton TX toward 80%</p>
    </div>`}
  </div>

  <div class="section">
    <div class="section-header">
      <h2 style="color:#38bdf8">Near Ready (60-79%)</h2>
      <span class="count">${nearReady.length} counties</span>
    </div>
    ${nearReady.length > 0 ? `<table>${thdr}<tbody>${nearReady.slice(0, 30).map(c => tierRow(c)).join("")}</tbody></table>` : `<div class="empty-state"><p>No counties in this tier yet</p></div>`}
  </div>

  <div class="section">
    <div class="section-header">
      <h2 style="color:#facc15">Pipeline (30-59%)</h2>
      <span class="count">${pipeline.length} counties</span>
    </div>
    ${pipeline.length > 0 ? `<table>${thdr}<tbody>${pipeline.slice(0, 40).map(c => tierRow(c)).join("")}</tbody></table>` : `<div class="empty-state"><p>No counties in this tier yet</p></div>`}
  </div>

  ${earlyStage.length > 0 ? `<div class="section">
    <div class="section-header">
      <h2 style="color:#f87171">Early Stage (10-29%, 5K+ properties)</h2>
      <span class="count">${earlyStage.length} counties</span>
    </div>
    <table>${thdr}<tbody>${earlyStage.slice(0, 30).map(c => tierRow(c)).join("")}</tbody></table>
  </div>` : ""}

  <div class="updated">Stats cache: ${cacheAge}s old (refreshes every 5 min) | Auto-refreshes page every 15s</div>
</body></html>`;
}

function renderPropertiesHTML(): string {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>MXRE Command Center — Properties</title>
<style>${SHARED_STYLES}
  .search-bar { display:flex; gap:10px; margin-bottom:20px; flex-wrap:wrap; }
  .search-bar input, .search-bar select { background:#1e293b; border:1px solid #334155; color:#e2e8f0; padding:10px 14px; border-radius:8px; font-size:14px; outline:none; }
  .search-bar input:focus, .search-bar select:focus { border-color:#38bdf8; }
  .search-bar input { flex:1; min-width:200px; }
  .search-bar select { min-width:140px; }
  .search-bar button { background:#38bdf8; color:#0f172a; border:none; padding:10px 20px; border-radius:8px; font-weight:700; cursor:pointer; font-size:14px; }
  .search-bar button:hover { background:#7dd3fc; }
  .prop-table { width:100%; border-collapse:collapse; background:#1e293b; border-radius:12px; overflow:hidden; }
  .prop-table th { text-align:left; padding:12px 12px; font-size:11px; text-transform:uppercase; letter-spacing:1px; color:#64748b; border-bottom:1px solid #334155; background:#334155; }
  .prop-table td { padding:8px 12px; border-bottom:1px solid #0f172a; font-size:13px; }
  .prop-table tr.prop-row { cursor:pointer; }
  .prop-table tr.prop-row:hover { background:#334155; }
  .detail-row { display:none; }
  .detail-row.open { display:table-row; }
  .detail-cell { padding:16px 20px; background:#0f172a; }
  .detail-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:10px; margin-bottom:16px; }
  .detail-field { font-size:12px; }
  .detail-field .lbl { color:#64748b; text-transform:uppercase; letter-spacing:0.5px; font-size:10px; }
  .detail-field .val { color:#e2e8f0; font-weight:600; margin-top:2px; }
  .linked-section { margin-top:12px; }
  .linked-section h4 { color:#38bdf8; font-size:13px; margin-bottom:8px; }
  .linked-table { width:100%; border-collapse:collapse; margin-bottom:12px; }
  .linked-table th { text-align:left; padding:6px 10px; font-size:10px; text-transform:uppercase; color:#64748b; border-bottom:1px solid #334155; }
  .linked-table td { padding:6px 10px; font-size:12px; border-bottom:1px solid #1e293b; }
  .pagination { display:flex; justify-content:center; align-items:center; gap:12px; margin-top:20px; }
  .pagination button { background:#334155; color:#e2e8f0; border:none; padding:8px 16px; border-radius:6px; cursor:pointer; font-size:13px; }
  .pagination button:disabled { opacity:0.4; cursor:default; }
  .pagination button:not(:disabled):hover { background:#475569; }
  .pagination span { color:#94a3b8; font-size:13px; }
  .loading { text-align:center; padding:40px; color:#64748b; }
  .no-results { text-align:center; padding:40px; color:#64748b; font-size:15px; }
</style></head><body>
  <div class="header">
    <h1>MXRE COMMAND CENTER</h1>
    <div class="subtitle">Property Explorer</div>
  </div>
  ${navBar("properties")}

  <div class="search-bar">
    <input type="text" id="searchInput" placeholder="Full address (e.g. 1342 Evalie Dr, Fairfield, OH 45014)" style="flex:2"/>
    <select id="stateFilter" onchange="loadCounties()">
      <option value="">— State (optional) —</option>
      ${ALL_STATES.map(s => `<option value="${s}">${s} — ${STATE_NAMES[s]}</option>`).join("")}
    </select>
    <select id="countyFilter">
      <option value="">— County (optional) —</option>
    </select>
    <button onclick="doSearch(1)">Search</button>
  </div>
  <div style="color:#64748b;font-size:12px;margin:-10px 0 16px;text-align:center">Full addresses auto-detected · State + county optional for browsing</div>

  <div id="results">
    <div class="loading">Select a state and county to browse, or enter an address above.</div>
  </div>
  <div id="pagination"></div>

<script>
let currentPage = 1;
let totalCount = 0;
const PER_PAGE = 50;

document.getElementById('searchInput').addEventListener('keydown', function(e) {
  if (e.key === 'Enter') doSearch(1);
});

async function loadCounties() {
  const state = document.getElementById('stateFilter').value;
  const sel = document.getElementById('countyFilter');
  sel.innerHTML = '<option value="">— All Counties —</option>';
  if (!state) return;
  try {
    const r = await fetch('/api/counties?state=' + state);
    const d = await r.json();
    for (const c of (d.counties || [])) {
      const opt = document.createElement('option');
      opt.value = c.county_name;
      opt.textContent = c.county_name;
      sel.appendChild(opt);
    }
  } catch(e) {}
}

async function doSearch(page) {
  currentPage = page;
  const q = document.getElementById('searchInput').value.trim();
  let state = document.getElementById('stateFilter').value;
  let county = document.getElementById('countyFilter').value;

  // Auto-detect state from a full typed address like "531 Summitview Dr, Lancaster, OH 43130"
  if (q.includes(',')) {
    const parts = q.split(',').map(p => p.trim());
    let hasZip = false;
    for (const part of parts.slice(1)) {
      const stateMatch = part.match(/\b([A-Z]{2})\b/);
      const zipMatch = part.match(/\b\d{5}\b/);
      if (zipMatch) hasZip = true;
      if (stateMatch && !state) {
        state = stateMatch[1];
        document.getElementById('stateFilter').value = state;
      }
    }
    // If address has a zip, don't send county — zip is more precise and city ≠ county
    if (hasZip) county = '';
  }

  const params = new URLSearchParams();
  if (q) params.set('q', q);
  if (state) params.set('state', state);
  if (county) params.set('county', county);
  params.set('page', String(page));

  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '<div class="loading">Loading...</div>';
  document.getElementById('pagination').innerHTML = '';

  try {
    const resp = await fetch('/api/properties?' + params.toString());
    const data = await resp.json();
    if (data.error) { resultsDiv.innerHTML = '<div class="no-results">Error: ' + data.error + '</div>'; return; }
    totalCount = data.total;
    if (!data.properties || data.properties.length === 0) {
      resultsDiv.innerHTML = '<div class="no-results">No properties found.</div>';
      return;
    }
    renderTable(data.properties);
    renderPagination(data.total, page);
  } catch(e) {
    resultsDiv.innerHTML = '<div class="no-results">Request failed: ' + e.message + '</div>';
  }
}

function esc(s) { if (s == null) return '—'; const d = document.createElement('div'); d.textContent = String(s); return d.innerHTML; }
function fmt(n) { return n != null ? Number(n).toLocaleString() : '—'; }
function money(n) { return n != null ? '$' + Number(n).toLocaleString() : '—'; }

function renderTable(props) {
  let html = '<table class="prop-table"><thead><tr>' +
    '<th>Address</th><th>City</th><th>St</th><th>Zip</th><th>County</th><th>Type</th>' +
    '<th>Assessed</th><th>Market</th><th>Last Sale</th><th>Yr Built</th><th>SqFt</th><th>Owner</th><th>Linked Data</th>' +
    '</tr></thead><tbody>';
  for (const p of props) {
    html += '<tr class="prop-row" onclick="toggleDetail(this, ' + p.id + ')">' +
      '<td>' + esc(p.address) + '</td>' +
      '<td>' + esc(p.city) + '</td>' +
      '<td>' + esc(p.state) + '</td>' +
      '<td>' + esc(p.zip) + '</td>' +
      '<td>' + esc(p.county_name) + '</td>' +
      '<td>' + esc(p.property_type) + '</td>' +
      '<td>' + money(p.assessed_value) + '</td>' +
      '<td>' + money(p.market_value) + '</td>' +
      '<td>' + money(p.last_sale_price) + '</td>' +
      '<td>' + esc(p.year_built) + '</td>' +
      '<td>' + fmt(p.total_sqft) + '</td>' +
      '<td>' + esc(p.owner_name) + '</td>' +
      '<td>' +
        (p.rent_count > 0 ? '<span class="badge badge-rent">' + p.rent_count + ' rent</span>' : '') +
        (p.mortgage_count > 0 ? '<span class="badge badge-mort">' + p.mortgage_count + ' lien</span>' : '') +
        (p.listing_count > 0 ? '<span class="badge badge-listing">' + p.listing_count + ' listing</span>' : '') +
      '</td></tr>';
    html += '<tr class="detail-row" id="detail-' + p.id + '"><td colspan="13" class="detail-cell"><div class="loading">Loading details...</div></td></tr>';
  }
  html += '</tbody></table>';
  document.getElementById('results').innerHTML = html;
}

function renderPagination(total, page) {
  const totalPages = Math.ceil(total / PER_PAGE);
  if (totalPages <= 1) { document.getElementById('pagination').innerHTML = ''; return; }
  let html = '<div class="pagination">';
  html += '<button ' + (page <= 1 ? 'disabled' : 'onclick="doSearch(' + (page-1) + ')"') + '>&laquo; Prev</button>';
  html += '<span>Page ' + page + ' of ' + totalPages + ' (' + total.toLocaleString() + ' results)</span>';
  html += '<button ' + (page >= totalPages ? 'disabled' : 'onclick="doSearch(' + (page+1) + ')"') + '>Next &raquo;</button>';
  html += '</div>';
  document.getElementById('pagination').innerHTML = html;
}

async function toggleDetail(row, propId) {
  const detailRow = document.getElementById('detail-' + propId);
  if (detailRow.classList.contains('open')) {
    detailRow.classList.remove('open');
    return;
  }
  // Close all other open details
  document.querySelectorAll('.detail-row.open').forEach(r => r.classList.remove('open'));
  detailRow.classList.add('open');
  detailRow.querySelector('.detail-cell').innerHTML = '<div class="loading">Loading details...</div>';

  try {
    const resp = await fetch('/api/property/' + propId);
    const data = await resp.json();
    if (data.error) { detailRow.querySelector('.detail-cell').innerHTML = '<div class="no-results">' + data.error + '</div>'; return; }
    renderDetail(detailRow.querySelector('.detail-cell'), data);
  } catch(e) {
    detailRow.querySelector('.detail-cell').innerHTML = '<div class="no-results">Failed to load: ' + e.message + '</div>';
  }
}

function sectionHeader(title) {
  return '<div style="color:#38bdf8;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px;border-bottom:1px solid #1e293b;padding-bottom:4px;">' + title + '</div>';
}
function field(lbl, val) {
  if (val == null || val === '' || val === '—') return '';
  return '<div class="detail-field"><div class="lbl">' + lbl + '</div><div class="val">' + esc(val) + '</div></div>';
}
function boolBadge(val, trueLabel, falseLabel) {
  if (val == null) return '—';
  return val
    ? '<span style="color:#4ade80;font-weight:700;">' + (trueLabel||'Yes') + '</span>'
    : '<span style="color:#94a3b8;">' + (falseLabel||'No') + '</span>';
}

function renderDetail(cell, data) {
  const p = data.property;
  const eq = data.equity || {};
  const demo = data.demographics || {};

  // ── On-Market Banner ───────────────────────────────────────────────────
  // Normalize listing_signals columns (actual schema uses mls_list_price, is_on_market, etc.)
  function normListing(l) {
    return {
      list_price: l.mls_list_price ?? l.list_price ?? null,
      is_active: l.is_on_market ?? (l.listing_status && l.listing_status.toLowerCase().includes('active')) ?? false,
      status: l.raw?.status ?? l.listing_status ?? (l.is_on_market ? 'Active' : 'Off Market'),
      agent_name: l.listing_agent_name ?? l.agent_name ?? null,
      agent_phone: l.listing_agent_phone ?? l.agent_phone ?? null,
      brokerage: l.listing_brokerage ?? l.brokerage ?? null,
      source: l.listing_source ?? l.source ?? null,
      listing_url: l.listing_url ?? null,
      days_on_market: l.days_on_market ?? null,
      date: l.first_seen_at ?? l.snapshot_date ?? l.created_at ?? null,
    };
  }

  let html = '';
  const allListings = (data.listing_signals || []).map(normListing);
  const activeListings = allListings.filter(l => l.is_active);

  if (activeListings.length > 0) {
    const l = activeListings[0];
    html += '<div style="background:#064e3b;border:1px solid #059669;border-radius:6px;padding:12px 16px;margin-bottom:14px;display:flex;align-items:center;flex-wrap:wrap;gap:16px;">' +
      '<span style="color:#34d399;font-size:15px;font-weight:700;">🟢 ON MARKET</span>' +
      '<span style="color:#6ee7b7;font-size:14px;font-weight:600;">' + money(l.list_price) + '</span>' +
      (l.days_on_market != null ? '<span style="color:#a7f3d0;font-size:12px;">' + l.days_on_market + ' days on market</span>' : '') +
      (l.listing_url ? '<a href="' + esc(l.listing_url) + '" target="_blank" style="color:#6ee7b7;font-size:12px;">View Listing ↗</a>' : '') +
      (l.agent_name ? '<span style="color:#a7f3d0;font-size:12px;">Agent: ' + esc(l.agent_name) + (l.agent_phone ? ' · ' + esc(l.agent_phone) : '') + '</span>' : '') +
      (l.brokerage ? '<span style="color:#6b7280;font-size:11px;">' + esc(l.brokerage) + '</span>' : '') +
      (l.source ? '<span style="color:#6b7280;font-size:11px;">' + esc(l.source) + '</span>' : '') +
      '</div>';
  } else if (allListings.length > 0) {
    const l = allListings[0];
    html += '<div style="background:#1c1917;border:1px solid #44403c;border-radius:6px;padding:10px 16px;margin-bottom:14px;display:flex;align-items:center;flex-wrap:wrap;gap:16px;">' +
      '<span style="color:#a8a29e;font-size:13px;font-weight:600;">OFF MARKET</span>' +
      '<span style="color:#78716c;font-size:12px;">Last listed: ' + money(l.list_price) + ' (' + esc(l.status) + ')</span>' +
      (l.date ? '<span style="color:#57534e;font-size:11px;">' + esc(l.date?.slice(0,10)) + '</span>' : '') +
      '</div>';
  }

  // ── Equity Summary Bar ─────────────────────────────────────────────────
  if (eq.equityPercent != null) {
    const epct = Math.max(0, Math.min(100, eq.equityPercent));
    const ecolor = epct >= 60 ? '#4ade80' : epct >= 30 ? '#facc15' : '#f87171';
    html += '<div style="background:#0f172a;border:1px solid #334155;border-radius:6px;padding:10px 16px;margin-bottom:14px;display:flex;align-items:center;gap:20px;">' +
      '<span style="color:#94a3b8;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:1px;">Equity</span>' +
      '<div style="flex:1;background:#1e293b;border-radius:4px;height:8px;"><div style="width:' + epct + '%;background:' + ecolor + ';height:100%;border-radius:4px;"></div></div>' +
      '<span style="color:' + ecolor + ';font-weight:700;font-size:15px;">' + eq.equityPercent + '%</span>' +
      (eq.freeClear ? '<span style="color:#4ade80;font-size:11px;font-weight:600;">FREE & CLEAR</span>' : '') +
      (eq.openBalance > 0 ? '<span style="color:#94a3b8;font-size:12px;">Balance: ' + money(eq.openBalance) + '</span>' : '') +
      '</div>';
  }

  // ── Location & ID ─────────────────────────────────────────────────────
  html += sectionHeader('📍 Location & Identity');
  html += '<div class="detail-grid">';
  html += field('Address', p.address);
  html += field('City', p.city);
  html += field('State', p.state || p.state_code);
  html += field('Zip', p.zip);
  html += field('County', p.county_name);
  html += field('Parcel ID', p.parcel_id || p.apn_formatted);
  html += field('Lat / Lng', (p.lat && p.lng) ? p.lat.toFixed(5) + ', ' + p.lng.toFixed(5) : null);
  html += field('Census Tract', p.census_tract);
  html += field('Subdivision', p.subdivision);
  html += field('Legal Description', p.legal_description);
  html += '</div>';

  // ── Ownership ─────────────────────────────────────────────────────────
  html += sectionHeader('👤 Ownership');
  html += '<div class="detail-grid">';
  html += field('Owner 1', p.owner_name);
  html += field('Company', p.company_name);
  html += field('Owner-Occupied', boolBadge(p.owner_occupied, 'Yes — Owner Occupied', 'No'));
  html += field('Absentee Owner', boolBadge(p.absentee_owner, 'Yes — Absentee', 'No'));
  html += field('Out-of-State Absentee', (p.absentee_owner && !p.in_state_absentee) ? '<span style="color:#f87171;font-weight:700;">Yes</span>' : null);
  html += field('Corporate Owned', boolBadge(p.corporate_owned, 'Yes — Corporate', 'No'));
  html += field('Ownership Since', p.ownership_start_date);
  html += field('Ownership Length', p.ownership_length_months ? p.ownership_length_months + ' months (' + Math.round(p.ownership_length_months/12*10)/10 + ' yrs)' : null);
  if (p.mail_address) {
    html += field('Mailing Address', [p.mail_address, p.mail_city, p.mail_state, p.mail_zip].filter(Boolean).join(', '));
  }
  html += '</div>';

  // ── Property Details ───────────────────────────────────────────────────
  html += sectionHeader('🏠 Property Details');
  html += '<div class="detail-grid">';
  html += field('Type', p.property_type || p.property_use);
  html += field('Use / Land Use', p.land_use);
  html += field('Zoning', p.zoning);
  html += field('Year Built', p.year_built);
  html += field('Year Remodeled', p.year_remodeled);
  html += field('Stories', p.stories);
  html += field('Total SqFt', p.total_sqft ? fmt(p.total_sqft) : null);
  html += field('Living SqFt', p.living_sqft ? fmt(p.living_sqft) : null);
  html += field('Lot SqFt', p.lot_sqft ? fmt(p.lot_sqft) : null);
  html += field('Lot Acres', p.lot_acres);
  html += field('Bedrooms', p.bedrooms);
  html += field('Bathrooms', p.bathrooms_full != null ? p.bathrooms_full + (p.bathrooms_half ? '.' + p.bathrooms_half : '') : p.bathrooms);
  html += field('Total Rooms', p.total_rooms);
  html += field('Basement', p.basement ? (p.basement + (p.basement_sqft ? ' (' + fmt(p.basement_sqft) + ' sqft)' : '')) : null);
  html += field('Garage', p.garage ? (p.garage + (p.garage_spaces ? ' · ' + p.garage_spaces + ' spaces' : '') + (p.garage_sqft ? ' · ' + fmt(p.garage_sqft) + ' sqft' : '')) : null);
  html += field('Heating', p.heating ? (p.heating + (p.fuel_type ? ' / ' + p.fuel_type : '')) : null);
  html += field('A/C', p.air_conditioning);
  html += field('Exterior Walls', p.exterior_walls);
  html += field('Roof Type', p.roof_type);
  html += field('Foundation', p.foundation);
  html += field('Condition', p.condition);
  html += field('Fireplace', p.fireplace ? (p.fireplace_count ? p.fireplace_count + ' fireplace(s)' : 'Yes') : null);
  html += field('Pool', boolBadge(p.pool));
  html += field('Deck', p.deck ? ('Yes' + (p.deck_sqft ? ' (' + fmt(p.deck_sqft) + ' sqft)' : '')) : null);
  html += field('Porch', boolBadge(p.porch));
  html += field('HOA', p.hoa ? ('Yes' + (p.hoa_amount ? ' — ' + money(p.hoa_amount) + '/mo' : '')) : null);
  html += field('Flood Zone', p.flood_zone ? (p.flood_zone_type || 'Yes') : null);
  html += '</div>';

  // ── Valuation ──────────────────────────────────────────────────────────
  html += sectionHeader('💰 Valuation');
  html += '<div class="detail-grid">';
  const sqft = p.living_sqft || p.total_sqft;
  const ppsf = (p.market_value && sqft) ? Math.round(p.market_value / sqft) : null;
  html += field('Market Value', money(p.market_value));
  html += field('Assessed Value', money(p.assessed_value));
  html += field('Taxable Value', money(p.taxable_value));
  html += field('Estimated Value (AVM)', money(p.estimated_value));
  html += field('Appraised Land', money(p.appraised_land));
  html += field('Appraised Building', money(p.appraised_building));
  html += field('Price per SqFt', ppsf ? '$' + ppsf : null);
  html += field('Annual Tax', money(p.annual_tax || p.property_tax));
  html += field('Tax Year', p.tax_year || p.assessment_year);
  if (p.tax_delinquent_year) {
    html += '<div class="detail-field"><div class="lbl">Tax Delinquent</div><div class="val"><span style="color:#f87171;font-weight:700;">⚠ Since ' + esc(p.tax_delinquent_year) + '</span></div></div>';
  }
  html += field('Last Sale Price', money(p.last_sale_price));
  html += field('Last Sale Date', p.last_sale_date);
  html += '</div>';

  // ── Liens / Mortgages ──────────────────────────────────────────────────
  if (data.mortgage_records && data.mortgage_records.length > 0) {
    html += sectionHeader('🏦 Liens & Mortgages (' + data.mortgage_records.length + ')');
    html += '<table class="linked-table"><thead><tr><th>Type</th><th>Original Amount</th><th>Rate</th><th>Est. Payment</th><th>Est. Balance</th><th>Maturity</th><th>Borrower</th><th>Lender</th><th>Recorded</th><th>Instrument #</th><th>Source</th></tr></thead><tbody>';
    for (const m of data.mortgage_records) {
      const rateLabel = m.interest_rate ? (m.interest_rate_type === 'estimated' ? '~' : '') + m.interest_rate.toFixed(2) + '%' : '—';
      const isEstimated = m.interest_rate_type === 'estimated' || (m.source_url && m.source_url.startsWith('assessor-sale'));
      html += '<tr' + (isEstimated ? ' style="opacity:0.75"' : '') + '>' +
        '<td>' + esc(m.document_type) + (isEstimated ? ' <span style="color:#64748b;font-size:10px;">(est)</span>' : '') + '</td>' +
        '<td>' + money(m.original_amount || m.loan_amount) + '</td>' +
        '<td>' + rateLabel + '</td>' +
        '<td>' + money(m.estimated_monthly_payment) + '</td>' +
        '<td>' + money(m.estimated_current_balance) + '</td>' +
        '<td>' + esc(m.maturity_date ? m.maturity_date.slice(0,7) : null) + '</td>' +
        '<td>' + esc(m.borrower_name) + '</td>' +
        '<td>' + esc(m.lender_name) + '</td>' +
        '<td>' + esc(m.recording_date) + '</td>' +
        '<td>' + esc(m.document_number) + '</td>' +
        '<td style="color:#64748b;font-size:11px;">' + esc(m.source_url) + '</td>' +
        '</tr>';
    }
    html += '</tbody></table>';
  }

  // ── Rent ───────────────────────────────────────────────────────────────
  const hasRentSnapshots = data.rent_snapshots && data.rent_snapshots.length > 0;
  const hasFmr = demo && (demo.fmr_1 || demo.fmr_2);
  if (hasRentSnapshots || hasFmr) {
    html += sectionHeader('🏘 Rent Data');
    if (hasRentSnapshots) {
      html += '<table class="linked-table"><thead><tr><th>Source</th><th>Rent Est</th><th>Rent Low</th><th>Rent High</th><th>Date</th></tr></thead><tbody>';
      for (const r of data.rent_snapshots) {
        html += '<tr><td>' + esc(r.source) + '</td><td>' + money(r.rent_estimate) + '</td><td>' + money(r.rent_range_low) + '</td><td>' + money(r.rent_range_high) + '</td><td>' + esc(r.snapshot_date) + '</td></tr>';
      }
      html += '</tbody></table>';
    }
    if (hasFmr) {
      html += '<div style="margin-top:8px;"><span style="color:#64748b;font-size:11px;font-weight:600;text-transform:uppercase;">HUD Fair Market Rents</span>';
      if (demo.hud_area_name) html += ' <span style="color:#475569;font-size:11px;">(' + esc(demo.hud_area_name) + (demo.fmr_year ? ' · ' + esc(demo.fmr_year) : '') + ')</span>';
      html += '</div>';
      html += '<div class="detail-grid" style="margin-top:6px;">';
      html += field('Studio', money(demo.fmr_0));
      html += field('1 BR', money(demo.fmr_1));
      html += field('2 BR', money(demo.fmr_2));
      html += field('3 BR', money(demo.fmr_3));
      html += field('4 BR', money(demo.fmr_4));
      if (sqft && demo.fmr_2) html += field('Rent/SqFt (2BR)', '$' + Math.round(demo.fmr_2 / sqft * 100) / 100);
      if (demo.median_income) html += field('Area Median Income', money(demo.median_income));
      html += '</div>';
    }
  }

  // ── Listing History ────────────────────────────────────────────────────
  if (allListings.length > 0) {
    html += sectionHeader('📋 Listing History (' + allListings.length + ')');
    html += '<table class="linked-table"><thead><tr><th>Source</th><th>Status</th><th>Price</th><th>DOM</th><th>Agent</th><th>Brokerage</th><th>Date</th><th>Link</th></tr></thead><tbody>';
    for (const l of allListings) {
      html += '<tr>' +
        '<td>' + esc(l.source) + '</td>' +
        '<td><span style="color:' + (l.is_active ? '#4ade80' : '#94a3b8') + '">' + esc(l.status) + '</span></td>' +
        '<td>' + money(l.list_price) + '</td>' +
        '<td>' + (l.days_on_market != null ? l.days_on_market + 'd' : '—') + '</td>' +
        '<td>' + esc(l.agent_name) + '</td>' +
        '<td>' + esc(l.brokerage) + '</td>' +
        '<td>' + esc(l.date?.slice(0,10)) + '</td>' +
        '<td>' + (l.listing_url ? '<a href="' + esc(l.listing_url) + '" target="_blank" style="color:#38bdf8;">↗</a>' : '—') + '</td>' +
        '</tr>';
    }
    html += '</tbody></table>';
  }

  if (!data.mortgage_records?.length && !hasRentSnapshots && !hasFmr && !allListings.length) {
    html += '<div style="color:#64748b;font-size:13px;margin-top:8px;">No linked records found.</div>';
  }

  cell.innerHTML = html;
}
</script>
</body></html>`;
}

// --- Property search API ---
interface PropertySearchParams {
  q?: string;
  state?: string;
  city?: string;
  county?: string;
  page?: number;
}

// County name → county_id cache (loaded once at startup)
const countyIdCache = new Map<string, number>(); // "NAME_STATE" → id

async function ensureCountyCache() {
  if (countyIdCache.size > 0) return;
  const { data } = await db.from("counties").select("id, county_name, state_code").eq("active", true);
  for (const c of data || []) {
    countyIdCache.set(`${c.county_name}_${c.state_code}`, c.id);
  }
}

async function searchProperties(params: PropertySearchParams) {
  await ensureCountyCache();

  const page = params.page || 1;
  const perPage = 50;
  const offset = (page - 1) * perPage;

  // Parse address components FIRST so we can decide geo strategy
  let addressTerm = params.q || "";
  let autoCity = "";
  let autoZip = "";
  let autoState = "";

  if (params.q && params.q.includes(",")) {
    // "1342 Evalie Dr, Fairfield, OH 45014" → parts
    const parts = params.q.split(",").map(p => p.trim());
    addressTerm = parts[0]; // street portion only
    for (const part of parts.slice(1)) {
      const p = part.trim();
      const zipMatch = p.match(/\b(\d{5})\b/);
      if (zipMatch) autoZip = zipMatch[1];
      const stateMatch = p.match(/\b([A-Z]{2})\b/);
      if (stateMatch) autoState = stateMatch[1];
      if (!zipMatch && !stateMatch && p.length > 1) autoCity = p;
    }
  }

  // Geo strategy: zip > county > state
  // When zip is present it is more precise than county — the city name in an address
  // (e.g. "Fairfield") is often the CITY, not the COUNTY, so never let county override zip.
  const resolvedState = autoState || params.state || "";
  let countyId: number | null = null;
  if (!autoZip && params.county && resolvedState) {
    countyId = countyIdCache.get(`${params.county}_${resolvedState.toUpperCase()}`) ?? null;
  }

  // No count: "exact" — too slow on 48M rows. Use limit+1 trick for hasMore.
  let query = db.from("properties").select(
    "*"
  );

  if (autoZip) {
    // Zip is the tightest geo filter — use it alone (state implied by zip)
    query = query.eq("zip", autoZip);
    if (resolvedState) query = query.eq("state_code", resolvedState.toUpperCase());
  } else if (countyId) {
    query = query.eq("county_id", countyId);
  } else if (resolvedState) {
    query = query.eq("state_code", resolvedState.toUpperCase());
  }

  if (params.city) {
    query = query.ilike("city", `%${params.city}%`);
  }
  if (params.q) {
    // Sanitize: remove chars that break PostgREST logic tree parsing
    const safe = (s: string) => s.replace(/[,()]/g, " ").trim();
    const addr = safe(addressTerm);
    if (addr) {
      query = query.ilike("address", `%${addr}%`);
    }
    // Only add city filter if no zip (city name in address is often not the DB city)
    if (autoCity && !autoZip) query = query.ilike("city", `%${safe(autoCity)}%`);
  }

  query = query.order("id", { ascending: true }).range(offset, offset + perPage - 1);

  const { data, error } = await query;
  if (error) throw new Error(error.message);

  // Resolve county names from cache
  const cidToName = new Map<number, string>();
  for (const [key, id] of countyIdCache) {
    const name = key.split("_")[0];
    if (!cidToName.has(id)) cidToName.set(id, name);
  }

  const props = (data || []).map((p: any) => ({
    ...p,
    county_name: cidToName.get(p.county_id) ?? "",
    state: p.state_code ?? "",
  }));

  // Note: linked counts (mortgage/rent) load in the detail view on click
  const enriched = props;

  return { properties: enriched, total: enriched.length, page, perPage, hasMore: enriched.length === perPage };
}

async function getPropertyBasic(id: number) {
  const { data: property, error } = await db.from("properties")
    .select(`
      id, address, city, zip, state_code, property_type, property_use, land_use,
      owner_name, company_name,
      assessed_value, market_value, taxable_value,
      last_sale_price, last_sale_date,
      property_tax, annual_tax, tax_year,
      year_built, bedrooms, bathrooms, total_sqft, lot_acres,
      lat, lng,
      counties(county_name, state_code)
    `)
    .eq("id", id).single();
  if (error) throw new Error(error.message);
  if (!property) throw new Error("Property not found");

  if (property.counties) {
    (property as any).county_name = (property.counties as any).county_name;
    (property as any).state = (property.counties as any).state_code;
  }

  if (property.ownership_start_date) {
    const months = (Date.now() - new Date(property.ownership_start_date as string).getTime()) / (1000 * 60 * 60 * 24 * 30.44);
    (property as any).ownership_length_months = Math.round(months);
  }

  return property;
}

async function getPropertySupplemental(id: number, property: any) {
  const [rentResult, mortResult, listingByIdResult, demoResult] = await Promise.all([
    db.from("rent_snapshots").select("*").eq("property_id", id).order("snapshot_date", { ascending: false }).limit(5),
    db.from("mortgage_records")
      .select("id, document_type, deed_type, recording_date, loan_amount, borrower_name, lender_name, document_number")
      .eq("property_id", id)
      .order("recording_date", { ascending: false })
      .limit(10),
    db.from("listing_signals").select("*").eq("property_id", id).order("first_seen_at", { ascending: false }).limit(5),
    property.zip
      ? db.from("zip_demographics").select("fmr_0,fmr_1,fmr_2,fmr_3,fmr_year").eq("zip", property.zip as string).single()
      : Promise.resolve({ data: null, error: null }),
  ]);

  let listingResult = listingByIdResult;
  if ((!listingResult.data || listingResult.data.length === 0) && property.address && property.state_code) {
    const addr = (property.address as string).replace(/[,()]/g, " ").trim();
    const { data: byAddr } = await db.from("listing_signals")
      .select("*")
      .ilike("address", addr)
      .eq("state_code", property.state_code.toUpperCase())
      .order("first_seen_at", { ascending: false })
      .limit(5);
    if (byAddr && byAddr.length > 0) {
      listingResult = { data: byAddr, error: null } as any;
    }
  }

  const seen = new Set<string>();
  const mortgage_records = (mortResult.data || []).filter((m: any) => {
    const key = m.document_number ? `doc:${m.document_number}` : `amt:${m.recording_date}:${m.loan_amount}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const marketValue = (property.market_value as number) ?? 0;
  let openBalance = 0;
  for (const m of mortgage_records) {
    if ((m as any).estimated_current_balance && (m as any).estimated_current_balance > 0) {
      openBalance += (m as any).estimated_current_balance;
    }
  }
  const equityPercent = marketValue > 0 ? Math.round(((marketValue - openBalance) / marketValue) * 100) : null;
  const freeClear = mortgage_records.length === 0 || openBalance === 0;

  return {
    rent_snapshots: rentResult.data || [],
    mortgage_records,
    listing_signals: listingResult.data || [],
    demographics: demoResult.data || null,
    equity: { openBalance, equityPercent, freeClear },
  };
}

async function getPropertyDetail(id: number) {
  const property = await getPropertyBasic(id);
  const supplemental = await getPropertySupplemental(id, property);
  return { property, ...supplemental };
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://localhost:${PORT}`);
  const pathname = url.pathname;

  if (pathname === "/" || pathname === "/index.html") {
    try {
      const stats = getStats();
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(renderHTML(stats));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end(`Error: ${(err as Error).message}`);
    }
  } else if (pathname === "/published") {
    try {
      const stats = getStats();
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(renderPublishedHTML(stats));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end(`Error: ${(err as Error).message}`);
    }
  } else if (pathname === "/api/counties") {
    try {
      const state = url.searchParams.get("state") || "";
      let q = db.from("counties").select("id, county_name, state_code").eq("active", true).order("county_name");
      if (state) q = q.eq("state_code", state.toUpperCase());
      const { data, error } = await q;
      if (error) throw new Error(error.message);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ counties: data ?? [] }));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname === "/api/properties") {
    try {
      const params: PropertySearchParams = {
        q: url.searchParams.get("q") || undefined,
        state: url.searchParams.get("state") || undefined,
        city: url.searchParams.get("city") || undefined,
        county: url.searchParams.get("county") || undefined,
        page: parseInt(url.searchParams.get("page") || "1", 10),
      };
      const result = await searchProperties(params);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname.match(/^\/api\/property\/\d+\/basic$/)) {
    try {
      const id = parseInt(pathname.split("/")[3] || "0", 10);
      if (!id) throw new Error("Invalid property ID");
      const property = await getPropertyBasic(id);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(property));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname.match(/^\/api\/property\/\d+\/supplemental$/)) {
    try {
      const id = parseInt(pathname.split("/")[3] || "0", 10);
      if (!id) throw new Error("Invalid property ID");
      const property = await getPropertyBasic(id);
      const supplemental = await getPropertySupplemental(id, property);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(supplemental));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname.startsWith("/api/property/")) {
    try {
      const id = parseInt(pathname.split("/").pop() || "0", 10);
      if (!id) throw new Error("Invalid property ID");
      const result = await getPropertyDetail(id);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname === "/api/stats") {
    // Return cached stats immediately (always responds, no blocking queries)
    const stats = getStats();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(stats));
  } else if (pathname === "/api/heartbeat") {
    // Ingest layers call this to signal they're active
    lastIngestTime = Date.now();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
  } else if (pathname === "/properties") {
    // Properties filter page
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(propertiesPageHTML());
  } else if (pathname.startsWith("/api/properties")) {
    // Get filtered properties with pagination
    try {
      const url = new URL(req.url!, `http://${req.headers.host}`);
      const state = url.searchParams.get("state") || "";
      const county = url.searchParams.get("county") || "";
      const limit = parseInt(url.searchParams.get("limit") || "50", 10);
      const offset = parseInt(url.searchParams.get("offset") || "0", 10);

      let query = db.from("properties").select("id, address, county_name, state_code, assessed_value, tax_amount", { count: "exact" });

      if (state) query = query.eq("state_code", state);
      if (county) query = query.eq("county_name", county);

      const { data, error, count } = await query.range(offset, offset + limit - 1);

      if (error) throw error;

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        properties: data || [],
        total: count || 0,
        offset,
        limit,
        hasMore: count ? (offset + limit < count) : false
      }));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else if (pathname.startsWith("/api/property/")) {
    // Get property details
    try {
      const id = pathname.split("/").pop();
      const { data, error } = await db.from("properties").select("*").eq("id", id).single();

      if (error) throw error;

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data || {}));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(PORT, () => {
  console.log(`\nMXRE Command Center running at http://localhost:${PORT}`);
  console.log(`Database: ${SUPABASE_URL}`);
  console.log(`Auto-refreshes every 15 seconds`);
  console.log(`MVs auto-refresh every 5 minutes\n`);

  // Auto-refresh materialized views every 5 minutes
  // Refresh stats cache every 5 minutes (async background job)
  console.log(`📊 Stats Cache: Updating in background...`);
  refreshStatsCache().catch(err => console.error("Cache refresh error:", err.message));
  setInterval(refreshStatsCache, 5 * 60 * 1000);
});

// Also refresh when ingest heartbeat indicates activity just stopped
setInterval(() => {
  if (Date.now() - lastIngestTime > 60_000 && Date.now() - lastIngestTime < 65_000) {
    // Ingest just stopped, refresh stats
    console.log(`✓ Ingest activity stopped, refreshing stats cache`);
    refreshStatsCache();
  }
}, 10_000);

// Refresh materialized views every 10 minutes (background, non-blocking)
async function refreshMVs() {
  try {
    for (const view of ["county_stats_mv", "county_lien_counts", "county_rent_counts"]) {
      try {
        const res = await fetch(`${SUPABASE_URL}/pg/query`, {
          method: "POST",
          headers: { apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}` }),
        });
        if (res.ok) {
          console.log(`  ✓ MV ${view} refreshed`);
        }
      } catch (viewErr) {
        // Silent fail - MV refresh is not critical for dashboard
      }
    }
  } catch (err) {
    // Silent fail
  }
}

refreshMVs().catch(err => console.error("MV refresh error:", err.message));
setInterval(refreshMVs, 10 * 60 * 1000);
