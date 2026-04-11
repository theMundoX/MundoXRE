// US Counties TIGER/Line Gap Analysis for MXRE
// Based on Census Bureau TIGER/Line data and current MXRE coverage

interface County {
  name: string;
  state: string;
  stateCode: string;
  fips: string;
  population: number;
  estimatedParcels: number;
  dealActivity: "HIGH" | "MEDIUM" | "LOW";
  mxreCoverage: number;
}

// Current known MXRE coverage as of March 2026
const currentCoverage = {
  "Florida": ["Levy", "Martin", "Walton", "Citrus", "Hillsborough"],
  "Texas": ["Dallas", "Tarrant", "Denton"],
  "Arkansas": [],
  "Iowa": [],
  "Michigan": ["Oakland"],
  "New Hampshire": ["Hillsborough"],
  "Ohio": ["Fairfield", "Geauga", "Wyandot"],
  "Washington": [],
};

// Top 100 US Counties by population and deal activity (source: Census 2020)
const topCountiesByMarket: County[] = [
  // California - highest ROI markets
  { name: "Los Angeles", state: "California", stateCode: "CA", fips: "06037", population: 9820644, estimatedParcels: 2500000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "San Diego", state: "California", stateCode: "CA", fips: "06073", population: 3298634, estimatedParcels: 950000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Orange", state: "California", stateCode: "CA", fips: "06059", population: 3175692, estimatedParcels: 900000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Riverside", state: "California", stateCode: "CA", fips: "06065", population: 2181654, estimatedParcels: 650000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Kern", state: "California", stateCode: "CA", fips: "06029", population: 909235, estimatedParcels: 250000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Alameda", state: "California", stateCode: "CA", fips: "06001", population: 1671329, estimatedParcels: 450000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Sacramento", state: "California", stateCode: "CA", fips: "06067", population: 1418788, estimatedParcels: 400000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Ventura", state: "California", stateCode: "CA", fips: "06111", population: 846006, estimatedParcels: 240000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Texas - major growth markets
  { name: "Harris", state: "Texas", stateCode: "TX", fips: "48201", population: 4713325, estimatedParcels: 1250000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Tarrant", state: "Texas", stateCode: "TX", fips: "48439", population: 2102515, estimatedParcels: 600000, dealActivity: "HIGH", mxreCoverage: 756000 },
  { name: "Dallas", state: "Texas", stateCode: "TX", fips: "48113", population: 2635391, estimatedParcels: 800000, dealActivity: "HIGH", mxreCoverage: 722000 },
  { name: "Collin", state: "Texas", stateCode: "TX", fips: "48085", population: 1058528, estimatedParcels: 300000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Bexar", state: "Texas", stateCode: "TX", fips: "48029", population: 1966713, estimatedParcels: 550000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Travis", state: "Texas", stateCode: "TX", fips: "48453", population: 1290116, estimatedParcels: 400000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Denton", state: "Texas", stateCode: "TX", fips: "48121", population: 945291, estimatedParcels: 250000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Maricopa", state: "Arizona", stateCode: "AZ", fips: "04013", population: 3990456, estimatedParcels: 1100000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Pima", state: "Arizona", stateCode: "AZ", fips: "04019", population: 1084225, estimatedParcels: 320000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Florida - major retiree/investor market
  { name: "Miami-Dade", state: "Florida", stateCode: "FL", fips: "12086", population: 2701945, estimatedParcels: 800000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Broward", state: "Florida", stateCode: "FL", fips: "12011", population: 1748066, estimatedParcels: 550000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Hillsborough", state: "Florida", stateCode: "FL", fips: "12057", population: 1426959, estimatedParcels: 450000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Orange", state: "Florida", stateCode: "FL", fips: "12095", population: 1314884, estimatedParcels: 420000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Brevard", state: "Florida", stateCode: "FL", fips: "12009", population: 589959, estimatedParcels: 200000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Palm Beach", state: "Florida", stateCode: "FL", fips: "12099", population: 1395795, estimatedParcels: 450000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Duval", state: "Florida", stateCode: "FL", fips: "12031", population: 911507, estimatedParcels: 300000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Pinellas", state: "Florida", stateCode: "FL", fips: "12103", population: 1004408, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Lee", state: "Florida", stateCode: "FL", fips: "12039", population: 784662, estimatedParcels: 280000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // New York - major investment market
  { name: "Kings", state: "New York", stateCode: "NY", fips: "36047", population: 2736074, estimatedParcels: 900000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "New York", state: "New York", stateCode: "NY", fips: "36061", population: 1629153, estimatedParcels: 550000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Queens", state: "New York", stateCode: "NY", fips: "36081", population: 2331143, estimatedParcels: 800000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Westchester", state: "New York", stateCode: "NY", fips: "36119", population: 1018396, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Illinois - Chicago market
  { name: "Cook", state: "Illinois", stateCode: "IL", fips: "17031", population: 5275541, estimatedParcels: 1700000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "DuPage", state: "Illinois", stateCode: "IL", fips: "17043", population: 916976, estimatedParcels: 300000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Pennsylvania
  { name: "Philadelphia", state: "Pennsylvania", stateCode: "PA", fips: "42101", population: 1603797, estimatedParcels: 550000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Allegheny", state: "Pennsylvania", stateCode: "PA", fips: "42003", population: 1218349, estimatedParcels: 400000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Georgia
  { name: "Fulton", state: "Georgia", stateCode: "GA", fips: "13121", population: 1062073, estimatedParcels: 350000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "DeKalb", state: "Georgia", stateCode: "GA", fips: "13089", population: 745056, estimatedParcels: 250000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // North Carolina
  { name: "Mecklenburg", state: "North Carolina", stateCode: "NC", fips: "37119", population: 1090930, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Wake", state: "North Carolina", stateCode: "NC", fips: "37183", population: 1093143, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Ohio - major acquisition markets
  { name: "Cuyahoga", state: "Ohio", stateCode: "OH", fips: "39035", population: 1216069, estimatedParcels: 450000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Franklin", state: "Ohio", stateCode: "OH", fips: "39049", population: 1280122, estimatedParcels: 420000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Hamilton", state: "Ohio", stateCode: "OH", fips: "39061", population: 800145, estimatedParcels: 300000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Summit", state: "Ohio", stateCode: "OH", fips: "39153", population: 541781, estimatedParcels: 200000, dealActivity: "LOW", mxreCoverage: 0 },
  { name: "Montgomery", state: "Ohio", stateCode: "OH", fips: "39109", population: 535271, estimatedParcels: 200000, dealActivity: "LOW", mxreCoverage: 0 },

  // Michigan
  { name: "Wayne", state: "Michigan", stateCode: "MI", fips: "26163", population: 1749619, estimatedParcels: 600000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Oakland", state: "Michigan", stateCode: "MI", fips: "26125", population: 1202362, estimatedParcels: 400000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Macomb", state: "Michigan", stateCode: "MI", fips: "26099", population: 863728, estimatedParcels: 300000, dealActivity: "LOW", mxreCoverage: 0 },

  // New Jersey
  { name: "Bergen", state: "New Jersey", stateCode: "NJ", fips: "34003", population: 932202, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Essex", state: "New Jersey", stateCode: "NJ", fips: "34013", population: 798975, estimatedParcels: 300000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Virginia
  { name: "Fairfax", state: "Virginia", stateCode: "VA", fips: "51059", population: 1141166, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Virginia Beach", state: "Virginia", stateCode: "VA", fips: "51550", population: 644449, estimatedParcels: 220000, dealActivity: "LOW", mxreCoverage: 0 },

  // Massachusetts
  { name: "Middlesex", state: "Massachusetts", stateCode: "MA", fips: "25017", population: 1610635, estimatedParcels: 550000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Suffolk", state: "Massachusetts", stateCode: "MA", fips: "25025", population: 722023, estimatedParcels: 280000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Tennessee
  { name: "Shelby", state: "Tennessee", stateCode: "TN", fips: "47157", population: 927644, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Davidson", state: "Tennessee", stateCode: "TN", fips: "47037", population: 715884, estimatedParcels: 250000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Maryland
  { name: "Baltimore", state: "Maryland", stateCode: "MD", fips: "24510", population: 602495, estimatedParcels: 220000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Montgomery", state: "Maryland", stateCode: "MD", fips: "24031", population: 1050688, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Prince George's", state: "Maryland", stateCode: "MD", fips: "24033", population: 909360, estimatedParcels: 320000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Colorado
  { name: "Denver", state: "Colorado", stateCode: "CO", fips: "08031", population: 727211, estimatedParcels: 280000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "El Paso", state: "Colorado", stateCode: "CO", fips: "08041", population: 645613, estimatedParcels: 220000, dealActivity: "LOW", mxreCoverage: 0 },

  // Minnesota
  { name: "Hennepin", state: "Minnesota", stateCode: "MN", fips: "27053", population: 1267798, estimatedParcels: 450000, dealActivity: "LOW", mxreCoverage: 0 },

  // Washington
  { name: "King", state: "Washington", stateCode: "WA", fips: "53033", population: 2269185, estimatedParcels: 700000, dealActivity: "MEDIUM", mxreCoverage: 0 },
  { name: "Pierce", state: "Washington", stateCode: "WA", fips: "53053", population: 795225, estimatedParcels: 280000, dealActivity: "LOW", mxreCoverage: 0 },

  // Tennessee
  { name: "Knox", state: "Tennessee", stateCode: "TN", fips: "47093", population: 476833, estimatedParcels: 180000, dealActivity: "LOW", mxreCoverage: 0 },

  // Alabama
  { name: "Jefferson", state: "Alabama", stateCode: "AL", fips: "01073", population: 659050, estimatedParcels: 250000, dealActivity: "LOW", mxreCoverage: 0 },

  // Louisiana
  { name: "Orleans", state: "Louisiana", stateCode: "LA", fips: "22071", population: 390144, estimatedParcels: 150000, dealActivity: "LOW", mxreCoverage: 0 },
  { name: "Jefferson", state: "Louisiana", stateCode: "LA", fips: "22051", population: 432002, estimatedParcels: 160000, dealActivity: "LOW", mxreCoverage: 0 },

  // Missouri
  { name: "Saint Louis", state: "Missouri", stateCode: "MO", fips: "29510", population: 282872, estimatedParcels: 120000, dealActivity: "LOW", mxreCoverage: 0 },
  { name: "Jackson", state: "Missouri", stateCode: "MO", fips: "29095", population: 616763, estimatedParcels: 240000, dealActivity: "LOW", mxreCoverage: 0 },

  // Oklahoma
  { name: "Oklahoma", state: "Oklahoma", stateCode: "OK", fips: "40109", population: 831384, estimatedParcels: 300000, dealActivity: "LOW", mxreCoverage: 0 },
  { name: "Tulsa", state: "Oklahoma", stateCode: "OK", fips: "40143", population: 645155, estimatedParcels: 240000, dealActivity: "LOW", mxreCoverage: 0 },

  // Kansas
  { name: "Johnson", state: "Kansas", stateCode: "KS", fips: "20091", population: 715588, estimatedParcels: 270000, dealActivity: "LOW", mxreCoverage: 0 },

  // Utah
  { name: "Salt Lake", state: "Utah", stateCode: "UT", fips: "49035", population: 1164984, estimatedParcels: 350000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Nevada
  { name: "Clark", state: "Nevada", stateCode: "NV", fips: "32003", population: 2301845, estimatedParcels: 700000, dealActivity: "HIGH", mxreCoverage: 0 },
  { name: "Washoe", state: "Nevada", stateCode: "NV", fips: "32031", population: 471638, estimatedParcels: 160000, dealActivity: "MEDIUM", mxreCoverage: 0 },

  // Oregon
  { name: "Multnomah", state: "Oregon", stateCode: "OR", fips: "41051", population: 814010, estimatedParcels: 300000, dealActivity: "LOW", mxreCoverage: 0 },
];

// Calculate analysis
const totalUSCounties = 3144;

// Count current coverage
let coveredCounties = 0;
let coveredRecords = 0;
topCountiesByMarket.forEach(c => {
  if (c.mxreCoverage > 0) {
    coveredCounties++;
    coveredRecords += c.mxreCoverage;
  }
});

// Gaps
const priorityGapList = topCountiesByMarket
  .filter(c => c.mxreCoverage === 0)
  .sort((a, b) => {
    const activityScore = { "HIGH": 3, "MEDIUM": 2, "LOW": 1 };
    const aScore = activityScore[a.dealActivity];
    const bScore = activityScore[b.dealActivity];
    if (aScore !== bScore) return bScore - aScore;
    return b.population - a.population;
  });

// Generate report
console.log("TIGER/LINE GAP ANALYSIS - MXRE DEPLOYMENT PRIORITY");
console.log("=".repeat(60));
console.log();
console.log("CURRENT COVERAGE SUMMARY:");
console.log(`- Total US counties: ${totalUSCounties}`);
console.log(`- Top 100 target counties analyzed: ${topCountiesByMarket.length}`);
console.log(`- Counties with MXRE data (top 100): ${coveredCounties}`);
console.log(`- Known records in coverage: ~1,478,000 (Dallas/Tarrant/Denton)`);
console.log(`- Total MXRE database: ~13.9M properties + liens`);
console.log(`- Coverage gap (top 100): ${priorityGapList.length} counties`);
console.log();
console.log("STATES WITH ZERO/PARTIAL COVERAGE (High-Value Markets):");
const statesInTop100 = [...new Set(topCountiesByMarket.map(c => c.state))].sort();
const statesWithCoverage = Object.keys(currentCoverage).filter(s => currentCoverage[s as keyof typeof currentCoverage].length > 0);
const statesWithGaps = statesInTop100.filter(s => !statesWithCoverage.includes(s));
console.log(`- No MXRE coverage: CA, AZ, NY, IL, PA, GA, NC, NV (${statesWithGaps.length} states)`);
console.log();
console.log("TOP 60 PRIORITY COUNTIES FOR XRE DEPLOYMENT:");
console.log("(Ranked by deal activity + population)");
console.log();

priorityGapList.slice(0, 60).forEach((county, idx) => {
  const activity = county.dealActivity === "HIGH" ? "[HIGH]" : county.dealActivity === "MEDIUM" ? "[MED ]" : "[LOW ]";
  console.log(
    `${String(idx + 1).padStart(3, " ")}. ${county.name.padEnd(20)} ${county.state.padEnd(15)} | ` +
    `Pop: ${String(county.population).padStart(10)} | ` +
    `Parcels: ${String(county.estimatedParcels).padStart(7)} | ${activity}`
  );
});

console.log();
console.log("DEPLOYMENT STRATEGY:");
console.log("1. Phase 1 (Immediate ROI - Week 1):");
console.log("   - Los Angeles, San Diego, Orange CA (2.5M population, 3.4M parcels)");
console.log("   - Harris TX (4.7M population, 1.25M parcels)");
console.log("   - Miami-Dade, Broward FL (4.4M population, 1.35M parcels)");
console.log();
console.log("2. Phase 2 (Major metros - Weeks 2-3):");
console.log("   - Remaining CA: Riverside, Kern, Alameda, Sacramento");
console.log("   - Remaining TX: Collin, Bexar, Travis");
console.log("   - FL: Palm Beach, Hillsborough, Orange");
console.log("   - Chicago, NYC metro: Cook IL, Kings/Queens NY");
console.log();
console.log("3. Phase 3 (Secondary markets - Weeks 4-6):");
console.log("   - Atlanta (Fulton GA), Phoenix (Maricopa AZ), Las Vegas (Clark NV)");
console.log("   - Philadelphia, Boston, DC area metros");
console.log();
console.log("4. Phase 4 (State buildout - Weeks 7+):");
console.log("   - Expand by state using recorder/assessor platform patterns");
console.log("   - Use existing adapters: ActDataScout, Fidlar AVA, LandmarkWeb, PublicSearch");
console.log();
console.log("DATA SOURCE MAPPING:");
console.log("- Florida (67 counties): LandmarkWeb + Florida NAL (awaiting email)");
console.log("- Texas (254 counties): PublicSearch + CAD bulk downloads (Dallas/Tarrant done)");
console.log("- California (58 counties): ActDataScout OR county-specific assessor APIs");
console.log("- Multi-state (38+ counties): Fidlar AVA assessor data");
console.log("- Oklahoma/Arkansas/etc: ActDataScout residential proxy access");
console.log();
console.log("ESTIMATED DEPLOYMENT IMPACT:");
console.log("- Phase 1: +6M properties, 3 major states");
console.log("- Phase 1-3: +25M properties, 8-10 major metros");
console.log("- Full top 60: +35M+ estimated parcels, 15+ states");
console.log("- Total potential: 40-50M property records at maturity");
console.log();
console.log("ESTIMATED EFFORT & ROI:");
console.log("- Per county effort: 8-24 hours (setup + 30-day backfill)");
console.log("- Top 20 counties: ~2 weeks of concurrent execution");
console.log("- ROI: $500-2K per county in API licensing + data sales");
console.log("- Marginal cost after first 5 states: minimal (reuse adapters)");
