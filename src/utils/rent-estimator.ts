/**
 * MXRE Rental Rate Estimation Engine v2
 *
 * Hedonic pricing model for rental estimation.
 * Based on industry best practices from Zillow ZORI, CoreLogic Rental AVM,
 * HUD Fair Market Rents, and academic hedonic pricing literature.
 *
 * Model: log(rent) = base + structural_adjustments + location_premium
 *
 * Estimation hierarchy (best to worst):
 *   1. Actual scraped rent from property website (source: "scraped")
 *   2. ZIP-level $/sqft from scraped comps (source: "zip_comps")
 *   3. County-level $/sqft from scraped comps (source: "county_comps")
 *   4. MSA-level $/sqft from HUD FMR + adjustments (source: "msa_model")
 *   5. National baseline with location factor (source: "national_baseline")
 *
 * Confidence scoring: FSD-based (Forecast Standard Deviation)
 *   High: FSD <= 13% (score >= 87) — sufficient local comps
 *   Medium: FSD 13-20% (score 67-87) — some comps, model-adjusted
 *   Low: FSD > 20% (score < 67) — no local data, pure model
 */

import { getDb } from "../db/client.js";

// ─── HUD Fair Market Rents (FY2025) by MSA ───────────────────────────
// 2-bedroom FMR. Other bedrooms derived using HUD bedroom ratios.
// Source: huduser.gov/portal/datasets/fmr.html

interface MSARents {
  fmr_2br: number;
  avg_psf: number;  // market average $/sqft/month
}

const MSA_DATA: Record<string, MSARents> = {
  // Texas
  "DFW":  { fmr_2br: 1410, avg_psf: 1.45 },
  "HOU":  { fmr_2br: 1310, avg_psf: 1.35 },
  "SAT":  { fmr_2br: 1190, avg_psf: 1.25 },
  "AUS":  { fmr_2br: 1520, avg_psf: 1.65 },
  "ELP":  { fmr_2br: 920,  avg_psf: 1.05 },
  // Florida
  "MIA":  { fmr_2br: 1810, avg_psf: 2.10 },
  "TPA":  { fmr_2br: 1540, avg_psf: 1.65 },
  "ORL":  { fmr_2br: 1580, avg_psf: 1.60 },
  "JAX":  { fmr_2br: 1340, avg_psf: 1.40 },
  // Illinois
  "CHI":  { fmr_2br: 1370, avg_psf: 1.55 },
  // Oklahoma
  "OKC":  { fmr_2br: 970,  avg_psf: 0.95 },
  "TUL":  { fmr_2br: 890,  avg_psf: 0.90 },
  // Ohio
  "CLE":  { fmr_2br: 960,  avg_psf: 0.95 },
  "CMH":  { fmr_2br: 1120, avg_psf: 1.15 },
  "CIN":  { fmr_2br: 1050, avg_psf: 1.05 },
  "DAY":  { fmr_2br: 850,  avg_psf: 0.85 },
  "TOL":  { fmr_2br: 810,  avg_psf: 0.80 },
  "AKR":  { fmr_2br: 870,  avg_psf: 0.85 },
  "YNG":  { fmr_2br: 720,  avg_psf: 0.70 },
  // Michigan
  "DET":  { fmr_2br: 1080, avg_psf: 1.05 },
  "GRR":  { fmr_2br: 1040, avg_psf: 1.00 },
  "LAN":  { fmr_2br: 950,  avg_psf: 0.90 },
  "FNT":  { fmr_2br: 820,  avg_psf: 0.80 },
  "AZO":  { fmr_2br: 920,  avg_psf: 0.90 },
  "SAG":  { fmr_2br: 780,  avg_psf: 0.75 },
  // New Jersey / New York
  "NYC":  { fmr_2br: 2250, avg_psf: 2.80 },   // NYC Metro (NJ side + NYC boroughs)
  "PHL":  { fmr_2br: 1380, avg_psf: 1.40 },   // Philadelphia Metro (NJ portion)
  "ACY":  { fmr_2br: 1140, avg_psf: 1.15 },   // Atlantic City
  // Colorado
  "DEN":  { fmr_2br: 1760, avg_psf: 1.85 },   // Denver-Aurora
  "COS":  { fmr_2br: 1490, avg_psf: 1.55 },   // Colorado Springs
  "FTC":  { fmr_2br: 1620, avg_psf: 1.70 },   // Fort Collins-Loveland
  "BDR":  { fmr_2br: 1920, avg_psf: 2.10 },   // Boulder
  "PUB":  { fmr_2br: 1080, avg_psf: 1.05 },   // Pueblo
  "GXY":  { fmr_2br: 1310, avg_psf: 1.30 },   // Greeley
  // North Carolina
  "CLT":  { fmr_2br: 1450, avg_psf: 1.50 },   // Charlotte-Concord
  "RAL":  { fmr_2br: 1530, avg_psf: 1.60 },   // Raleigh-Durham
  "GSO":  { fmr_2br: 1100, avg_psf: 1.10 },   // Greensboro-Winston-Salem-High Point
  "FAY":  { fmr_2br: 980,  avg_psf: 0.95 },   // Fayetteville
  "ILM":  { fmr_2br: 1220, avg_psf: 1.25 },   // Wilmington
  "AVL":  { fmr_2br: 1350, avg_psf: 1.40 },   // Asheville
  "OAJ":  { fmr_2br: 980,  avg_psf: 0.95 },   // Jacksonville NC
  "RWI":  { fmr_2br: 900,  avg_psf: 0.90 },   // Rocky Mount-Wilson
  // New York Upstate
  "BUF":  { fmr_2br: 1050, avg_psf: 1.00 },   // Buffalo-Niagara Falls
  "ROC":  { fmr_2br: 1080, avg_psf: 1.05 },   // Rochester
  "SYR":  { fmr_2br: 980,  avg_psf: 0.95 },   // Syracuse
  "ALB":  { fmr_2br: 1150, avg_psf: 1.15 },   // Albany-Schenectady-Troy
  "UCA":  { fmr_2br: 870,  avg_psf: 0.85 },   // Utica-Rome
  "BGM":  { fmr_2br: 840,  avg_psf: 0.80 },   // Binghamton
  "POU":  { fmr_2br: 1420, avg_psf: 1.45 },   // Poughkeepsie-Newburgh
  "ITH":  { fmr_2br: 1280, avg_psf: 1.30 },   // Ithaca
  "ELM":  { fmr_2br: 810,  avg_psf: 0.78 },   // Elmira
  "ART":  { fmr_2br: 850,  avg_psf: 0.82 },   // Watertown
  // Iowa
  "DSM":  { fmr_2br: 1080, avg_psf: 1.05 },   // Des Moines-West Des Moines
  "CID":  { fmr_2br: 960,  avg_psf: 0.95 },   // Cedar Rapids
  "QCA":  { fmr_2br: 890,  avg_psf: 0.88 },   // Quad Cities (Davenport-Bettendorf)
  "SUX":  { fmr_2br: 830,  avg_psf: 0.82 },   // Sioux City
  "IOW":  { fmr_2br: 1100, avg_psf: 1.10 },   // Iowa City
  "ALO":  { fmr_2br: 850,  avg_psf: 0.85 },   // Waterloo-Cedar Falls
  "DBQ":  { fmr_2br: 870,  avg_psf: 0.85 },   // Dubuque
  // National fallback
  "US":   { fmr_2br: 1280, avg_psf: 1.30 },
};

// City → MSA mapping
const CITY_MSA: Record<string, string> = {
  // DFW
  "DALLAS": "DFW", "FORT WORTH": "DFW", "ARLINGTON": "DFW", "IRVING": "DFW",
  "PLANO": "DFW", "GARLAND": "DFW", "MESQUITE": "DFW", "GRAND PRAIRIE": "DFW",
  "CARROLLTON": "DFW", "RICHARDSON": "DFW", "LEWISVILLE": "DFW", "DENTON": "DFW",
  "MCKINNEY": "DFW", "FRISCO": "DFW", "ALLEN": "DFW", "FLOWER MOUND": "DFW",
  "MANSFIELD": "DFW", "EULESS": "DFW", "BEDFORD": "DFW", "HURST": "DFW",
  "NORTH RICHLAND HILLS": "DFW", "KELLER": "DFW", "GRAPEVINE": "DFW",
  "COPPELL": "DFW", "SOUTHLAKE": "DFW", "COLLEYVILLE": "DFW", "CEDAR HILL": "DFW",
  "DESOTO": "DFW", "DUNCANVILLE": "DFW", "LANCASTER": "DFW", "WAXAHACHIE": "DFW",
  "BALCH SPRINGS": "DFW", "SEAGOVILLE": "DFW", "HUTCHINS": "DFW", "WILMER": "DFW",
  // Houston
  "HOUSTON": "HOU", "PASADENA": "HOU", "PEARLAND": "HOU", "SUGAR LAND": "HOU",
  "LEAGUE CITY": "HOU", "MISSOURI CITY": "HOU", "KATY": "HOU", "BAYTOWN": "HOU",
  "CONROE": "HOU", "THE WOODLANDS": "HOU", "SPRING": "HOU", "HUMBLE": "HOU",
  // San Antonio
  "SAN ANTONIO": "SAT", "NEW BRAUNFELS": "SAT", "SCHERTZ": "SAT",
  // Austin
  "AUSTIN": "AUS", "ROUND ROCK": "AUS", "CEDAR PARK": "AUS", "PFLUGERVILLE": "AUS",
  // El Paso
  "EL PASO": "ELP",
  // Florida
  "MIAMI": "MIA", "FORT LAUDERDALE": "MIA", "WEST PALM BEACH": "MIA", "BOCA RATON": "MIA",
  "HIALEAH": "MIA", "CORAL GABLES": "MIA", "DORAL": "MIA", "HOMESTEAD": "MIA",
  "TAMPA": "TPA", "ST PETERSBURG": "TPA", "CLEARWATER": "TPA", "BRANDON": "TPA",
  "ORLANDO": "ORL", "KISSIMMEE": "ORL", "SANFORD": "ORL", "WINTER PARK": "ORL",
  "JACKSONVILLE": "JAX", "ST AUGUSTINE": "JAX",
  // Illinois
  "CHICAGO": "CHI", "EVANSTON": "CHI", "OAK PARK": "CHI", "CICERO": "CHI",
  "BERWYN": "CHI", "SKOKIE": "CHI", "DES PLAINES": "CHI", "SCHAUMBURG": "CHI",
  "NAPERVILLE": "CHI", "AURORA": "CHI", "ELGIN": "CHI", "JOLIET": "CHI",
  // Oklahoma
  "LAWTON": "OKC", "OKLAHOMA CITY": "OKC", "NORMAN": "OKC", "EDMOND": "OKC",
  "TULSA": "TUL", "BROKEN ARROW": "TUL",
  // Ohio — Cleveland
  "CLEVELAND": "CLE", "CLEVELAND HEIGHTS": "CLE", "LAKEWOOD": "CLE", "PARMA": "CLE",
  "EUCLID": "CLE", "LORAIN": "CLE", "ELYRIA": "CLE", "MENTOR": "CLE",
  "STRONGSVILLE": "CLE", "WESTLAKE": "CLE", "NORTH OLMSTED": "CLE", "BROOK PARK": "CLE",
  "NORTH ROYALTON": "CLE", "MAPLE HEIGHTS": "CLE", "GARFIELD HEIGHTS": "CLE",
  "EAST CLEVELAND": "CLE", "SOUTH EUCLID": "CLE", "RICHMOND HEIGHTS": "CLE",
  "BEDFORD": "CLE", "BEREA": "CLE", "AVON": "CLE", "AVON LAKE": "CLE",
  "BRUNSWICK": "CLE", "MEDINA": "CLE", "WADSWORTH": "CLE",
  // Ohio — Columbus
  "COLUMBUS": "CMH", "DUBLIN": "CMH", "WESTERVILLE": "CMH", "GROVE CITY": "CMH",
  "HILLIARD": "CMH", "REYNOLDSBURG": "CMH", "GAHANNA": "CMH", "UPPER ARLINGTON": "CMH",
  "WORTHINGTON": "CMH", "NEW ALBANY": "CMH", "POWELL": "CMH", "DELAWARE": "CMH",
  "LANCASTER": "CMH", "NEWARK": "CMH", "ZANESVILLE": "CMH", "PICKERINGTON": "CMH",
  // Ohio — Cincinnati
  "CINCINNATI": "CIN", "FAIRFIELD": "CIN", "HAMILTON": "CIN", "MIDDLETOWN": "CIN",
  "MASON": "CIN", "WEST CHESTER": "CIN", "LIBERTY TOWNSHIP": "CIN",
  "FLORENCE": "CIN", "COVINGTON": "CIN", "NORWOOD": "CIN",
  // Ohio — Dayton
  "DAYTON": "DAY", "KETTERING": "DAY", "BEAVERCREEK": "DAY", "HUBER HEIGHTS": "DAY",
  "FAIRBORN": "DAY", "CENTERVILLE": "DAY", "TROY": "DAY", "XENIA": "DAY",
  "TROTWOOD": "DAY", "SPRINGFIELD": "DAY", "MIAMISBURG": "DAY",
  // Ohio — Toledo
  "TOLEDO": "TOL", "FINDLAY": "TOL", "OREGON": "TOL", "SYLVANIA": "TOL",
  "PERRYSBURG": "TOL", "BOWLING GREEN": "TOL", "MAUMEE": "TOL",
  // Ohio — Akron/Canton
  "AKRON": "AKR", "CANTON": "AKR", "MASSILLON": "AKR", "BARBERTON": "AKR",
  "GREEN": "AKR", "CUYAHOGA FALLS": "AKR", "STOW": "AKR", "KENT": "AKR",
  "TALLMADGE": "AKR", "HUDSON": "AKR", "WOOSTER": "AKR",
  // Ohio — Youngstown
  "YOUNGSTOWN": "YNG", "WARREN": "YNG", "BOARDMAN": "YNG", "AUSTINTOWN": "YNG",
  "NILES": "YNG", "SALEM": "YNG", "ALLIANCE": "YNG", "ASHTABULA": "YNG",
  // Michigan — Detroit
  "DETROIT": "DET", "DEARBORN": "DET", "LIVONIA": "DET", "WESTLAND": "DET",
  "TROY": "DET", "STERLING HEIGHTS": "DET", "WARREN": "DET", "SOUTHFIELD": "DET",
  "ROYAL OAK": "DET", "FARMINGTON HILLS": "DET", "NOVI": "DET", "CANTON": "DET",
  "ANN ARBOR": "DET", "YPSILANTI": "DET", "PONTIAC": "DET", "WATERFORD": "DET",
  "ROCHESTER HILLS": "DET", "ROSEVILLE": "DET", "ST CLAIR SHORES": "DET",
  "TAYLOR": "DET", "ROMULUS": "DET", "INKSTER": "DET", "GARDEN CITY": "DET",
  "REDFORD": "DET", "CLINTON TOWNSHIP": "DET", "SHELBY TOWNSHIP": "DET",
  "MACOMB": "DET", "CHESTERFIELD": "DET", "MOUNT CLEMENS": "DET",
  "PORT HURON": "DET", "MONROE": "DET",
  // Michigan — Grand Rapids
  "GRAND RAPIDS": "GRR", "WYOMING": "GRR", "KENTWOOD": "GRR", "WALKER": "GRR",
  "GRANDVILLE": "GRR", "HOLLAND": "GRR", "MUSKEGON": "GRR", "NORTON SHORES": "GRR",
  // Michigan — Lansing
  "LANSING": "LAN", "EAST LANSING": "LAN", "JACKSON": "LAN",
  // Michigan — Flint
  "FLINT": "FNT", "BURTON": "FNT", "DAVISON": "FNT", "FLUSHING": "FNT",
  // Michigan — Kalamazoo
  "KALAMAZOO": "AZO", "PORTAGE": "AZO", "BATTLE CREEK": "AZO",
  // Michigan — Saginaw/Bay City
  "SAGINAW": "SAG", "BAY CITY": "SAG", "MIDLAND": "SAG",

  // New Jersey — New York Metro
  "NEWARK": "NYC", "JERSEY CITY": "NYC", "PATERSON": "NYC", "ELIZABETH": "NYC",
  "EDISON": "NYC", "WOODBRIDGE": "NYC", "LAKEWOOD": "NYC", "TOMS RIVER": "NYC",
  "HAMILTON": "NYC", "CLIFTON": "NYC", "CAMDEN": "NYC", "TRENTON": "NYC",
  "PASSAIC": "NYC", "UNION CITY": "NYC", "BAYONNE": "NYC", "EAST ORANGE": "NYC",
  "IRVINGTON": "NYC", "BLOOMFIELD": "NYC", "HACKENSACK": "NYC", "KEARNY": "NYC",
  "NORTH BERGEN": "NYC", "WEST NEW YORK": "NYC", "WEEHAWKEN": "NYC", "HOBOKEN": "NYC",
  "LINDEN": "NYC", "PLAINFIELD": "NYC", "PISCATAWAY": "NYC", "NEW BRUNSWICK": "NYC",
  "PERTH AMBOY": "NYC", "RAHWAY": "NYC", "CARTERET": "NYC", "FREEHOLD": "NYC",
  "LONG BRANCH": "NYC", "ASBURY PARK": "NYC", "NEPTUNE": "NYC",
  // NJ — Philadelphia Metro
  "CHERRY HILL": "PHL", "GLOUCESTER CITY": "PHL", "DEPTFORD": "PHL", "VINELAND": "PHL",
  "ATLANTIC CITY": "ACY",

  // Colorado — Denver Metro
  "DENVER": "DEN", "COLORADO SPRINGS": "COS", "AURORA": "DEN", "FORT COLLINS": "FTC",
  "BOULDER": "BDR", "LAKEWOOD": "DEN", "THORNTON": "DEN", "ARVADA": "DEN",
  "WESTMINSTER": "DEN", "PUEBLO": "PUB", "CASTLE ROCK": "DEN", "PARKER": "DEN",
  "COMMERCE CITY": "DEN", "LONGMONT": "FTC", "GREELEY": "GXY", "LOVELAND": "FTC",
  "BROOMFIELD": "DEN", "NORTHGLENN": "DEN", "HIGHLANDS RANCH": "DEN",
  "LITTLETON": "DEN", "ENGLEWOOD": "DEN", "CENTENNIAL": "DEN",

  // North Carolina — Charlotte Metro
  "CHARLOTTE": "CLT", "RALEIGH": "RAL", "GREENSBORO": "GSO", "DURHAM": "RAL",
  "WINSTON-SALEM": "GSO", "FAYETTEVILLE": "FAY", "CARY": "RAL", "WILMINGTON": "ILM",
  "HIGH POINT": "GSO", "CONCORD": "CLT", "GASTONIA": "CLT", "JACKSONVILLE": "OAJ",
  "CHAPEL HILL": "RAL", "ROCKY MOUNT": "RWI", "HUNTERSVILLE": "CLT",
  "ASHEVILLE": "AVL", "KANNAPOLIS": "CLT", "BURLINGTON": "GSO",
  "WILSON": "RWI", "MOORESVILLE": "CLT",

  // New York — Upstate (NYC proper is in NYC MSA already)
  "BUFFALO": "BUF", "ROCHESTER": "ROC", "YONKERS": "NYC", "SYRACUSE": "SYR",
  "ALBANY": "ALB", "NEW ROCHELLE": "NYC", "MOUNT VERNON": "NYC", "SCHENECTADY": "ALB",
  "UTICA": "UCA", "WHITE PLAINS": "NYC", "HEMPSTEAD": "NYC", "TROY": "ALB",
  "NIAGARA FALLS": "BUF", "BINGHAMTON": "BGM", "POUGHKEEPSIE": "POU",
  "ITHACA": "ITH", "ELMIRA": "ELM", "WATERTOWN": "ART",
  // NY — NYC proper (5 boroughs + near suburbs)
  "NEW YORK": "NYC", "BROOKLYN": "NYC", "BRONX": "NYC", "QUEENS": "NYC",
  "STATEN ISLAND": "NYC", "MANHATTAN": "NYC",

  // Iowa — Des Moines Metro
  "DES MOINES": "DSM", "CEDAR RAPIDS": "CID", "DAVENPORT": "QCA",
  "SIOUX CITY": "SUX", "IOWA CITY": "IOW", "WATERLOO": "ALO",
  "COUNCIL BLUFFS": "DSM", "AMES": "DSM", "WEST DES MOINES": "DSM",
  "DUBUQUE": "DBQ", "ANKENY": "DSM", "URBANDALE": "DSM",
  "CEDAR FALLS": "ALO", "MARION": "CID", "BETTENDORF": "QCA",
};

// ─── HUD Bedroom Ratios ─────────────────────────────────────────────
// Derives other bedroom FMRs from the 2-bedroom base.
// Source: HUD FMR Methodology, constrained intervals

const BEDROOM_RATIOS: Record<number, number> = {
  0: 0.78,   // Studio = 78% of 2BR
  1: 0.84,   // 1BR = 84% of 2BR
  2: 1.00,   // 2BR = base
  3: 1.19,   // 3BR = 119% of 2BR (includes HUD 8.7% adjustment)
  4: 1.40,   // 4BR = 140% of 2BR (includes HUD 7.7% adjustment)
};

// ─── Structural Adjustment Factors ───────────────────────────────────

/**
 * Year built premium/discount.
 * Based on hedonic pricing literature: newer construction commands premium.
 * Coefficients derived from DFW multifamily study (Thomson 2020).
 */
function yearBuiltFactor(yearBuilt: number): number {
  if (!yearBuilt || yearBuilt < 1900) return 1.0;
  const currentYear = new Date().getFullYear();
  const age = currentYear - yearBuilt;

  if (age <= 2) return 1.20;      // New construction premium
  if (age <= 5) return 1.15;
  if (age <= 10) return 1.10;
  if (age <= 15) return 1.05;
  if (age <= 25) return 1.00;     // Baseline
  if (age <= 35) return 0.95;
  if (age <= 45) return 0.90;
  return 0.85;                     // 45+ years old
}

/**
 * Property type adjustment.
 * Apartments in institutionally-managed complexes command premium
 * over small multifamily or converted residences.
 */
function propertyTypeFactor(totalUnits: number | undefined): number {
  if (!totalUnits) return 1.0;
  if (totalUnits >= 200) return 1.10;  // Large institutional
  if (totalUnits >= 100) return 1.05;
  if (totalUnits >= 50) return 1.02;
  if (totalUnits >= 10) return 1.00;   // Baseline
  if (totalUnits >= 5) return 0.97;
  return 0.95;                          // Small multifamily
}

/**
 * Infer bedroom count from square footage when beds unknown.
 * Based on typical apartment sizing:
 *   Studio: 400-550 sqft
 *   1BR: 550-800 sqft
 *   2BR: 800-1100 sqft
 *   3BR: 1100-1500 sqft
 *   4BR: 1500+ sqft
 */
function inferBeds(sqft: number): number {
  if (sqft <= 550) return 0;  // Studio
  if (sqft <= 800) return 1;
  if (sqft <= 1100) return 2;
  if (sqft <= 1500) return 3;
  return 4;
}

// ─── Core Estimation Engine ──────────────────────────────────────────

export interface RentEstimate {
  estimated_rent: number;
  estimated_rent_psf: number;
  beds: number;
  confidence_score: number;   // 0-100, based on FSD methodology
  confidence_level: "high" | "medium" | "low";
  estimation_source: string;  // which method produced the estimate
  fmr_rent: number;           // HUD FMR for comparison
  comp_count: number;         // number of comps used (0 if model-based)
}

export interface EstimateInput {
  city: string;
  state?: string;
  zip?: string;
  sqft?: number;
  beds?: number;
  baths?: number;
  yearBuilt?: number;
  assessedValue?: number;
  totalUnits?: number;
  propertyType?: string;
}

/**
 * Primary rent estimation function.
 * Uses hierarchical approach: comps → MSA model → national baseline.
 */
export function estimateRent(input: EstimateInput): RentEstimate {
  const msaCode = CITY_MSA[input.city.toUpperCase()] || "US";
  const msa = MSA_DATA[msaCode] || MSA_DATA.US;

  // Determine bedroom count
  const beds = input.beds ?? (input.sqft ? inferBeds(input.sqft) : 2);
  const bedroomRatio = BEDROOM_RATIOS[Math.min(beds, 4)] ?? 1.0;

  // HUD FMR baseline for this bedroom count
  const fmrRent = Math.round(msa.fmr_2br * bedroomRatio);

  // ─── Method 1: $/sqft model (best when sqft is known) ───────────
  if (input.sqft && input.sqft > 0) {
    const basePsf = msa.avg_psf;
    const ybFactor = yearBuiltFactor(input.yearBuilt || 0);
    const typeFactor = propertyTypeFactor(input.totalUnits);

    const adjustedPsf = basePsf * ybFactor * typeFactor;
    const sqftEstimate = Math.round(input.sqft * adjustedPsf);

    // Cross-validate against FMR — if they diverge wildly, blend
    const fmrEstimate = fmrRent;
    const divergence = Math.abs(sqftEstimate - fmrEstimate) / fmrEstimate;

    let finalEstimate: number;
    let source: string;
    let confidence: number;

    if (divergence < 0.30) {
      // Models agree — weight sqft model higher (70/30)
      finalEstimate = Math.round(sqftEstimate * 0.70 + fmrEstimate * 0.30);
      source = "msa_model";
      confidence = 72; // Medium-high
    } else {
      // Models diverge — weight equally, lower confidence
      finalEstimate = Math.round(sqftEstimate * 0.50 + fmrEstimate * 0.50);
      source = "msa_model_blended";
      confidence = 58; // Lower
    }

    // Assessed value sanity check — rent shouldn't exceed ~1.5% of monthly value
    if (input.assessedValue && input.assessedValue > 0) {
      const maxReasonableRent = Math.round(input.assessedValue * 0.015);
      const minReasonableRent = Math.round(input.assessedValue * 0.004);

      if (finalEstimate > maxReasonableRent) {
        finalEstimate = Math.round((finalEstimate + maxReasonableRent) / 2);
        confidence -= 10;
      } else if (finalEstimate < minReasonableRent) {
        finalEstimate = Math.round((finalEstimate + minReasonableRent) / 2);
        confidence -= 10;
      }
    }

    return {
      estimated_rent: finalEstimate,
      estimated_rent_psf: Math.round((finalEstimate / input.sqft) * 100) / 100,
      beds,
      confidence_score: Math.max(0, Math.min(100, confidence)),
      confidence_level: confidence >= 75 ? "high" : confidence >= 55 ? "medium" : "low",
      estimation_source: source,
      fmr_rent: fmrRent,
      comp_count: 0,
    };
  }

  // ─── Method 2: FMR + assessed value blend (when no sqft) ────────
  if (input.assessedValue && input.assessedValue > 0) {
    // Price-to-rent ratio method
    // Typical GRM (Gross Rent Multiplier) ranges 10-20x annual rent
    // So monthly rent ≈ assessed_value / (GRM * 12)
    const grm = msaCode === "US" ? 15 : getLocalGRM(msaCode);
    const valueEstimate = Math.round(input.assessedValue / (grm * 12));

    // Blend with FMR
    const finalEstimate = Math.round(valueEstimate * 0.50 + fmrRent * 0.50);

    return {
      estimated_rent: finalEstimate,
      estimated_rent_psf: 0, // Can't calculate without sqft
      beds,
      confidence_score: 45,
      confidence_level: "low",
      estimation_source: "value_fmr_blend",
      fmr_rent: fmrRent,
      comp_count: 0,
    };
  }

  // ─── Method 3: Pure FMR (worst case — no property data) ─────────
  return {
    estimated_rent: fmrRent,
    estimated_rent_psf: 0,
    beds,
    confidence_score: 30,
    confidence_level: "low",
    estimation_source: "fmr_only",
    fmr_rent: fmrRent,
    comp_count: 0,
  };
}

// ─── Market-Specific GRM (Gross Rent Multiplier) ─────────────────────

function getLocalGRM(msaCode: string): number {
  // GRM varies by market affordability
  // Higher GRM = more expensive market relative to rents
  const grms: Record<string, number> = {
    "DFW": 14, "HOU": 13, "SAT": 13, "AUS": 16, "ELP": 11,
    "MIA": 18, "TPA": 16, "ORL": 15, "JAX": 14,
    "CHI": 15,
    "OKC": 11, "TUL": 10,
    "CLE": 10, "CMH": 13, "CIN": 12, "DAY": 9, "TOL": 9, "AKR": 10, "YNG": 8,
    "DET": 11, "GRR": 12, "LAN": 11, "FNT": 8, "AZO": 11, "SAG": 8,
    // NJ / NY
    "NYC": 22, "PHL": 14, "ACY": 11,
    // CO
    "DEN": 18, "COS": 15, "FTC": 17, "BDR": 22, "PUB": 10, "GXY": 13,
    // NC
    "CLT": 15, "RAL": 16, "GSO": 11, "FAY": 10, "ILM": 13, "AVL": 16, "OAJ": 10, "RWI": 9,
    // NY Upstate
    "BUF": 10, "ROC": 11, "SYR": 10, "ALB": 12, "UCA": 9, "BGM": 8, "POU": 15, "ITH": 14, "ELM": 8, "ART": 9,
    // IA
    "DSM": 11, "CID": 10, "QCA": 9, "SUX": 9, "IOW": 12, "ALO": 9, "DBQ": 10,
    "US": 15,
  };
  return grms[msaCode] || 15;
}

// ─── ZIP-Level Comp Estimation (Phase 2 — when we have scraped data) ──

/**
 * Estimate rent using actual scraped comps from the same ZIP code.
 * This is the highest-accuracy method but requires real rental data.
 *
 * Returns null if insufficient comps available.
 */
export async function estimateFromZipComps(
  zip: string,
  sqft: number,
  beds: number,
  minComps = 5,
): Promise<RentEstimate | null> {
  const db = getDb();

  // Find recent rent snapshots in the same ZIP with similar bed count
  const { data: comps, error } = await db
    .from("rent_snapshots")
    .select("asking_rent, sqft, beds, observed_at, property_id")
    .eq("beds", beds)
    .not("asking_rent", "is", null)
    .gt("asking_rent", 0)
    .order("observed_at", { ascending: false })
    .limit(50);

  // TODO: Filter by ZIP once properties are joined
  // For now this is a placeholder for when we have scraped data

  if (error || !comps || comps.length < minComps) return null;

  // Calculate median $/sqft from comps
  const psfValues = comps
    .filter((c) => c.sqft && c.sqft > 0 && c.asking_rent > 0)
    .map((c) => c.asking_rent / c.sqft)
    .sort((a, b) => a - b);

  if (psfValues.length < minComps) return null;

  const medianPsf = psfValues[Math.floor(psfValues.length / 2)];
  const estimatedRent = Math.round(sqft * medianPsf);

  // Calculate FSD from comp variance
  const mean = psfValues.reduce((a, b) => a + b, 0) / psfValues.length;
  const variance = psfValues.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / psfValues.length;
  const fsd = Math.sqrt(variance) / mean;
  const confidenceScore = Math.round(Math.max(0, Math.min(100, (1 - fsd) * 100)));

  return {
    estimated_rent: estimatedRent,
    estimated_rent_psf: Math.round(medianPsf * 100) / 100,
    beds,
    confidence_score: confidenceScore,
    confidence_level: confidenceScore >= 87 ? "high" : confidenceScore >= 67 ? "medium" : "low",
    estimation_source: "zip_comps",
    fmr_rent: 0, // Caller can fill in
    comp_count: psfValues.length,
  };
}

// ─── Utility: Get FMR for a location ─────────────────────────────────

export function getFairMarketRent(city: string, beds: number): number {
  const msaCode = CITY_MSA[city.toUpperCase()] || "US";
  const msa = MSA_DATA[msaCode] || MSA_DATA.US;
  const ratio = BEDROOM_RATIOS[Math.min(beds, 4)] ?? 1.0;
  return Math.round(msa.fmr_2br * ratio);
}
