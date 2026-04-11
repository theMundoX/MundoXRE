# MXRE TIGER/LINE GAP DEPLOYMENT - PRIORITY CHECKLIST

**Goal:** Deploy XRE scripts to top 60 US counties for 35M+ property records coverage
**Timeline:** 12-16 weeks (4 phases)
**Current Status:** 13 counties covered, 13.9M properties, 0.5% US coverage

---

## PHASE 1: IMMEDIATE ROI (Week 1) - DO THIS FIRST

### Primary Targets: Top 3 States (CA, TX, IL)
**Goal:** 10 counties, 10.5M estimated parcels, establish proof of concept**

#### Week 1, Day 1-2: Texas (Already Familiar)

- [ ] **Harris County TX (Houston) - PRIORITY #1**
  - Population: 4.7M | Parcels: 1.25M
  - Status: PublicSearch adapter exists
  - Effort: 4-6 hours (reuse existing code)
  - Action: Run PublicSearch for Houston 30-day backfill
  - Expected result: +1.25M properties by end of week
  - Data source: PublicSearch recorder system

- [ ] **Collin County TX (Dallas suburbs)**
  - Population: 1.06M | Parcels: 300K
  - Status: PublicSearch adapter exists
  - Effort: 2-3 hours (config only)
  - Action: Extend PublicSearch adapter to Collin
  - Expected result: +300K properties

- [ ] **Bexar County TX (San Antonio)**
  - Population: 1.97M | Parcels: 550K
  - Status: PublicSearch adapter exists
  - Effort: 2-3 hours (config only)
  - Action: Run PublicSearch for San Antonio
  - Expected result: +550K properties

**Texas Subtotal: 3 counties, 2.1M parcels, 6-12 hours effort**

---

#### Week 1, Day 3-4: California (High ROI, New Territory)

- [ ] **Los Angeles County CA - PRIORITY #2**
  - Population: 9.8M | Parcels: 2.5M
  - Current status: ZERO coverage
  - Effort estimate: 12-16 hours
  - Data source research:
    - [ ] Check if ActDataScout covers CA assessor data
    - [ ] Investigate LA County Assessor API (publicly available)
    - [ ] Fallback: County deed recorder (PublicSearch alternative)
  - Action: Identify working data source, test extraction on 100 records
  - Next: Full 30-day backfill
  - Expected result: +2.5M properties

- [ ] **San Diego County CA**
  - Population: 3.3M | Parcels: 950K
  - Data source research: Same as LA (CA assessor access)
  - Effort: 4-6 hours (reuse CA adapter)
  - Action: Test extraction, schedule 30-day backfill
  - Expected result: +950K properties

- [ ] **Orange County CA**
  - Population: 3.2M | Parcels: 900K
  - Data source research: Same as LA
  - Effort: 2-3 hours (config only)
  - Action: Extend CA adapter
  - Expected result: +900K properties

**California Subtotal: 3 counties, 4.35M parcels, 18-25 hours effort**

---

#### Week 1, Day 5: Illinois (Establish Midwest Presence)

- [ ] **Cook County IL (Chicago) - PRIORITY #3**
  - Population: 5.3M | Parcels: 1.7M
  - Current status: ZERO coverage
  - Effort estimate: 8-12 hours
  - Data source research:
    - [ ] Chicago Socrata API (free, well-documented)
    - [ ] Explore Tyler/CCIS recorder system
    - [ ] County assessor website scraping option
  - Action: Validate Socrata API access, test schema mapping
  - Expected result: +1.7M properties

**Illinois Subtotal: 1 county, 1.7M parcels, 8-12 hours effort**

---

#### Additional Quick Wins (If Time Allows)

- [ ] **Clark County NV (Las Vegas)**
  - Population: 2.3M | Parcels: 700K
  - Effort: 6-8 hours (assess data source)
  - Action: Research county assessor API, test extraction
  - Priority: Medium (investor market, but requires new adapter)

- [ ] **Kings County NY (Brooklyn)**
  - Population: 2.7M | Parcels: 900K
  - Effort: 8-10 hours (assess PLUTO access)
  - Action: Test NYC PLUTO API (free public data)
  - Priority: Medium (investor market, but data structure unfamiliar)

---

**PHASE 1 TOTAL: 7-10 counties, 10-12M estimated parcels, 32-47 hours effort**

### Success Criteria for Phase 1
- [ ] Harris County properties ingesting at 50K+/day
- [ ] LA County data source identified and tested
- [ ] Cook County Socrata API connected
- [ ] All 3 new adapters committed to repo
- [ ] Database total: 25-26M properties
- [ ] Zero data quality issues on random validation

---

## PHASE 2: MAJOR METROS (Weeks 2-3)

### Targets: 10-12 Additional Counties (6M+ parcels)

#### California Expansion (Complete CA Phase 1)
- [ ] Riverside County CA (Moreno Valley) - 650K parcels
- [ ] Kern County CA (Bakersfield) - 250K parcels
- [ ] Alameda County CA (Oakland Bay Area) - 450K parcels
- [ ] Sacramento County CA - 400K parcels
- **CA Total: 4 counties, 1.75M parcels, 8-12 hours (reuse adapter)**

#### Texas Expansion
- [ ] Travis County TX (Austin) - 400K parcels
- [ ] **NOTE:** Tarrant County already done from earlier session
- **TX New: 1 county, 400K parcels, 2-3 hours**

#### Florida Expansion (LandmarkWeb Coverage)
- [ ] **Miami-Dade County FL** - 800K parcels (HIGH PRIORITY - largest FL market)
- [ ] **Broward County FL** - 550K parcels
- [ ] **Palm Beach County FL** - 450K parcels
- [ ] **Orange County FL** (Orlando) - 420K parcels
- [ ] Duval County FL (Jacksonville) - 300K parcels
- [ ] Lee County FL (Fort Myers) - 280K parcels
- **FL New: 6 counties, 3M parcels, 12-18 hours (reuse LandmarkWeb)**

#### New York Metro (NYC PLUTO)
- [ ] **Queens County NY** - 800K parcels
- [ ] **New York County NY** (Manhattan) - 550K parcels
- **NY: 2 counties, 1.35M parcels, 8-12 hours (NYC PLUTO API)**

---

**PHASE 2 TOTAL: 13-14 counties, 7-8M parcels, 30-45 hours effort**

### Success Criteria for Phase 2
- [ ] CA scaled to 6+ counties (2.1M new parcels)
- [ ] Florida expanded to 6+ recorder counties (3M+ new parcels)
- [ ] NYC data ingesting successfully
- [ ] All Phase 2 adapters tested on 1K+ records
- [ ] Database total: 35M+ properties
- [ ] Lien data collection expanded (Phase 1: focus on tax liens)

---

## PHASE 3: SECONDARY MARKETS (Weeks 4-6)

### Targets: Major Metros + Regional Hubs (10-15M parcels)

#### Arizona Expansion
- [ ] **Maricopa County AZ** (Phoenix) - 1.1M parcels [HIGH PRIORITY]
- [ ] Pima County AZ (Tucson) - 320K parcels
- **AZ: 2 counties, 1.42M parcels**

#### Additional TX Markets
- [ ] **Tarrant County TX** (Arlington/Fort Worth) - Already done (600K)
- [ ] Denton County TX - Already done (250K)
- [ ] **NOTE:** Phase 2 adds Travis, so TX has 8+ counties

#### Georgia/Atlanta
- [ ] **Fulton County GA** (Atlanta) - 350K parcels
- [ ] DeKalb County GA - 250K parcels
- **GA: 2 counties, 600K parcels**

#### North Carolina
- [ ] **Wake County NC** (Raleigh) - 350K parcels
- [ ] **Mecklenburg County NC** (Charlotte) - 350K parcels
- **NC: 2 counties, 700K parcels**

#### Pennsylvania/Philadelphia
- [ ] **Philadelphia County PA** - 550K parcels
- [ ] **Allegheny County PA** (Pittsburgh) - 400K parcels
- **PA: 2 counties, 950K parcels**

#### Washington/Seattle
- [ ] **King County WA** - 700K parcels
- **WA: 1 county, 700K parcels**

#### Massachusetts/Boston
- [ ] **Middlesex County MA** - 550K parcels
- [ ] **Suffolk County MA** (Boston) - 280K parcels
- **MA: 2 counties, 830K parcels**

#### Michigan Expansion
- [ ] **Wayne County MI** (Detroit) - 600K parcels
- [ ] **Oakland County MI** - 400K parcels (may already have partial coverage)
- **MI: 2 counties, 1M parcels**

#### Other Priority Markets
- [ ] **Clark County NV** (Las Vegas) - 700K parcels
- [ ] **Salt Lake County UT** (Salt Lake City) - 350K parcels
- [ ] **Denver County CO** - 280K parcels
- [ ] **Multnomah County OR** (Portland) - 300K parcels

---

**PHASE 3 TOTAL: 20-22 counties, 10-12M parcels, 100-150 hours effort**

### Success Criteria for Phase 3
- [ ] Establish patterns for multi-state deployment
- [ ] Every county in top 60 has working data source mapped
- [ ] Database total: 45-50M properties
- [ ] All adapters validated on 5K+ records each
- [ ] Ready for Phase 4 automation

---

## PHASE 4: STATE-LEVEL BUILDOUT (Weeks 7-16)

### Full Deployment of Proven Adapters

#### California (58 counties, 39M population, ~10M parcels)
- [ ] Leverage ActDataScout + county assessor APIs
- [ ] Estimated effort: 2-3 weeks concurrent execution
- [ ] Expected coverage: 10M+ parcels, 90%+ of state

#### Texas (254 counties, 30M population, ~9M parcels)
- [ ] PublicSearch (144 recorder counties) + CAD bulk downloads
- [ ] Estimated effort: 4-6 weeks (large state)
- [ ] Expected coverage: 9M+ parcels, 85%+ of state

#### Florida (67 counties, 22M population, ~7M parcels)
- [ ] LandmarkWeb (40+ recorder counties) + Florida NAL (pending)
- [ ] Estimated effort: 2-3 weeks
- [ ] Expected coverage: 7M+ parcels, 95%+ of state

#### Multi-State Fidlar AVA Coverage (38+ counties)
- [ ] Arkansas, Iowa, Michigan, New Hampshire, Ohio, Washington
- [ ] Estimated effort: 1-2 weeks
- [ ] Expected coverage: 1-2M parcels

#### Selective Additional States (Top 20 Metro Areas)
- [ ] Illinois (Cook + DuPage counties first, scale to 102 counties)
- [ ] New York (NYC metro, then upstate)
- [ ] Georgia (Atlanta metro, then statewide)
- [ ] North Carolina, Virginia, Pennsylvania, etc.

---

**PHASE 4 TOTAL: 300+ counties, 25-30M parcels, 200-300 hours effort**

---

## DEPLOYMENT EXECUTION TEMPLATE

For each county, follow this checklist:

### Pre-Deployment (2-4 hours)
- [ ] Identify data source (recorder, assessor, Fidlar, ActDataScout)
- [ ] Document API/scraping method
- [ ] Test data extraction on 50-100 sample records
- [ ] Map county data schema to MXRE `properties` table
- [ ] Estimate total record count
- [ ] Set expected daily ingest rate (e.g., 50K/day)

### Deployment (2-6 hours)
- [ ] Create county-specific adapter or config
- [ ] Test on first 1K records (validate schema, counts, dates)
- [ ] Schedule 30-day backfill (or until complete)
- [ ] Monitor daily ingestion (errors, rate limits, data gaps)
- [ ] Commit adapter code to repo

### Post-Deployment (2-4 hours)
- [ ] Validate random 100 records against source
- [ ] Check for data quality issues (missing fields, duplicates)
- [ ] Extract lien/mortgage data if available
- [ ] Document coverage timeline and source limitations
- [ ] Update county status in tracking sheet

**Total per county: 6-14 hours depending on data source complexity**

---

## DATA SOURCE PRIORITY MATRIX

| Data Source | Coverage | Complexity | Scalability | Effort |
|-------------|----------|-----------|------------|--------|
| **PublicSearch** | TX (144 counties) | Low | High | 2-3 hrs/county |
| **LandmarkWeb** | FL (40+ counties) | Low | High | 3-4 hrs/county |
| **Fidlar AVA** | 38 counties across 6 states | Medium | High | 4-6 hrs/county |
| **County Assessor APIs** | All (varies by county) | Medium | Medium | 6-8 hrs/county |
| **ActDataScout** | 8 states, expandable | High | Medium | 8-12 hrs/county |
| **NYC PLUTO** | Kings, Queens, New York, Bronx | Low | High | 4-6 hrs/county |
| **State CAD Systems** | TX major metros | High | Medium | 8-12 hrs/county |

**Recommendation:** Prioritize PublicSearch + LandmarkWeb (low effort, high scale) first, then expand to Fidlar AVA and county assessor APIs.

---

## RISK CHECKLIST

### Data Source Viability
- [ ] Test each adapter on 1K+ records BEFORE scheduling full backfill
- [ ] Document data quality issues per county (missing fields, invalid dates, etc.)
- [ ] Validate parcel counts vs. county records
- [ ] Monitor for rate limiting or blocking

### Technical Debt
- [ ] Implement per-county validation rules
- [ ] Add `data_source`, `ingestion_date`, `coverage_end_date` columns to properties table
- [ ] Tag properties as "estimated rent" vs. "market rent"
- [ ] Flag liens as "actual recorded" vs. "estimated"

### Legal/Compliance
- [ ] Verify data use is legal (public records, no copyright)
- [ ] Document ToS compliance for each data source
- [ ] Get legal sign-off before commercial use (API licensing)

---

## SUCCESS METRICS & MILESTONES

### Week 1 (End of Phase 1)
- [ ] Harris County TX live (1.25M properties)
- [ ] LA County CA data source identified
- [ ] Cook County IL Socrata API connected
- [ ] Database: 15M+ properties
- [ ] Commit: 3 new adapters to repo

### Week 3 (End of Phase 2)
- [ ] CA 6+ counties (2.1M new properties)
- [ ] Florida 6 counties (3M+ properties)
- [ ] NYC 2 counties (1.35M properties)
- [ ] Database: 35M+ properties
- [ ] Commit: 8 new adapters/configurations

### Week 6 (End of Phase 3)
- [ ] 20-22 counties across 10+ states
- [ ] All major metros established
- [ ] Database: 45M+ properties
- [ ] First API licensing inquiry expected
- [ ] Begin Phase 4 state-level deployment

### Week 16 (End of Phase 4)
- [ ] 100+ counties in 15+ states
- [ ] Database: 40-50M properties
- [ ] Comprehensive TIGER/Line coverage in major markets
- [ ] $5K+/month API licensing revenue (target)
- [ ] Position: "Largest independent property dataset" (non-vendor)

---

## QUESTIONS FOR EXECUTION

**Before starting Phase 1, clarify:**

1. Should Lien extraction start immediately, or focus on property records first?
   - Recommend: Property records first (more ROI), lien extraction in Phases 2-3

2. Which recorder systems have you had best results with?
   - PublicSearch (TX): Proven
   - LandmarkWeb (FL): Proven
   - Fidlar AVA: Proven
   - ActDataScout: Proven (but slower for large datasets)

3. What's the acceptable error rate for property data?
   - Recommend: <2% duplicates, <5% missing key fields

4. Should we pursue Tyler EagleWeb (149 counties, registration required)?
   - Recommend: Defer to Phase 4 (high effort, medium ROI)

5. Is commercial API licensing the primary monetization path?
   - Assume yes; adjust if data is for internal use only

---

## ROLLOUT COMMUNICATION PLAN

### Week 1: Internal Launch
- Announce Phase 1 results to Buy Box Club stakeholders
- Show 10-15M property growth proof
- Promise Phase 2 FL + NY expansion

### Week 3: External Preview
- Begin discussions with real estate API customers
- Offer "early access" to CA, TX, FL, NY datasets
- Collect feedback on data quality, schema, pricing

### Week 6: Product Launch
- Announce MXRE as "largest independent property dataset"
- Highlight TIGER/Line coverage vs. competitors (HelloData, CoreLogic)
- Begin commercial API licensing sales

### Week 16: Market Positioning
- Market as "comprehensive nationwide property records platform"
- Highlight lien data as competitive differentiator
- Pursue institutional licensing (Buy Box Club partners, MLS integrations)

---

**Ready to execute? Start with Phase 1, Week 1, Day 1: Harris County TX**
