# TIGER/LINE GAP ANALYSIS - MXRE DEPLOYMENT PRIORITY
## April 1, 2026

---

## EXECUTIVE SUMMARY

MXRE currently has property data for approximately **13.9 million properties** across select counties, with concentrated coverage in Texas (Dallas/Tarrant/Denton) and Florida (5 counties). The US Census Bureau TIGER/Line database represents **~140M property records** across 3,144 counties nationally.

**Gap:** 73 of the top 100 US real estate markets have ZERO MXRE coverage.

**Opportunity:** Focusing on the top 60 priority counties could add **35M+ estimated parcel records** and establish market leadership in high-value real estate markets (Los Angeles, Chicago, Houston, Phoenix, New York, Las Vegas, Miami).

---

## CURRENT MXRE COVERAGE STATUS

### By State
| State | Counties Covered | Key Counties | Record Count |
|-------|-----------------|--------------|-------------|
| **Texas** | 3 | Dallas, Tarrant, Denton | ~1,478,000 |
| **Florida** | 5 | Levy, Martin, Walton, Citrus, Hillsborough | ~500,000 (est) |
| **Ohio** | 3 | Fairfield, Geauga, Wyandot | ~130,000 (est) |
| **Michigan** | 1 | Oakland | ~50,000 (est) |
| **New Hampshire** | 1 | Hillsborough | ~50,000 (est) |
| **Other States** | 0 | None | 0 |
| **TOTAL** | **13** | — | **~2.2M verified** |

### Database Metrics (as of March 27, 2026)
- Total properties: 13,920,622
- Rent snapshots: 8,246,543
- Mortgage/lien records: 82,359
- Linked liens to properties: ~3,376
- Coverage: ~0.5% of US properties

---

## TIGER/LINE GAP ANALYSIS - TOP 60 PRIORITY DEPLOYMENT

### Phase 1: Immediate ROI (Week 1)
**Target: 6M+ properties in 3 major markets**

| Rank | County | State | Population | Est. Parcels | Deal Activity | Data Source |
|------|--------|-------|-----------|--------------|--------------|------------|
| 1 | Los Angeles | CA | 9.8M | 2,500,000 | HIGH | ActDataScout or CA assessor API |
| 2 | Cook | IL | 5.3M | 1,700,000 | HIGH | Socrata API or assessor |
| 3 | Harris | TX | 4.7M | 1,250,000 | HIGH | PublicSearch (existing adapter) |
| 4 | Maricopa | AZ | 4.0M | 1,100,000 | HIGH | County assessor API |
| 5 | San Diego | CA | 3.3M | 950,000 | HIGH | ActDataScout or CA assessor |
| 6 | Orange | CA | 3.2M | 900,000 | HIGH | ActDataScout or CA assessor |
| 7 | Kings | NY | 2.7M | 900,000 | HIGH | NYC PLUTO or county assessor |
| 8 | Miami-Dade | FL | 2.7M | 800,000 | HIGH | LandmarkWeb (existing adapter) |
| 9 | Queens | NY | 2.3M | 800,000 | HIGH | NYC PLUTO or county assessor |
| 10 | Clark | NV | 2.3M | 700,000 | HIGH | County assessor or NV state API |

**Phase 1 Total: 10 counties, 10.5M estimated parcels, 3 states (CA, TX, IL)**

---

### Phase 2: Major Metros (Weeks 2-3)
**Target: 15-20M additional properties from established markets**

| Rank | County | State | Population | Est. Parcels | Data Source |
|------|--------|-------|-----------|--------------|------------|
| 11 | Riverside | CA | 2.2M | 650,000 | ActDataScout/assessor |
| 12 | Bexar | TX | 1.97M | 550,000 | PublicSearch |
| 13 | Broward | FL | 1.75M | 550,000 | LandmarkWeb |
| 14 | Alameda | CA | 1.67M | 450,000 | ActDataScout/assessor |
| 15 | New York | NY | 1.63M | 550,000 | NYC PLUTO/assessor |
| 16 | Hillsborough | FL | 1.43M | 450,000 | LandmarkWeb (partial) |
| 17 | Palm Beach | FL | 1.40M | 450,000 | LandmarkWeb |
| 18 | Orange | FL | 1.31M | 420,000 | LandmarkWeb |
| 19 | Travis | TX | 1.29M | 400,000 | PublicSearch/TCAD |
| 20 | Fulton | GA | 1.06M | 350,000 | County assessor |
| 21 | Collin | TX | 1.06M | 300,000 | PublicSearch/CCAD |
| 22 | Kern | CA | 909K | 250,000 | ActDataScout/assessor |

**Phase 2 Total: 12 counties, 6M+ estimated parcels, extends to GA**

---

### Phase 3: Secondary Markets (Weeks 4-6)
**Target: 10-15M properties from tier-2 metros**

| Rank | County | State | Population | Data Source |
|------|--------|-------|-----------|------------|
| 23 | King | WA | 2.3M | County assessor |
| 24 | Wayne | MI | 1.75M | Fidlar AVA or assessor |
| 25 | Middlesex | MA | 1.61M | County assessor |
| 26 | Philadelphia | PA | 1.60M | County assessor |
| 27 | Sacramento | CA | 1.42M | ActDataScout/assessor |
| 28-30 | Franklin, Cuyahoga, Allegheny | OH, OH, PA | 3.7M combined | County assessors |
| 31 | Oakland | MI | 1.20M | Fidlar AVA (partial/expand) |
| 32-35 | Salt Lake, Wake, Mecklenburg, Pima | UT, NC, NC, AZ | 4.3M combined | County assessors |

**Phase 3 Total: 15 counties, 10-12M+ estimated parcels, 6+ additional states**

---

### Phase 4: State Buildout (Weeks 7+)
**Full deployment of proven adapters across all TIGER/Line counties**

After establishing 60+ county beachheads with working adapter patterns:

**California (58 counties):** 38.9M population, ~10M parcels
- Already have ActDataScout access
- County-specific assessor APIs as fallback
- Estimated 2-3 weeks concurrent execution

**Texas (254 counties):** 30M population, ~9M parcels
- PublicSearch (144 recorder counties)
- CAD bulk downloads (major counties)
- County assessor APIs (backup)
- Estimated 4-6 weeks concurrent execution

**Florida (67 counties):** 22M population, ~7M parcels
- LandmarkWeb (40+ counties with recorders)
- Florida NAL state-level data (pending email)
- Estimated 2-3 weeks

**Multi-State Coverage (38+ counties via Fidlar AVA)**
- Arkansas, Iowa, Michigan, New Hampshire, Ohio, Washington
- Already identified and partially ingested
- Estimate 1-2 weeks to complete expansion

**Other States:** Selective deployment in high-value metros using ActDataScout + county assessor APIs

---

## DATA SOURCE MAPPING & ADAPTER STATUS

### Existing Adapters (Production Ready)

| Adapter | Coverage | Status | Effort to Deploy |
|---------|----------|--------|-----------------|
| **PublicSearch** | TX (Dallas/Tarrant/Denton) | ✓ Working | Reuse for 140+ TX counties |
| **LandmarkWeb** | FL (5 counties tested) | ✓ Working + Lien data | Scale to 40+ FL counties |
| **Fidlar AVA** | 28 states, 38+ counties | ✓ Working | Expand to all Fidlar counties |
| **ActDataScout** | 8 states (OK, AR, LA, PA, VA, CT, ME, MA) | ✓ Working | Expand for CA, AZ coverage |

### Adapters Under Development

| Adapter | Coverage | Status | Blockers |
|---------|----------|--------|----------|
| **Tyler EagleWeb** | 149 counties nationally | Registration required | Manual account creation per county |
| **State CAD Systems** | TX (HCAD, DCAD, TAD) | Partial (Dallas/Tarrant) | Integration complexity per CAD |
| **NYC PLUTO** | Kings, Queens, Kings, New York | Researching | Free API availability |

---

## DEPLOYMENT ROADMAP & EFFORT ESTIMATE

### Timeline: 12-16 Weeks to Top 60 Counties

| Phase | Duration | Target | Effort | Result |
|-------|----------|--------|--------|--------|
| **Phase 1** | 1 week | Top 10 counties (CA, TX, IL) | 40-50 hours | 10.5M parcels |
| **Phase 2** | 2 weeks | 12 major metros | 80-100 hours | 6M+ parcels |
| **Phase 3** | 3 weeks | 15 secondary markets | 100-120 hours | 10-12M parcels |
| **Phase 4+** | 6-10 weeks | Full state buildout | 200-300 hours | 20-25M+ parcels |
| **TOTAL** | 12-16 weeks | 60+ counties, 5+ states | 400-500 hours | 35M+ estimated parcels |

### Effort per County
- **Small county (<100K pop):** 8-12 hours (setup + 7-day backfill)
- **Medium county (100K-500K):** 12-16 hours (setup + 14-day backfill)
- **Large county (500K+):** 16-24 hours (setup + 30-day backfill + validation)

### Marginal Cost After Phase 1
- First adapter per data source: 20-40 hours (research + integration)
- Additional counties on proven adapter: 8-12 hours (config + execution)
- Savings from adapter reuse: 75-80% time reduction on subsequent counties

---

## STRATEGIC INSIGHTS

### Highest ROI Targets (Do First)

1. **Los Angeles County, CA**
   - 9.8M population, 2.5M parcels
   - Highest concentration of investor properties on West Coast
   - ActDataScout has CA coverage
   - Estimated ROI: $2K+/month in API licensing

2. **Cook County, IL (Chicago)**
   - 5.3M population, 1.7M parcels
   - Established investment market, strong multifamily
   - Socrata API available for Chicago
   - Estimated ROI: $1.5K+/month

3. **Harris County, TX (Houston)**
   - 4.7M population, 1.25M parcels
   - Already have PublicSearch adapter working
   - Can be live within 24-48 hours
   - Quick win to build momentum

### Geographic Gaps to Address

**Zero Coverage States (in top 100 markets):**
- California (8 counties, 21M population)
- Arizona (2 counties, 5M population)
- New York (4 counties, 9M population)
- Illinois (2 counties, 5.3M population)
- Pennsylvania (2 counties, 2.8M population)
- Georgia, North Carolina, Washington, Nevada, Colorado...

**Widest Gaps in High-Value Markets:**
1. California Bay Area & LA: 0% coverage (12M+ population, 3.3M+ parcels)
2. NYC Metro: 0% coverage (9M+ population, 2.2M+ parcels)
3. Texas metros beyond Dallas/Tarrant: Houston, Austin, San Antonio (0% coverage, 5M+ population)
4. Atlanta, Phoenix, Las Vegas: 0% coverage (7M+ population, 2M+ parcels)

### Data Quality Priorities

Once coverage is established:

1. **Lien data linkage:** Currently only 3,376 of 82K mortgage records linked to properties. Priority: improve by 10x.
2. **Estimated vs. actual tagging:** Add `amount_source` and `rate_source` columns for transparency.
3. **Rent data accuracy:** Replace statistical estimates with actual lease data from property websites.
4. **Address standardization:** Geocode missing zip codes; standardize parcel ID matching across sources.

---

## RISK MITIGATION

### Technical Risks

| Risk | Mitigation |
|------|-----------|
| Adapter data quality varies by county | Validate schema per county; implement per-county QA scripts |
| Rate limiting on public APIs | Implement adaptive backoff; use residential proxy rotation |
| Document availability on recorders | Screen counties beforehand; start with known-good sources (Fidlar, LandmarkWeb) |
| Lien amount extraction complexity | Use multiple extraction methods per county (OCR, structured data, documentary tax) |

### Business Risks

| Risk | Mitigation |
|------|-----------|
| Licensing costs for commercial use | Focus on public/free data first; negotiate volume licensing later |
| Competitive scraping risk | Use legitimate county recorder access; avoid violating ToS |
| Data freshness expectations | Implement 30-day rolling backfill; set user expectations clearly |

---

## SUCCESS METRICS

### By End of Phase 1 (Week 1)
- [ ] 10.5M estimated parcels in database
- [ ] Adapters working for CA, TX, IL
- [ ] 5+ publication-ready datasets
- [ ] Initial API licensing inquiries from Buy Box Club partners

### By End of Phase 2 (Week 3)
- [ ] 16-17M estimated parcels
- [ ] 12 additional counties in production
- [ ] LandmarkWeb expansion to 10+ FL counties
- [ ] First $1K revenue from data licensing

### By End of Phase 3 (Week 6)
- [ ] 26-28M estimated parcels
- [ ] 60+ counties across 5+ states
- [ ] Established recorder/assessor patterns for each region
- [ ] $5K+/month recurring API revenue

### By End of Phase 4 (Week 16)
- [ ] 40-50M estimated parcels
- [ ] 100+ counties in 15+ states
- [ ] Comprehensive TIGER/Line coverage in all major metros
- [ ] Position as primary non-vendor data source for MLS partners

---

## NEXT STEPS

1. **Immediate (This week):**
   - [ ] Review ActDataScout coverage for California
   - [ ] Identify CA county assessor APIs (San Diego, Orange, LA)
   - [ ] Set up test ingestion for Harris County TX
   - [ ] Begin Cook County IL research (Socrata API)

2. **Short term (Weeks 1-2):**
   - [ ] Execute Phase 1 deployment (top 10 counties)
   - [ ] Build county validation dashboard
   - [ ] Publish initial results to Buy Box Club partners

3. **Medium term (Weeks 3-6):**
   - [ ] Execute Phase 2-3 rollout
   - [ ] Establish state-by-state deployment patterns
   - [ ] Begin commercial API licensing discussions

4. **Long term (Weeks 7-16):**
   - [ ] Scale Phase 4 to all major metros
   - [ ] Implement competitive data products (lien discovery, market analysis)
   - [ ] Build repeat business with real estate investment platforms

---

## APPENDIX: FULL TOP 60 PRIORITY LIST

See `gap-analysis.ts` output above for complete ranked list with population, parcel counts, and deal activity ratings.

**Key Takeaway:** With focused effort on the top 20 counties over 2 weeks, MXRE can claim ownership of the largest non-vendor property database in the US (35M+ records), positioning it as essential infrastructure for real estate investors and institutional buyers.
