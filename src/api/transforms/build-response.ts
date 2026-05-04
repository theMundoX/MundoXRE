import type {
  MXREPropertyResponse,
  SaleRecord,
  RentHistory,
  MLSHistoryEntry,
  PublicPropertySignal,
  DataQualityEntry,
  FMRData,
  SourceMix,
  SourceMixSection,
} from '../types.js';
import { parseOwnerName } from './parse-owner.js';
import { splitLiens } from './split-liens.js';
import { computeSignals } from './compute-signals.js';

type Row = Record<string, unknown>;

/**
 * Build a complete MXREPropertyResponse from DB rows.
 */
export function buildPropertyResponse(
  propertyRow: Row,
  county: Row,
  mortgages: Row[],
  rentSnapshots: Row[],
  listingSignals: Row[],
  saleHistory: Row[],
  mlsHistory: Row[],
  demographics: Row | null,
  foreclosures: Row[],
  publicSignals: Row[] = [],
): MXREPropertyResponse {
  const p = propertyRow;
  const c = county;

  const marketValue = (p.market_value as number) ?? null;

  const owner1 = parseOwnerName(p.owner_name as string | null);
  const owner2 = parseOwnerName(p.owner2_name as string | null);

  const ownershipStart = (p.ownership_start_date as string) ?? null;
  let ownershipLengthMonths: number | null = null;
  if (ownershipStart) {
    const months = (Date.now() - new Date(ownershipStart).getTime()) / (1000 * 60 * 60 * 24 * 30.44);
    ownershipLengthMonths = Math.round(months);
  }

  // Latest rent snapshot
  const latestRent = rentSnapshots.length > 0 ? rentSnapshots[0] : null;
  const livingSqft = (p.living_sqft as number) ?? (p.sqft as number) ?? null;

  // Latest listing signal — normalize actual column names
  // Schema: mls_list_price, is_on_market (bool), listing_agent_name, listing_brokerage,
  //         listing_source, listing_url, days_on_market, first_seen_at
  const rawListing = listingSignals.length > 0 ? listingSignals[0] : null;
  const latestListing: Record<string, unknown> | null = rawListing ? {
    ...rawListing,
    list_price: rawListing.mls_list_price ?? rawListing.list_price ?? null,
    status: rawListing.is_on_market != null
      ? (rawListing.is_on_market ? 'Active' : 'Off Market')
      : (rawListing.listing_status ?? null),
    agent_name: rawListing.listing_agent_name ?? rawListing.agent_name ?? null,
    brokerage: rawListing.listing_brokerage ?? rawListing.brokerage ?? null,
    source: rawListing.listing_source ?? rawListing.source ?? null,
    listing_url: rawListing.listing_url ?? null,
    list_date: rawListing.first_seen_at ?? rawListing.list_date ?? rawListing.snapshot_date ?? null,
  } : null;
  const listPrice = (latestListing?.list_price as number) ?? (latestListing?.price as number) ?? null;
  const listStatus = latestListing?.status as string | null;
  const marketStatus = mapMarketStatus(listStatus);
  const equityBasis = (marketStatus === 'active' || marketStatus === 'pending') && listPrice
    ? 'list_price'
    : p.estimated_value
      ? 'estimated_value'
      : marketValue
        ? 'market_value'
        : p.assessed_value
          ? 'assessed_value'
          : null;
  const equityValue = equityBasis === 'list_price'
    ? listPrice
    : equityBasis === 'estimated_value'
      ? (p.estimated_value as number)
      : equityBasis === 'market_value'
        ? marketValue
        : equityBasis === 'assessed_value'
          ? (p.assessed_value as number)
          : null;
  const { summary: lienSummary, current: currentLiens, history: lienHistory } = splitLiens(mortgages, equityValue, equityBasis);

  const signals = computeSignals({
    property: p,
    currentLiens,
    equityPercent: lienSummary.equityPercent,
    freeClear: lienSummary.freeClear,
    sales: saleHistory,
    foreclosures,
  });

  // Build MLS history
  const mlsHistoryMapped: MLSHistoryEntry[] = mlsHistory.map((m) => ({
    status: (m.status as string) ?? 'unknown',
    statusDate: (m.status_date as string) ?? '',
    lastStatusDate: (m.last_status_date as string) ?? null,
    price: (m.price as number) ?? null,
    pricePerSqft: (m.price_per_sqft as number) ?? null,
    daysOnMarket: (m.days_on_market as number) ?? null,
    agentName: (m.agent_name as string) ?? null,
    agentPhone: (m.agent_phone as string) ?? null,
    agentEmail: (m.agent_email as string) ?? null,
    brokerage: (m.brokerage as string) ?? null,
    listingType: (m.listing_type as string) ?? null,
    source: normalizePublicSource((m.source as string) ?? 'unknown'),
  }));

  // Build rent history
  const rentHistory: RentHistory[] = rentSnapshots.map((r) => ({
    date: (r.observed_at as string) ?? '',
    askingRent: (r.asking_rent as number) ?? (r.rent as number) ?? 0,
    effectiveRent: (r.effective_rent as number) ?? null,
    beds: (r.beds as number) ?? null,
    sqft: (r.sqft as number) ?? null,
  }));

  // Build sales
  const salesMapped: SaleRecord[] = saleHistory.map((s) => ({
    saleDate: (s.sale_date as string) ?? null,
    recordingDate: (s.recording_date as string) ?? '',
    saleAmount: (s.sale_amount as number) ?? (s.amount as number) ?? null,
    documentType: (s.document_type as string) ?? 'Unknown',
    documentNumber: (s.document_number as string) ?? null,
    bookPage: s.book && s.page ? `${s.book}/${s.page}` : null,
    buyerNames: (s.buyer_names as string) ?? (s.grantee as string) ?? '',
    sellerNames: (s.seller_names as string) ?? (s.grantor as string) ?? '',
    armsLength: (s.arms_length as boolean) ?? null,
    purchaseMethod: (s.purchase_method as 'cash' | 'financed') ?? null,
    downPayment: (s.down_payment as number) ?? null,
    ltv: (s.ltv as number) ?? null,
    source: normalizePublicSource((s.source as string) ?? 'unknown'),
  }));

  const saleKey = (sale: SaleRecord) => [
    sale.documentNumber ?? '',
    sale.recordingDate ?? '',
    sale.saleAmount ?? '',
  ].join('|');
  const seenSales = new Set(salesMapped.map(saleKey));

  for (const deed of mortgages.filter(isSaleDocument)) {
    const sale: SaleRecord = {
      saleDate: (deed.recording_date as string) ?? null,
      recordingDate: (deed.recording_date as string) ?? '',
      saleAmount: (deed.original_amount as number) ?? (deed.loan_amount as number) ?? null,
      documentType: (deed.document_type as string) ?? 'deed',
      documentNumber: (deed.document_number as string) ?? null,
      bookPage: (deed.book_page as string) ?? null,
      buyerNames: (deed.lender_name as string) ?? (deed.grantee_name as string) ?? '',
      sellerNames: (deed.borrower_name as string) ?? '',
      armsLength: null,
      purchaseMethod: null,
      downPayment: null,
      ltv: null,
      source: normalizePublicSource((deed.source_url as string) ?? 'recorder'),
    };
    const key = saleKey(sale);
    if (!seenSales.has(key)) {
      salesMapped.push(sale);
      seenSales.add(key);
    }
  }

  if ((p.last_sale_price != null || p.last_sale_date != null) && !hasMatchingActualSale(salesMapped, p)) {
    const sale: SaleRecord = {
      saleDate: (p.last_sale_date as string) ?? null,
      recordingDate: (p.last_sale_date as string) ?? '',
      saleAmount: (p.last_sale_price as number) ?? null,
      documentType: 'Assessor reported last sale',
      documentNumber: null,
      bookPage: null,
      buyerNames: '',
      sellerNames: '',
      armsLength: null,
      purchaseMethod: null,
      downPayment: null,
      ltv: null,
      source: normalizePublicSource((p.source as string) ?? 'assessor'),
    };
    const key = saleKey(sale);
    if (!seenSales.has(key)) salesMapped.push(sale);
  }

  salesMapped.sort((a, b) => (b.recordingDate || '').localeCompare(a.recordingDate || ''));

  // Demographics / FMR
  const fmr: FMRData | null = demographics ? {
    efficiency: (demographics.fmr_0 as number) ?? null,
    oneBed: (demographics.fmr_1 as number) ?? null,
    twoBed: (demographics.fmr_2 as number) ?? null,
    threeBed: (demographics.fmr_3 as number) ?? null,
    fourBed: (demographics.fmr_4 as number) ?? null,
    year: (demographics.fmr_year as number) ?? null,
    hudArea: (demographics.hud_area_name as string) ?? null,
    medianIncome: (demographics.median_income as number) ?? null,
  } : null;

  // Listing status
  const listDate = (latestListing?.list_date as string) ?? (latestListing?.first_seen_at as string) ?? null;
  const daysOnMarket = listDate
    ? Math.round((Date.now() - new Date(listDate).getTime()) / (1000 * 60 * 60 * 24))
    : null;

  // Meta
  const { dataSources, dataQuality, sourceMix } = buildMeta(p, c, mortgages, rentSnapshots, listingSignals, saleHistory, mlsHistory, demographics, foreclosures, publicSignals);
  const completeness = computeCompleteness(p, owner1, marketValue, lienSummary.openMortgageBalance, latestRent, latestListing, fmr);

  const bedrooms = typeof p.bedrooms === 'number' ? Math.max(0, Math.min(4, p.bedrooms)) : null;
  const fmrEstimate = !latestRent && fmr ? fmrRentForBedrooms(fmr, bedrooms) : null;
  const unitCount = normalizeUnitCount((p.total_units as number) ?? null);
  const rentPerDoor = (latestRent?.rent_per_door as number)
    ?? (latestRent?.asking_rent as number)
    ?? (latestRent?.rent as number)
    ?? fmrEstimate;
  const totalMonthlyRent = (latestRent?.total_monthly_rent as number)
    ?? (rentPerDoor && unitCount ? rentPerDoor * unitCount : null);
  const currentRent = totalMonthlyRent ?? rentPerDoor;
  const unitBasis = unitCount && unitCount > 1 ? 'per_unit' : (rentPerDoor ? 'total' : null);
  const rentSqft = (latestRent?.sqft as number) ?? (fmrEstimate ? livingSqft : null);
  const rentPsfBase = unitBasis === 'per_unit' ? rentPerDoor : currentRent;
  const publicSignalsMapped: PublicPropertySignal[] = publicSignals.map((signal) => ({
    type: (signal.signal_type as string) ?? 'unknown',
    status: (signal.status as string) ?? null,
    observedDate: (signal.observed_date as string) ?? null,
    amount: (signal.amount as number) ?? null,
    address: (signal.address as string) ?? null,
    source: (signal.source_system as string) ?? 'public_record',
  }));

  return {
    id: p.id as number,

    property: {
      address: (p.address as string) ?? '',
      city: (p.city as string) ?? '',
      state: (c.state_code as string) ?? '',
      zip: (p.zip as string) ?? '',
      county: (c.county_name as string) ?? '',
      parcelId: (p.parcel_id as string) ?? '',
      apn: (p.apn_formatted as string) ?? (p.parcel_id as string) ?? '',
      lat: (p.latitude as number) ?? (p.lat as number) ?? null,
      lng: (p.longitude as number) ?? (p.lng as number) ?? null,
      type: (p.property_type as string) ?? 'SFR',
      assetType: (p.asset_type as string) ?? null,
      assetSubtype: (p.asset_subtype as string) ?? null,
      unitCount,
      unitCountSource: (p.unit_count_source as string) ?? null,
      assetConfidence: (p.asset_confidence as string) ?? null,
      use: (p.property_use as string) ?? null,
      landUse: (p.land_use as string) ?? null,
      zoning: (p.zoning as string) ?? null,
      yearBuilt: (p.year_built as number) ?? null,
      yearRemodeled: (p.year_remodeled as number) ?? null,
      stories: (p.stories as number) ?? null,
      livingSqft,
      totalSqft: (p.total_sqft as number) ?? (p.sqft as number) ?? null,
      lotSqft: (p.lot_sqft as number) ?? null,
      lotAcres: (p.lot_acres as number) ?? null,
      lotDepthFeet: (p.lot_depth_feet as number) ?? null,
      lotWidthFeet: (p.lot_width_feet as number) ?? null,
      bedrooms: (p.bedrooms as number) ?? null,
      bathroomsFull: (p.bathrooms_full as number) ?? null,
      bathroomsHalf: (p.bathrooms_half as number) ?? null,
      totalRooms: (p.total_rooms as number) ?? null,
      basement: (p.basement as string) ?? null,
      basementSqft: (p.basement_sqft as number) ?? null,
      basementFinishedPct: (p.basement_finished_pct as number) ?? null,
      garage: (p.garage as string) ?? null,
      garageSqft: (p.garage_sqft as number) ?? null,
      garageSpaces: (p.garage_spaces as number) ?? null,
      heating: (p.heating as string) ?? null,
      fuelType: (p.fuel_type as string) ?? null,
      airConditioning: (p.air_conditioning as string) ?? null,
      exteriorWalls: (p.exterior_walls as string) ?? null,
      roofType: (p.roof_type as string) ?? null,
      foundation: (p.foundation as string) ?? null,
      condition: (p.condition as string) ?? null,
      fireplace: Boolean(p.fireplace),
      fireplaceCount: (p.fireplace_count as number) ?? null,
      pool: Boolean(p.pool),
      deck: Boolean(p.deck),
      deckSqft: (p.deck_sqft as number) ?? null,
      porch: Boolean(p.porch),
      porchSqft: (p.porch_sqft as number) ?? null,
      parkingSpaces: (p.parking_spaces as number) ?? null,
      hoa: Boolean(p.hoa),
      hoaAmount: (p.hoa_amount as number) ?? null,
      legalDescription: (p.legal_description as string) ?? null,
      subdivision: (p.subdivision as string) ?? null,
      lotNumber: (p.lot_number as string) ?? null,
      censusTract: (p.census_tract as string) ?? null,
      censusBlock: (p.census_block as string) ?? null,
      floodZone: Boolean(p.flood_zone),
      floodZoneType: (p.flood_zone_type as string) ?? null,
      pricePerSqft: marketValue && livingSqft ? Math.round((marketValue / livingSqft) * 100) / 100 : null,
    },

    ownership: {
      owner1,
      owner2,
      companyName: (p.company_name as string) ?? null,
      mailingAddress: p.mail_address ? {
        address: (p.mail_address as string) ?? '',
        city: (p.mail_city as string) ?? '',
        state: (p.mail_state as string) ?? '',
        zip: (p.mail_zip as string) ?? '',
      } : null,
      ownerOccupied: Boolean(p.owner_occupied),
      absenteeOwner: Boolean(p.absentee_owner),
      inStateAbsentee: Boolean(p.in_state_absentee),
      outOfStateAbsentee: Boolean(p.absentee_owner) && !Boolean(p.in_state_absentee),
      corporateOwned: Boolean(p.corporate_owned),
      ownershipStartDate: ownershipStart,
      ownershipLengthMonths,
    },

    valuation: {
      marketValue,
      assessedValue: (p.assessed_value as number) ?? null,
      appraisedLand: (p.appraised_land as number) ?? null,
      appraisedBuilding: (p.appraised_building as number) ?? null,
      taxableValue: (p.taxable_value as number) ?? null,
      annualTax: (p.annual_tax as number) ?? null,
      annualTaxSource: (p.annual_tax_source as 'county_auditor' | 'computed_from_millage') ?? null,
      taxYear: (p.tax_year as number) ?? null,
      taxDelinquentYear: (p.tax_delinquent_year as number) ?? null,
      assessmentYear: (p.assessment_year as number) ?? null,
      estimatedValue: (p.estimated_value as number) ?? null,
    },

    liens: {
      summary: lienSummary,
      current: currentLiens,
      history: lienHistory,
    },

    sales: salesMapped,

    rent: {
      currentRent,
      rentSource: latestRent ? 'scraped' : (fmrEstimate ? 'estimated_fmr' : null),
      unitBasis,
      unitCount,
      rentPerDoor,
      totalMonthlyRent,
      observedAt: (latestRent?.observed_at as string) ?? (fmrEstimate ? new Date().toISOString().slice(0, 10) : null),
      beds: (latestRent?.beds as number) ?? bedrooms,
      baths: (latestRent?.baths as number) ?? null,
      sqft: rentSqft,
      rentPerSqft: rentPsfBase && rentSqft ? Math.round((rentPsfBase / rentSqft) * 100) / 100 : null,
      fmr,
      history: rentHistory,
    },

    market: {
      onMarket: marketStatus === 'active' || marketStatus === 'pending',
      listPrice,
      listDate,
      daysOnMarket,
      status: marketStatus,
      listingSource: latestListing?.source ? normalizePublicSource(latestListing.source as string) : null,
      listingUrl: (latestListing?.listing_url as string) ?? (latestListing?.url as string) ?? null,
      agent: latestListing?.agent_name ? {
        name: (latestListing.agent_name as string) ?? null,
        firstName: (latestListing.agent_first_name as string) ?? null,
        lastName: (latestListing.agent_last_name as string) ?? null,
        phone: (latestListing.agent_phone as string) ?? null,
        email: (latestListing.agent_email as string) ?? null,
        brokerage: (latestListing.brokerage as string) ?? null,
        office: (latestListing.office as string) ?? null,
        licenseNumber: (latestListing.license_number as string) ?? null,
        licenseState: (latestListing.license_state as string) ?? null,
        licenseStatus: (latestListing.license_status as string) ?? null,
        licenseType: (latestListing.license_type as string) ?? null,
        source: (latestListing.agent_source as 'state_license_db' | 'mls') ?? null,
      } : null,
      history: mlsHistoryMapped,
    },

    publicSignals: publicSignalsMapped,

    signals,

    meta: {
      lastUpdated: new Date().toISOString(),
      dataSources,
      completeness,
      mxreNativeCoverage: sourceMix.mxreNativePercent,
      fallbackCoverage: sourceMix.fallbackPercent,
      sourceMix,
      dataQuality,
    },
  };
}

function mapMarketStatus(status: string | null): MXREPropertyResponse['market']['status'] {
  if (!status) return 'off_market';
  const lower = status.toLowerCase();
  if (lower.includes('active')) return 'active';
  if (lower.includes('pending') || lower.includes('contingent')) return 'pending';
  if (lower.includes('sold') || lower.includes('closed')) return 'sold';
  if (lower.includes('cancel') || lower.includes('withdrawn') || lower.includes('expired')) return 'cancelled';
  return 'off_market';
}

function normalizePublicSource(source: string | null): string {
  if (!source) return 'unknown';
  const lower = source.toLowerCase();
  if (lower.includes('stats.indiana.edu') || lower.includes('sdfdata')) return 'public_recorder';
  if (lower.includes('in-data-harvest') || lower.includes('assessor') || lower.includes('auditor') || lower.includes('parcel')) {
    return 'county_assessor';
  }
  if (lower.includes('redfin')) return 'redfin';
  if (lower.includes('zillow')) return 'zillow';
  if (lower.includes('realtor')) return 'realtor';
  if (lower.startsWith('http://') || lower.startsWith('https://')) return 'public_record';
  return source;
}

function hasMatchingActualSale(sales: SaleRecord[], property: Row): boolean {
  const lastSalePrice = typeof property.last_sale_price === 'number' ? property.last_sale_price : null;
  const lastSaleDate = typeof property.last_sale_date === 'string' ? property.last_sale_date : null;
  if (lastSalePrice == null && !lastSaleDate) return false;

  return sales.some((sale) => {
    if (sale.documentType.toLowerCase().includes('assessor')) return false;
    const amountMatches = lastSalePrice == null || sale.saleAmount === lastSalePrice;
    const saleDate = sale.saleDate ?? sale.recordingDate;
    const dateMatches = !lastSaleDate || datesWithinDays(saleDate, lastSaleDate, 10);
    return amountMatches && dateMatches;
  });
}

function datesWithinDays(a: string | null, b: string, days: number): boolean {
  if (!a || !b) return false;
  const left = new Date(`${a.slice(0, 10)}T00:00:00.000Z`).getTime();
  const right = new Date(`${b.slice(0, 10)}T00:00:00.000Z`).getTime();
  if (Number.isNaN(left) || Number.isNaN(right)) return false;
  return Math.abs(left - right) <= days * 24 * 60 * 60 * 1000;
}

function fmrRentForBedrooms(fmr: FMRData, bedrooms: number | null): number | null {
  const rents = [fmr.efficiency, fmr.oneBed, fmr.twoBed, fmr.threeBed, fmr.fourBed];
  const preferred = rents[bedrooms ?? 2];
  if (typeof preferred === 'number' && preferred > 0) return preferred;
  const fallback = fmr.twoBed ?? fmr.threeBed ?? fmr.oneBed ?? fmr.fourBed ?? fmr.efficiency;
  return typeof fallback === 'number' && fallback > 0 ? fallback : null;
}

function normalizeUnitCount(value: number | null): number | null {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return null;
  return Math.round(value);
}

function isSaleDocument(row: Row): boolean {
  const type = String(row.document_type ?? '').toLowerCase();
  return type.includes('deed') && !type.includes('trust') && !type.includes('release') && !type.includes('satisfaction');
}

function buildMeta(
  p: Row,
  c: Row,
  mortgages: Row[],
  rents: Row[],
  listings: Row[],
  sales: Row[],
  mls: Row[],
  demographics: Row | null,
  foreclosures: Row[],
  publicSignals: Row[] = [],
): { dataSources: string[]; dataQuality: DataQualityEntry[]; sourceMix: SourceMix } {
  const sources = new Set<string>();
  const quality: DataQualityEntry[] = [];
  const addQuality = (entry: DataQualityEntry) => {
    quality.push(enrichQualityEntry(entry));
  };

  // County assessor as property source
  const countyName = (c.county_name as string) ?? 'unknown';
  const stateCode = (c.state_code as string) ?? '';
  const assessorSource = `${countyName.toLowerCase().replace(/\s+/g, '_')}_county_assessor`;
  sources.add(assessorSource);

  // Property fields from assessor
  const assessorFields = ['address', 'market_value', 'assessed_value', 'year_built', 'sqft', 'bedrooms', 'owner_name'];
  for (const f of assessorFields) {
    if (p[f] != null) {
      addQuality({ field: f, source: assessorSource, type: 'actual' });
    }
  }

  if (p.annual_tax != null) {
    const taxSource = (p.annual_tax_source as string) ?? 'county_auditor';
    addQuality({ field: 'annual_tax', source: taxSource, type: taxSource === 'computed_from_millage' ? 'computed' : 'actual' });
  }

  // Mortgage sources
  for (const m of mortgages) {
    const src = (m.source as string) ?? (m.source_url as string) ?? 'recorder';
    sources.add(src);
  }
  if (mortgages.length > 0) {
    addQuality({ field: 'liens', source: (mortgages[0].source as string) ?? (mortgages[0].source_url as string) ?? 'recorder', type: 'actual' });
    if (mortgages.some((m) => m.current_balance != null || m.estimated_current_balance != null)) {
      const balanceRow = mortgages.find((m) => m.current_balance != null || m.estimated_current_balance != null);
      const balanceSource = (balanceRow?.source as string) ?? (balanceRow?.source_url as string) ?? 'recorder';
      addQuality({ field: 'liens.summary.openMortgageBalance', source: balanceSource, type: sourceImpliesFallback(balanceSource) ? 'fallback' : 'computed' });
      addQuality({ field: 'liens.summary.estimatedEquity', source: 'mxre_engine', type: 'computed' });
    }
  }

  // Rent sources
  for (const r of rents) {
    const src = (r.source as string) ?? 'scraped';
    sources.add(src);
  }
  if (rents.length > 0) {
    addQuality({ field: 'rent', source: (rents[0].source as string) ?? 'scraped', type: 'actual' });
  } else if (demographics) {
    addQuality({ field: 'rent', source: (demographics.source as string) ?? 'rent_baselines', type: 'estimated' });
  }

  // Listing sources
  for (const l of listings) {
    const src = (l.listing_source as string) ?? (l.source as string) ?? 'listing_site';
    sources.add(src);
  }
  if (listings.length > 0) {
    const latestListing = listings[0];
    const listingSource = (latestListing.listing_source as string) ?? (latestListing.source as string) ?? 'listing_site';
    addQuality({ field: 'market', source: listingSource, type: 'actual' });
    if (latestListing.listing_agent_email || latestListing.listing_agent_phone || latestListing.listing_agent_name) {
      addQuality({ field: 'market.agent', source: (latestListing.agent_contact_source as string) ?? listingSource, type: 'actual' });
    }
  }

  // Sale sources
  for (const s of sales) {
    const src = (s.source as string) ?? 'recorder';
    sources.add(src);
  }
  if (sales.length > 0) {
    addQuality({ field: 'sales', source: (sales[0].source as string) ?? 'recorder', type: 'actual' });
  }

  // MLS sources
  for (const m of mls) {
    const src = (m.source as string) ?? 'mls';
    sources.add(src);
  }
  if (mls.length > 0) {
    addQuality({ field: 'market.history', source: (mls[0].source as string) ?? 'mls', type: 'actual' });
  }

  // Demographics
  if (demographics) {
    sources.add('hud_fmr');
    addQuality({ field: 'fmr', source: 'hud_fmr', type: 'actual' });
  }

  // Foreclosures
  if (foreclosures.length > 0) {
    sources.add('foreclosure_filings');
    addQuality({ field: 'foreclosure', source: 'foreclosure_filings', type: 'actual' });
  }

  if (publicSignals.length > 0) {
    sources.add('indy_public_gis');
    addQuality({ field: 'public_signals', source: 'indy_public_gis', type: 'actual' });
  }

  // Computed signals
  addQuality({ field: 'signals', source: 'mxre_engine', type: 'computed' });
  addQuality({ field: 'equity', source: 'mxre_engine', type: 'computed' });

  return { dataSources: [...sources], dataQuality: quality, sourceMix: buildSourceMix(quality) };
}

function enrichQualityEntry(entry: DataQualityEntry): DataQualityEntry {
  const provider = classifyProvider(entry.source);
  const mxreNative = provider !== 'realestateapi' && provider !== 'zillow_api';
  const type = sourceImpliesFallback(entry.source) && entry.type !== 'computed' ? 'fallback' : entry.type;
  return {
    ...entry,
    type,
    provider,
    mxreNative,
    confidence: entry.confidence ?? confidenceForQuality(provider, type),
    observedAt: entry.observedAt ?? null,
  };
}

function classifyProvider(source: string): DataQualityEntry['provider'] {
  const lower = source.toLowerCase();
  if (lower.includes('realestateapi') || lower.includes('real_estate_api') || lower.includes('reapi')) return 'realestateapi';
  if (lower.includes('zillow_api') || lower.includes('zillow-api')) return 'zillow_api';
  if (lower.includes('hud') || lower.includes('fmr')) return 'hud';
  if (lower.includes('redfin') || lower.includes('realtor') || lower.includes('movoto') || lower.includes('zillow')) return 'listing_site';
  if (lower.includes('assessor') || lower.includes('auditor') || lower.includes('recorder') || lower.includes('fidlar') || lower.includes('county') || lower.includes('public')) return 'public_record';
  if (lower.includes('mxre')) return 'mxre';
  return 'unknown';
}

function sourceImpliesFallback(source: string): boolean {
  const provider = classifyProvider(source);
  return provider === 'realestateapi' || provider === 'zillow_api';
}

function confidenceForQuality(provider: DataQualityEntry['provider'], type: DataQualityEntry['type']): number {
  if (type === 'computed') return 80;
  if (type === 'estimated') return 55;
  if (provider === 'realestateapi' || provider === 'zillow_api') return 90;
  if (provider === 'public_record' || provider === 'hud') return 95;
  if (provider === 'listing_site') return 85;
  if (provider === 'mxre') return 90;
  return 60;
}

function buildSourceMix(quality: DataQualityEntry[]): SourceMix {
  const sections: Record<string, SourceMixSection> = {};
  const byProvider: Record<string, number> = {};
  const sectionPriority = (type: DataQualityEntry['type']) => {
    if (type === 'actual') return 4;
    if (type === 'fallback') return 3;
    if (type === 'computed') return 2;
    return 1;
  };

  for (const entry of quality) {
    const section = entry.field.split('.')[0] || entry.field;
    const current = sections[section];
    const provider = entry.provider ?? 'unknown';
    byProvider[provider] = (byProvider[provider] ?? 0) + 1;
    if (!current || sectionPriority(entry.type) > sectionPriority(current.type)) {
      sections[section] = {
        provider,
        source: entry.source,
        mxreNative: Boolean(entry.mxreNative),
        type: entry.type,
        confidence: entry.confidence ?? null,
      };
    }
  }

  const entries = Object.values(sections);
  const total = entries.length || 1;
  const mxreNative = entries.filter((entry) => entry.mxreNative).length;
  const fallback = entries.filter((entry) => !entry.mxreNative).length;
  const publicRecord = entries.filter((entry) => entry.provider === 'public_record' || entry.provider === 'hud').length;
  const paidProvider = entries.filter((entry) => entry.provider === 'realestateapi' || entry.provider === 'zillow_api').length;

  return {
    policy: 'mxre_first_with_paid_fallback',
    mxreNativePercent: Math.round((mxreNative / total) * 100),
    fallbackPercent: Math.round((fallback / total) * 100),
    publicRecordPercent: Math.round((publicRecord / total) * 100),
    paidProviderPercent: Math.round((paidProvider / total) * 100),
    bySection: sections,
    byProvider,
  };
}

function computeCompleteness(
  p: Row,
  owner1: ReturnType<typeof parseOwnerName>,
  marketValue: number | null,
  mortgageBalance: number | null,
  latestRent: Row | null,
  latestListing: Row | null,
  fmr: FMRData | null,
): number {
  const checks = [
    p.address != null,
    p.city != null,
    p.zip != null,
    p.parcel_id != null,
    p.year_built != null,
    (p.living_sqft ?? p.sqft) != null,
    p.bedrooms != null,
    p.bathrooms_full != null,
    (p.lot_sqft != null && (p.lot_sqft as number) > 0),
    marketValue != null,
    p.assessed_value != null,
    p.annual_tax != null,
    owner1 != null,
    (p.latitude ?? p.lat) != null,
    (p.longitude ?? p.lng) != null,
    p.property_type != null,
    mortgageBalance != null,
    latestRent != null,
    latestListing != null,
    fmr != null,
    p.basement != null,
    p.garage != null,
    p.heating != null,
    p.roof_type != null,
    p.foundation != null,
  ];

  const populated = checks.filter(Boolean).length;
  return Math.round((populated / checks.length) * 100);
}
