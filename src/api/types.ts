// ── MXRE Property API Response Types ────────────────────────────

export interface OwnerName {
  fullName: string;
  firstName: string | null;
  lastName: string | null;
  type: 'individual' | 'corporate' | 'trust';
}

export interface MailingAddress {
  address: string;
  city: string;
  state: string;
  zip: string;
}

export interface AgentInfo {
  name: string | null;
  firstName: string | null;
  lastName: string | null;
  phone: string | null;
  email: string | null;
  brokerage: string | null;
  office: string | null;
  licenseNumber: string | null;
  licenseState: string | null;
  licenseStatus: string | null;
  licenseType: string | null;
  source: 'state_license_db' | 'mls' | null;
}

export interface LienRecord {
  type: 'mortgage' | 'heloc' | 'tax_lien' | 'mech_lien' | 'judgment' | 'satisfaction' | 'assignment' | 'deed';
  open: boolean;
  position: number | null;
  originalAmount: number | null;
  currentBalance: number | null;
  balanceSource: 'actual' | 'computed' | null;
  interestRate: number | null;
  interestRateSource: 'hmda_match' | 'agency_lld' | 'pmms_weekly' | 'manual' | null;
  interestRateConfidence: number | null;  // 0-100; 77+=HMDA match, 40=PMMS baseline
  interestRateType: 'fixed' | 'adjustable' | null;
  term: number | null;
  monthlyPayment: number | null;
  maturityDate: string | null;
  recordingDate: string;
  documentNumber: string | null;
  bookPage: string | null;
  lenderName: string | null;
  lenderType: string | null;
  borrowerName: string | null;
  loanType: string | null;
  source: string;
}

export interface SaleRecord {
  saleDate: string | null;
  recordingDate: string;
  saleAmount: number | null;
  documentType: string;
  documentNumber: string | null;
  bookPage: string | null;
  buyerNames: string;
  sellerNames: string;
  armsLength: boolean | null;
  purchaseMethod: 'cash' | 'financed' | null;
  downPayment: number | null;
  ltv: number | null;
  source: string;
}

export interface LienSummary {
  openMortgageBalance: number | null;
  totalMonthlyPayment: number | null;
  estimatedEquity: number | null;
  equityPercent: number | null;
  freeClear: boolean;
  lienCount: number;
  openLienCount: number;
}

export interface RentHistory {
  date: string;
  askingRent: number;
  effectiveRent: number | null;
  beds: number | null;
  sqft: number | null;
}

export interface FMRData {
  efficiency: number | null;
  oneBed: number | null;
  twoBed: number | null;
  threeBed: number | null;
  fourBed: number | null;
  year: number | null;
  hudArea: string | null;
  medianIncome: number | null;
}

export interface MLSHistoryEntry {
  status: string;
  statusDate: string;
  lastStatusDate: string | null;
  price: number | null;
  pricePerSqft: number | null;
  daysOnMarket: number | null;
  agentName: string | null;
  agentPhone: string | null;
  agentEmail: string | null;
  brokerage: string | null;
  listingType: string | null;
  source: string;
}

export interface PublicPropertySignal {
  type: string;
  status: string | null;
  observedDate: string | null;
  amount: number | null;
  address: string | null;
  source: string;
}

export interface InvestorSignals {
  // Equity
  highEquity: boolean;
  freeClear: boolean;
  negativeEquity: boolean;
  // Distress
  preForeclosure: boolean;
  taxLien: boolean;
  judgment: boolean;
  mechLien: boolean;
  // Owner situation
  absenteeOwner: boolean;
  corporateOwned: boolean;
  ownerOccupied: boolean;
  longTermOwner: boolean;
  recentPurchase: boolean;
  inherited: boolean;
  deathTransfer: boolean;
  // Financing
  adjustableRate: boolean;
  privateLender: boolean;
  sellerFinanced: boolean;
  cashPurchase: boolean;
  highLTV: boolean;
  // Property
  vacant: boolean;
  mobileHome: boolean;
  multifamily: boolean;
}

export interface DataQualityEntry {
  field: string;
  source: string;
  type: 'actual' | 'computed' | 'estimated';
}

export interface MXREPropertyResponse {
  id: number;

  property: {
    address: string;
    city: string;
    state: string;
    zip: string;
    county: string;
    parcelId: string;
    apn: string;
    lat: number | null;
    lng: number | null;
    type: string;
    use: string | null;
    landUse: string | null;
    zoning: string | null;
    yearBuilt: number | null;
    yearRemodeled: number | null;
    stories: number | null;
    livingSqft: number | null;
    totalSqft: number | null;
    lotSqft: number | null;
    lotAcres: number | null;
    lotDepthFeet: number | null;
    lotWidthFeet: number | null;
    bedrooms: number | null;
    bathroomsFull: number | null;
    bathroomsHalf: number | null;
    totalRooms: number | null;
    basement: string | null;
    basementSqft: number | null;
    basementFinishedPct: number | null;
    garage: string | null;
    garageSqft: number | null;
    garageSpaces: number | null;
    heating: string | null;
    fuelType: string | null;
    airConditioning: string | null;
    exteriorWalls: string | null;
    roofType: string | null;
    foundation: string | null;
    condition: string | null;
    fireplace: boolean;
    fireplaceCount: number | null;
    pool: boolean;
    deck: boolean;
    deckSqft: number | null;
    porch: boolean;
    porchSqft: number | null;
    parkingSpaces: number | null;
    hoa: boolean;
    hoaAmount: number | null;
    legalDescription: string | null;
    subdivision: string | null;
    lotNumber: string | null;
    censusTract: string | null;
    censusBlock: string | null;
    floodZone: boolean;
    floodZoneType: string | null;
    pricePerSqft: number | null;
  };

  ownership: {
    owner1: OwnerName | null;
    owner2: OwnerName | null;
    companyName: string | null;
    mailingAddress: MailingAddress | null;
    ownerOccupied: boolean;
    absenteeOwner: boolean;
    inStateAbsentee: boolean;
    outOfStateAbsentee: boolean;
    corporateOwned: boolean;
    ownershipStartDate: string | null;
    ownershipLengthMonths: number | null;
  };

  valuation: {
    marketValue: number | null;
    assessedValue: number | null;
    appraisedLand: number | null;
    appraisedBuilding: number | null;
    taxableValue: number | null;
    annualTax: number | null;
    annualTaxSource: 'county_auditor' | 'computed_from_millage' | null;
    taxYear: number | null;
    taxDelinquentYear: number | null;
    assessmentYear: number | null;
    estimatedValue: number | null;
  };

  liens: {
    summary: LienSummary;
    current: LienRecord[];
    history: LienRecord[];
  };

  sales: SaleRecord[];

  rent: {
    currentRent: number | null;
    rentSource: 'scraped' | 'estimated_fmr' | null;
    observedAt: string | null;
    beds: number | null;
    baths: number | null;
    sqft: number | null;
    rentPerSqft: number | null;
    fmr: FMRData | null;
    history: RentHistory[];
  };

  market: {
    onMarket: boolean;
    listPrice: number | null;
    listDate: string | null;
    daysOnMarket: number | null;
    status: 'active' | 'pending' | 'sold' | 'cancelled' | 'off_market';
    listingSource: string | null;
    listingUrl: string | null;
    agent: AgentInfo | null;
    history: MLSHistoryEntry[];
  };

  publicSignals: PublicPropertySignal[];

  signals: InvestorSignals;

  meta: {
    lastUpdated: string;
    dataSources: string[];
    completeness: number;
    dataQuality: DataQualityEntry[];
  };
}

/** Lightweight summary for search results */
export interface PropertySummary {
  id: number;
  address: string;
  city: string;
  state: string;
  zip: string;
  county: string;
  type: string;
  marketValue: number | null;
  assessedValue: number | null;
  ownerName: string | null;
  yearBuilt: number | null;
  livingSqft: number | null;
  bedrooms: number | null;
  bathroomsFull: number | null;
  ownerOccupied: boolean;
  absenteeOwner: boolean;
  taxDelinquent: boolean;
  parcelId: string | null;
}
