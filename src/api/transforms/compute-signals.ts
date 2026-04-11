import type { InvestorSignals, LienRecord } from '../types.js';

interface SignalInputs {
  property: Record<string, unknown>;
  currentLiens: LienRecord[];
  equityPercent: number | null;
  freeClear: boolean;
  sales: Record<string, unknown>[];
  foreclosures: Record<string, unknown>[];
}

/**
 * Derive ~25 investor signal booleans from property + lien + sale + foreclosure data.
 */
export function computeSignals(inputs: SignalInputs): InvestorSignals {
  const { property, currentLiens, equityPercent, freeClear, sales, foreclosures } = inputs;
  const now = new Date();

  // ── Equity signals ──────────────────────────────
  const highEquity = equityPercent != null && equityPercent > 40;
  const negativeEquity = equityPercent != null && equityPercent < 0;

  // ── Distress signals ────────────────────────────
  const preForeclosure = foreclosures.length > 0;
  const taxLien = currentLiens.some((l) => l.type === 'tax_lien');
  const judgment = currentLiens.some((l) => l.type === 'judgment');
  const mechLien = currentLiens.some((l) => l.type === 'mech_lien');

  // ── Owner situation ─────────────────────────────
  const absenteeOwner = Boolean(property.absentee_owner);
  const corporateOwned = Boolean(property.corporate_owned);
  const ownerOccupied = Boolean(property.owner_occupied);

  const ownershipStart = property.ownership_start_date as string | null;
  let longTermOwner = false;
  let recentPurchase = false;
  if (ownershipStart) {
    const startDate = new Date(ownershipStart);
    const monthsOwned = (now.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24 * 30.44);
    longTermOwner = monthsOwned >= 120; // 10+ years
    recentPurchase = monthsOwned <= 12;
  }

  // Check most recent sale for inheritance/death transfer
  const latestSale = sales.length > 0 ? sales[0] : null;
  const latestDocType = ((latestSale?.document_type as string) ?? '').toUpperCase();
  const inherited = latestDocType.includes('EXECUTOR') ||
    latestDocType.includes('ADMINISTRATOR') ||
    latestDocType.includes('PERSONAL REP') ||
    latestDocType.includes('PROBATE');
  const deathTransfer = latestDocType.includes('DEATH') ||
    latestDocType.includes('TOD') ||
    latestDocType.includes('TRANSFER ON DEATH') ||
    latestDocType.includes('SURVIVORSHIP');

  // ── Financing signals ───────────────────────────
  const adjustableRate = currentLiens.some((l) => l.interestRateType === 'adjustable');
  const privateLender = currentLiens.some((l) => {
    const lender = (l.lenderName ?? '').toUpperCase();
    return lender.includes('PRIVATE') ||
      lender.includes('INDIVIDUAL') ||
      lender.includes('HARD MONEY');
  });
  const sellerFinanced = currentLiens.some((l) => {
    const lender = (l.lenderName ?? '').toUpperCase();
    const loanType = (l.loanType ?? '').toUpperCase();
    return loanType.includes('SELLER') ||
      lender.includes('SELLER') ||
      loanType.includes('LAND CONTRACT') ||
      loanType.includes('CONTRACT FOR DEED');
  });

  // Cash purchase: most recent sale had no concurrent mortgage
  const cashPurchase = latestSale != null && (latestSale.purchase_method === 'cash' ||
    (sales.length > 0 && currentLiens.length === 0 && freeClear));

  // High LTV: any current mortgage has LTV > 80%
  const marketValue = property.market_value as number | null;
  const highLTV = marketValue != null && marketValue > 0 && currentLiens.some((l) => {
    const balance = l.currentBalance ?? l.originalAmount ?? 0;
    return (balance / marketValue) > 0.8;
  });

  // ── Property signals ────────────────────────────
  const vacant = Boolean(property.vacant);
  const mobileHome = Boolean(property.mobile_home) ||
    ((property.property_type as string) ?? '').toUpperCase().includes('MOBILE');
  const propertyType = ((property.property_type as string) ?? '').toUpperCase();
  const multifamily = propertyType.includes('MFH') ||
    propertyType.includes('MULTI') ||
    propertyType.includes('DUPLEX') ||
    propertyType.includes('TRIPLEX') ||
    propertyType.includes('FOURPLEX') ||
    propertyType.includes('APARTMENT');

  return {
    highEquity,
    freeClear,
    negativeEquity,
    preForeclosure,
    taxLien,
    judgment,
    mechLien,
    absenteeOwner,
    corporateOwned,
    ownerOccupied,
    longTermOwner,
    recentPurchase,
    inherited,
    deathTransfer,
    adjustableRate,
    privateLender,
    sellerFinanced,
    cashPurchase,
    highLTV,
    vacant,
    mobileHome,
    multifamily,
  };
}
