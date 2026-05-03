import type { LienRecord, LienSummary } from '../types.js';

/** Map a DB mortgage_records row to a LienRecord
 * Actual mortgage_records columns:
 *   loan_amount, original_amount, estimated_current_balance, estimated_monthly_payment,
 *   interest_rate, interest_rate_type, term_months, maturity_date, recording_date,
 *   document_number, document_type, borrower_name, lender_name, loan_type,
 *   source_url, balance_as_of, grantor, grantee
 */
function mapMortgageRow(row: Record<string, unknown>): LienRecord {
  const balance = estimateCurrentBalance(row);
  return {
    type: mapDocumentType(row.document_type as string | null),
    open: (row.open as boolean) ?? true,
    position: (row.position as number) ?? null,
    originalAmount: (row.loan_amount as number) ?? (row.original_amount as number) ?? null,
    currentBalance: balance.currentBalance,
    balanceSource: balance.balanceSource,
    interestRate: (row.interest_rate as number) ?? null,
    interestRateSource: (row.rate_source as LienRecord['interestRateSource']) ?? null,
    interestRateConfidence: (row.rate_match_confidence as number) ?? null,
    interestRateType: mapRateType(row.interest_rate_type as string | null),
    term: (row.term_months as number) ?? null,
    monthlyPayment: (row.estimated_monthly_payment as number) ?? null,
    maturityDate: (row.maturity_date as string) ?? null,
    recordingDate: (row.recording_date as string) ?? '',
    documentNumber: (row.document_number as string) ?? null,
    bookPage: (row.book_page as string) ?? null,
    lenderName: (row.lender_name as string) ?? null,
    lenderType: (row.lender_type as string) ?? null,
    borrowerName: (row.borrower_name as string) ?? (row.grantee_name as string) ?? null,
    loanType: (row.loan_type as string) ?? null,
    source: normalizePublicSource((row.source_url as string) ?? null),
  };
}

function normalizePublicSource(source: string | null): string {
  if (!source) return 'unknown';
  const lower = source.toLowerCase();
  if (lower.includes('stats.indiana.edu') || lower.includes('sdfdata')) return 'public_recorder';
  if (lower.includes('assessor') || lower.includes('auditor') || lower.includes('parcel')) return 'county_assessor';
  if (lower.startsWith('http://') || lower.startsWith('https://')) return 'public_record';
  return source;
}

function mapDocumentType(docType: string | null): LienRecord['type'] {
  if (!docType) return 'mortgage';
  const upper = docType.toUpperCase();
  if (upper.includes('HELOC') || upper.includes('HOME EQUITY')) return 'heloc';
  if (upper.includes('TAX LIEN') || upper.includes('TAX CERTIFICATE')) return 'tax_lien';
  if (upper.includes('MECHANIC') || upper.includes('MATERIALMAN')) return 'mech_lien';
  if (upper.includes('JUDGMENT')) return 'judgment';
  if (upper.includes('SATISFACTION') || upper.includes('RELEASE') || upper.includes('DISCHARGE')) return 'satisfaction';
  if (upper.includes('ASSIGNMENT')) return 'assignment';
  if (upper.includes('DEED')) return 'deed';
  return 'mortgage';
}

function mapRateType(rateType: string | null): 'fixed' | 'adjustable' | null {
  if (!rateType) return null;
  const upper = rateType.toUpperCase();
  if (upper.includes('ADJ') || upper.includes('ARM') || upper.includes('VARIABLE')) return 'adjustable';
  if (upper.includes('FIX')) return 'fixed';
  return null;
}

/**
 * Separate mortgage_records into current (open) vs history (closed/satisfied),
 * assign positions to current liens, and compute summary stats.
 */
export function splitLiens(
  mortgageRows: Record<string, unknown>[],
  equityValue: number | null,
  equityBasis: LienSummary['equityBasis'] = 'market_value',
): { summary: LienSummary; current: LienRecord[]; history: LienRecord[] } {
  const all = mortgageRows.map(mapMortgageRow);
  const now = new Date();

  const current: LienRecord[] = [];
  const history: LienRecord[] = [];

  for (const lien of all) {
    // Deeds are sale/transfer events, not liens. They are mapped into the sales array upstream.
    if (lien.type === 'deed') {
      continue;
    }

    // Satisfactions/assignments are lien history, but never active liens.
    if (lien.type === 'satisfaction' || lien.type === 'assignment') {
      history.push(lien);
      continue;
    }

    const isOpen = lien.open || (lien.maturityDate && new Date(lien.maturityDate) > now);

    if (isOpen) {
      current.push(lien);
    } else {
      history.push(lien);
    }
  }

  // Sort current by recording date ascending (oldest = 1st position)
  current.sort((a, b) => new Date(a.recordingDate).getTime() - new Date(b.recordingDate).getTime());
  for (let i = 0; i < current.length; i++) {
    current[i].position = i + 1;
  }

  // Compute summary
  const mortgageTypes = new Set<LienRecord['type']>(['mortgage', 'heloc']);
  const openMortgages = current.filter((l) => mortgageTypes.has(l.type));

  const openMortgageBalance = openMortgages.reduce((sum, l) => sum + (l.currentBalance ?? 0), 0) || null;
  const openMortgageBalanceSource = summarizeBalanceSource(openMortgages);
  const openMortgageBalanceConfidence = confidenceForBalanceSource(openMortgageBalanceSource);

  const totalMonthlyPayment = openMortgages.reduce((sum, l) => {
    return sum + (l.monthlyPayment ?? 0);
  }, 0) || null;

  const estimatedEquity = equityValue != null && openMortgageBalance != null
    ? equityValue - openMortgageBalance
    : null;

  const equityPercent = equityValue != null && equityValue > 0 && estimatedEquity != null
    ? Math.round((estimatedEquity / equityValue) * 100)
    : null;

  const summary: LienSummary = {
    openMortgageBalance,
    openMortgageBalanceSource,
    openMortgageBalanceConfidence,
    totalMonthlyPayment,
    estimatedEquity,
    equityPercent,
    equityBasis: equityValue != null ? equityBasis : null,
    equityValue,
    freeClear: openMortgages.length === 0,
    lienCount: current.length + history.length,
    openLienCount: current.length,
  };

  return { summary, current, history };
}

function estimateCurrentBalance(row: Record<string, unknown>): Pick<LienRecord, 'currentBalance' | 'balanceSource'> {
  const explicit = positiveNumber(row.estimated_current_balance);
  if (explicit != null) return { currentBalance: explicit, balanceSource: 'computed' };

  const original = positiveNumber(row.loan_amount) ?? positiveNumber(row.original_amount);
  if (original == null) return { currentBalance: null, balanceSource: null };

  const rate = positiveNumber(row.interest_rate);
  const term = positiveNumber(row.term_months);
  const recordingDate = typeof row.recording_date === 'string' ? row.recording_date : null;
  const elapsedMonths = recordingDate ? monthsSince(recordingDate) : null;
  if (rate != null && term != null && term > 0 && elapsedMonths != null && elapsedMonths > 0) {
    const balance = amortizedBalance(original, rate, term, Math.min(elapsedMonths, term));
    if (balance != null) return { currentBalance: balance, balanceSource: 'amortized_estimate' };
  }

  // Conservative fallback: overstates debt and understates equity until a better balance is available.
  return { currentBalance: original, balanceSource: 'original_amount_proxy' };
}

function positiveNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : null;
}

function monthsSince(date: string): number | null {
  const started = new Date(`${date.slice(0, 10)}T00:00:00.000Z`).getTime();
  if (Number.isNaN(started)) return null;
  return Math.max(0, Math.floor((Date.now() - started) / (1000 * 60 * 60 * 24 * 30.44)));
}

function amortizedBalance(original: number, annualRate: number, termMonths: number, elapsedMonths: number): number | null {
  const monthlyRate = annualRate > 1 ? annualRate / 100 / 12 : annualRate / 12;
  if (!Number.isFinite(monthlyRate) || monthlyRate < 0) return null;
  if (monthlyRate === 0) return Math.max(0, Math.round(original * (1 - elapsedMonths / termMonths)));
  const payment = original * (monthlyRate * Math.pow(1 + monthlyRate, termMonths)) / (Math.pow(1 + monthlyRate, termMonths) - 1);
  const remaining = original * Math.pow(1 + monthlyRate, elapsedMonths)
    - payment * ((Math.pow(1 + monthlyRate, elapsedMonths) - 1) / monthlyRate);
  return Number.isFinite(remaining) ? Math.max(0, Math.round(remaining)) : null;
}

function summarizeBalanceSource(openMortgages: LienRecord[]): LienSummary['openMortgageBalanceSource'] {
  const sources = [...new Set(openMortgages.map((lien) => lien.balanceSource).filter(Boolean))] as Exclude<LienSummary['openMortgageBalanceSource'], 'mixed' | null>[];
  if (sources.length === 0) return null;
  if (sources.length === 1) return sources[0];
  return 'mixed';
}

function confidenceForBalanceSource(source: LienSummary['openMortgageBalanceSource']): number | null {
  switch (source) {
    case 'actual': return 95;
    case 'computed': return 75;
    case 'amortized_estimate': return 65;
    case 'original_amount_proxy': return 35;
    case 'mixed': return 50;
    default: return null;
  }
}
