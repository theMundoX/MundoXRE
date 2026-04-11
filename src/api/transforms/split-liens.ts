import type { LienRecord, LienSummary } from '../types.js';

/** Map a DB mortgage_records row to a LienRecord
 * Actual mortgage_records columns:
 *   loan_amount, original_amount, estimated_current_balance, estimated_monthly_payment,
 *   interest_rate, interest_rate_type, term_months, maturity_date, recording_date,
 *   document_number, document_type, borrower_name, lender_name, loan_type,
 *   source_url, balance_as_of, grantor, grantee
 */
function mapMortgageRow(row: Record<string, unknown>): LienRecord {
  const hasBalance = row.estimated_current_balance != null;
  return {
    type: mapDocumentType(row.document_type as string | null),
    open: (row.open as boolean) ?? true,
    position: (row.position as number) ?? null,
    originalAmount: (row.loan_amount as number) ?? (row.original_amount as number) ?? null,
    currentBalance: (row.estimated_current_balance as number) ?? null,
    balanceSource: hasBalance ? 'computed' : null,
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
    source: (row.source_url as string) ?? 'unknown',
  };
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
  marketValue: number | null,
): { summary: LienSummary; current: LienRecord[]; history: LienRecord[] } {
  const all = mortgageRows.map(mapMortgageRow);
  const now = new Date();

  const current: LienRecord[] = [];
  const history: LienRecord[] = [];

  for (const lien of all) {
    // Satisfactions/assignments are always history
    if (lien.type === 'satisfaction' || lien.type === 'assignment' || lien.type === 'deed') {
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

  const openMortgageBalance = openMortgages.reduce((sum, l) => {
    return sum + (l.currentBalance ?? l.originalAmount ?? 0);
  }, 0) || null;

  const totalMonthlyPayment = openMortgages.reduce((sum, l) => {
    return sum + (l.monthlyPayment ?? 0);
  }, 0) || null;

  const estimatedEquity = marketValue != null && openMortgageBalance != null
    ? marketValue - openMortgageBalance
    : null;

  const equityPercent = marketValue != null && marketValue > 0 && estimatedEquity != null
    ? Math.round((estimatedEquity / marketValue) * 100)
    : null;

  const summary: LienSummary = {
    openMortgageBalance,
    totalMonthlyPayment,
    estimatedEquity,
    equityPercent,
    freeClear: openMortgages.length === 0,
    lienCount: all.length,
    openLienCount: current.length,
  };

  return { summary, current, history };
}
