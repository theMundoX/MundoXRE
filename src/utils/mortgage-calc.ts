/**
 * Mortgage calculation utilities.
 *
 * RULES (per project standing instruction):
 *   1. Real recorder data ALWAYS wins.
 *   2. Estimates are allowed BUT MUST BE FLAGGED via rate_source = 'estimated'.
 *   3. When real OCR/API data arrives later, the estimate gets overwritten
 *      and rate_source flips to 'recorded'.
 *   4. Downstream consumers (BBC chat, dashboards, queries) MUST honour the
 *      rate_source flag and present estimated values clearly as estimates.
 *
 * Background: until 2026-04-06 this file was estimating interest rates
 * silently from a Freddie Mac PMMS yearly-average table without any flag,
 * which led to ~464K rows of fake-but-unmarked rate data in mortgage_records.
 * Audit showed only 3 distinct rate values across a 200-row sample (one per year),
 * confirming all of them were estimates. They've been retroactively flagged
 * via migration 005.
 */

// ─── Historical Average 30-Year Fixed Rates (Freddie Mac PMMS) ──────
// Annual averages — used ONLY when actual rate is unknown AND the caller
// explicitly accepts an estimate by reading rate_source from the result.

const HISTORICAL_RATES: Record<number, number> = {
  2000: 8.05, 2001: 6.97, 2002: 6.54, 2003: 5.83, 2004: 5.84,
  2005: 5.87, 2006: 6.41, 2007: 6.34, 2008: 6.03, 2009: 5.04,
  2010: 4.69, 2011: 4.45, 2012: 3.66, 2013: 3.98, 2014: 4.17,
  2015: 3.85, 2016: 3.65, 2017: 3.99, 2018: 4.54, 2019: 3.94,
  2020: 3.11, 2021: 2.96, 2022: 5.34, 2023: 6.81, 2024: 6.72,
  2025: 6.65, 2026: 6.50,
};

/**
 * Estimate the 30-year fixed mortgage rate for a given recording date.
 * Returns the Freddie Mac PMMS yearly average for that year (closest known).
 * Caller is responsible for setting rate_source = 'estimated' on the
 * resulting record.
 */
export function estimateRate(recordingDate: string | Date): number {
  const date = typeof recordingDate === "string" ? new Date(recordingDate) : recordingDate;
  const year = date.getFullYear();

  if (HISTORICAL_RATES[year]) return HISTORICAL_RATES[year];

  const years = Object.keys(HISTORICAL_RATES).map(Number).sort((a, b) => a - b);
  const closest = years.reduce((prev, curr) =>
    Math.abs(curr - year) < Math.abs(prev - year) ? curr : prev,
  );
  return HISTORICAL_RATES[closest];
}

/**
 * Calculate monthly mortgage payment (principal + interest).
 * Standard amortization formula: M = P * [r(1+r)^n] / [(1+r)^n - 1]
 */
export function monthlyPayment(
  principal: number,
  annualRate: number,
  termMonths: number,
): number {
  if (principal <= 0 || annualRate <= 0 || termMonths <= 0) return 0;

  const r = annualRate / 100 / 12; // monthly rate
  const n = termMonths;
  const payment = principal * (r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);
  return Math.round(payment);
}

/**
 * Calculate remaining loan balance after N months of payments.
 * B = P * [(1+r)^n - (1+r)^p] / [(1+r)^n - 1]
 * where p = payments made
 */
export function remainingBalance(
  principal: number,
  annualRate: number,
  termMonths: number,
  paymentsMade: number,
): number {
  if (principal <= 0 || annualRate <= 0 || termMonths <= 0) return 0;
  if (paymentsMade >= termMonths) return 0;

  const r = annualRate / 100 / 12;
  const n = termMonths;
  const p = paymentsMade;

  const balance =
    principal *
    (Math.pow(1 + r, n) - Math.pow(1 + r, p)) /
    (Math.pow(1 + r, n) - 1);

  return Math.max(0, Math.round(balance));
}

/**
 * Calculate months elapsed since a recording date.
 */
export function monthsElapsed(recordingDate: string | Date, asOf?: Date): number {
  const start = typeof recordingDate === "string" ? new Date(recordingDate) : recordingDate;
  const end = asOf || new Date();

  const years = end.getFullYear() - start.getFullYear();
  const months = end.getMonth() - start.getMonth();
  return Math.max(0, years * 12 + months);
}

/**
 * Calculate maturity date from recording date + term.
 */
export function maturityDate(recordingDate: string | Date, termMonths: number): string {
  const start = typeof recordingDate === "string" ? new Date(recordingDate) : recordingDate;
  const maturity = new Date(start);
  maturity.setMonth(maturity.getMonth() + termMonths);
  return maturity.toISOString().split("T")[0];
}

export type RateSource = "estimated" | "recorded";

export interface MortgageFields {
  interest_rate: number | null;
  term_months: number;
  estimated_monthly_payment: number | null;
  estimated_current_balance: number | null;
  balance_as_of: string;
  maturity_date: string | null;
  rate_source: RateSource | null;
}

/**
 * Compute all mortgage fields from original amount + recording date.
 *
 * If `params.interestRate` is supplied, that's treated as a REAL recorded
 * rate from a county recorder API or OCR'd document, and the result is
 * marked rate_source = 'recorded'.
 *
 * If `params.interestRate` is NOT supplied, we fall back to the Freddie Mac
 * PMMS yearly average and mark rate_source = 'estimated'. Downstream code
 * MUST honour this flag.
 *
 * Either way, the rate_source field is ALWAYS set on the returned record.
 */
export function computeMortgageFields(params: {
  originalAmount: number;
  recordingDate: string;
  interestRate?: number;
  termMonths?: number;
  asOf?: Date;
}): MortgageFields {
  const term = params.termMonths ?? 360; // Default 30-year
  const asOf = params.asOf ?? new Date();

  // Determine rate + source
  let rate: number;
  let rateSource: RateSource;
  if (typeof params.interestRate === "number" && params.interestRate > 0) {
    rate = params.interestRate;
    rateSource = "recorded";
  } else {
    rate = estimateRate(params.recordingDate);
    rateSource = "estimated";
  }

  const payment = monthlyPayment(params.originalAmount, rate, term);
  const elapsed = monthsElapsed(params.recordingDate, asOf);
  const balance = remainingBalance(params.originalAmount, rate, term, elapsed);
  const maturity = maturityDate(params.recordingDate, term);

  return {
    interest_rate: rate,
    term_months: term,
    estimated_monthly_payment: payment,
    estimated_current_balance: balance,
    balance_as_of: asOf.toISOString().split("T")[0],
    maturity_date: maturity,
    rate_source: rateSource,
  };
}

/**
 * Helper for the OCR/labeling pipeline:
 * given a record that currently has rate_source='estimated', accept a real
 * recorded rate from a document and return the updated fields with
 * rate_source='recorded'. The caller writes this back to mortgage_records.
 */
export function upgradeEstimateToRecorded(
  existing: { recording_date: string; original_amount: number; term_months?: number },
  recordedInterestRate: number,
  asOf: Date = new Date(),
): MortgageFields {
  return computeMortgageFields({
    originalAmount: existing.original_amount,
    recordingDate: existing.recording_date,
    interestRate: recordedInterestRate,
    termMonths: existing.term_months,
    asOf,
  });
}
