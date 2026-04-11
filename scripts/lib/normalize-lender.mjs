/**
 * Normalize a raw lender name so fuzzy matching between county-recorded
 * mortgages and HMDA panel entries can collapse punctuation/case differences.
 *
 * Rules (intentionally conservative — we keep distinguishing tokens like
 * "BANK" and "NATIONAL ASSOCIATION"):
 *   1. Uppercase
 *   2. Replace any non-alphanumeric char with a single space
 *   3. Collapse runs of whitespace
 *   4. Trim leading/trailing whitespace
 *
 * Example:
 *   "U.S. Bank, N.A."                    -> "U S BANK N A"
 *   "US BANK NATIONAL ASSOCIATION"       -> "US BANK NATIONAL ASSOCIATION"
 *   "Wells Fargo Bank, N.A."             -> "WELLS FARGO BANK N A"
 */
export function normalizeLenderName(rawName) {
  if (rawName === null || rawName === undefined) return "";
  return String(rawName)
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
