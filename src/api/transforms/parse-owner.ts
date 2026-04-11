import type { OwnerName } from '../types.js';

const CORPORATE_KEYWORDS = [
  'LLC', 'INC', 'CORP', 'CORPORATION', 'LP', 'LTD', 'LIMITED',
  'TRUST', 'ESTATE', 'BANK', 'INVESTMENTS', 'HOLDINGS', 'PROPERTIES',
  'REALTY', 'GROUP', 'FUND', 'CAPITAL', 'ASSOCIATES', 'PARTNERS',
  'COMPANY', 'CO', 'ENTERPRISES', 'MANAGEMENT', 'DEVELOPMENT',
  'VENTURES', 'SERVICES', 'FINANCIAL', 'NATIONAL', 'FEDERAL',
];

const TRUST_KEYWORDS = ['TRUST', 'ESTATE', 'REVOCABLE', 'IRREVOCABLE', 'LIVING TRUST', 'FAMILY TRUST'];

/**
 * Detect if a name is corporate, trust, or individual.
 */
function detectType(raw: string): 'corporate' | 'trust' | 'individual' {
  const upper = raw.toUpperCase();
  // Check trust first (more specific)
  for (const kw of TRUST_KEYWORDS) {
    if (upper.includes(kw)) return 'trust';
  }
  for (const kw of CORPORATE_KEYWORDS) {
    // Match as whole word to avoid false positives
    const regex = new RegExp(`\\b${kw}\\b`);
    if (regex.test(upper)) return 'corporate';
  }
  return 'individual';
}

/**
 * Title-case a string: "SMITH" → "Smith", "MARY ANN" → "Mary Ann"
 */
function titleCase(s: string): string {
  return s
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Parse an owner name from assessor data into structured form.
 *
 * Common formats:
 *   "LUCKETT CHRISTOPHER J"        → { firstName: "Christopher J", lastName: "Luckett", type: "individual" }
 *   "SMITH, JOHN"                  → { firstName: "John", lastName: "Smith", type: "individual" }
 *   "ACME HOLDINGS LLC"            → { fullName: "Acme Holdings Llc", type: "corporate" }
 *   "SMITH FAMILY TRUST"           → { fullName: "Smith Family Trust", type: "trust" }
 */
export function parseOwnerName(raw: string | null | undefined): OwnerName | null {
  if (!raw || !raw.trim()) return null;

  const cleaned = raw.trim().replace(/\s+/g, ' ');
  const ownerType = detectType(cleaned);

  // Corporate or trust — return full name, no first/last split
  if (ownerType === 'corporate' || ownerType === 'trust') {
    return {
      fullName: titleCase(cleaned),
      firstName: null,
      lastName: null,
      type: ownerType,
    };
  }

  // Individual: try "LASTNAME, FIRSTNAME" pattern first
  if (cleaned.includes(',')) {
    const [lastRaw, ...firstParts] = cleaned.split(',');
    const lastName = titleCase(lastRaw.trim());
    const firstName = titleCase(firstParts.join(',').trim());
    return {
      fullName: `${firstName} ${lastName}`.trim(),
      firstName: firstName || null,
      lastName: lastName || null,
      type: 'individual',
    };
  }

  // Individual: "LASTNAME FIRSTNAME MIDDLE" pattern (most common in assessor data)
  const parts = cleaned.split(' ');
  if (parts.length === 1) {
    const name = titleCase(parts[0]);
    return {
      fullName: name,
      firstName: null,
      lastName: name,
      type: 'individual',
    };
  }

  const lastName = titleCase(parts[0]);
  const firstName = titleCase(parts.slice(1).join(' '));

  return {
    fullName: `${firstName} ${lastName}`.trim(),
    firstName,
    lastName,
    type: 'individual',
  };
}
