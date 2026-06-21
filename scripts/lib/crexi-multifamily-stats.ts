export type CrexiMultifamilyStats = {
  units: number | null;
  apartmentUnits: number | null;
  buildings: number | null;
  stories: number | null;
  yearBuilt: number | null;
  yearsBuilt: number[];
  squareFootage: number | null;
  occupancyPct: number | null;
  occupancyStatus: string | null;
  tenancy: string | null;
  zoning: string | null;
  keys: number | null;
  pricePerUnit: number | null;
  pricePerSqft: number | null;
  avgUnitSqft: number | null;
  evidence: Record<string, string | number | string[] | null>;
};

function detailValue(detail: Record<string, unknown> | undefined, label: RegExp): unknown {
  const summary = Array.isArray(detail?.summaryDetails) ? detail.summaryDetails as Record<string, unknown>[] : [];
  const match = summary.find((item) => label.test(String(item.label ?? item.key ?? "")));
  if (match) return match.value ?? match.display;
  const details = detail?.details && typeof detail.details === "object" ? detail.details as Record<string, unknown> : {};
  const detailsMatch = Object.entries(details).find(([key]) => label.test(key));
  if (detailsMatch) return detailsMatch[1];
  const directMatch = Object.entries(detail ?? {}).find(([key]) => label.test(key));
  return directMatch?.[1];
}

function cleanText(value: unknown): string | null {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text || null;
}

function numberFrom(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = cleanText(value);
  if (!text) return null;
  const match = text.replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function integerFrom(value: unknown): number | null {
  const parsed = numberFrom(value);
  return parsed !== null ? Math.round(parsed) : null;
}

function yearsFrom(value: unknown): number[] {
  const text = cleanText(value);
  if (!text) return [];
  return [...text.matchAll(/\b(18\d{2}|19\d{2}|20\d{2})\b/g)]
    .map((match) => Number(match[1]))
    .filter((year) => Number.isFinite(year));
}

function occupancy(value: unknown): { pct: number | null; status: string | null } {
  const text = cleanText(value);
  if (!text) return { pct: null, status: null };
  if (/vacant/i.test(text)) return { pct: 0, status: "vacant" };
  if (/full|100\s*%/i.test(text)) return { pct: 100, status: "occupied" };
  const parsed = numberFrom(text);
  if (parsed === 0) return { pct: 0, status: "vacant" };
  if (parsed === 100) return { pct: 100, status: "occupied" };
  return {
    pct: parsed !== null && parsed >= 0 && parsed <= 100 ? parsed : null,
    status: parsed !== null ? "partial" : text,
  };
}

export function parseCrexiMultifamilyStats(
  detail: Record<string, unknown> | undefined,
  listPrice?: number | null,
): CrexiMultifamilyStats {
  const units = integerFrom(detailValue(detail, /^units?$|number\s*of\s*units?/i));
  const buildings = integerFrom(detailValue(detail, /^buildings?$/i));
  const stories = integerFrom(detailValue(detail, /^stories?$/i));
  const squareFootage = integerFrom(detailValue(detail, /^square\s*footage|building\s*size|net\s*rentable|nrsf/i));
  const yearText = detailValue(detail, /^years?\s*built$/i);
  const yearsBuilt = yearsFrom(yearText);
  const yearBuilt = yearsBuilt.length > 0 ? Math.min(...yearsBuilt) : null;
  const occ = occupancy(detailValue(detail, /^occupancy$/i));
  const tenancy = cleanText(detailValue(detail, /^tenancy$/i));
  const zoning = cleanText(detailValue(detail, /^zoning$|permitted\s*zoning/i));
  const keys = integerFrom(detailValue(detail, /^keys?$|number\s*of\s*keys/i));
  const price = typeof listPrice === "number" && Number.isFinite(listPrice) ? Math.round(listPrice) : null;
  const pricePerUnit = price && units ? Math.round(price / units) : null;
  const pricePerSqft = price && squareFootage ? Number((price / squareFootage).toFixed(2)) : null;
  const avgUnitSqft = squareFootage && units ? Math.round(squareFootage / units) : null;

  return {
    units,
    apartmentUnits: units,
    buildings,
    stories,
    yearBuilt,
    yearsBuilt,
    squareFootage,
    occupancyPct: occ.pct,
    occupancyStatus: occ.status,
    tenancy,
    zoning,
    keys,
    pricePerUnit,
    pricePerSqft,
    avgUnitSqft,
    evidence: {
      units: cleanText(detailValue(detail, /^units?$|number\s*of\s*units?/i)),
      buildings: cleanText(detailValue(detail, /^buildings?$/i)),
      stories: cleanText(detailValue(detail, /^stories?$/i)),
      yearBuilt: cleanText(yearText),
      squareFootage: cleanText(detailValue(detail, /^square\s*footage|building\s*size|net\s*rentable|nrsf/i)),
      occupancy: cleanText(detailValue(detail, /^occupancy$/i)),
      tenancy,
      zoning,
      keys: cleanText(detailValue(detail, /^keys?$|number\s*of\s*keys/i)),
    },
  };
}
