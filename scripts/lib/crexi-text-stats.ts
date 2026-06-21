export type CrexiParsedTextStats = {
  noi: number | null;
  proFormaNoi: number | null;
  capRate: number | null;
  proFormaCapRate: number | null;
  cashOnCashReturn: number | null;
  grossIncome: number | null;
  units: number | null;
  pads: number | null;
  nightlyRent: number | null;
  weeklyRent: number | null;
  monthlyRent: number | null;
  unitMix: {
    rvSites: number | null;
    pullThroughSites: number | null;
    cabins: number | null;
    singleFamilyCabins: number | null;
    apartmentUnits: number | null;
    mobileHomeSites: number | null;
    parkModels: number | null;
    tentSites: number | null;
    storageUnits: number | null;
  };
};

function normalizedText(value: unknown): string {
  return String(value ?? "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\u200b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function moneyToNumber(value: string, suffix?: string): number | null {
  const parsed = Number(value.replace(/,/g, ""));
  if (!Number.isFinite(parsed)) return null;
  const s = suffix?.toLowerCase();
  const multiplier = s === "m" || s === "mm" || s === "million" ? 1_000_000 : s === "k" ? 1_000 : 1;
  return Math.round(parsed * multiplier);
}

function firstMoney(text: string, patterns: RegExp[]): number | null {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const value = match[1] ?? match[2];
    const suffix = match[2] && match[1] ? match[2] : match[3];
    if (!value) continue;
    const parsed = moneyToNumber(value, suffix);
    if (parsed !== null) return parsed;
  }
  return null;
}

function firstRate(text: string, patterns: RegExp[]): number | null {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const raw = match[1] ?? match[2];
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function firstInteger(text: string, patterns: RegExp[]): number | null {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const raw = match[1] ?? match[2];
    const parsed = Number.parseInt(raw.replace(/,/g, ""), 10);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return null;
}

export function parseCrexiTextStats(...values: unknown[]): CrexiParsedTextStats {
  const text = normalizedText(values.filter(Boolean).join(" "));
  const money = "\\$?\\s*([0-9][0-9,]*(?:\\.\\d+)?)\\s*(m|mm|million|k)?";
  const dollarMoney = "\\$\\s*([0-9][0-9,]*(?:\\.\\d+)?)\\s*(m|mm|million|k)?";
  const rate = "([0-9]{1,2}(?:\\.\\d+)?)\\s*%";

  const noi = firstMoney(text, [
    new RegExp(`(?:\\bnoi\\b|net operating income)[^$0-9]{0,50}${money}`, "i"),
    new RegExp(`${money}[^a-z0-9]{0,20}(?:\\bnoi\\b|net operating income)`, "i"),
  ]);
  const proFormaNoi = firstMoney(text, [
    new RegExp(`(?:pro[-\\s]?forma\\s+noi|pro[-\\s]?forma\\s+net operating income)[^$0-9]{0,60}${money}`, "i"),
    new RegExp(`${money}[^a-z0-9]{0,30}(?:pro[-\\s]?forma\\s+noi|pro[-\\s]?forma\\s+net operating income)`, "i"),
  ]);
  const grossIncome = firstMoney(text, [
    new RegExp(`(?:top[-\\s]?line income|gross income|gross revenue|annual revenue|revenue)[^$0-9]{0,80}${money}`, "i"),
    new RegExp(`${money}[^a-z0-9]{0,50}(?:top[-\\s]?line income|gross income|gross revenue|annual revenue|revenue)`, "i"),
  ]);

  const proFormaCapRate = firstRate(text, [
    new RegExp(`(?:pro[-\\s]?forma\\s+cap rate)[^0-9]{0,40}${rate}`, "i"),
    new RegExp(`${rate}[^a-z0-9]{0,30}(?:pro[-\\s]?forma\\s+cap rate)`, "i"),
  ]);
  const capRate = firstRate(text, [
    new RegExp(`(?:^|[^a-z])(?:cap rate)[^0-9]{0,40}${rate}`, "i"),
    new RegExp(`${rate}[^a-z0-9]{0,30}(?:cap rate)`, "i"),
  ]);
  const cashOnCashReturn = firstRate(text, [
    new RegExp(`(?:cash[-\\s]?on[-\\s]?cash|\\bcoc\\b)[^0-9]{0,40}${rate}`, "i"),
    new RegExp(`${rate}[^a-z0-9]{0,30}(?:cash[-\\s]?on[-\\s]?cash|\\bcoc\\b)`, "i"),
  ]);

  const units = firstInteger(text, [
    /\b([0-9]{1,5})\s*(?:total\s*)?(?:rv\s*)?(?:sites?|spaces?|units?)\b/i,
    /\b(?:consists of|property consists of)\s*([0-9]{1,5})\s*(?:rv\s*)?(?:sites?|spaces?|units?)\b/i,
  ]);
  const pads = firstInteger(text, [
    /\b([0-9]{1,5})\s*(?:pads?)\b/i,
    /\b(?:pads?)\s*[:\-]?\s*([0-9]{1,5})\b/i,
  ]);

  const nightlyRent = firstMoney(text, [
    new RegExp(`${dollarMoney}\\s*(?:per night|\\/night|nightly)`, "i"),
    new RegExp(`(?:per night|nightly)[^$0-9]{0,40}${dollarMoney}`, "i"),
  ]);
  const weeklyRent = firstMoney(text, [
    new RegExp(`${dollarMoney}\\s*(?:per week|\\/week|weekly)`, "i"),
    new RegExp(`(?:per week|weekly)[^$0-9]{0,40}${dollarMoney}`, "i"),
  ]);
  const monthlyRent = firstMoney(text, [
    new RegExp(`${dollarMoney}\\s*(?:per month|\\/month|monthly|\\/m\\b)`, "i"),
    new RegExp(`(?:per month|monthly)[^$0-9]{0,40}${dollarMoney}`, "i"),
  ]);
  const unitMix = {
    rvSites: firstInteger(text, [
      /\b([0-9]{1,5})\s*rv\s*(?:lots?|sites?|spaces?|pads?)\b/i,
    ]),
    pullThroughSites: firstInteger(text, [
      /\b([0-9]{1,5})\s*pull[-\s]?through\s*(?:sites?|spaces?)\b/i,
    ]),
    cabins: firstInteger(text, [
      /\b([0-9]{1,5})\s*(?:cabins?|cottages?)\b/i,
    ]),
    singleFamilyCabins: firstInteger(text, [
      /\b([0-9]{1,5})\s*(?:single[-\s]?family\s*home\/?cabins?|sfh\/?cabins?|home\/?cabins?)\b/i,
    ]),
    apartmentUnits: firstInteger(text, [
      /\b([0-9]{1,5})\s*(?:apartment\s+units?|apartments?)\b/i,
    ]),
    mobileHomeSites: firstInteger(text, [
      /\b([0-9]{1,5})\s*(?:mobile home|mh|manufactured home)\s*(?:sites?|spaces?|pads?)\b/i,
    ]),
    parkModels: firstInteger(text, [
      /\b([0-9]{1,5})\s*park\s*models?\b/i,
    ]),
    tentSites: firstInteger(text, [
      /\b([0-9]{1,5})\s*tent\s*(?:sites?|spaces?)\b/i,
    ]),
    storageUnits: firstInteger(text, [
      /\b([0-9]{1,5})\s*(?:storage\s*)units?\b/i,
    ]),
  };

  return {
    noi,
    proFormaNoi,
    capRate,
    proFormaCapRate,
    cashOnCashReturn,
    grossIncome,
    units,
    pads,
    nightlyRent,
    weeklyRent,
    monthlyRent,
    unitMix,
  };
}
