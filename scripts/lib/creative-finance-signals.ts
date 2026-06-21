export type CreativeFinanceSignal = {
  hasSignal: boolean;
  status: "positive" | "negative" | "no_data";
  score: number | null;
  confidence: "none" | "low" | "medium" | "high";
  signals: string[];
  negativeSignals: string[];
  evidence: string[];
};

function normalizeText(values: unknown[]): string {
  return values
    .flatMap((value) => {
      if (value === null || value === undefined) return [];
      if (typeof value === "string") return [value];
      if (Array.isArray(value)) return [normalizeText(value)];
      if (typeof value === "object") return [normalizeText(Object.values(value as Record<string, unknown>))];
      return [String(value)];
    })
    .join(" ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function snippets(text: string, pattern: RegExp): string[] {
  const results: string[] = [];
  for (const match of text.matchAll(pattern)) {
    const index = match.index ?? 0;
    const start = Math.max(0, index - 80);
    const end = Math.min(text.length, index + match[0].length + 80);
    results.push(text.slice(start, end).trim());
    if (results.length >= 3) break;
  }
  return results;
}

export function detectCreativeFinanceSignals(...values: unknown[]): CreativeFinanceSignal {
  const text = normalizeText(values);
  const lower = text.toLowerCase();
  const negativePatterns: Array<[string, RegExp]> = [
    ["no_seller_financing", /\b(?:no|not|without)\s+(?:seller|owner)\s+financ(?:e|ing)\b/gi],
    ["cash_only", /\bcash\s+only\b|\ball\s+cash\b/gi],
    ["not_first_time_no_capital", /\bnot\s+seeking\s+first[-\s]?time\s+purchasers?\b|\bno\s+capital\s+investment\b/gi],
  ];
  const positivePatterns: Array<[string, RegExp, "low" | "medium" | "high"]> = [
    ["seller_financing", /\bseller\s+financ(?:e|ing|ed)\s+(?:available|offered|possible|terms|option)?\b|\bseller\s+will\s+financ(?:e|ing)\b/gi, "high"],
    ["owner_financing", /\bowner\s+financ(?:e|ing|ed)\s+(?:available|offered|possible|terms|option)?\b|\bowner\s+will\s+financ(?:e|ing)\b/gi, "high"],
    ["assumable_debt", /\bassumable\s+(?:loan|debt|mortgage|financing)\b|\bloan\s+assumption\b/gi, "high"],
    ["subject_to", /\bsubject\s+to\s+(?:existing\s+)?(?:financing|mortgage|loan|debt)\b|\bsubto\b|\bsub\s*to\b/gi, "medium"],
    ["lease_option", /\blease\s+(?:option|purchase)\b|\boption\s+to\s+purchase\b/gi, "medium"],
    ["seller_terms", /\bseller\s+terms\b|\bflexible\s+seller\s+terms\b|\bcreative\s+financ(?:e|ing)\b/gi, "medium"],
  ];

  const negativeSignals = negativePatterns
    .filter(([, pattern]) => {
      pattern.lastIndex = 0;
      return pattern.test(lower);
    })
    .map(([name]) => name);

  const matched = positivePatterns.filter(([, pattern]) => {
    pattern.lastIndex = 0;
    return pattern.test(lower);
  });

  const signals = matched.map(([name]) => name);
  const evidence = matched.flatMap(([, pattern]) => {
    pattern.lastIndex = 0;
    return snippets(text, pattern);
  }).slice(0, 5);

  if (negativeSignals.length > 0 && signals.length === 0) {
    return { hasSignal: false, status: "negative", score: 0, confidence: "none", signals: [], negativeSignals, evidence: [] };
  }

  const confidence =
    signals.includes("seller_financing") || signals.includes("owner_financing") || signals.includes("assumable_debt")
      ? "high"
      : signals.length > 0
        ? "medium"
        : "none";

  const hasSignal = signals.length > 0 && negativeSignals.length === 0;

  return {
    hasSignal,
    status: hasSignal ? "positive" : "no_data",
    score: hasSignal ? (confidence === "high" ? 90 : confidence === "medium" ? 70 : 50) : null,
    confidence,
    signals,
    negativeSignals,
    evidence,
  };
}
