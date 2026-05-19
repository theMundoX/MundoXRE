export type RealEstateApiAgent = {
  name: string | null;
  email: string | null;
  phone: string | null;
  brokerage: string | null;
};

function stringOrNull(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function arrayOfObjects(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object" && !Array.isArray(item)))
    : [];
}

function agentScore(agent: RealEstateApiAgent): number {
  return (agent.email ? 8 : 0)
    + (agent.phone ? 4 : 0)
    + (agent.name ? 2 : 0)
    + (agent.brokerage ? 1 : 0);
}

export function bestRealEstateApiAgent(response: Record<string, unknown>): RealEstateApiAgent | null {
  const candidates = arrayOfObjects(response.mlsHistory)
    .map((row) => ({
      name: stringOrNull(row.agentName),
      email: stringOrNull(row.agentEmail),
      phone: stringOrNull(row.agentPhone),
      brokerage: stringOrNull(row.agentOffice),
    }))
    .filter((agent) => agent.email || agent.phone || agent.name || agent.brokerage)
    .sort((a, b) => agentScore(b) - agentScore(a));

  return candidates[0] ?? null;
}
