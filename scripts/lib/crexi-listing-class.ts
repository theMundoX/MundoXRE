export type CrexiBuildingClassGrade = "A" | "B" | "C" | "D";

export type CrexiBuildingClassResult = {
  sourceClass: string | null;
  buildingClass: CrexiBuildingClassGrade | null;
  status: "explicit_value" | "no_data" | "needs_review";
  evidence: string | null;
};

export function normalizeCrexiBuildingClass(value: unknown): CrexiBuildingClassResult {
  const sourceClass = String(value ?? "").replace(/\s+/g, " ").trim() || null;
  if (!sourceClass) {
    return {
      sourceClass: null,
      buildingClass: null,
      status: "no_data",
      evidence: null,
    };
  }

  const text = sourceClass.toUpperCase();
  const exact = text.match(/^(?:CLASS\s*)?([ABCD])$/);
  const labeled = text.match(/\bCLASS\s*([ABCD])\b/);
  const reverseLabeled = text.match(/\b([ABCD])\s*[- ]?CLASS\b/);
  const grade = (exact?.[1] ?? labeled?.[1] ?? reverseLabeled?.[1]) as CrexiBuildingClassGrade | undefined;

  return {
    sourceClass,
    buildingClass: grade ?? null,
    status: grade ? "explicit_value" : "needs_review",
    evidence: `Class: ${sourceClass}`,
  };
}
