/** Shared eval parsing, validation and row projection; no database imports. */
import { rowToCamel } from "../caseMapping";
import type {
  EvalMessage,
  EvalCaseRecord,
  EvalSuiteRecord,
  EvalTargetDescriptor,
  EvalTargetType,
  EvalCaseStrategy,
  EvalRunSummary,
  PersistedEvalRun,
} from "../evals";
type JsonRecord = Record<string, unknown>;

function parseJsonRecord(value: unknown): JsonRecord {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as JsonRecord;
  }

  if (typeof value !== "string" || value.trim().length === 0) {
    return {};
  }

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as JsonRecord)
      : {};
  } catch {
    return {};
  }
}

function parseJsonArray(value: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(value)) {
    return value.filter(
      (entry): entry is Record<string, unknown> =>
        !!entry && typeof entry === "object" && !Array.isArray(entry)
    );
  }

  if (typeof value !== "string" || value.trim().length === 0) {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed.filter(
          (entry): entry is Record<string, unknown> =>
            !!entry && typeof entry === "object" && !Array.isArray(entry)
        )
      : [];
  } catch {
    return [];
  }
}

function parseStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .filter((entry): entry is string => typeof entry === "string")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  }

  if (typeof value !== "string" || value.trim().length === 0) {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed
          .filter((entry): entry is string => typeof entry === "string")
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0)
      : [];
  } catch {
    return [];
  }
}

function parseMessages(value: unknown): EvalMessage[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((entry) => {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
        return null;
      }
      const row = entry as Record<string, unknown>;
      const role = typeof row.role === "string" ? row.role.trim() : "";
      const content = typeof row.content === "string" ? row.content : "";
      if (!role || !content.trim()) {
        return null;
      }
      return {
        role,
        content,
      } satisfies EvalMessage;
    })
    .filter((entry): entry is EvalMessage => entry !== null);
}

function parseNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function sanitizeEvalCaseInput(value: unknown): EvalCaseRecord["input"] {
  const record =
    value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
  const maxTokens = parseNumber(record.max_tokens);
  const input: EvalCaseRecord["input"] = {
    messages: parseMessages(record.messages),
  };

  if (maxTokens > 0) {
    input.max_tokens = Math.floor(maxTokens);
  }

  return input;
}

export function sanitizeEvalExpected(value: unknown): EvalCaseRecord["expected"] {
  const record =
    value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
  const strategy = typeof record.strategy === "string" ? record.strategy.trim() : "";
  const normalizedStrategy: EvalCaseStrategy =
    strategy === "exact" || strategy === "regex" || strategy === "custom" ? strategy : "contains";
  const expectedValue =
    typeof record.value === "string" && record.value.trim().length > 0 ? record.value : undefined;

  return {
    strategy: normalizedStrategy,
    ...(expectedValue ? { value: expectedValue } : {}),
  };
}

export function createScorecardFromRuns(
  runs: Array<{
    suiteId: string;
    suiteName: string;
    summary: EvalRunSummary;
    results: Array<Record<string, unknown>>;
  }>
) {
  const totalCases = runs.reduce((sum, run) => sum + run.summary.total, 0);
  const totalPassed = runs.reduce((sum, run) => sum + run.summary.passed, 0);

  return {
    suites: runs.length,
    totalCases,
    totalPassed,
    overallPassRate: totalCases > 0 ? Math.round((totalPassed / totalCases) * 100) : 0,
    perSuite: runs.map((run) => ({
      id: run.suiteId,
      name: run.suiteName,
      passRate: run.summary.passRate,
    })),
  };
}

export function serializeEvalTargetKey(type: EvalTargetType, id?: string | null): string {
  return `${type}:${typeof id === "string" && id.trim().length > 0 ? id.trim() : "__default__"}`;
}

function toTargetDescriptor(row: JsonRecord): EvalTargetDescriptor {
  const type = row.targetType;
  const rawId = row.targetId;
  const id = typeof rawId === "string" && rawId.trim().length > 0 ? rawId.trim() : null;
  const normalizedType: EvalTargetType =
    type === "combo" || type === "model" || type === "suite-default" ? type : "suite-default";

  return {
    type: normalizedType,
    id,
    key: serializeEvalTargetKey(normalizedType, id),
    label:
      typeof row.targetLabel === "string" && row.targetLabel.trim().length > 0
        ? row.targetLabel.trim()
        : normalizedType === "combo"
          ? `Combo: ${id || "Unknown"}`
          : normalizedType === "model"
            ? `Model: ${id || "Unknown"}`
            : "Suite defaults",
  };
}

export function toPersistedEvalRun(row: unknown): PersistedEvalRun | null {
  const camel = rowToCamel(row) as JsonRecord | null;
  if (!camel) return null;

  // rowToCamel auto-parses `*_json` columns and exposes them under the base name
  // (summary_json → camel.summary); keep *Json fallbacks for alternate adapters/callers.
  const summaryRecord = parseJsonRecord(camel.summary ?? camel.summaryJson);
  const outputsRecord = parseJsonRecord(camel.outputs ?? camel.outputsJson);
  const outputs = Object.fromEntries(
    Object.entries(outputsRecord)
      .filter((entry): entry is [string, string] => typeof entry[0] === "string")
      .map(([key, value]) => [key, typeof value === "string" ? value : String(value ?? "")])
  );

  return {
    id: typeof camel.id === "string" ? camel.id : "",
    runGroupId:
      typeof camel.runGroupId === "string" && camel.runGroupId.trim().length > 0
        ? camel.runGroupId
        : null,
    suiteId: typeof camel.suiteId === "string" ? camel.suiteId : "",
    suiteName: typeof camel.suiteName === "string" ? camel.suiteName : "",
    target: toTargetDescriptor(camel),
    apiKeyId:
      typeof camel.apiKeyId === "string" && camel.apiKeyId.trim().length > 0
        ? camel.apiKeyId
        : null,
    avgLatencyMs: parseNumber(camel.avgLatencyMs),
    summary: {
      total: parseNumber(summaryRecord.total ?? camel.total),
      passed: parseNumber(summaryRecord.passed ?? camel.passed),
      failed: parseNumber(summaryRecord.failed ?? camel.failed),
      passRate: parseNumber(summaryRecord.passRate ?? camel.passRate),
    },
    results: parseJsonArray(camel.results ?? camel.resultsJson),
    outputs,
    createdAt: typeof camel.createdAt === "string" ? camel.createdAt : "",
  };
}

export function toEvalCaseRecord(row: unknown): EvalCaseRecord | null {
  const camel = rowToCamel(row) as JsonRecord | null;
  if (!camel) return null;

  const input = sanitizeEvalCaseInput(parseJsonRecord(camel.input ?? camel.inputJson));
  const expected = sanitizeEvalExpected({
    strategy: camel.expectedStrategy,
    value: camel.expectedValue,
  });

  return {
    id: typeof camel.id === "string" ? camel.id : "",
    suiteId: typeof camel.suiteId === "string" ? camel.suiteId : "",
    name: typeof camel.name === "string" ? camel.name : "",
    ...(typeof camel.model === "string" && camel.model.trim().length > 0
      ? { model: camel.model.trim() }
      : {}),
    input,
    expected,
    tags: parseStringArray(camel.tags ?? camel.tagsJson),
    sortOrder: parseNumber(camel.sortOrder),
    createdAt: typeof camel.createdAt === "string" ? camel.createdAt : "",
    updatedAt: typeof camel.updatedAt === "string" ? camel.updatedAt : "",
  };
}

export function toEvalSuiteRecord(row: unknown, cases: EvalCaseRecord[]): EvalSuiteRecord | null {
  const camel = rowToCamel(row) as JsonRecord | null;
  if (!camel) return null;

  return {
    id: typeof camel.id === "string" ? camel.id : "",
    name: typeof camel.name === "string" ? camel.name : "",
    ...(typeof camel.description === "string" && camel.description.trim().length > 0
      ? { description: camel.description }
      : {}),
    source: "custom",
    caseCount: cases.length,
    cases,
    createdAt: typeof camel.createdAt === "string" ? camel.createdAt : "",
    updatedAt: typeof camel.updatedAt === "string" ? camel.updatedAt : "",
  };
}
