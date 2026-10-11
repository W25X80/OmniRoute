import { randomUUID } from "node:crypto";
import type {
  EvalSuiteRecord,
  EvalCaseRecord,
  PersistedEvalRun,
  EvalRoutingRunQuery,
} from "../evals";
import {
  toPersistedEvalRun,
  toEvalCaseRecord,
  toEvalSuiteRecord,
  sanitizeEvalCaseInput,
  sanitizeEvalExpected,
  createScorecardFromRuns,
} from "../evals/persistenceSemantics";
import type { PostgresTransaction } from "./database";
import { first, rows, affected, lockServices } from "./serviceQuery";

export async function saveEvalRun(
  transaction: PostgresTransaction,
  input: Parameters<typeof import("../evals").saveEvalRun>[0]
): Promise<PersistedEvalRun> {
  const id = randomUUID();
  const createdAt = input.createdAt || new Date().toISOString();
  const targetId =
    typeof input.target.id === "string" && input.target.id.trim().length > 0
      ? input.target.id.trim()
      : null;
  const latency = Number.isFinite(Number(input.avgLatencyMs))
    ? Math.max(0, Math.round(Number(input.avgLatencyMs)))
    : 0;
  const row = await first(
    transaction,
    `INSERT INTO omniroute.eval_runs
    (id, run_group_id, suite_id, suite_name, target_type, target_id, target_label, api_key_id,
    pass_rate, total, passed, failed, avg_latency_ms, summary_json, results_json, outputs_json, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING *`,
    [
      id,
      input.runGroupId || null,
      input.suiteId,
      input.suiteName,
      input.target.type,
      targetId,
      input.target.label,
      input.apiKeyId || null,
      input.summary.passRate,
      input.summary.total,
      input.summary.passed,
      input.summary.failed,
      latency,
      JSON.stringify(input.summary),
      JSON.stringify(input.results || []),
      JSON.stringify(input.outputs || {}),
      createdAt,
    ]
  );
  const run = toPersistedEvalRun(row);
  if (!run) throw new Error("Failed to persist eval run");
  return run;
}
export async function listEvalRuns(
  transaction: PostgresTransaction,
  options: Parameters<typeof import("../evals").listEvalRuns>[0] = {}
): Promise<PersistedEvalRun[]> {
  const conditions: string[] = [];
  const values: unknown[] = [];
  if (options.suiteId) {
    values.push(options.suiteId);
    conditions.push(`suite_id = $${values.length}`);
  }
  if (options.runGroupId) {
    values.push(options.runGroupId);
    conditions.push(`run_group_id = $${values.length}`);
  }
  values.push(
    Number.isFinite(Number(options.limit))
      ? Math.min(200, Math.max(1, Math.floor(Number(options.limit))))
      : 20
  );
  return (
    await rows(
      transaction,
      `SELECT * FROM omniroute.eval_runs ${conditions.length ? `WHERE ${conditions.join(" AND ")}` : ""}
    ORDER BY created_at COLLATE "C" DESC, id COLLATE "C" DESC LIMIT $${values.length}`,
      values
    )
  )
    .map(toPersistedEvalRun)
    .filter((row): row is PersistedEvalRun => row !== null);
}
export async function listModelEvalRunsForRouting(
  transaction: PostgresTransaction,
  options: EvalRoutingRunQuery
): Promise<PersistedEvalRun[]> {
  const targets = [...new Set(options.targetIds.map((id) => id.trim()).filter(Boolean))].slice(
    0,
    200
  );
  if (!targets.length) return [];
  const suites = Array.isArray(options.suiteIds)
    ? [...new Set(options.suiteIds.map((id) => id.trim()).filter(Boolean))].slice(0, 50)
    : [];
  const values: unknown[] = [targets];
  const conditions = ["target_type = 'model'", "target_id = ANY($1::text[])"];
  if (suites.length) {
    values.push(suites);
    conditions.push(`suite_id = ANY($${values.length}::text[])`);
  }
  const age = Number(options.maxAgeHours);
  if (Number.isFinite(age) && age > 0) {
    values.push(new Date(Date.now() - age * 3600000).toISOString());
    conditions.push(`created_at >= $${values.length}`);
  }
  values.push(
    Number.isFinite(Number(options.limit))
      ? Math.min(1000, Math.max(1, Math.floor(Number(options.limit))))
      : Math.min(1000, Math.max(50, targets.length * Math.max(3, suites.length || 5) * 2))
  );
  return (
    await rows(
      transaction,
      `SELECT * FROM omniroute.eval_runs WHERE ${conditions.join(" AND ")}
    ORDER BY created_at COLLATE "C" DESC, id COLLATE "C" DESC LIMIT $${values.length}`,
      values
    )
  )
    .map(toPersistedEvalRun)
    .filter((row): row is PersistedEvalRun => row !== null);
}
export async function getEvalScorecard(
  transaction: PostgresTransaction,
  options: Parameters<typeof import("../evals").getEvalScorecard>[0] = {}
) {
  const runs = await listEvalRuns(transaction, {
    suiteId: options.suiteId,
    limit: options.limit || 50,
  });
  if (!runs.length) return null;
  const latest = new Map<string, PersistedEvalRun>();
  for (const run of runs) {
    const key = `${run.suiteId}:${run.target.key}`;
    if (!latest.has(key)) latest.set(key, run);
  }
  return createScorecardFromRuns(
    [...latest.values()].map((run) => ({
      suiteId: `${run.suiteId}:${run.target.key}`,
      suiteName: `${run.suiteName} · ${run.target.label}`,
      summary: run.summary,
      results: run.results,
    }))
  );
}
export async function listCustomEvalSuites(
  transaction: PostgresTransaction
): Promise<EvalSuiteRecord[]> {
  await lockServices(transaction, "eval-suites");
  const suites = await rows(
    transaction,
    'SELECT * FROM omniroute.eval_suites ORDER BY updated_at COLLATE "C" DESC, created_at COLLATE "C" DESC, id COLLATE "C" DESC'
  );
  const cases = (
    await rows(
      transaction,
      'SELECT * FROM omniroute.eval_cases ORDER BY sort_order ASC, created_at COLLATE "C" ASC, id COLLATE "C" ASC'
    )
  )
    .map(toEvalCaseRecord)
    .filter((row): row is EvalCaseRecord => row !== null);
  return suites
    .map((suite) =>
      toEvalSuiteRecord(
        suite,
        cases.filter((item) => item.suiteId === suite.id)
      )
    )
    .filter((row): row is EvalSuiteRecord => row !== null);
}
export async function getCustomEvalSuite(
  transaction: PostgresTransaction,
  suiteId: string
): Promise<EvalSuiteRecord | null> {
  const normalized = suiteId.trim();
  if (!normalized) return null;
  return (await listCustomEvalSuites(transaction)).find((suite) => suite.id === normalized) || null;
}
export async function saveCustomEvalSuite(
  transaction: PostgresTransaction,
  input: Parameters<typeof import("../evals").saveCustomEvalSuite>[0]
): Promise<EvalSuiteRecord> {
  const now = new Date().toISOString();
  const id = typeof input.id === "string" && input.id.trim() ? input.id.trim() : randomUUID();
  const name = input.name.trim();
  const description =
    typeof input.description === "string" && input.description.trim()
      ? input.description.trim()
      : null;
  if (!name) throw new Error("Suite name is required");
  if (!Array.isArray(input.cases) || !input.cases.length)
    throw new Error("At least one eval case is required");
  await lockServices(transaction, "eval-suites");
  await transaction.query(
    `INSERT INTO omniroute.eval_suites (id, name, description, created_at, updated_at) VALUES ($1, $2, $3, $4, $4)
    ON CONFLICT (id) DO UPDATE SET name = excluded.name, description = excluded.description, updated_at = excluded.updated_at`,
    [id, name, description, now]
  );
  await transaction.query("DELETE FROM omniroute.eval_cases WHERE suite_id = $1", [id]);
  for (const [index, raw] of input.cases.entries()) {
    const caseId = typeof raw.id === "string" && raw.id.trim() ? raw.id.trim() : randomUUID();
    const caseName = raw.name.trim();
    const model = typeof raw.model === "string" && raw.model.trim() ? raw.model.trim() : null;
    const sanitizedInput = sanitizeEvalCaseInput(raw.input);
    const expected = sanitizeEvalExpected(raw.expected);
    const tags = Array.isArray(raw.tags) ? raw.tags.map((tag) => tag.trim()).filter(Boolean) : [];
    if (!caseName) throw new Error(`Case ${index + 1} is missing a name`);
    if (!sanitizedInput.messages.length)
      throw new Error(`Case ${index + 1} must include at least one message`);
    if (["contains", "exact", "regex"].includes(expected.strategy) && !expected.value)
      throw new Error(`Case ${index + 1} must include an expected value`);
    await transaction.query(
      `INSERT INTO omniroute.eval_cases (id, suite_id, sort_order, name, model, input_json, expected_strategy,
      expected_value, tags_json, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
      [
        caseId,
        id,
        index,
        caseName,
        model,
        JSON.stringify(sanitizedInput),
        expected.strategy,
        expected.value || null,
        JSON.stringify(tags),
        now,
      ]
    );
  }
  const saved = await getCustomEvalSuite(transaction, id);
  if (!saved) throw new Error("Failed to persist eval suite");
  return saved;
}
export async function deleteCustomEvalSuite(
  transaction: PostgresTransaction,
  suiteId: string
): Promise<boolean> {
  const id = suiteId.trim();
  if (!id) return false;
  await lockServices(transaction, "eval-suites");
  await transaction.query("DELETE FROM omniroute.eval_cases WHERE suite_id = $1", [id]);
  return (await affected(transaction, "DELETE FROM omniroute.eval_suites WHERE id = $1", [id])) > 0;
}
