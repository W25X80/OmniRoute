import { randomUUID } from "node:crypto";
import {
  reasoningRuleParams,
  reasoningRuleReferenceErrors,
  rowToReasoningRoutingRule,
  type RuleRow,
} from "../reasoningRoutingRuleModel";
import type { ReasoningRoutingRule, ReasoningRoutingRuleInput } from "../reasoningRoutingRules";
import {
  afterPostgresCatalogCommit,
  catalogTableRows,
  compareCatalogText,
  hasPostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  readPostgresCatalog,
  withPostgresCatalogSnapshot,
  writePostgresCatalog,
} from "./catalogSnapshot";
import type { PostgresDatabase } from "./database";

/** The prepared view belongs to the selected database/request, never a global rule cache. */
async function withRules<T>(database: PostgresDatabase, body: () => T | Promise<T>): Promise<T> {
  return hasPostgresCatalogSnapshot(database)
    ? body()
    : withPostgresCatalogSnapshot(database, body);
}

function findRule(id: string): ReasoningRoutingRule | null {
  const row = catalogTableRows<RuleRow>("reasoning_routing_rules").find((rule) => rule.id === id);
  return row ? rowToReasoningRoutingRule(row) : null;
}

export function getPostgresReasoningRoutingRuleReferenceErrors(
  rule: ReasoningRoutingRuleInput
): string[] {
  return reasoningRuleReferenceErrors(rule, (table, id) =>
    Boolean(id && catalogTableRows<{ id: string }>(table).some((row) => row.id === id))
  );
}

export function getPostgresReasoningRoutingRuleReferenceErrorsAsync(
  database: PostgresDatabase,
  rule: ReasoningRoutingRuleInput
): Promise<string[]> {
  return withRules(database, () => getPostgresReasoningRoutingRuleReferenceErrors(rule));
}

export function getPostgresReasoningRoutingRules(
  database: PostgresDatabase,
  options: { enabledOnly?: boolean } = {}
): Promise<ReasoningRoutingRule[]> {
  return withRules(database, () => {
    const rows = catalogTableRows<RuleRow>("reasoning_routing_rules");
    rows.sort(
      (a, b) =>
        Number(b.priority) - Number(a.priority) ||
        compareCatalogText(a.created_at, b.created_at) ||
        compareCatalogText(a.id, b.id)
    );
    const rules = rows.map(rowToReasoningRoutingRule);
    return options.enabledOnly ? rules.filter((rule) => rule.enabled) : rules;
  });
}

export function getPostgresReasoningRoutingRuleById(
  database: PostgresDatabase,
  id: string
): Promise<ReasoningRoutingRule | null> {
  // Source find-by-id is a current read even while the list cache holds an earlier view.
  const text = "SELECT * FROM omniroute.reasoning_routing_rules WHERE id=$1";
  const read = isPostgresCatalogMutation(database)
    ? readPostgresCatalog<RuleRow>(text, [id])
    : database.transaction((transaction) => transaction.query<RuleRow>(text, [id]));
  return read.then(({ rows }) => {
    const row = rows[0];
    if (!row) return null;
    return rowToReasoningRoutingRule({
      ...row,
      budget_tokens: row.budget_tokens == null ? null : Number(row.budget_tokens),
    });
  });
}

async function assertCurrentReferences(rule: ReasoningRoutingRuleInput): Promise<void> {
  const { rows } = await readPostgresCatalog<{
    api_key: boolean;
    source_combo: boolean;
    connection: boolean;
    target_combo: boolean;
  }>(
    `SELECT
      EXISTS(SELECT 1 FROM omniroute.api_keys WHERE id=$1) AS api_key,
      EXISTS(SELECT 1 FROM omniroute.combos WHERE id=$2) AS source_combo,
      EXISTS(SELECT 1 FROM omniroute.provider_connections WHERE id=$3) AS connection,
      EXISTS(SELECT 1 FROM omniroute.combos WHERE id=$4) AS target_combo`,
    [rule.apiKeyId, rule.comboId, rule.connectionId, rule.targetComboId]
  );
  const found = rows[0];
  const errors = reasoningRuleReferenceErrors(rule, (table, id) => {
    if (!id) return false;
    if (table === "api_keys") return found.api_key;
    if (table === "provider_connections") return found.connection;
    return Boolean(
      (id === rule.comboId && found.source_combo) ||
      (id === rule.targetComboId && found.target_combo)
    );
  });
  if (errors.length) throw new Error(errors.join("; "));
}

const PARAMETER_KEYS = [
  "id",
  "name",
  "description",
  "scope",
  "apiKeyId",
  "comboId",
  "connectionId",
  "modelPattern",
  "sourceEffort",
  "requestTags",
  "tagMatchMode",
  "effortMode",
  "targetEffort",
  "targetKind",
  "targetModel",
  "targetComboId",
  "budgetAction",
  "budgetTokens",
  "priority",
  "enabled",
  "createdAt",
  "updatedAt",
] as const;
const COLUMN_NAMES = [
  "id",
  "name",
  "description",
  "scope",
  "api_key_id",
  "combo_id",
  "connection_id",
  "model_pattern",
  "source_effort",
  "request_tags",
  "tag_match_mode",
  "effort_mode",
  "target_effort",
  "target_kind",
  "target_model",
  "target_combo_id",
  "budget_action",
  "budget_tokens",
  "priority",
  "enabled",
  "created_at",
  "updated_at",
] as const;
// Closed source-owned field maps only; neither SQL identifiers nor operators come from callers.
function values(rule: ReasoningRoutingRuleInput, id: string, createdAt: string, updatedAt: string) {
  const params = reasoningRuleParams(rule, id, createdAt, updatedAt);
  return PARAMETER_KEYS.map((key) => params[key]);
}

/** Routing(2), providers(4), catalog(18), KV(20), policy(5); references cannot race deletion. */
const MUTATION_LOCKS = { routing: true, providers: true } as const;

export async function createPostgresReasoningRoutingRule(
  database: PostgresDatabase,
  rule: ReasoningRoutingRuleInput,
  invalidate: () => void
): Promise<ReasoningRoutingRule> {
  return mutatePostgresCatalog(
    database,
    async () => {
      await assertCurrentReferences(rule);
      const id = randomUUID(),
        now = new Date().toISOString();
      await writePostgresCatalog(
        `INSERT INTO omniroute.reasoning_routing_rules(${COLUMN_NAMES.join(",")})
       VALUES(${PARAMETER_KEYS.map((_, index) => `$${index + 1}`).join(",")})`,
        values(rule, id, now, now)
      );
      afterPostgresCatalogCommit(invalidate);
      return findRule(id)!;
    },
    MUTATION_LOCKS
  );
}

export async function updatePostgresReasoningRoutingRule(
  database: PostgresDatabase,
  id: string,
  patch: Partial<ReasoningRoutingRuleInput>,
  invalidate: () => void
): Promise<ReasoningRoutingRule | null> {
  return mutatePostgresCatalog(
    database,
    async () => {
      const existing = findRule(id);
      if (!existing) return null;
      const { id: _id, createdAt, updatedAt: _updatedAt, ...base } = existing;
      const next = { ...base, ...patch } as ReasoningRoutingRuleInput;
      await assertCurrentReferences(next);
      const now = new Date().toISOString();
      const columns = COLUMN_NAMES.filter((column) => column !== "id" && column !== "created_at");
      const params = values(next, id, createdAt, now);
      await writePostgresCatalog(
        `UPDATE omniroute.reasoning_routing_rules SET ${columns
          .map((column, index) => `${column}=$${index + 2}`)
          .join(",")} WHERE id=$1`,
        [id, ...columns.map((column) => params[COLUMN_NAMES.indexOf(column)])]
      );
      afterPostgresCatalogCommit(invalidate);
      return findRule(id);
    },
    MUTATION_LOCKS
  );
}

export async function deletePostgresReasoningRoutingRule(
  database: PostgresDatabase,
  id: string,
  invalidate: () => void
): Promise<boolean> {
  return mutatePostgresCatalog(
    database,
    async () => {
      const result = await writePostgresCatalog(
        "DELETE FROM omniroute.reasoning_routing_rules WHERE id=$1",
        [id]
      );
      if ((result.rowCount ?? 0) > 0) afterPostgresCatalogCommit(invalidate);
      return (result.rowCount ?? 0) > 0;
    },
    MUTATION_LOCKS
  );
}
