import { randomUUID } from "node:crypto";
import { getDbInstance } from "./core";
import { registerDbStateResetter } from "./stateReset";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  rowToReasoningRoutingRule as rowToRule,
  reasoningRuleParams as ruleParams,
  reasoningRuleReferenceErrors,
  type RuleRow,
} from "./reasoningRoutingRuleModel";
import {
  getPostgresReasoningRoutingRules,
  getPostgresReasoningRoutingRuleById,
  getPostgresReasoningRoutingRuleReferenceErrors,
  getPostgresReasoningRoutingRuleReferenceErrorsAsync,
  createPostgresReasoningRoutingRule,
  updatePostgresReasoningRoutingRule,
  deletePostgresReasoningRoutingRule,
} from "./postgres/reasoningRuleRepository";

export type ReasoningRuleScope = "global" | "apiKey" | "combo" | "model" | "connection";
export type ReasoningEffort = "none" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra";
export type ReasoningSourceEffort = "any" | "missing" | ReasoningEffort;
export type ReasoningEffortMode = "inherit" | "default" | "force";
export type ReasoningTargetKind = "keep" | "model" | "combo";
export type ReasoningBudgetAction = "preserve" | "remove" | "set";

export interface ReasoningRoutingRule {
  id: string;
  name: string;
  description: string;
  scope: ReasoningRuleScope;
  apiKeyId: string | null;
  comboId: string | null;
  connectionId: string | null;
  modelPattern: string | null;
  sourceEffort: ReasoningSourceEffort;
  requestTags: string[];
  tagMatchMode: "any" | "all";
  effortMode: ReasoningEffortMode;
  targetEffort: ReasoningEffort | null;
  targetKind: ReasoningTargetKind;
  targetModel: string | null;
  targetComboId: string | null;
  budgetAction: ReasoningBudgetAction;
  budgetTokens: number | null;
  priority: number;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
}

export type ReasoningRoutingRuleInput = Omit<
  ReasoningRoutingRule,
  "id" | "createdAt" | "updatedAt"
>;

let cache: { version: number; rules: ReasoningRoutingRule[] } | null = null;
let version = 0;

function invalidate(): void {
  version += 1;
  cache = null;
}

function referenceExists(table: string, id: string | null): boolean {
  if (!id) return false;
  const allowedTables = new Set(["api_keys", "combos", "provider_connections"]);
  if (!allowedTables.has(table)) return false;
  return Boolean(getDbInstance().prepare(`SELECT 1 FROM ${table} WHERE id = ?`).get(id));
}

export function getReasoningRoutingRuleReferenceErrors(rule: ReasoningRoutingRuleInput): string[] {
  const database = getSelectedPostgresDatabase();
  if (database) return getPostgresReasoningRoutingRuleReferenceErrors(rule);
  return reasoningRuleReferenceErrors(rule, referenceExists);
}

export async function getReasoningRoutingRuleReferenceErrorsAsync(
  rule: ReasoningRoutingRuleInput
): Promise<string[]> {
  const database = getSelectedPostgresDatabase();
  if (database) return getPostgresReasoningRoutingRuleReferenceErrorsAsync(database, rule);
  return getReasoningRoutingRuleReferenceErrors(rule);
}

function assertReferences(rule: ReasoningRoutingRuleInput): void {
  const errors = getReasoningRoutingRuleReferenceErrors(rule);
  if (errors.length > 0) throw new Error(errors.join("; "));
}

export async function getReasoningRoutingRules(
  options: { enabledOnly?: boolean } = {}
): Promise<ReasoningRoutingRule[]> {
  const database = getSelectedPostgresDatabase();
  if (database) return getPostgresReasoningRoutingRules(database, options);
  if (!cache || cache.version !== version) {
    const rows = getDbInstance()
      .prepare(
        "SELECT * FROM reasoning_routing_rules ORDER BY priority DESC, created_at ASC, id ASC"
      )
      .all() as RuleRow[];
    cache = { version, rules: rows.map(rowToRule) };
  }
  const rules = options.enabledOnly ? cache.rules.filter((rule) => rule.enabled) : cache.rules;
  return rules.map((rule) => ({ ...rule, requestTags: [...rule.requestTags] }));
}

export async function getReasoningRoutingRuleById(
  id: string
): Promise<ReasoningRoutingRule | null> {
  const database = getSelectedPostgresDatabase();
  if (database) return getPostgresReasoningRoutingRuleById(database, id);
  const row = getDbInstance()
    .prepare("SELECT * FROM reasoning_routing_rules WHERE id = ?")
    .get(id) as RuleRow | undefined;
  return row ? rowToRule(row) : null;
}

export async function createReasoningRoutingRule(
  rule: ReasoningRoutingRuleInput
): Promise<ReasoningRoutingRule> {
  const database = getSelectedPostgresDatabase();
  if (database) return createPostgresReasoningRoutingRule(database, rule, invalidate);
  assertReferences(rule);
  const id = randomUUID();
  const now = new Date().toISOString();
  getDbInstance()
    .prepare(
      `INSERT INTO reasoning_routing_rules (
      id, name, description, scope, api_key_id, combo_id, connection_id, model_pattern,
      source_effort, request_tags, tag_match_mode, effort_mode, target_effort, target_kind,
      target_model, target_combo_id, budget_action, budget_tokens, priority, enabled, created_at, updated_at
    ) VALUES (
      @id, @name, @description, @scope, @apiKeyId, @comboId, @connectionId, @modelPattern,
      @sourceEffort, @requestTags, @tagMatchMode, @effortMode, @targetEffort, @targetKind,
      @targetModel, @targetComboId, @budgetAction, @budgetTokens, @priority, @enabled, @createdAt, @updatedAt
    )`
    )
    .run(ruleParams(rule, id, now, now));
  invalidate();
  return (await getReasoningRoutingRuleById(id))!;
}

export async function updateReasoningRoutingRule(
  id: string,
  patch: Partial<ReasoningRoutingRuleInput>
): Promise<ReasoningRoutingRule | null> {
  const database = getSelectedPostgresDatabase();
  if (database) return updatePostgresReasoningRoutingRule(database, id, patch, invalidate);
  const existing = await getReasoningRoutingRuleById(id);
  if (!existing) return null;
  const { id: _id, createdAt, updatedAt: _updatedAt, ...base } = existing;
  const next = { ...base, ...patch } as ReasoningRoutingRuleInput;
  assertReferences(next);
  const now = new Date().toISOString();
  getDbInstance()
    .prepare(
      `UPDATE reasoning_routing_rules SET
      name=@name, description=@description, scope=@scope, api_key_id=@apiKeyId,
      combo_id=@comboId, connection_id=@connectionId, model_pattern=@modelPattern,
      source_effort=@sourceEffort, request_tags=@requestTags, tag_match_mode=@tagMatchMode,
      effort_mode=@effortMode, target_effort=@targetEffort, target_kind=@targetKind,
      target_model=@targetModel, target_combo_id=@targetComboId, budget_action=@budgetAction,
      budget_tokens=@budgetTokens, priority=@priority, enabled=@enabled, updated_at=@updatedAt
      WHERE id=@id`
    )
    .run(ruleParams(next, id, createdAt, now));
  invalidate();
  return getReasoningRoutingRuleById(id);
}

export async function deleteReasoningRoutingRule(id: string): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (database) return deletePostgresReasoningRoutingRule(database, id, invalidate);
  const result = getDbInstance()
    .prepare("DELETE FROM reasoning_routing_rules WHERE id = ?")
    .run(id);
  if ((result.changes ?? 0) > 0) invalidate();
  return (result.changes ?? 0) > 0;
}

export function invalidateReasoningRoutingRuleCache(): void {
  invalidate();
}

registerDbStateResetter(invalidateReasoningRoutingRuleCache);
