import type {
  ReasoningRoutingRule,
  ReasoningRoutingRuleInput,
  ReasoningRuleScope,
  ReasoningSourceEffort,
  ReasoningEffortMode,
  ReasoningEffort,
  ReasoningTargetKind,
  ReasoningBudgetAction,
} from "./reasoningRoutingRules";
export type RuleRow = Record<string, unknown>;
export type ReasoningReferenceTable = "api_keys" | "combos" | "provider_connections";

export function parseReasoningRuleTags(value: unknown): string[] {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    return Array.isArray(parsed)
      ? [
          ...new Set(
            parsed
              .filter((tag): tag is string => typeof tag === "string")
              .map((tag) => tag.trim().toLowerCase())
              .filter(Boolean)
          ),
        ]
      : [];
  } catch {
    return [];
  }
}

export function rowToReasoningRoutingRule(row: RuleRow): ReasoningRoutingRule {
  return {
    id: String(row.id),
    name: String(row.name),
    description: typeof row.description === "string" ? row.description : "",
    scope: row.scope as ReasoningRuleScope,
    apiKeyId: typeof row.api_key_id === "string" ? row.api_key_id : null,
    comboId: typeof row.combo_id === "string" ? row.combo_id : null,
    connectionId: typeof row.connection_id === "string" ? row.connection_id : null,
    modelPattern: typeof row.model_pattern === "string" ? row.model_pattern : null,
    sourceEffort: row.source_effort as ReasoningSourceEffort,
    requestTags: parseReasoningRuleTags(row.request_tags),
    tagMatchMode: row.tag_match_mode === "all" ? "all" : "any",
    effortMode: row.effort_mode as ReasoningEffortMode,
    targetEffort:
      typeof row.target_effort === "string" ? (row.target_effort as ReasoningEffort) : null,
    targetKind: row.target_kind as ReasoningTargetKind,
    targetModel: typeof row.target_model === "string" ? row.target_model : null,
    targetComboId: typeof row.target_combo_id === "string" ? row.target_combo_id : null,
    budgetAction: row.budget_action as ReasoningBudgetAction,
    budgetTokens: typeof row.budget_tokens === "number" ? row.budget_tokens : null,
    priority: typeof row.priority === "number" ? row.priority : 0,
    enabled: row.enabled === 1 || row.enabled === true,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

export function reasoningRuleParams(
  rule: ReasoningRoutingRuleInput,
  id: string,
  createdAt: string,
  updatedAt: string
) {
  return {
    id,
    name: rule.name,
    description: rule.description || "",
    scope: rule.scope,
    apiKeyId: rule.apiKeyId ?? null,
    comboId: rule.comboId ?? null,
    connectionId: rule.connectionId ?? null,
    modelPattern: rule.modelPattern ?? null,
    sourceEffort: rule.sourceEffort,
    requestTags: JSON.stringify(parseReasoningRuleTags(rule.requestTags)),
    tagMatchMode: rule.tagMatchMode,
    effortMode: rule.effortMode,
    targetEffort: rule.effortMode === "inherit" ? null : rule.targetEffort,
    targetKind: rule.targetKind,
    targetModel: rule.targetKind === "model" ? rule.targetModel : null,
    targetComboId: rule.targetKind === "combo" ? rule.targetComboId : null,
    budgetAction: rule.targetEffort === "none" ? "remove" : rule.budgetAction,
    budgetTokens: rule.budgetAction === "set" ? rule.budgetTokens : null,
    priority: rule.priority,
    enabled: rule.enabled ? 1 : 0,
    createdAt,
    updatedAt,
  };
}

export function reasoningRuleReferenceErrors(
  rule: ReasoningRoutingRuleInput,
  referenceExists: (table: ReasoningReferenceTable, id: string | null) => boolean
): string[] {
  const errors: string[] = [];
  if (rule.scope === "apiKey" && !referenceExists("api_keys", rule.apiKeyId)) {
    errors.push("The referenced API key does not exist");
  }
  if (rule.scope === "combo" && !referenceExists("combos", rule.comboId)) {
    errors.push("The referenced source combo does not exist");
  }
  if (rule.scope === "connection" && !referenceExists("provider_connections", rule.connectionId)) {
    errors.push("The referenced provider connection does not exist");
  }
  if (rule.targetKind === "combo" && !referenceExists("combos", rule.targetComboId)) {
    errors.push("The referenced target combo does not exist");
  }
  return errors;
}
