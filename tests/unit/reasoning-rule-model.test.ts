import assert from "node:assert/strict";
import test from "node:test";
import {
  parseReasoningRuleTags,
  reasoningRuleParams,
  reasoningRuleReferenceErrors,
  rowToReasoningRoutingRule,
} from "../../src/lib/db/reasoningRoutingRuleModel.ts";
import type { ReasoningRoutingRuleInput } from "../../src/lib/db/reasoningRoutingRules.ts";

function input(patch: Partial<ReasoningRoutingRuleInput> = {}): ReasoningRoutingRuleInput {
  return {
    name: "Rule",
    description: "",
    scope: "global",
    apiKeyId: null,
    comboId: null,
    connectionId: null,
    modelPattern: null,
    sourceEffort: "any",
    requestTags: [],
    tagMatchMode: "any",
    effortMode: "inherit",
    targetEffort: null,
    targetKind: "keep",
    targetModel: null,
    targetComboId: null,
    budgetAction: "preserve",
    budgetTokens: null,
    priority: 0,
    enabled: true,
    ...patch,
  };
}

test("reasoning tags preserve source normalization and defensive corrupt-row handling", () => {
  assert.deepEqual(parseReasoningRuleTags('[" Alpha ","ALPHA","Beta",null,2,""]'), [
    "alpha",
    "beta",
  ]);
  assert.deepEqual(parseReasoningRuleTags("{broken"), []);
  assert.deepEqual(parseReasoningRuleTags({ tag: "alpha" }), []);
});

test("rule projection preserves source primitive/null and enabled semantics", () => {
  const row = rowToReasoningRoutingRule({
    id: "r",
    name: "Name",
    description: 7,
    scope: "global",
    source_effort: "any",
    request_tags: '["X"," x "]',
    tag_match_mode: "unexpected",
    effort_mode: "inherit",
    target_kind: "keep",
    budget_action: "preserve",
    budget_tokens: "123",
    priority: "5",
    enabled: "1",
    created_at: "a",
    updated_at: "b",
  });
  assert.deepEqual(row.requestTags, ["x"]);
  assert.equal(row.description, "");
  assert.equal(row.tagMatchMode, "any");
  assert.equal(row.budgetTokens, null);
  assert.equal(row.priority, 0);
  assert.equal(row.enabled, false);
  assert.equal(row.targetModel, null);
  assert.equal(rowToReasoningRoutingRule({ ...row, enabled: true }).enabled, true);
});

test("rule serialization retains source effort, target and budget field coupling", () => {
  const params = reasoningRuleParams(
    input({
      description: "",
      requestTags: [" X ", "x"],
      targetEffort: "none",
      targetModel: "unused",
      targetComboId: "unused",
      budgetAction: "set",
      budgetTokens: 8192,
    }),
    "r",
    "created",
    "updated"
  );
  assert.equal(params.targetEffort, null);
  assert.equal(params.targetModel, null);
  assert.equal(params.targetComboId, null);
  assert.equal(params.requestTags, '["x"]');
  assert.equal(params.budgetAction, "remove");
  assert.equal(params.budgetTokens, 8192);
  assert.equal(params.enabled, 1);
  assert.equal(params.createdAt, "created");
  assert.equal(reasoningRuleParams(input({ enabled: false }), "r", "a", "b").enabled, 0);
});

test("reference errors preserve source scope filtering and message order", () => {
  const referenced: string[] = [];
  const missing = reasoningRuleReferenceErrors(
    input({
      scope: "combo",
      comboId: "source",
      apiKeyId: "unused",
      connectionId: "unused",
      targetKind: "combo",
      targetComboId: "target",
    }),
    (table, id) => {
      referenced.push(`${table}:${id}`);
      return false;
    }
  );
  assert.deepEqual(referenced, ["combos:source", "combos:target"]);
  assert.deepEqual(missing, [
    "The referenced source combo does not exist",
    "The referenced target combo does not exist",
  ]);
  assert.deepEqual(
    reasoningRuleReferenceErrors(input(), () => false),
    []
  );
});
