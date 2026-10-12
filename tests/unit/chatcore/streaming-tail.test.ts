import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import ts from "typescript";

const ROOT = path.resolve(import.meta.dirname, "../../..");
const read = (p) => readFileSync(path.join(ROOT, p), "utf8");
const LEAF = "open-sse/handlers/chatCore/streamingTail.ts";
const BARREL = "open-sse/handlers/chatCore.ts";

test("the streaming tail lives in its own leaf", () => {
  const leaf = read(LEAF);
  assert.match(leaf, /export async function runStreamingTail\(/);
  assert.doesNotMatch(read(BARREL), /const onStreamComplete = \(\{/);
});

test("the barrel calls the leaf and writes the carried state back", () => {
  const barrel = read(BARREL);
  assert.match(barrel, /runStreamingTail\(/);
  for (const name of [
    "onPipelineStreamError",
    "onClientDisconnectFinalize",
    "turnExecutionHandedOffToStream",
  ]) {
    assert.match(barrel, new RegExp("carry\\." + name + "\\b"));
  }
});

test("the barrel imports the leaf (no-undef is disabled for TS, so assert it)", () => {
  const barrel = read(BARREL);
  assert.match(
    barrel,
    /import \{\s*runStreamingTail,?\s*\} from "\.\/chatCore\/streamingTail\.ts";/
  );
});

test("streaming cost accounting lives in the tail leaf, not the barrel", () => {
  const leaf = read(LEAF);
  const parsed = ts.createSourceFile(LEAF, leaf, ts.ScriptTarget.Latest, true);
  const calls: ts.CallExpression[] = [];
  const walk = (node: ts.Node) => {
    if (ts.isCallExpression(node)) calls.push(node);
    ts.forEachChild(node, walk);
  };
  walk(parsed);
  const recording = calls.filter((call) => {
    const expression = call.expression;
    return (
      ts.isParenthesizedExpression(expression) &&
      ts.isConditionalExpression(expression.expression) &&
      expression.expression.whenTrue.getText(parsed) === "recordStreamingCostAsync"
    );
  });
  assert.equal(recording.length, 1, "the tail must invoke its selected cost recorder exactly once");
  const selection = (recording[0].expression as ts.ParenthesizedExpression)
    .expression as ts.ConditionalExpression;
  assert.equal(selection.condition.getText(parsed), "nativePersistence");
  assert.equal(selection.whenFalse.getText(parsed), "recordStreamingCost");
  const args = recording[0].arguments;
  assert.equal(args.length, 1);
  assert.ok(ts.isObjectLiteralExpression(args[0]));
  const fields = new Map<string, ts.Expression>();
  for (const prop of args[0].properties) {
    if (ts.isPropertyAssignment(prop)) fields.set(prop.name.getText(parsed), prop.initializer);
    else if (ts.isShorthandPropertyAssignment(prop)) fields.set(prop.name.text, prop.name);
  }
  assert.deepEqual(
    [...fields.keys()].sort(),
    [
      "apiKeyId",
      "calculateCost",
      "ledger",
      "model",
      "provider",
      "recordCost",
      "serviceTier",
      "streamUsage",
    ].sort()
  );
  for (const [key, value] of [
    ["apiKeyId", "completedApiKeyInfo?.id"],
    ["provider", "provider"],
    ["model", "model"],
    ["streamUsage", "streamUsage"],
    ["serviceTier", "effectiveServiceTier"],
    ["calculateCost", "calculateCost"],
  ])
    assert.equal(fields.get(key)?.getText(parsed), value);
  const ledger = fields.get("ledger")!;
  assert.ok(ts.isBinaryExpression(ledger));
  assert.equal(ledger.operatorToken.kind, ts.SyntaxKind.QuestionQuestionToken);
  assert.equal(ledger.left.getText(parsed), "completionMetadata?.ledger");
  assert.ok(ts.isCallExpression(ledger.right));
  assert.equal(ledger.right.expression.getText(parsed), "buildStreamLedgerDetails");
  assert.deepEqual(
    ledger.right.arguments.map((arg) => arg.getText(parsed)),
    ["effectiveServiceTier", "normalizedStreamStatus < 400", "traceId"]
  );
  const recorder = fields.get("recordCost")!;
  assert.ok(ts.isConditionalExpression(recorder));
  assert.equal(recorder.condition.getText(parsed), "nativePersistence");
  assert.match(
    recorder.whenTrue.getText(parsed),
    /await recordCostAsync\(apiKeyId, budgetCost, details\)/
  );
  assert.match(recorder.whenFalse.getText(parsed), /recordCost\(apiKeyId, budgetCost, details\)/);
  const barrel = read(BARREL);
  assert.doesNotMatch(barrel, /recordStreamingCost\(/);
});
