import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import ts from "typescript";
function sourceNodes(root: ts.Node): ts.Node[] {
  const nodes: ts.Node[] = [];
  function visit(node: ts.Node) {
    nodes.push(node);
    ts.forEachChild(node, visit);
  }
  visit(root);
  return nodes;
}

function namedCalls(root: ts.Node, name: string): ts.CallExpression[] {
  return sourceNodes(root).filter(
    (node): node is ts.CallExpression =>
      ts.isCallExpression(node) && ts.isIdentifier(node.expression) && node.expression.text === name
  );
}

const chatSource = fs.readFileSync(
  new URL("../../src/sse/handlers/chat.ts", import.meta.url),
  "utf8"
);

const chatAst = ts.createSourceFile("chat.ts", chatSource, ts.ScriptTarget.Latest, true);

function getNonAntigravityStreamFailureBlock(): ts.Block {
  const branches = sourceNodes(chatAst).filter((node): node is ts.IfStatement => {
    if (!ts.isIfStatement(node) || !ts.isBinaryExpression(node.expression)) return false;
    const condition = node.expression;
    return (
      condition.operatorToken.kind === ts.SyntaxKind.AmpersandAmpersandToken &&
      ts.isPrefixUnaryExpression(condition.right) &&
      condition.right.operator === ts.SyntaxKind.ExclamationToken &&
      condition.right.operand.getText(chatAst) === "isAntigravityStreamReadinessFailure" &&
      sourceNodes(condition.left).some(
        (part) => ts.isStringLiteral(part) && part.text === "stream_early_eof"
      )
    );
  });
  assert.equal(branches.length, 1, "non-Antigravity stream-failure branch must exist");
  assert.ok(ts.isBlock(branches[0].thenStatement));
  return branches[0].thenStatement;
}

function getNonAntigravityStreamFailureBranch(): string {
  return getNonAntigravityStreamFailureBlock().getText(chatAst);
}

test("terminal STREAM_EARLY_EOF evicts affinity after the bounded retry (#8928)", () => {
  const branch = getNonAntigravityStreamFailureBlock();
  const boundedRetry = branch.statements.find(
    (node): node is ts.IfStatement =>
      ts.isIfStatement(node) &&
      namedCalls(node.expression, "shouldRetryStreamEarlyEof").length === 1
  );
  assert.ok(boundedRetry, "the existing bounded retry must remain");
  assert.deepEqual(
    namedCalls(boundedRetry.expression, "shouldRetryStreamEarlyEof")[0].arguments.map((argument) =>
      argument.getText(chatAst)
    ),
    ["result.errorCode", "streamEarlyEofRetries"]
  );
  assert.ok(ts.isBlock(boundedRetry.thenStatement));
  assert.ok(
    ts.isContinueStatement(boundedRetry.thenStatement.statements.at(-1)),
    "the bounded same-connection retry must continue before terminal eviction"
  );
  const eviction = namedCalls(branch, "evictSessionAccountAffinityForConnectionAsync");
  assert.equal(eviction.length, 1);
  assert.ok(ts.isAwaitExpression(eviction[0].parent), "terminal eviction must settle");
  assert.ok(
    eviction[0].getStart(chatAst) > boundedRetry.end,
    "eviction must happen only after retry is exhausted"
  );
  const terminalReturn = branch.statements.find(
    (node): node is ts.ReturnStatement =>
      ts.isReturnStatement(node) &&
      node.expression !== undefined &&
      namedCalls(node.expression, "withSelectedConnectionHeader").length === 1
  );
  assert.ok(
    terminalReturn && terminalReturn.getStart(chatAst) > eviction[0].end,
    "eviction must happen before the terminal 502 is returned"
  );
});

test("early-EOF eviction recognizes both typed error signals (#8928)", () => {
  const branch = getNonAntigravityStreamFailureBranch();

  assert.match(
    branch,
    /result\.errorCode === "STREAM_EARLY_EOF"/,
    "the canonical STREAM_EARLY_EOF code must trigger eviction"
  );
  assert.match(
    branch,
    /result\.errorType === "stream_early_eof"/,
    "the typed stream_early_eof fallback must trigger eviction"
  );
});

test("early-EOF eviction is session-key and connection guarded (#8928)", () => {
  const branch = getNonAntigravityStreamFailureBlock();
  const guards = sourceNodes(branch).filter(
    (node): node is ts.IfStatement =>
      ts.isIfStatement(node) &&
      node.expression.getText(chatAst).replace(/\s/g, "") ===
        "isTerminalStreamEarlyEof&&runtimeOptions.sessionAffinityKey"
  );
  assert.equal(guards.length, 1, "only terminal early EOF with an affinity key may evict");
  const eviction = namedCalls(
    guards[0].thenStatement,
    "evictSessionAccountAffinityForConnectionAsync"
  );
  assert.equal(
    eviction.length,
    1,
    "the connection-matched owner must protect a pin that moved elsewhere"
  );
  assert.ok(ts.isAwaitExpression(eviction[0].parent));
  assert.deepEqual(
    eviction[0].arguments.map((argument) => argument.getText(chatAst)),
    ["runtimeOptions.sessionAffinityKey", "provider", "credentials.connectionId"]
  );
  assert.equal(
    namedCalls(branch, "deleteSessionAccountAffinity").length,
    0,
    "the terminal branch must not perform an unguarded affinity deletion"
  );
  assert.equal(namedCalls(branch, "deleteSessionAccountAffinityAsync").length, 0);
});
