// #6219 follow-up — a COMBO per-model timeout must also evict the sticky session
// pin.
//
// Observed in production: combo "coding" [priority] pinned a session to one codex
// account. That account stalled past comboTargetTimeoutMs, the combo aborted the
// target and synthesized a 524, and — because a stall is not a quota/auth failure —
// nothing called markAccountUnavailable. The #6219 eviction only runs on that
// generic failover path, so the pin survived its full 30-minute TTL and every
// following request in the session was handed straight back to the stalled
// account: four consecutive requests, four 120s timeouts, "all targets exhausted"
// each time, while four sibling codex accounts sat healthy and unused.
//
// The fix classifies the abort reason (open-sse/services/combo/comboAbortReasons.ts)
// and evicts the connection-matched pin from the dispatch site in chat.ts.

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-combo-timeout-affinity-"));
process.env.DATA_DIR = TEST_DATA_DIR;
process.env.API_KEY_SECRET = process.env.API_KEY_SECRET || "combo-timeout-affinity-test-secret";

const core = await import("../../src/lib/db/core.ts");
const affinityDb = await import("../../src/lib/db/sessionAccountAffinity.ts");
const pin = await import("../../src/sse/services/sessionAffinityPin.ts");
const abortReasons = await import("../../open-sse/services/combo/comboAbortReasons.ts");

const PROVIDER = "codex";
const SESSION = "session-combo-timeout";
const STALLED = "conn-stalled";
const HEALTHY = "conn-healthy";
const TTL = 30 * 60_000;

function abortedWith(reason: unknown): AbortSignal {
  const controller = new AbortController();
  controller.abort(reason);
  return controller.signal;
}

const timedOutSignal = () => abortedWith(new Error(abortReasons.COMBO_PER_MODEL_TIMEOUT_REASON));

test.beforeEach(() => {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
});

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("evicts the pin when the combo per-model timeout abandons the pinned account", () => {
  affinityDb.upsertSessionAccountAffinity(SESSION, PROVIDER, STALLED, Date.now(), TTL);

  const evicted = pin.evictSessionAffinityOnComboTimeout({
    sessionKey: SESSION,
    provider: PROVIDER,
    connectionId: STALLED,
    modelAbortSignal: timedOutSignal(),
  });

  assert.equal(evicted, true, "a timed-out pinned account must lose its pin");
  assert.equal(
    affinityDb.getSessionAccountAffinity(SESSION, PROVIDER, TTL),
    null,
    "the next request must be free to pick another account"
  );
});

test("leaves the pin intact on a client disconnect", () => {
  affinityDb.upsertSessionAccountAffinity(SESSION, PROVIDER, STALLED, Date.now(), TTL);

  const evicted = pin.evictSessionAffinityOnComboTimeout({
    sessionKey: SESSION,
    provider: PROVIDER,
    connectionId: STALLED,
    modelAbortSignal: abortedWith(new Error("request_signal_aborted")),
  });

  assert.equal(evicted, false, "a client hanging up says nothing about account health");
  assert.equal(
    affinityDb.getSessionAccountAffinity(SESSION, PROVIDER, TTL)?.connectionId,
    STALLED,
    "pin must survive so the session keeps its prompt-cache locality"
  );
});

test("leaves the pin intact when a hedged sibling cancelled this target", () => {
  affinityDb.upsertSessionAccountAffinity(SESSION, PROVIDER, STALLED, Date.now(), TTL);

  const evicted = pin.evictSessionAffinityOnComboTimeout({
    sessionKey: SESSION,
    provider: PROVIDER,
    connectionId: STALLED,
    modelAbortSignal: abortedWith(new Error(abortReasons.COMBO_HEDGE_CANCELLED_REASON)),
  });

  assert.equal(evicted, false, "losing a hedge race is not an account failure");
  assert.equal(affinityDb.getSessionAccountAffinity(SESSION, PROVIDER, TTL)?.connectionId, STALLED);
});

test("leaves the pin intact when the dispatch was never aborted", () => {
  affinityDb.upsertSessionAccountAffinity(SESSION, PROVIDER, STALLED, Date.now(), TTL);

  const evicted = pin.evictSessionAffinityOnComboTimeout({
    sessionKey: SESSION,
    provider: PROVIDER,
    connectionId: STALLED,
    modelAbortSignal: new AbortController().signal,
  });

  assert.equal(evicted, false);
  assert.equal(affinityDb.getSessionAccountAffinity(SESSION, PROVIDER, TTL)?.connectionId, STALLED);
});

test("never evicts a pin that points at a different (healthy) account", () => {
  affinityDb.upsertSessionAccountAffinity(SESSION, PROVIDER, HEALTHY, Date.now(), TTL);

  const evicted = pin.evictSessionAffinityOnComboTimeout({
    sessionKey: SESSION,
    provider: PROVIDER,
    connectionId: STALLED,
    modelAbortSignal: timedOutSignal(),
  });

  assert.equal(evicted, false, "connection-matched guard must hold");
  assert.equal(affinityDb.getSessionAccountAffinity(SESSION, PROVIDER, TTL)?.connectionId, HEALTHY);
});

test("no-ops without a session key or connection id", () => {
  const signal = timedOutSignal();
  assert.equal(
    pin.evictSessionAffinityOnComboTimeout({
      sessionKey: null,
      provider: PROVIDER,
      connectionId: STALLED,
      modelAbortSignal: signal,
    }),
    false
  );
  assert.equal(
    pin.evictSessionAffinityOnComboTimeout({
      sessionKey: SESSION,
      provider: PROVIDER,
      connectionId: null,
      modelAbortSignal: signal,
    }),
    false
  );
});

test("isComboPerModelTimeoutAbort accepts a bare string abort reason", () => {
  assert.equal(
    abortReasons.isComboPerModelTimeoutAbort(
      abortedWith(abortReasons.COMBO_PER_MODEL_TIMEOUT_REASON)
    ),
    true
  );
  assert.equal(abortReasons.isComboPerModelTimeoutAbort(null), false);
});

test("the combo timeout runner aborts with the shared reason constant", () => {
  const src = fs.readFileSync(
    new URL("../../open-sse/services/combo/targetTimeoutRunner.ts", import.meta.url),
    "utf8"
  );
  assert.match(
    src,
    /const abortErr = new Error\(COMBO_PER_MODEL_TIMEOUT_REASON\)/,
    "the runner must construct the shared timeout reason"
  );
  assert.match(
    src,
    /timeoutController\.abort\(abortErr\)/,
    "the runner must abort with that Error so the eviction predicate matches"
  );
});

test("chat.ts routes its upstream dispatch through the eviction-aware seam", () => {
  const src = fs.readFileSync(new URL("../../src/sse/handlers/chat.ts", import.meta.url), "utf8");
  assert.match(
    src,
    /dispatchChatWithAffinityEviction\(/,
    "chat.ts must dispatch through the seam that owns the eviction"
  );
  assert.doesNotMatch(
    src,
    /await executeChatWithBreaker\(/,
    "chat.ts must not bypass the seam by calling executeChatWithBreaker directly"
  );
});

test("the dispatch seam evicts when a dispatch is abandoned", () => {
  const src = fs.readFileSync(
    new URL("../../src/sse/handlers/chatDispatch.ts", import.meta.url),
    "utf8"
  );
  const source = ts.createSourceFile("chatDispatch.ts", src, ts.ScriptTarget.Latest, true);
  const dispatch = sourceNodes(source).find(
    (node): node is ts.FunctionDeclaration =>
      ts.isFunctionDeclaration(node) && node.name?.text === "dispatchChatWithAffinityEviction"
  );
  assert.ok(dispatch?.body, "the eviction-aware dispatch seam must exist");
  const eviction = namedCalls(dispatch.body, "evictSessionAffinityOnComboTimeoutAsync");
  assert.equal(
    eviction.length,
    1,
    "the seam must call the async connection-matched eviction owner"
  );
  assert.ok(
    ts.isArrowFunction(eviction[0].parent) && eviction[0].parent.body === eviction[0],
    "the callback must return the eviction Promise to its awaiting callers"
  );
  assert.ok(ts.isVariableDeclaration(eviction[0].parent.parent));
  assert.equal(eviction[0].parent.parent.name.getText(source), "evict");
  const input = eviction[0].arguments[0];
  assert.ok(ts.isObjectLiteralExpression(input));
  assert.deepEqual(
    input.properties.map((property) => {
      assert.ok(ts.isPropertyAssignment(property));
      return [property.name.getText(source), property.initializer.getText(source)];
    }),
    [
      ["sessionKey", "runtimeOptions.sessionAffinityKey"],
      ["provider", "args.provider"],
      ["connectionId", "args.credentials?.connectionId"],
      ["modelAbortSignal", "runtimeOptions.modelAbortSignal"],
    ]
  );
  const attempt = sourceNodes(dispatch.body).find(
    (node): node is ts.TryStatement =>
      ts.isTryStatement(node) && namedCalls(node.tryBlock, "executeChatWithBreaker").length === 1
  );
  assert.ok(attempt?.catchClause, "dispatch rejection must retain its eviction path");
  assert.ok(
    ts.isAwaitExpression(namedCalls(attempt.tryBlock, "executeChatWithBreaker")[0].parent),
    "upstream dispatch must settle before success or failure is classified"
  );
  const catchEvictions = namedCalls(attempt.catchClause.block, "evict");
  assert.equal(catchEvictions.length, 1);
  assert.ok(ts.isAwaitExpression(catchEvictions[0].parent), "rejection eviction must settle");
  const rethrow = attempt.catchClause.block.statements.find(ts.isThrowStatement);
  assert.ok(rethrow && rethrow.getStart(source) > catchEvictions[0].end);
  assert.equal(
    rethrow.expression.getText(source),
    "dispatchErr",
    "original dispatch error is retained"
  );
  const failed = dispatch.body.statements.find(
    (node): node is ts.IfStatement =>
      ts.isIfStatement(node) && node.expression.getText(source) === "!dispatched.result?.success"
  );
  assert.ok(failed, "unsuccessful results must retain their eviction path");
  const failedEvictions = namedCalls(failed.thenStatement, "evict");
  assert.equal(failedEvictions.length, 1);
  assert.ok(ts.isAwaitExpression(failedEvictions[0].parent), "failed-result eviction must settle");
  const localPressure = dispatch.body.statements.find(
    (node): node is ts.IfStatement =>
      ts.isIfStatement(node) &&
      node.expression.getText(source) === '"localResourcePressureResult" in dispatched'
  );
  assert.ok(localPressure && ts.isReturnStatement(localPressure.thenStatement));
  assert.ok(localPressure.end < failed.getStart(source), "no-dispatch pressure must skip eviction");
  const returned = dispatch.body.statements.at(-1);
  assert.ok(returned && ts.isReturnStatement(returned));
  assert.equal(returned.expression.getText(source), "dispatched");
  assert.ok(returned.getStart(source) > failedEvictions[0].end);
});
