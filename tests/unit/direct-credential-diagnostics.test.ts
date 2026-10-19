import test from "node:test";
import assert from "node:assert/strict";
import { connectionScopeFromConfiguredIds as configured } from "../../open-sse/services/connectionScope.ts";
import {
  isCredentialDiagnosticSentinel,
  credentialDiagnosticResponse,
  withoutKeyScopeBlocked,
} from "../../open-sse/services/comboKeyScope.ts";
import { scopedCredentialSelector } from "../../open-sse/services/scopedCredentialSelection.ts";
import { runImageComboTargets } from "../../open-sse/services/imageCombo.ts";
import { executeImageWithCredentialFallback } from "../../src/sse/services/imageCredentialRetry.ts";

for (const [diagnostic, status] of [
  [{ allRateLimited: true }, 429],
  [{ allExpired: true }, 401],
  [{ blockedByKeyPolicy: true }, 403],
  [{ leaseConnectionMismatch: true }, 409],
] as const) {
  test(`diagnostic ${Object.keys(diagnostic)[0]} never grants dispatch authority`, async () => {
    assert.equal(isCredentialDiagnosticSentinel(diagnostic), true);
    assert.deepEqual(withoutKeyScopeBlocked(diagnostic), status === 429 ? diagnostic : null);
    assert.equal(credentialDiagnosticResponse("p", diagnostic)?.status, status);
    const select = scopedCredentialSelector(async () => diagnostic);
    assert.deepEqual(await select("p", null, configured(["A"])), diagnostic);
    let sent = 0;
    const result = await executeImageWithCredentialFallback({
      provider: "openai",
      requestedModel: "image",
      credentials: diagnostic,
      connectionScope: configured(["A"]),
      execute: async () => {
        sent += 1;
        return { success: true };
      },
      selectNextCredentials: async () => {
        throw new Error("diagnostics must not rotate");
      },
    });
    const combo = await runImageComboTargets([{ modelStr: "p/image" }], {
      resolveProvider: () => ({ provider: "p", model: "image" }),
      resolveCredentials: async () => diagnostic,
      dispatch: async () => {
        sent += 1;
        return { success: true };
      },
      onSuccess: async () => {
        throw new Error("diagnostics cannot clear provider state");
      },
    });
    assert.equal(combo.outcome, "exhausted");
    assert.equal(sent, 0);
    assert.equal(result.result.success, false);
  });
}

test("image retry checks actual scope and excludes the actual attempted account", async () => {
  const attempted: string[] = [];
  const excluded: string[][] = [];
  const scope = configured(["A", "B"]);
  const result = await executeImageWithCredentialFallback({
    provider: "openai",
    requestedModel: "image",
    credentials: { connectionId: "A", apiKey: "test" },
    connectionScope: scope,
    execute: async (credentials: { connectionId: string }) => {
      attempted.push(credentials.connectionId);
      return { success: false, status: 401 };
    },
    selectNextCredentials: async (_p, _m, ids, receivedScope) => {
      excluded.push([...ids]);
      assert.equal(receivedScope, scope);
      return { connectionId: "outside", apiKey: "outside" };
    },
  });
  assert.deepEqual(attempted, ["A"]);
  assert.deepEqual(excluded, [["A"]]);
  assert.equal(result.result.status, 401);
});

test("forced image target never rotates after the first actual send", async () => {
  let selections = 0;
  let sends = 0;
  await executeImageWithCredentialFallback({
    provider: "openai",
    requestedModel: "image",
    credentials: { connectionId: "A", apiKey: "test" },
    connectionScope: configured(["A", "B"]),
    forcedConnectionId: "A",
    execute: async () => {
      sends += 1;
      return { success: false, status: 401 };
    },
    selectNextCredentials: async () => {
      selections += 1;
      return { connectionId: "B" };
    },
  });
  assert.equal(sends, 1);
  assert.equal(selections, 0);
});

test("missing credentials cannot become a keyless send for a scoped keyed account", async () => {
  let sends = 0;
  const result = await executeImageWithCredentialFallback({
    provider: "comfyui",
    requestedModel: "image",
    credentials: null,
    connectionScope: configured(["A"]),
    execute: async () => {
      sends += 1;
      return { success: true };
    },
  });
  assert.equal(sends, 0);
  assert.equal(result.result.status, 403);
});
