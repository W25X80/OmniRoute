import test from "node:test";
import assert from "node:assert/strict";
import {
  connectionScopeFromConfiguredIds as configured,
  connectionScopeFromResolvedIds as resolved,
  UNRESTRICTED_CONNECTION_SCOPE as unrestricted,
} from "../../open-sse/services/connectionScope.ts";
import { scopedCredentialSelector } from "../../open-sse/services/scopedCredentialSelection.ts";
import {
  targetConnectionScope,
  narrowConnectionScope,
  keylessProviderRejection,
} from "../../open-sse/services/comboKeyScope.ts";

test("configured empty remains unrestricted while computed empty is absorbing deny", async () => {
  let selections = 0;
  const select = scopedCredentialSelector(
    async (_p: string, _e: string | null, _ids: string[] | null) => {
      selections += 1;
      return { connectionId: "A", apiKey: "test" };
    }
  );
  assert.equal((await select("p", null, configured([])))?.connectionId, "A");
  const empty = narrowConnectionScope(configured(["A"]), ["B"]);
  assert.deepEqual(empty, resolved([]));
  assert.deepEqual(targetConnectionScope(empty, { allowedConnectionIds: [] }), resolved([]));
  assert.equal(await select("p", null, empty), null);
  assert.equal(selections, 1, "deny is resolved before any credential/lease selection");
});

test("target pins use chat's explicit-list precedence and never widen the parent scope", () => {
  for (const [target, expected] of [
    [{ connectionId: "A" }, configured(["A"])],
    [{ connectionId: "A", allowedConnectionIds: [] }, configured(["A"])],
    [{ connectionId: "A", allowedConnectionIds: ["B"] }, configured(["B"])],
    [{ allowedConnectionIds: [] }, unrestricted],
  ] as const) {
    assert.deepEqual(targetConnectionScope(unrestricted, target), expected);
    assert.deepEqual(targetConnectionScope(resolved([]), target), resolved([]));
  }
  assert.deepEqual(targetConnectionScope(configured(["A"]), { connectionId: "B" }), resolved([]));
  assert.deepEqual(
    targetConnectionScope(configured(["A", "B"]), {
      connectionId: "A",
      allowedConnectionIds: ["B", "C"],
    }),
    configured(["B"])
  );
});

test("the actual returned identity is checked after a selector mutates its array argument", async () => {
  const scope = configured(["A"]);
  const select = scopedCredentialSelector(
    async (_p: string, _e: string | null, ids: string[] | null) => {
      ids?.push("B");
      return { connectionId: "B", apiKey: "outside" };
    }
  );
  assert.equal(await select("p", null, scope), null);
  assert.deepEqual(scope, configured(["A"]), "legacy input is a copy");
});

test("initial forced target compatibility retains an explicit pool while threading the pin", async () => {
  const calls: unknown[][] = [];
  const select = scopedCredentialSelector(
    async (
      ...args: [string, string | null, string[] | null, string, { forcedConnectionId: string }]
    ) => {
      calls.push(args);
      return { connectionId: "B" };
    }
  );
  const scope = targetConnectionScope(configured(["A", "B"]), {
    connectionId: "A",
    allowedConnectionIds: ["A", "B"],
  });
  assert.equal(
    (await select("p", null, scope, "m", { forcedConnectionId: "A" }))?.connectionId,
    "B"
  );
  assert.deepEqual(calls, [["p", null, ["A", "B"], "m", { forcedConnectionId: "A" }]]);
});

test("keyless admission requires noauth or a proven in-scope actual identity", async () => {
  let lookups = 0;
  const outside = async () => {
    lookups += 1;
    return { connectionId: "B" };
  };
  assert.equal(await keylessProviderRejection("p", configured(["noauth"]), outside), null);
  assert.equal(lookups, 0);
  assert.equal((await keylessProviderRejection("p", configured(["A"]), outside))?.status, 404);
  assert.equal(
    await keylessProviderRejection("p", configured(["A"]), async () => ({ connectionId: "A" })),
    null
  );
});
