import assert from "node:assert/strict";
import test from "node:test";
import {
  runConnectionRecoveryTick,
  selectRecoverableConnections,
} from "../../src/lib/quota/connectionRecovery.ts";

test("elapsed time never proves recovery of expired credentials", () => {
  const nowMs = Date.now();
  const expired = [null, "unauthorized", "refresh_token_invalidated", "invalid_grant"].map(
    (lastErrorType, index) => ({
      id: `expired-${index}`,
      testStatus: "expired",
      isActive: false,
      lastErrorType,
      lastErrorAt: index === 0 ? null : new Date(nowMs - 86_400_000).toISOString(),
    })
  );
  const transient = {
    id: "transient",
    testStatus: "unavailable",
    rateLimitedUntil: new Date(nowMs - 1000).toISOString(),
  };
  assert.deepEqual(selectRecoverableConnections([...expired, transient], nowMs), [transient]);
});

test("recovery tick preserves terminal authentication metadata and inactive state", async () => {
  const connection = {
    id: "revoked-oauth",
    isActive: false,
    testStatus: "expired",
    lastErrorType: "refresh_token_invalidated",
    lastErrorAt: null,
  };
  const before = structuredClone(connection);
  let clears = 0;
  const result = await runConnectionRecoveryTick({
    loadConnections: async () => [connection],
    clearConnectionError: async () => {
      clears++;
      connection.testStatus = "active";
    },
  });
  assert.equal(clears, 0);
  assert.equal(result.recovered, 0);
  assert.deepEqual(connection, before);
});
