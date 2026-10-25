import assert from "node:assert/strict";
import test from "node:test";
import { getDbInstance } from "../../src/lib/db/core.ts";
import {
  createProviderConnection,
  getProviderConnectionById,
  updateProviderConnection,
} from "../../src/lib/db/providers.ts";
import { expireProviderCredentialsIfUnchanged } from "../../src/lib/db/providers/credentialFailure.ts";
import { credentialVersion } from "../../src/lib/providers/credentialFailure.ts";
import { clearAccountError, getProviderCredentials } from "../../src/sse/services/auth.ts";

const oldTime = "2026-10-24T01:00:00.000Z";
async function seed() {
  getDbInstance().prepare("DELETE FROM provider_connections").run();
  return createProviderConnection({
    provider: "codex",
    authType: "oauth",
    accessToken: "old-access",
    refreshToken: "old-refresh",
    expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
    isActive: true,
    testStatus: "active",
    lastError: "earlier transient failure",
    lastErrorAt: oldTime,
    lastErrorType: "transient",
    errorCode: "503",
  });
}

test("a late successful request cannot erase a newer terminal OAuth failure", async () => {
  const connection = await seed();
  const credentials = await getProviderCredentials("codex");
  assert.equal(credentials.connectionId, connection.id);
  await expireProviderCredentialsIfUnchanged(
    connection.id,
    credentialVersion(credentials),
    "invalid_grant"
  );
  const terminal = await getProviderConnectionById(connection.id);
  await clearAccountError(connection.id, credentials);
  assert.equal((await getProviderConnectionById(connection.id))?.testStatus, "expired");
  assert.deepEqual(await getProviderConnectionById(connection.id), terminal);
});

test("a late success cannot erase a newer error with the same status and cooldown", async () => {
  const connection = await seed();
  const credentials = await getProviderCredentials("codex");
  await updateProviderConnection(connection.id, {
    lastError: "new transient failure",
    errorCode: "502",
    lastErrorAt: "2026-10-25T02:00:00.000Z",
  });
  const newer = await getProviderConnectionById(connection.id);
  await clearAccountError(connection.id, credentials);
  assert.deepEqual(await getProviderConnectionById(connection.id), newer);
});

test("already observed unrecoverable credentials cannot be cleared by generic success", async () => {
  const connection = await seed();
  await expireProviderCredentialsIfUnchanged(
    connection.id,
    credentialVersion({ accessToken: "old-access", refreshToken: "old-refresh" }),
    "invalid_grant"
  );
  const terminal = await getProviderConnectionById(connection.id);
  assert.ok(terminal);
  await clearAccountError(connection.id, terminal);
  assert.deepEqual(await getProviderConnectionById(connection.id), terminal);
});

test("the request credential snapshot carries the exact error timestamp", async () => {
  await seed();
  const credentials = await getProviderCredentials("codex");
  assert.equal(credentials.lastErrorAt, oldTime);
});

test("matched recovery clears errors without enabling an operator-disabled account or erasing model cooldowns", async () => {
  const connection = await seed();
  const data = {
    operatorDisabledAt: oldTime,
    kept: true,
    codexScopeRateLimitedUntil: { spark: new Date(Date.now() + 86_400_000).toISOString() },
    codexScopeRateLimitSource: { spark: "quota_reset" },
  };
  await updateProviderConnection(connection.id, { isActive: false, providerSpecificData: data });
  const snapshot = await getProviderConnectionById(connection.id);
  assert.ok(snapshot);
  await clearAccountError(connection.id, snapshot);
  const after = await getProviderConnectionById(connection.id);
  assert.ok(after);
  assert.equal(after.testStatus, "active");
  assert.equal(after.isActive, false);
  assert.equal(after.lastError, undefined);
  assert.equal(after.lastErrorAt, undefined);
  assert.deepEqual(after.providerSpecificData, snapshot.providerSpecificData);
});
