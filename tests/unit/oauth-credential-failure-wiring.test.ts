import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { getDbInstance } from "../../src/lib/db/core.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { getProviderConnectionById } from "../../src/lib/db/providers.ts";
import { expireProviderCredentialsIfUnchanged } from "../../src/lib/db/providers/credentialFailure.ts";
import { credentialVersion } from "../../src/lib/providers/credentialFailure.ts";
import { isUnrecoverableRefreshError } from "../../open-sse/services/tokenRefresh/shared.ts";
import { wasRefreshTokenRotated } from "../../open-sse/services/refreshSerializer.ts";
import { controlledOwnerModule } from "../helpers/persistence/controlledOwnerModule.ts";

process.env.STORAGE_ENCRYPTION_KEY = "test-only-oauth-wiring-key";

type Patch = Record<string, unknown>;
type Persist = (id: string, patch: Patch) => Promise<unknown>;

async function persistencePort(write = expireProviderCredentialsIfUnchanged) {
  return controlledOwnerModule<{ updateProviderCredentials: Persist }>(
    new URL("../../src/sse/services/tokenRefresh.ts", import.meta.url),
    { "@/lib/db/providers/credentialFailure": { expireProviderCredentialsIfUnchanged: write } },
    { log: { error() {} } },
    (source) =>
      source.slice(
        source.indexOf("export async function updateProviderCredentials("),
        source.indexOf("// ─", source.indexOf("export async function updateProviderCredentials("))
      )
  );
}

async function requestCallback(connectionId: string, updateProviderCredentials: Persist) {
  const owner = await controlledOwnerModule<{
    callback: { onCredentialsRefreshed(patch: Patch): Promise<void> };
  }>(
    new URL("../../src/sse/handlers/chatHelpers.ts", import.meta.url),
    {},
    { credentials: { connectionId }, updateProviderCredentials },
    (source) => {
      const start = source.indexOf("onCredentialsRefreshed: async (newCreds:");
      const end = source.indexOf("onRequestSuccess: async", start);
      assert.ok(start > 0 && end > start);
      return `export const callback = { ${source.slice(start, end)} };`;
    }
  );
  return owner.callback.onCredentialsRefreshed;
}

for (const rotateAfterReread of [false, true]) {
  test(`actual request callback fences terminal OAuth state; concurrent rotation=${rotateAfterReread}`, async () => {
    const id = randomUUID();
    const credentials = { accessToken: "old-access", refreshToken: "old-refresh" };
    const db = getDbInstance();
    db.prepare(
      `INSERT INTO provider_connections
      (id,provider,auth_type,is_active,test_status,access_token,refresh_token,created_at,updated_at)
      VALUES (?,'codex','oauth',1,'active',?,?,'created','initial')`
    ).run(id, encrypt(credentials.accessToken), encrypt(credentials.refreshToken));
    const port = await persistencePort();
    const callback = await requestCallback(id, port.updateProviderCredentials);
    let callbacks = 0;
    const owner = await controlledOwnerModule<{ fail(): Promise<void> }>(
      new URL("../../open-sse/handlers/chatCore/streamingResponse.ts", import.meta.url),
      {},
      {
        isUnrecoverableRefreshError,
        wasRefreshTokenRotated,
        getProviderConnectionById,
        newCredentials: { error: "unrecoverable_refresh_error", code: "refresh_token_invalidated" },
        connectionId: id,
        provider: "codex",
        attemptedRefreshToken: credentials.refreshToken,
        attemptedCredentialVersion: credentialVersion(credentials),
        log: { warn() {} },
        onCredentialsRefreshed: async (patch: Patch) => {
          callbacks++;
          if (rotateAfterReread) {
            db.prepare(
              "UPDATE provider_connections SET access_token=?,refresh_token=? WHERE id=?"
            ).run(encrypt("new-access"), encrypt("new-refresh"), id);
          }
          await callback(patch);
        },
      },
      (source) => {
        const start = source.indexOf("      if (isUnrecoverableRefreshError(newCredentials)");
        const end = source.indexOf("\n    }\n  }\n\n  // Check provider response", start);
        assert.ok(start > 0 && end > start);
        return `export async function fail() { ${source.slice(start, end)} }`;
      }
    );
    await owner.fail();
    assert.equal(callbacks, 1);
    const row = await getProviderConnectionById(id);
    assert.ok(row);
    assert.equal(row.isActive, rotateAfterReread);
    assert.equal(row.testStatus, rotateAfterReread ? "active" : "expired");
    assert.equal(row.accessToken, rotateAfterReread ? "new-access" : "old-access");
    if (!rotateAfterReread) {
      assert.equal(row.errorCode, "refresh_token_invalidated");
      assert.equal(row.lastErrorType, "unrecoverable_refresh_error");
      assert.equal(row.lastErrorSource, "oauth");
    }
  });
}

test("a terminal-state persistence failure is reported without replay or breaking fallback", async () => {
  let attempts = 0;
  const owner = await persistencePort(async () => {
    attempts++;
    throw new Error("PostgreSQL commit outcome is unknown");
  });
  assert.equal(
    await owner.updateProviderCredentials("id", {
      expectedCredentialVersion: "a".repeat(64),
      errorCode: "invalid_grant",
    }),
    false
  );
  assert.equal(attempts, 1);
});
