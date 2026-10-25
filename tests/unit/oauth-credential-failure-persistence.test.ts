import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { getDbInstance } from "../../src/lib/db/core.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { expireProviderCredentialsIfUnchanged } from "../../src/lib/db/providers/credentialFailure.ts";
import {
  credentialFailureState,
  credentialVersion,
} from "../../src/lib/providers/credentialFailure.ts";

process.env.STORAGE_ENCRYPTION_KEY = "test-only-credential-state-encryption";

function seed() {
  const id = randomUUID();
  const credentials = { accessToken: "old-access", refreshToken: "old-refresh" };
  const until = new Date(Date.now() + 600_000).toISOString();
  getDbInstance()
    .prepare(
      `INSERT INTO provider_connections
       (id,provider,auth_type,is_active,test_status,access_token,refresh_token,
        provider_specific_data,rate_limited_until,created_at,updated_at)
       VALUES (?,'codex','oauth',1,'active',?,?,?,?,'created','initial')`
    )
    .run(
      id,
      encrypt(credentials.accessToken),
      encrypt(credentials.refreshToken),
      JSON.stringify({ kept: true }),
      until
    );
  return { id, credentials, until };
}

function row(id: string) {
  return getDbInstance().prepare("SELECT * FROM provider_connections WHERE id=?").get(id) as Record<
    string,
    unknown
  >;
}

test("terminal failure preserves credentials, quota, unrelated metadata and its exact reason", async () => {
  const { id, credentials, until } = seed();
  const before = row(id);
  assert.equal(
    await expireProviderCredentialsIfUnchanged(
      id,
      credentialVersion(credentials),
      "refresh_token_invalidated"
    ),
    true
  );
  const after = row(id);
  assert.equal(after.is_active, 0);
  assert.equal(after.test_status, "expired");
  assert.equal(after.error_code, "refresh_token_invalidated");
  assert.equal(after.last_error_type, "unrecoverable_refresh_error");
  assert.equal(after.last_error_source, "oauth");
  assert.ok(Date.parse(String(after.last_error_at)) > 0);
  assert.equal(after.access_token, before.access_token);
  assert.equal(after.refresh_token, before.refresh_token);
  assert.equal(after.rate_limited_until, until);
  assert.equal(after.provider_specific_data, before.provider_specific_data);
});

for (const changed of ["accessToken", "refreshToken"] as const) {
  test(`a late failure cannot disable a newer ${changed}`, async () => {
    const { id, credentials } = seed();
    getDbInstance()
      .prepare(
        `UPDATE provider_connections SET ${changed === "accessToken" ? "access_token" : "refresh_token"}=?,updated_at='rotated' WHERE id=?`
      )
      .run(encrypt(`new-${changed}`), id);
    const before = row(id);
    assert.equal(
      await expireProviderCredentialsIfUnchanged(
        id,
        credentialVersion(credentials),
        "refresh_token_invalidated"
      ),
      false
    );
    assert.deepEqual(row(id), before);
  });
}

test("an operator-disabled connection stays disabled and retains its intent", async () => {
  const { id, credentials } = seed();
  const data = JSON.stringify({ operatorDisabledAt: "2026-10-25T18:00:00Z" });
  getDbInstance()
    .prepare("UPDATE provider_connections SET is_active=0,provider_specific_data=? WHERE id=?")
    .run(data, id);
  await expireProviderCredentialsIfUnchanged(id, credentialVersion(credentials), "invalid_grant");
  assert.equal(row(id).is_active, 0);
  assert.equal(row(id).provider_specific_data, data);
});

test("missing rows and invalid version tokens do not write", async () => {
  assert.equal(
    await expireProviderCredentialsIfUnchanged("missing", "a".repeat(64), "invalid_grant"),
    false
  );
  const { id } = seed();
  const before = row(id);
  assert.equal(
    await expireProviderCredentialsIfUnchanged(id, "not-a-version", "invalid_grant"),
    false
  );
  assert.deepEqual(row(id), before);
});

test("provider text cannot enter the durable authentication error", () => {
  const secret = "raw upstream text with an access token";
  const result = credentialFailureState(secret, "now");
  assert.equal(result.errorCode, "unrecoverable_refresh_error");
  assert.ok(!JSON.stringify(result).includes(secret));
});
