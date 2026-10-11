import assert from "node:assert/strict";
import test from "node:test";
import { decrypt, looksEncrypted } from "../../../src/lib/db/encryption.ts";
import { connectionPersistence } from "../../../src/lib/db/postgres/providerConnectionSemantics.ts";

test("PostgreSQL credential fields reject non-string values before encryption and SQL binding", () => {
  for (const value of [
    { secret: "private-sentinel" },
    ["private-sentinel"],
    7,
    0,
    Buffer.from("private-sentinel"),
    true,
  ]) {
    for (const field of ["apiKey", "accessToken", "refreshToken", "idToken"]) {
      const data = { [field]: value };
      assert.throws(
        () => connectionPersistence(data, data),
        (error: Error) => {
          assert.equal(error.message, "PostgreSQL provider credentials must be strings or null");
          assert.doesNotMatch(error.message, /private-sentinel/);
          return true;
        }
      );
      assert.throws(
        () => connectionPersistence(data, data, { [field]: "enc:v1:previous" }),
        /credentials must be strings or null/
      );
    }
  }
});

test("PostgreSQL credential persistence rejects plaintext fallback without exposing the credential", () => {
  const previous = process.env.STORAGE_ENCRYPTION_KEY;
  const previousNodeEnv = process.env.NODE_ENV;
  process.env.NODE_ENV = "test";
  delete process.env.STORAGE_ENCRYPTION_KEY;
  try {
    for (const field of ["apiKey", "accessToken", "refreshToken", "idToken"]) {
      const data = { [field]: "private-sentinel" };
      assert.throws(
        () => connectionPersistence(data, data),
        (error: Error) => {
          assert.equal(error.message, "PostgreSQL provider credential encryption failed");
          assert.doesNotMatch(error.message, /private-sentinel/);
          return true;
        }
      );
    }
    assert.equal(connectionPersistence({ apiKey: null }, { apiKey: null }).apiKey, null);
    assert.equal(connectionPersistence({ apiKey: "" }, { apiKey: "" }).apiKey, "");
    const stored = { apiKey: "enc:v1:existing-ciphertext", name: "old" };
    assert.equal(
      connectionPersistence({ ...stored, name: "new" }, { name: "new" }, stored).apiKey,
      stored.apiKey
    );
  } finally {
    if (previous === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = previous;
    if (previousNodeEnv === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = previousNodeEnv;
  }
});

test("PostgreSQL newly supplied credentials use the existing encrypted representation", () => {
  const previous = process.env.STORAGE_ENCRYPTION_KEY;
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-postgres-provider-encryption-key";
  try {
    const data = { apiKey: "private-sentinel" };
    const persisted = connectionPersistence(data, data);
    assert.equal(looksEncrypted(persisted.apiKey), true);
    assert.notEqual(persisted.apiKey, data.apiKey);
    assert.equal(decrypt(String(persisted.apiKey)), data.apiKey);
  } finally {
    if (previous === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = previous;
  }
});
