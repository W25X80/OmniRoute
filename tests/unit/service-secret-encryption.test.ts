import assert from "node:assert/strict";
import test from "node:test";

const originalKey = process.env.STORAGE_ENCRYPTION_KEY;
process.env.STORAGE_ENCRYPTION_KEY = "service-secret-test-stable-key";
const { protectSecret } = await import("../../src/lib/db/postgres/serviceQuery.ts");
const { decrypt, looksEncrypted } = await import("../../src/lib/db/encryption.ts");
test.after(() => {
  if (originalKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
  else process.env.STORAGE_ENCRYPTION_KEY = originalKey;
});

test("new service secrets encrypt and existing valid ciphertext is preserved exactly", () => {
  const ciphertext = protectSecret("private-service-token")!;
  assert.ok(looksEncrypted(ciphertext));
  assert.notEqual(ciphertext, "private-service-token");
  assert.equal(decrypt(ciphertext), "private-service-token");
  assert.equal(protectSecret(ciphertext), ciphertext);
});

test("an invalid ciphertext prefix cannot bypass credential encryption validation", () => {
  assert.throws(
    () => protectSecret("enc:v1:not-a-valid-ciphertext"),
    /Credential encryption failed/
  );
  assert.throws(() => protectSecret({ secret: "private" } as never), /Invalid credential value/);
  assert.equal(protectSecret(null), null);
  assert.equal(protectSecret(""), "");
});
