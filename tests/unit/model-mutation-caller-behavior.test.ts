import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const directory = mkdtempSync(join(tmpdir(), "model-mutation-callers-sqlite-"));
const environmentNames = [
  "DATA_DIR",
  "OMNIROUTE_DATABASE_BACKEND",
  "JWT_SECRET",
  "API_KEY_SECRET",
  "STORAGE_ENCRYPTION_KEY",
];
const previous = new Map(environmentNames.map((name) => [name, process.env[name]]));
process.env.DATA_DIR = directory;
delete process.env.OMNIROUTE_DATABASE_BACKEND;
process.env.JWT_SECRET = "test-only-model-mutation-jwt-secret";
process.env.API_KEY_SECRET = "test-only-model-mutation-api-key-secret";
process.env.STORAGE_ENCRYPTION_KEY = "test-only-model-mutation-encryption-key";
const core = await import("../../src/lib/db/core.ts");
const { runModelMutationCallerBehavior } =
  await import("../helpers/persistence/modelMutationCallerBehavior.ts");

test.after(() => {
  core.resetDbInstance();
  rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("default SQLite preserves actual model mutation caller behavior", async (t) => {
  core.resetDbInstance();
  await runModelMutationCallerBehavior(t, {
    prefix: "l65-sqlite",
    selected: async (body) => body(),
    async readDurable(namespace, key) {
      const row = core
        .getDbInstance()
        .prepare("SELECT value FROM key_value WHERE namespace=? AND key=?")
        .get(namespace, key) as { value: string } | undefined;
      return row?.value;
    },
  });
  assert.equal(existsSync(join(directory, "storage.sqlite")), true);
});
