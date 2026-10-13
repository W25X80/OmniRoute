import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("schema CLI refuses implicit online operation before reading connection secrets", () => {
  const result = spawnSync(
    process.execPath,
    ["--import", "tsx/esm", "scripts/ops/postgres-migrate-schema.ts"],
    {
      cwd: fileURLToPath(new URL("../../../", import.meta.url)),
      env: { ...process.env, OMNIROUTE_POSTGRES_PASSWORD: "secret-sentinel" },
      encoding: "utf8",
      timeout: 10000,
    }
  );
  assert.equal(result.status, 1);
  assert.match(result.stderr, /offline acknowledgement/);
  assert.doesNotMatch(result.stderr, /secret-sentinel/);
  assert.equal(result.stdout, "");
});
