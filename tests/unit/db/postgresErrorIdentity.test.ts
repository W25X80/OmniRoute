import assert from "node:assert/strict";
import { test } from "node:test";
import vm from "node:vm";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { isPostgresErrorKind, PostgresError } from "../../../src/lib/db/postgres/database.ts";

test("ambiguous commits retain their classification across independently bundled classes", async () => {
  const result = await build({
    entryPoints: [
      fileURLToPath(new URL("../../../src/lib/db/postgres/database.ts", import.meta.url)),
    ],
    bundle: true,
    platform: "node",
    format: "cjs",
    write: false,
    logLevel: "silent",
  });
  const independent = { exports: {} as { PostgresError: typeof PostgresError } };
  vm.runInNewContext(result.outputFiles[0].text, {
    module: independent,
    exports: independent.exports,
    require: createRequire(import.meta.url),
  });
  const error = new independent.exports.PostgresError("commit_unknown");
  assert.notEqual(independent.exports.PostgresError, PostgresError);
  assert.equal(error instanceof PostgresError, false);
  assert.equal(isPostgresErrorKind(error, "commit_unknown"), true);
  assert.equal(isPostgresErrorKind(error, "query_failed"), false);
  assert.equal(isPostgresErrorKind({ kind: "commit_unknown" }, "commit_unknown"), false);
});
