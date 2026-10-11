import assert from "node:assert/strict";
import { test } from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import {
  ArtifactStorageError,
  isArtifactStorageErrorKind,
} from "../../../src/domain/persistence/artifactStore.ts";

test("integrity failures retain their identity across independently bundled artifact owners", async () => {
  const result = await build({
    entryPoints: [
      fileURLToPath(new URL("../../../src/domain/persistence/artifactStore.ts", import.meta.url)),
    ],
    bundle: true,
    platform: "node",
    format: "cjs",
    write: false,
    logLevel: "silent",
  });
  const independent = { exports: {} as { ArtifactStorageError: typeof ArtifactStorageError } };
  vm.runInNewContext(result.outputFiles[0].text, {
    module: independent,
    exports: independent.exports,
  });
  const error = new independent.exports.ArtifactStorageError("integrity");
  assert.notEqual(independent.exports.ArtifactStorageError, ArtifactStorageError);
  assert.equal(error instanceof ArtifactStorageError, false);
  assert.equal(isArtifactStorageErrorKind(error, "integrity"), true);
  assert.equal(isArtifactStorageErrorKind(error, "put_unknown"), false);
  assert.equal(isArtifactStorageErrorKind({ kind: "integrity" }, "integrity"), false);
});
