import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import {
  artifactReplacementFixture as fixture,
  database,
  store,
} from "../helpers/persistence/artifactReplacementFixture.ts";

const source = new URL("../../src/lib/usage/nativeArtifactRecovery.ts", import.meta.url).href;
const helper = new URL("../helpers/persistence/artifactReplacementFixture.ts", import.meta.url)
  .href;
const seams = new Set([
  "../db/controlPlaneContext",
  "../db/persistenceRuntime",
  "../db/postgres/artifactRecoveryRepository",
  "../db/postgres/artifactPublicationRepository",
  "./nativeArtifactPublication",
  "../db/postgres/cleanupRepository",
]);
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL === source && seams.has(specifier))
      return { url: helper, shortCircuit: true };
    return next(specifier, context);
  },
});
const { recoverNativeArtifact } = await import(source);
test.after(() => hooks.deregister());
test.beforeEach(() => fixture.reset());

// These tests prove orchestration only. The production SQL/credit protocol is
// exercised separately against real PostgreSQL, not implemented by this seam.
test("confirmed absence requests a fresh owner without inline PUT, DELETE or false acknowledgement", async () => {
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), false);
  assert.deepEqual(fixture.calls, ["read", "get", "replace"]);
});

test("a positive GET attaches the original instead of allocating a replacement", async () => {
  fixture.positive = true;
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), true);
  assert.deepEqual(fixture.calls, ["read", "get", "attach"]);
});

test("a late positive GET refused attachment confirms only the retired deletion owner", async () => {
  fixture.positive = true;
  fixture.attach = false;
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), false);
  assert.deepEqual(fixture.calls, ["read", "get", "attach", "confirm-deletion"]);
});

test("a still-active raw PUT prevents GET and replacement", async () => {
  fixture.active = true;
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), false);
  assert.deepEqual(fixture.calls, ["read"]);
});

test("shared GET failure does not allocate a replacement and emits one sanitized JSON record", async (t) => {
  fixture.getError = new ArtifactStorageError("unavailable", undefined, undefined, {
    name: "AccessDenied",
    status: 403,
  });
  const warnings: unknown[][] = [];
  t.mock.method(console, "warn", (...values: unknown[]) => warnings.push(values));
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), false);
  assert.deepEqual(fixture.calls, ["read", "get", "defer"]);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].length, 1);
  assert.deepEqual(JSON.parse(String(warnings[0][0])), {
    event: "artifact.publication.deferred",
    kind: "unavailable",
    name: "AccessDenied",
    status: 403,
  });
});

test("unknown replacement COMMIT never triggers an inline retry or successor dispatch", async (t) => {
  fixture.replaceError = new Error("synthetic lost COMMIT acknowledgement");
  t.mock.method(console, "warn", () => {});
  assert.equal(await recoverNativeArtifact(fixture.row.relpath, database, store), false);
  assert.deepEqual(fixture.calls, ["read", "get", "replace", "defer"]);
});
