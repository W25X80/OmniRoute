import assert from "node:assert/strict";
import test from "node:test";
import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import {
  artifactFailureDiagnostic,
  recordArtifactReconciliationPass,
  getArtifactReconciliationStatus,
} from "../../src/lib/usage/artifactReconciliationStatus.ts";

test("an empty pass cannot erase a GET denial or claim recovery from a successful DELETE", () => {
  const counts = {
    pending: 1,
    uncertain: 1,
    legacy: 0,
    foreignScope: 0,
    stagedBytes: 0,
    oldestPendingAt: "2026-10-15",
  };
  const failure = new ArtifactStorageError("unavailable", undefined, undefined, {
    name: "AccessDenied",
    status: 403,
    requestId: "request",
  });
  assert.equal(recordArtifactReconciliationPass(counts, { failure, operation: "get" }), false);
  const degraded = getArtifactReconciliationStatus();
  assert.equal(degraded.state, "degraded");
  assert.ok(degraded.lastFailureAt);
  assert.equal(recordArtifactReconciliationPass(counts), false);
  assert.equal(recordArtifactReconciliationPass(counts, { succeeded: new Set(["delete"]) }), false);
  assert.equal(getArtifactReconciliationStatus().degradedSince, degraded.degradedSince);
  assert.equal(recordArtifactReconciliationPass(counts, { succeeded: new Set(["get"]) }), true);
  assert.equal(getArtifactReconciliationStatus().state, "pending");
  assert.equal(getArtifactReconciliationStatus().lastFailure, null);
});

test("object integrity failure does not become a common transport failure", async () => {
  const { isSharedArtifactFailure } =
    await import("../../src/lib/usage/artifactReconciliationStatus.ts");
  assert.equal(isSharedArtifactFailure(new ArtifactStorageError("integrity")), false);
  assert.equal(isSharedArtifactFailure(new ArtifactStorageError("invalid_reference")), false);
  assert.equal(isSharedArtifactFailure(new ArtifactStorageError("unavailable")), true);
});

test("old uncertainty requires review without inventing last-progress evidence or flooding logs", (t) => {
  let now = Date.parse("2026-10-20T02:00:00Z");
  t.mock.method(Date, "now", () => now);
  const warnings: unknown[][] = [];
  t.mock.method(console, "warn", (...args: unknown[]) => {
    warnings.push(args);
  });
  const counts = {
    pending: 28,
    uncertain: 28,
    legacy: 0,
    foreignScope: 0,
    stagedBytes: 4484523,
    publishing: 11,
    deletePending: 17,
    oldestPendingAt: "2026-10-19T00:00:00Z",
    oldestUncertainCreatedAt: "2026-10-20T01:30:01Z",
  };
  recordArtifactReconciliationPass(counts);
  assert.equal(getArtifactReconciliationStatus().reviewRequired, false);
  now += 1000;
  recordArtifactReconciliationPass(counts, { succeeded: new Set(["get"]) });
  const status = getArtifactReconciliationStatus();
  assert.equal(status.reviewRequired, true);
  assert.equal(status.state, "pending");
  assert.equal(status.publishing, 11);
  assert.equal(status.deletePending, 17);
  assert.equal(warnings.length, 1);
  assert.ok(!Object.hasOwn(status, "reviewWarningAfter"));
  recordArtifactReconciliationPass(counts, { succeeded: new Set(["get"]) });
  assert.equal(warnings.length, 1);
  now += 30 * 60000;
  recordArtifactReconciliationPass(counts);
  assert.equal(warnings.length, 2);
  recordArtifactReconciliationPass({
    ...counts,
    pending: 0,
    uncertain: 0,
    publishing: 0,
    deletePending: 0,
    oldestUncertainCreatedAt: null,
  });
  assert.equal(getArtifactReconciliationStatus().reviewRequired, false);
});

test("pending age alone cannot establish old uncertainty", () => {
  recordArtifactReconciliationPass({
    pending: 1,
    uncertain: 1,
    legacy: 0,
    foreignScope: 0,
    stagedBytes: 0,
    oldestPendingAt: "2000-01-01",
    oldestUncertainCreatedAt: null,
  });
  assert.equal(getArtifactReconciliationStatus().reviewRequired, false);
});

test("safe transport provenance and timings survive the management diagnostic projection", () => {
  const diagnostic = Object.freeze({
    name: "TimeoutError",
    timeoutSource: "request" as const,
    operation: "put" as const,
    phase: "transport" as const,
    elapsedMs: 4200,
    preparationMs: 200,
    transportMs: 4000,
    sizeBytes: 1024,
    activeOperations: 2,
    activeSockets: 2,
    queuedRequests: 0,
  });
  const failure = new ArtifactStorageError("put_unknown", "private-reference", true, diagnostic);
  assert.deepEqual(artifactFailureDiagnostic(failure), { kind: "put_unknown", ...diagnostic });
  assert.equal(
    JSON.stringify(artifactFailureDiagnostic(failure)).includes("private-reference"),
    false
  );
});
