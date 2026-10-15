import assert from "node:assert/strict";
import test from "node:test";
import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import {
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
