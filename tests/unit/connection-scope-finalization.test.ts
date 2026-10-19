import assert from "node:assert/strict";
import test from "node:test";
import { controlledOwnerModule } from "../helpers/persistence/controlledOwnerModule.ts";
import type { PendingRequestMetadata } from "../../src/lib/usage/usageHistory.ts";
import type { PendingRequestScope } from "../../src/lib/usage/pendingRequestScope.ts";

test("local scope finalization cannot retire another request and preserves preview privacy", async () => {
  const attempts: Array<{ id: string; metadata: PendingRequestMetadata }> = [];
  let fallbackCalls = 0;
  const owner = await controlledOwnerModule<
    typeof import("../../src/lib/usage/pendingRequestScope.ts")
  >(new URL("../../src/lib/usage/pendingRequestScope.ts", import.meta.url), {
    "./usageHistory": {
      finalizePendingRequestById(id: string, metadata: PendingRequestMetadata) {
        attempts.push({ id, metadata });
        return id === "present" ? id : null;
      },
      finalizePendingRequest() {
        fallbackCalls++;
      },
      updatePendingRequest() {},
      updatePendingRequestById() {},
    },
  });
  const scope: PendingRequestScope = {
    id: null,
    model: "model",
    provider: "codex",
    connectionId: "connection",
  };
  const metadata = {
    status: 403,
    error: "Local scope refusal",
    errorCode: "connection_scope_denied",
  };
  assert.equal(owner.finalizePendingScopeById(scope, metadata), null);
  assert.equal(attempts.length, 0);
  assert.equal(
    owner.finalizePendingScopeById({ ...scope, id: "already-finished" }, metadata),
    null
  );
  assert.equal(fallbackCalls, 0, "an absent ID must not consume a sibling's pending entry");
  assert.equal(
    owner.finalizePendingScopeById(
      { ...scope, id: "present", videoTranscriptSensitive: true },
      metadata
    ),
    "present"
  );
  assert.equal(attempts.length, 2);
  assert.equal(attempts[1].metadata.error, "[omitted: video transcript]");
  assert.equal(attempts[1].metadata.errorCode, "[omitted: video transcript]");
  assert.equal(attempts[1].metadata.status, 403);
});
