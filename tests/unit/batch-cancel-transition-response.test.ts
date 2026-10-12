import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { SignJWT } from "jose";

process.env.JWT_SECRET = "batch-transition-response-test-secret";
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
const { createFile } = await import("../../src/lib/db/files.ts");
const { createBatch, getBatch } = await import("../../src/lib/db/batches.ts");
const { POST } = await import("../../src/app/api/v1/batches/[id]/cancel/route.ts");
test.after(() => resetDbInstance());

async function cancel(id: string) {
  const token = await new SignJWT({ authenticated: true, sub: "admin" })
    .setProtectedHeader({ alg: "HS256" })
    .setExpirationTime("1h")
    .sign(new TextEncoder().encode(process.env.JWT_SECRET));
  return POST(
    new Request(`http://localhost/api/v1/batches/${id}/cancel`, {
      method: "POST",
      headers: { cookie: `auth_token=${token}` },
    }),
    { params: Promise.resolve({ id }) }
  );
}
function setup(status: "finalizing" | "completed") {
  const file = createFile({
    bytes: 1,
    content: Buffer.from("x"),
    filename: "input",
    purpose: "batch",
  });
  return createBatch({
    inputFileId: file.id,
    endpoint: "/v1/embeddings",
    completionWindow: "24h",
    status,
  });
}
test("cancel returns the acknowledged cancelling row and remains idempotent while pending", async () => {
  const batch = setup("finalizing");
  for (let attempt = 0; attempt < 2; attempt++) {
    const response = await cancel(batch.id);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.id, batch.id);
    assert.equal(body.status, "cancelling");
    assert.equal(body.cancelling_at, getBatch(batch.id)?.cancellingAt);
  }
});
test("cancel refuses a completed winner and does not return a stale success body", async () => {
  const batch = setup("completed");
  const response = await cancel(batch.id);
  assert.equal(response.status, 400);
  const body = await response.json();
  assert.equal(body.error.type, "invalid_request_error");
  assert.match(body.error.message, /already completed/);
  assert.equal(getBatch(batch.id)?.status, "completed");
});
test("cancel returns 404 for a missing batch", async () => {
  assert.equal((await cancel("absent-transition-batch")).status, 404);
});
