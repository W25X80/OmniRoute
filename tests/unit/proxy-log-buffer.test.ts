import assert from "node:assert/strict";
import test from "node:test";
import { NativeProxyLogBuffer } from "../../src/lib/proxyLogBuffer.ts";

test("row reservations include in-flight failed batches and reject newest events", () => {
  const buffer = new NativeProxyLogBuffer(2, 1000, 1000);
  assert.equal(buffer.reserve("old-a", 100), true);
  assert.equal(buffer.reserve("old-b", 200), true);
  // Moving old-a into an in-flight batch does not release its reservation.
  assert.equal(buffer.reserve("new-c", 100), false);
  assert.equal(buffer.reserve("old-a", 999), false);
  assert.equal(buffer.status().bufferedBytes, 300);
  assert.equal(buffer.has("old-b"), true);
  buffer.release("old-a");
  assert.equal(buffer.reserve("new-c", 100), true);
  assert.equal(buffer.has("old-b"), true);
  assert.equal(buffer.status().droppedEvents, 2);
});
test("byte bounds are independent of row bounds and release is idempotent", () => {
  const buffer = new NativeProxyLogBuffer(100, 500, 1000);
  assert.equal(buffer.reserve("a", 400), true);
  assert.equal(buffer.reserve("b", 100), true);
  assert.equal(buffer.reserve("c", 1), false);
  assert.equal(buffer.status().bufferedBytes, 500);
  buffer.release("a");
  buffer.release("a");
  buffer.release("missing");
  assert.equal(buffer.status().bufferedBytes, 100);
  assert.equal(buffer.status().bufferedRows, 1);
});
test("oversized or malformed events cannot consume reserved capacity", () => {
  const buffer = new NativeProxyLogBuffer(10, 1000, 100);
  for (const [id, bytes] of [
    ["oversize", 101],
    ["negative", -1],
    ["nan", NaN],
    ["unsafe", Number.MAX_SAFE_INTEGER + 1],
  ] as const) {
    assert.equal(buffer.reserve(id, bytes), false);
  }
  assert.equal(buffer.status().bufferedRows, 0);
  assert.equal(buffer.status().oversizedEvents, 4);
});
test("overflow warnings are coalesced independently of dropped-event counters", () => {
  const buffer = new NativeProxyLogBuffer();
  assert.equal(buffer.warningDue(0), true);
  assert.equal(buffer.warningDue(59_999), false);
  assert.equal(buffer.warningDue(60_000), true);
});
test("outage retry is capped and success restores normal scheduling", () => {
  const buffer = new NativeProxyLogBuffer();
  buffer.noteFlushFailure(0);
  assert.equal(buffer.canScheduleFlush(999), false);
  assert.equal(buffer.canScheduleFlush(1000), true);
  buffer.noteFlushFailure(1000);
  assert.equal(buffer.status().nextFlushAt, 3000);
  for (let i = 0; i < 20; i++) buffer.noteFlushFailure(10_000);
  assert.equal(buffer.status().nextFlushAt, 40_000);
  buffer.noteFlushSuccess();
  assert.equal(buffer.canScheduleFlush(0), true);
  assert.equal(buffer.status().consecutiveFlushFailures, 0);
});
test("sustained pressure cannot exceed either bound or evict accepted old rows", () => {
  const buffer = new NativeProxyLogBuffer(5, 500, 100);
  for (let index = 0; index < 10_000; index++) {
    buffer.reserve(String(index), 100);
    assert.ok(buffer.status().bufferedRows <= 5);
    assert.ok(buffer.status().bufferedBytes <= 500);
  }
  assert.equal(buffer.has("0"), true);
  assert.equal(buffer.has("4"), true);
  assert.equal(buffer.status().droppedEvents, 9995);
});

test("native projection bounds fields before sanitization and discards unknown proxy properties", async () => {
  const previous = process.env.DATABASE_BACKEND;
  process.env.DATABASE_BACKEND = "postgres";
  try {
    const { projectNativeProxyInput, hasOversizedNativeProxyIdentity } =
      await import("../../src/lib/proxyLogger.ts");
    const oversized = "x".repeat(1_000_000);
    const projected = projectNativeProxyInput({
      error: oversized,
      targetUrl: oversized,
      provider: oversized,
      proxy: {
        type: "http",
        host: oversized,
        port: "8080",
        name: oversized,
        password: "must-not-copy",
      } as { type: string; host: string; port: string; name: string },
      connectionId: "stable-connection",
      headersMs: 10,
      firstChunkMs: null,
    });
    assert.equal(projected.error!.length, 1024);
    assert.equal(projected.targetUrl!.length, 2048);
    assert.equal(projected.provider!.length, 4096);
    assert.equal(projected.proxy!.host.length, 4096);
    assert.equal(projected.connectionId, "stable-connection");
    assert.equal(projected.headersMs, 10);
    assert.equal("password" in projected.proxy!, false);
    assert.ok(JSON.stringify(projected).length < 20_000);
    assert.equal(hasOversizedNativeProxyIdentity({ correlationId: oversized }), true);
    assert.equal(hasOversizedNativeProxyIdentity({ correlationId: "c".repeat(2048) }), false);
    const identity = projectNativeProxyInput({
      correlationId: "c".repeat(2048),
    });
    assert.equal(identity.correlationId, "c".repeat(2048));
    const malformed = projectNativeProxyInput({
      proxy: { type: "http", host: "example.test", port: "invalid" },
      latencyMs: Infinity,
      upstreamStatus: Infinity,
      attemptNumber: 1e300,
      headersMs: 1e300,
      firstChunkMs: 1e300,
    });
    assert.equal(malformed.proxy!.port, 0);
    assert.equal(malformed.latencyMs, 0);
    assert.equal(malformed.upstreamStatus, null);
    assert.equal(malformed.attemptNumber, null);
    assert.equal(malformed.headersMs, null);
    assert.equal(malformed.firstChunkMs, null);
  } finally {
    if (previous === undefined) delete process.env.DATABASE_BACKEND;
    else process.env.DATABASE_BACKEND = previous;
  }
});

test("duplicate ids cannot bypass row or byte reservations", () => {
  const buffer = new NativeProxyLogBuffer(2, 200, 100);
  let accepted = 0;
  for (let index = 0; index < 10_000; index++) if (buffer.reserve("same-id", 100)) accepted++;
  assert.equal(accepted, 1);
  assert.equal(buffer.status().bufferedRows, 1);
  assert.equal(buffer.status().bufferedBytes, 100);
  assert.equal(buffer.status().droppedEvents, 9999);
});
