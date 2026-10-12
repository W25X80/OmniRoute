import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

// Both actual combo loops use their default SQLite owner and a controlled model result.
// This qualifies local classification/cascade behavior, not upstream model acceptance.
const names = ["OMNIROUTE_DATABASE_BACKEND", "DATA_DIR", "APP_LOG_TO_FILE"];
const previous = new Map(names.map((name) => [name, process.env[name]]));
const directory = mkdtempSync(join(tmpdir(), "omni-stream-capacity-combo-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DATA_DIR = directory;
process.env.APP_LOG_TO_FILE = "false";
const { handleComboChat } = await import("../../../open-sse/services/combo.ts");
const { createErrorResult } = await import("../../../open-sse/utils/error.ts");
const { PERSISTENCE_STREAM_CAPACITY_CODE } =
  await import("../../../src/lib/db/persistenceRuntime.ts");
const { getCircuitBreaker, resetAllCircuitBreakers } =
  await import("../../../src/shared/utils/circuitBreaker.ts");
const { resetDbInstance } = await import("../../../src/lib/db/core.ts");
const { resetAllComboMetrics } = await import("../../../open-sse/services/comboMetrics.ts");
const { rrCounters, rrStickyTargets } = await import("../../../open-sse/services/combo/rrState.ts");
test.after(() => {
  resetAllCircuitBreakers();
  resetAllComboMetrics();
  rrCounters.clear();
  rrStickyTargets.clear();
  resetDbInstance();
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  rmSync(directory, { recursive: true, force: true });
});
for (const strategy of ["priority", "round-robin"] as const) {
  test(`${strategy} returns stream capacity without retry, cascade or provider failure`, async () => {
    resetAllCircuitBreakers();
    const calls: string[] = [];
    const result = await handleComboChat({
      body: { stream: false, messages: [{ role: "user", content: "hi" }] },
      combo: {
        name: `stream-capacity-${strategy}`,
        strategy,
        models: ["openai/model-a", "anthropic/model-b"],
        config: { maxRetries: 2, retryDelayMs: 1, disableSessionStickiness: true },
      },
      handleSingleModel: async (_body: unknown, model: string) => {
        calls.push(model);
        return createErrorResult(
          503,
          "Gateway streaming capacity is unavailable. Please retry later.",
          null,
          PERSISTENCE_STREAM_CAPACITY_CODE,
          "local_queue_capacity"
        ).response;
      },
      isModelAvailable: async () => true,
      log: { info() {}, warn() {}, debug() {}, error() {} },
      settings: null,
      relayOptions: null,
      allCombos: null,
    });
    assert.equal(result.status, 503);
    const body = await result.json();
    assert.equal(body.error.code, PERSISTENCE_STREAM_CAPACITY_CODE);
    assert.equal(body.error.type, "local_queue_capacity");
    assert.deepEqual(calls, ["openai/model-a"]);
    assert.equal(getCircuitBreaker("openai").failureCount, 0);
    assert.equal(getCircuitBreaker("anthropic").failureCount, 0);
  });
}
