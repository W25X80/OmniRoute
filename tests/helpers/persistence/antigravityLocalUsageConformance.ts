import assert from "node:assert/strict";
import test from "node:test";

export type LocalUsageRow = {
  provider?: string;
  model?: string;
  connectionId?: string;
  timestamp?: string;
  input?: number | null;
  output?: number | null;
  reasoning?: number | null;
  cacheRead?: number;
  cacheCreation?: number;
  success?: number | null;
};
export const RESET = "2026-10-11T12:00:00.000Z";
export const MODEL = "gemini-3.7-flash-high";
export const CONNECTION = "l50-local-usage";
export function normalizedRow(row: LocalUsageRow) {
  return {
    provider: row.provider ?? "antigravity",
    model: row.model ?? MODEL,
    connectionId: row.connectionId ?? CONNECTION,
    timestamp: row.timestamp ?? "2026-10-11T10:00:00.000Z",
    input: row.input === undefined ? 0 : row.input,
    output: row.output === undefined ? 0 : row.output,
    reasoning: row.reasoning === undefined ? 0 : row.reasoning,
    cacheRead: row.cacheRead ?? 0,
    cacheCreation: row.cacheCreation ?? 0,
    success: row.success === undefined ? 1 : row.success,
  };
}
export interface LocalUsageFixture {
  seed(rows: LocalUsageRow[]): Promise<void>;
  units(
    provider: "antigravity" | "agy",
    connectionId: string | undefined,
    model: string,
    resetAt: string | null
  ): Promise<number>;
}
export function registerAntigravityLocalUsageConformance(
  name: string,
  create: () => Promise<LocalUsageFixture>
) {
  test(`${name}: exact identity, successful counters and half-open upstream window`, async () => {
    const fixture = await create();
    await fixture.seed([
      { timestamp: "2026-10-11T07:00:00.000Z", input: 1000 },
      { output: 1500, reasoning: 500, input: null, cacheRead: 900000, cacheCreation: 900000 },
      { timestamp: "2026-10-11T06:59:59.999Z", input: 900000 },
      { timestamp: RESET, input: 900000 },
      { timestamp: "2026-10-11T12:00:00.001Z", input: 900000 },
      { timestamp: "2026-10-11 10:00:00", input: 900000 },
      { provider: "agy", input: 9000 },
      { provider: "Antigravity", input: 900000 },
      { connectionId: "l50-other", input: 900000 },
      { model: "gemini-3.7-flash-low", input: 900000 },
      { model: MODEL.toUpperCase(), input: 900000 },
      { success: 0, input: 900000 },
      { success: 2, input: 900000 },
      { success: null, input: 900000 },
    ]);
    assert.equal(await fixture.units("antigravity", CONNECTION, MODEL, RESET), 3);
    assert.equal(await fixture.units("agy", CONNECTION, MODEL, RESET), 9);
  });
  test(`${name}: token units round upward and nonpositive totals remain zero`, async () => {
    const fixture = await create();
    for (const [tokens, expected] of [
      [1, 1],
      [1000, 1],
      [1001, 2],
      [3000, 3],
      [0, 0],
      [-1, 0],
    ]) {
      const key = `l50-unit-${tokens}`;
      await fixture.seed([{ connectionId: key, input: tokens }]);
      assert.equal(await fixture.units("antigravity", key, MODEL, RESET), expected);
    }
  });
  test(`${name}: resetAt normalization determines the window independently of the local clock`, async () => {
    const fixture = await create();
    await fixture.seed([{ timestamp: "2026-10-11T07:00:00.000Z", reasoning: 1 }]);
    const originalNow = Date.now;
    try {
      Date.now = () => 0;
      assert.equal(
        await fixture.units("antigravity", CONNECTION, MODEL, "2026-10-11T14:00:00+02:00"),
        1
      );
    } finally {
      Date.now = originalNow;
    }
    assert.equal(await fixture.units("antigravity", undefined, MODEL, RESET), 0);
    assert.equal(await fixture.units("antigravity", CONNECTION, "", RESET), 0);
    assert.equal(await fixture.units("antigravity", CONNECTION, MODEL, null), 0);
    assert.equal(await fixture.units("antigravity", CONNECTION, MODEL, "invalid"), 0);
    assert.equal(await fixture.units("antigravity", "l50-empty", MODEL, RESET), 0);
  });
}
