import assert from "node:assert/strict";
import test from "node:test";
import { retainedPlainDataBytes } from "../../src/shared/utils/retainedPlainData.ts";

test("shared retained-data estimation preserves bridge overhead, UTF-16 and exact boundaries", () => {
  assert.equal(retainedPlainDataBytes(null, 1000, 7), 23);
  assert.equal(retainedPlainDataBytes("A😀", 1000, 7), 77);
  assert.equal(retainedPlainDataBytes({ a: "ok" }, 278, 0), 278);
  assert.equal(retainedPlainDataBytes({ a: "ok" }, 277, 0), null);
  assert.equal(retainedPlainDataBytes([true], 1000, 0), 234);
  const shared = { x: 1 };
  assert.equal(retainedPlainDataBytes({ a: shared, b: shared }, 1000, 0), 518);
  const cycle: { self?: unknown } = {};
  cycle.self = cycle;
  assert.equal(retainedPlainDataBytes(cycle, 1000, 0), 232);
});
