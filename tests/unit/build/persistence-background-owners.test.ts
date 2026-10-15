import assert from "node:assert/strict";
import test from "node:test";
import { inspectBackgroundOwners } from "../../../scripts/check/check-persistence-background-owners.mjs";

test("background owner scanner sees aliases and member calls, ignoring comments and text", () => {
  assert.deepEqual(
    inspectBackgroundOwners(`
    import { runPersistenceBackgroundWork as background } from "./owner";
    background(async () => {});
    runtime.runPersistenceBackgroundWork(async () => {}, { owner: "lkgp.clear" });
    // runPersistenceBackgroundWork(async () => {});
    const text = "runPersistenceBackgroundWork()";
  `),
    { unnamed: 1, invalid: [] }
  );
});
test("dynamic and invented owner labels do not satisfy the explicit-owner contract", () => {
  assert.equal(
    inspectBackgroundOwners("runPersistenceBackgroundWork(f, {owner: value});").invalid.length,
    1
  );
  assert.equal(
    inspectBackgroundOwners('runPersistenceBackgroundWork(f, {owner: "anything"});').invalid.length,
    1
  );
});
