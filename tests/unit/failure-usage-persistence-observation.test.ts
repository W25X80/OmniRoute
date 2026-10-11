import assert from "node:assert/strict";
import test from "node:test";
import { observeFailureUsagePersistence } from "../../open-sse/handlers/chatCore/failureUsage.ts";

test("native optional failure usage reports a rejection and resolves its observed settlement", async (t) => {
  const warnings: unknown[][] = [];
  t.mock.method(console, "warn", (...args: unknown[]) => {
    warnings.push(args);
  });
  const rejected = Promise.reject(new Error("accounting write failed\n at /private/driver.ts:11"));
  const observed = observeFailureUsagePersistence(rejected);
  await assert.doesNotReject(observed);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0][0], "[USAGE] Failed to persist failed-request usage:");
  assert.equal(typeof warnings[0][1], "string");
  assert.doesNotMatch(String(warnings[0][1]), /at \/private\//);
});

test("explicit native callers still wait for the optional persistence settlement", async () => {
  let release!: () => void;
  const operation = new Promise<void>((resolve) => {
    release = resolve;
  });
  let settled = false;
  const observed = observeFailureUsagePersistence(operation).then(() => {
    settled = true;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  release();
  await observed;
  assert.equal(settled, true);
});

test("optional failure logging cannot reject the provider error path", async (t) => {
  t.mock.method(console, "warn", () => {
    throw new Error("logger unavailable");
  });
  await assert.doesNotReject(
    observeFailureUsagePersistence(Promise.reject(new Error("write failed")))
  );
});
