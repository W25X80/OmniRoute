import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { createPersistencePoller } from "../../src/lib/config/persistencePoller.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("stop waits for startup preparation and prevents its delayed timer from being armed", async () => {
  const preparing = deferred();
  const release = deferred();
  let checks = 0;
  const failures: string[] = [];
  const poller = createPersistencePoller({
    intervalMs: 5,
    initialDelayMs: async () => {
      preparing.resolve();
      await release.promise;
      return 5;
    },
    check: async () => {
      checks++;
    },
    onFailure: (phase) => failures.push(phase),
  });
  poller.start();
  await preparing.promise;
  let stopped = false;
  const stopping = poller.stop().then(() => {
    stopped = true;
  });
  await delay(10);
  assert.equal(stopped, false);
  release.resolve();
  await stopping;
  await delay(15);
  assert.equal(checks, 0);
  assert.deepEqual(failures, []);
});

test("checks are single-flight and stop waits for the active check while discarding queued polls", async () => {
  const entered = deferred();
  const release = deferred();
  let checks = 0;
  let active = 0;
  let maximumActive = 0;
  const poller = createPersistencePoller({
    intervalMs: 5,
    initialDelayMs: 0,
    overlap: "coalesce",
    check: async () => {
      checks++;
      active++;
      maximumActive = Math.max(maximumActive, active);
      entered.resolve();
      await release.promise;
      active--;
    },
    onFailure() {},
  });
  poller.start();
  await entered.promise;
  await delay(20);
  let stopped = false;
  const stopping = poller.stop().then(() => {
    stopped = true;
  });
  await delay(5);
  assert.equal(stopped, false);
  assert.equal(checks, 1);
  release.resolve();
  await stopping;
  await delay(10);
  assert.equal(checks, 1);
  assert.equal(maximumActive, 1);
});

test("a failed check is reported once and does not prevent a later poll", async () => {
  const recovered = deferred();
  const phases: string[] = [];
  let checks = 0;
  const poller = createPersistencePoller({
    intervalMs: 5,
    initialDelayMs: 0,
    check: async () => {
      checks++;
      if (checks === 1) throw new Error("private detail");
      recovered.resolve();
    },
    onFailure: (phase) => phases.push(phase),
  });
  poller.start();
  // Polling timers are unref'd; the bounded timeout also keeps this test alive.
  await Promise.race([
    recovered.promise,
    delay(500).then(() => assert.fail("later polling check did not run")),
  ]);
  await poller.stop();
  assert.deepEqual(phases, ["check"]);
  assert.ok(checks >= 2);
});

test("stop before a delayed first check is idempotent and permits a later fresh start", async () => {
  let checks = 0;
  const entered = deferred();
  const poller = createPersistencePoller({
    intervalMs: 5,
    initialDelayMs: 25,
    check: async () => {
      checks++;
      entered.resolve();
    },
    onFailure() {},
  });
  poller.start();
  await poller.stop();
  await poller.stop();
  await delay(30);
  assert.equal(checks, 0);
  poller.start();
  await Promise.race([entered.promise, delay(500).then(() => assert.fail("fresh start failed"))]);
  await poller.stop();
  assert.ok(checks >= 1);
});
