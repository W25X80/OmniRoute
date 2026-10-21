import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import { createRuntimeResourceMonitor } from "../../src/lib/monitoring/runtimeResources.ts";
import type { ResourceSignals } from "../../open-sse/utils/resourcePressurePolicy.ts";

const signals: ResourceSignals = {
  observedAtMs: 1,
  v8: { heapUsedBytes: 100, heapLimitBytes: 1000 },
  process: {
    rssBytes: 200,
    externalBytes: 10,
    arrayBuffersBytes: 5,
    availableBytes: null,
    constrainedBytes: null,
  },
  cgroup: { currentBytes: 200, maxBytes: 1000, highBytes: null, fileBytes: null, events: null },
  psi: null,
};

test("resource observations are single-flight and stop discards late samples", async () => {
  const gate = Promise.withResolvers<ResourceSignals>();
  let tick!: () => void;
  let sampled = 0;
  let emitted = 0;
  const monitor = createRuntimeResourceMonitor({
    sample: async () => {
      sampled++;
      return gate.promise;
    },
    emit: () => {
      emitted++;
    },
    schedule: (callback) => {
      tick = callback;
      return setInterval(() => {}, 60000);
    },
  });
  monitor.start();
  monitor.start();
  tick();
  tick();
  await setImmediate();
  assert.equal(sampled, 1);
  const stop = monitor.stop();
  gate.resolve(signals);
  await stop;
  tick();
  assert.equal(emitted, 0);
});

test("a failing diagnostic sink cannot stop subsequent observations", async () => {
  let tick!: () => void;
  let emissions = 0;
  const monitor = createRuntimeResourceMonitor({
    sample: async () => signals,
    emit: () => {
      emissions++;
      if (emissions === 1) throw new Error("diagnostic sink unavailable");
    },
    schedule: (callback) => {
      tick = callback;
      return setInterval(() => {}, 60000);
    },
  });
  monitor.start();
  try {
    tick();
    await setImmediate();
    tick();
    await setImmediate();
    assert.equal(emissions, 2);
  } finally {
    await monitor.stop();
  }
});
