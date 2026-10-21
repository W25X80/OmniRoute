import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import type { CallLogArtifact } from "../../../src/lib/usage/callLogArtifacts.ts";
import { retainedPlainDataBytes } from "../../../src/shared/utils/retainedPlainData.ts";
import { fixture } from "./artifactByteAdmissionFixture.ts";

const actorUrl = new URL("../../../src/lib/usage/callLogArtifactWriter.ts", import.meta.url).href;
const fixtureUrl = new URL("./artifactByteAdmissionFixture.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.split("?")[0] === actorUrl &&
      ["node:worker_threads", "../db/controlPlaneContext", "../db/persistenceRuntime"].includes(
        specifier
      )
    )
      return { url: fixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
const oldBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
test.after(() => {
  hooks.deregister();
  if (oldBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = oldBackend;
});

let sequence = 0;
const actors = new Set<typeof import("../../../src/lib/usage/callLogArtifactWriter.ts")>();
test.afterEach(async () => {
  await Promise.all([...actors].map((writer) => writer.closeCallLogArtifactWriter(0)));
  actors.clear();
});
async function actor(limits = { maxEntryBytes: 4096, maxRetainedBytes: 128 * 1024 }) {
  fixture.workers.length = 0;
  fixture.native = true;
  fixture.throwOnSend = false;
  fixture.throwOnSpawn = false;
  const loaded: typeof import("../../../src/lib/usage/callLogArtifactWriter.ts") = await import(
    `${actorUrl}?byte-admission-${++sequence}`
  );
  loaded.__setCallLogWorkerOverrideForTests({
    workerFile: "/unused/held-artifact-worker.mjs",
    execArgv: [],
    queueLimits: limits,
  });
  actors.add(loaded);
  return loaded;
}

function artifact(text = "original"): CallLogArtifact {
  return {
    schemaVersion: 5,
    summary: { id: "log", timestamp: "2026-10-21T00:00:00Z" },
    requestBody: { text },
    responseBody: null,
    error: null,
  } as CallLogArtifact;
}

function observe<T>(work: Promise<T>) {
  const observation = {
    state: "pending",
    value: undefined as T | undefined,
    error: undefined as unknown,
  };
  void work.then(
    (value) => {
      observation.state = "resolved";
      observation.value = value;
    },
    (error: unknown) => {
      observation.state = "rejected";
      observation.error = error;
    }
  );
  return observation;
}

function jobBytes(value: CallLogArtifact): number {
  return retainedPlainDataBytes(
    {
      artifact: value,
      environment: {
        pipelineMaxSizeKb: process.env.CALL_LOG_PIPELINE_MAX_SIZE_KB,
        chatDebugFile: process.env.CHAT_DEBUG_FILE,
        appLogLevel: process.env.APP_LOG_LEVEL,
      },
    },
    32 * 1024 * 1024,
    512
  )!;
}

test("raw entry admission rejects before cloning or sending an oversized graph", async (t) => {
  const writer = await actor();
  const copy = globalThis.structuredClone;
  let copies = 0;
  t.mock.method(globalThis, "structuredClone", (...args: Parameters<typeof structuredClone>) => {
    copies++;
    return Reflect.apply(copy, globalThis, args);
  });
  const result = observe(writer.serializeCallArtifactAsync(artifact("x".repeat(4096))));
  try {
    await setImmediate();
    assert.equal(result.state, "rejected");
    assert.ok(result.error instanceof writer.CallLogArtifactSerializationError);
    assert.equal(copies, 0);
    assert.equal(fixture.workers.length, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("queued raw graphs and environment metadata are detached from later caller mutation", async () => {
  const writer = await actor();
  const first = observe(writer.serializeCallArtifactAsync(artifact("first")));
  const source = artifact();
  const oldLevel = process.env.APP_LOG_LEVEL;
  process.env.APP_LOG_LEVEL = "info";
  const second = observe(writer.serializeCallArtifactAsync(source));
  (source.requestBody as { text: string }).text = "mutated";
  process.env.APP_LOG_LEVEL = "debug";
  try {
    await setImmediate();
    const worker = fixture.workers[0];
    assert.equal(worker.messages.length, 1);
    const initial = worker.messages[0];
    worker.reply({ id: initial.id, kind: "serialize", bytes: new Uint8Array([1]) });
    const sent = worker.messages[1];
    assert.ok(sent.kind === "serialize");
    assert.deepEqual(sent.artifact.requestBody, { text: "original" });
    assert.equal(sent.environment.appLogLevel, "info");
    worker.reply({ id: sent.id, kind: "serialize", bytes: new Uint8Array([2]) });
    await setImmediate();
    assert.equal(first.state, "resolved");
    assert.equal(second.state, "resolved");
  } finally {
    if (oldLevel === undefined) delete process.env.APP_LOG_LEVEL;
    else process.env.APP_LOG_LEVEL = oldLevel;
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("aggregate admission counts active plus queued graphs and refunds successful jobs exactly", async (t) => {
  const value = artifact("x".repeat(700));
  const one = jobBytes(value);
  const writer = await actor({ maxEntryBytes: 4096, maxRetainedBytes: one * 2 });
  const beforeRefusals = writer.getCallLogArtifactQueueStats().byteRefusals;
  const copy = globalThis.structuredClone;
  let copies = 0;
  t.mock.method(globalThis, "structuredClone", (...args: Parameters<typeof structuredClone>) => {
    copies++;
    return Reflect.apply(copy, globalThis, args);
  });
  const first = observe(writer.serializeCallArtifactAsync(value));
  const second = observe(writer.serializeCallArtifactAsync(value));
  assert.deepEqual(writer.getCallLogArtifactQueueStats(), {
    active: 1,
    queued: 1,
    retainedBytes: one * 2,
    byteRefusals: beforeRefusals,
  });
  await setImmediate();
  const before = copies;
  const refused = observe(writer.serializeCallArtifactAsync(value));
  try {
    await setImmediate();
    assert.equal(refused.state, "rejected");
    assert.ok(refused.error instanceof writer.CallLogArtifactSerializationError);
    assert.equal(copies, before, "aggregate refusal must precede clone and postMessage");
    const worker = fixture.workers[0];
    assert.equal(worker.messages.length, 1);
    worker.reply({ id: worker.messages[0].id, kind: "serialize", bytes: new Uint8Array([1]) });
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, one);
    const third = observe(writer.serializeCallArtifactAsync(value));
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, one * 2);
    worker.reply({ id: worker.messages[1].id, kind: "serialize", bytes: new Uint8Array([2]) });
    worker.reply({ id: worker.messages[2].id, kind: "serialize", bytes: new Uint8Array([3]) });
    await setImmediate();
    assert.deepEqual(
      [first.state, second.state, third.state],
      ["resolved", "resolved", "resolved"]
    );
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("independent Next actor copies share one process retained-byte ledger", async () => {
  const value = artifact("x".repeat(700));
  const one = jobBytes(value);
  const limits = { maxEntryBytes: 4096, maxRetainedBytes: one * 2 - 1 };
  const first = await actor(limits);
  const second = await actor(limits);
  const owned = observe(first.serializeCallArtifactAsync(value));
  const refused = observe(second.serializeCallArtifactAsync(value));
  try {
    await setImmediate();
    assert.equal(owned.state, "pending");
    assert.equal(refused.state, "rejected");
    assert.ok(refused.error instanceof second.CallLogArtifactSerializationError);
    assert.equal(fixture.workers.length, 1);
    assert.equal(second.getCallLogArtifactQueueStats().retainedBytes, one);
    const initial = fixture.workers[0];
    initial.reply({ id: initial.messages[0].id, kind: "serialize", bytes: new Uint8Array([1]) });
    const retry = observe(second.serializeCallArtifactAsync(value));
    await setImmediate();
    const replacement = fixture.workers[1];
    replacement.reply({
      id: replacement.messages[0].id,
      kind: "serialize",
      bytes: new Uint8Array([2]),
    });
    await setImmediate();
    assert.equal(retry.state, "resolved");
    assert.equal(first.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await Promise.all([first.closeCallLogArtifactWriter(0), second.closeCallLogArtifactWriter(0)]);
  }
});

test("invalid graphs and failed clones cannot consume a reservation or invoke payload code", async (t) => {
  const writer = await actor();
  let invoked = 0;
  const getter = artifact();
  Object.defineProperty(getter.requestBody, "text", {
    enumerable: true,
    get() {
      invoked++;
      return "private";
    },
  });
  const proxy = artifact();
  proxy.requestBody = new Proxy(
    {},
    {
      getPrototypeOf() {
        invoked++;
        return Object.prototype;
      },
    }
  );
  const invalid = [getter, proxy].map((value) => observe(writer.serializeCallArtifactAsync(value)));
  const clone = t.mock.method(globalThis, "structuredClone", () => {
    throw new Error("Fixture clone failure");
  });
  const failedClone = observe(writer.serializeCallArtifactAsync(artifact()));
  await setImmediate();
  assert.equal(invoked, 0);
  assert.ok([...invalid, failedClone].every((result) => result.state === "rejected"));
  assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  assert.equal(fixture.workers.length, 0);
  clone.mock.restore();
  const retry = observe(writer.serializeCallArtifactAsync(artifact()));
  try {
    await setImmediate();
    const worker = fixture.workers[0];
    worker.reply({ id: worker.messages[0].id, kind: "serialize", bytes: new Uint8Array([1]) });
    await setImmediate();
    assert.equal(retry.state, "resolved");
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("decode copies only its admitted view and ignores a caller byteLength getter", async () => {
  const writer = await actor();
  const first = observe(writer.serializeCallArtifactAsync(artifact()));
  const backing = new Uint8Array(1024).fill(9);
  const view = backing.subarray(100, 103);
  let reads = 0;
  Object.defineProperty(view, "byteLength", {
    get() {
      reads++;
      return 0;
    },
  });
  const decoded = observe(writer.deserializeCallArtifactAsync(view));
  view.fill(33);
  try {
    await setImmediate();
    assert.equal(reads, 0);
    const worker = fixture.workers[0];
    worker.reply({ id: worker.messages[0].id, kind: "serialize", bytes: new Uint8Array([1]) });
    const sent = worker.messages[1];
    assert.ok(sent.kind === "deserialize");
    assert.deepEqual(sent.bytes, new Uint8Array([9, 9, 9]));
    assert.equal(sent.bytes.buffer.byteLength, 3);
    worker.reply({ id: sent.id, kind: "deserialize", result: { artifact: null, state: "ready" } });
    await setImmediate();
    assert.equal(first.state, "resolved");
    assert.equal(decoded.state, "resolved");
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("decode preserves the exact eight MiB input limit and shares the aggregate reservation", async () => {
  const MiB = 1024 * 1024;
  const writer = await actor({ maxEntryBytes: 4096, maxRetainedBytes: 9 * MiB });
  const exact = observe(writer.deserializeCallArtifactAsync(new Uint8Array(8 * MiB)));
  const tooLarge = observe(writer.deserializeCallArtifactAsync(new Uint8Array(8 * MiB + 1)));
  const aggregate = observe(writer.deserializeCallArtifactAsync(new Uint8Array(2 * MiB)));
  try {
    await setImmediate();
    assert.equal(exact.state, "pending");
    assert.equal(tooLarge.state, "rejected");
    assert.equal(aggregate.state, "rejected");
    assert.equal(fixture.workers[0].messages.length, 1);
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 8 * MiB + 512);
    await writer.closeCallLogArtifactWriter(0);
    await setImmediate();
    assert.equal(exact.state, "rejected");
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("staged ciphertext and metadata are detached and charged against the shared byte limit", async () => {
  const writer = await actor({ maxEntryBytes: 4096, maxRetainedBytes: 2600 });
  const source = {
    ciphertext: "x".repeat(300),
    sizeBytes: 1,
    sha256: "hash",
    callId: "call",
    expiresAt: 1e15,
  };
  const first = observe(writer.deserializeStagedCallArtifactAsync(source));
  const bytes = writer.getCallLogArtifactQueueStats().retainedBytes;
  assert.ok(bytes >= 600, "ciphertext uses UTF-16 retained bytes, independently of decoded size");
  source.ciphertext = "mutated";
  source.callId = "changed";
  const refused = observe(
    writer.deserializeStagedCallArtifactAsync({ ...source, ciphertext: "x".repeat(300) })
  );
  try {
    await setImmediate();
    assert.equal(refused.state, "rejected");
    const worker = fixture.workers[0];
    const sent = worker.messages[0];
    assert.ok(sent.kind === "deserialize-staged");
    assert.equal(sent.staging.ciphertext, "x".repeat(300));
    assert.equal(sent.staging.callId, "call");
    worker.reply({
      id: sent.id,
      kind: "deserialize-staged",
      result: { artifact: null, state: "ready" },
    });
    await setImmediate();
    assert.equal(first.state, "resolved");
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});

test("staged encoded overflow and accessor metadata fail before clone or worker dispatch", async (t) => {
  const writer = await actor();
  let calls = 0;
  let reads = 0;
  t.mock.method(globalThis, "structuredClone", () => {
    calls++;
    throw new Error("Rejected staging must not be copied");
  });
  const base = { sizeBytes: 1, sha256: "hash", callId: "call", expiresAt: 1e15 };
  const invalid = observe(
    writer.deserializeStagedCallArtifactAsync({
      ...base,
      ciphertext: "x".repeat(22_369_721),
    })
  );
  const accessor = observe(
    writer.deserializeStagedCallArtifactAsync({
      ...base,
      get ciphertext() {
        reads++;
        return "cipher";
      },
    })
  );
  await setImmediate();
  assert.deepEqual([invalid.state, accessor.state], ["rejected", "rejected"]);
  assert.equal(reads, 0);
  assert.equal(calls, 0);
  assert.equal(fixture.workers.length, 0);
  assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
});

for (const failure of [
  "error",
  "exit",
  "messageerror",
  "malformed-reply",
  "send",
  "spawn",
] as const) {
  test(`${failure} releases every active and queued reservation and permits a new worker`, async () => {
    const writer = await actor();
    const jobs = [artifact("first"), artifact("second")].map((value) =>
      observe(writer.serializeCallArtifactAsync(value))
    );
    if (failure === "send") fixture.throwOnSend = true;
    if (failure === "spawn") fixture.throwOnSpawn = true;
    await setImmediate();
    const worker = fixture.workers[0];
    if (failure === "exit") worker.emit("exit", 1);
    else if (failure === "error" || failure === "messageerror")
      worker.emit(failure, new Error("Fixture failure"));
    else if (failure === "malformed-reply")
      worker.reply({ id: worker.messages[0].id, kind: "write", result: null });
    await setImmediate();
    if (failure === "malformed-reply") {
      assert.equal(jobs[0].state, "rejected");
      assert.ok(writer.getCallLogArtifactQueueStats().retainedBytes > 0);
      worker.reply({ id: worker.messages[1].id, kind: "serialize", bytes: new Uint8Array([1]) });
    } else assert.ok(jobs.every((job) => job.state === "rejected"));
    fixture.throwOnSend = false;
    fixture.throwOnSpawn = false;
    const retry = observe(writer.serializeCallArtifactAsync(artifact("retry")));
    try {
      await setImmediate();
      const replacement = fixture.workers.at(-1)!;
      const sent = replacement.messages.at(-1)!;
      replacement.reply({ id: sent.id, kind: "serialize", bytes: new Uint8Array([2]) });
      await setImmediate();
      assert.equal(retry.state, "resolved");
      assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
    } finally {
      await writer.closeCallLogArtifactWriter(0);
    }
  });
}

test("close timeout refunds retained jobs once and late worker replies cannot refund again", async (t) => {
  const writer = await actor();
  const first = observe(writer.serializeCallArtifactAsync(artifact()));
  const second = observe(writer.deserializeCallArtifactAsync(new Uint8Array([1, 2])));
  await setImmediate();
  const worker = fixture.workers[0];
  const id = worker.messages[0].id;
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const closing = writer.closeCallLogArtifactWriter(10);
  t.mock.timers.tick(10);
  await closing;
  worker.reply({ id, kind: "serialize", bytes: new Uint8Array([1]) });
  await setImmediate();
  assert.deepEqual([first.state, second.state], ["rejected", "rejected"]);
  assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  await writer.closeCallLogArtifactWriter(0);
  assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
});

test("SQLite write refusal preserves its null detail-unavailable protocol", async () => {
  const writer = await actor();
  fixture.native = false;
  try {
    assert.equal(await writer.writeCallArtifactAsync(artifact("x".repeat(4096))), null);
    assert.equal(fixture.workers.length, 0);
    assert.equal(writer.getCallLogArtifactQueueStats().retainedBytes, 0);
  } finally {
    await writer.closeCallLogArtifactWriter(0);
  }
});
