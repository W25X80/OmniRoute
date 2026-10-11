import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import {
  withPostgresControlPlane,
  UnsupportedPersistenceDomainError,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext";
import { ArtifactStorageError } from "../../../src/domain/persistence/artifactStore";
import { observedWorker } from "../../helpers/persistence/artifactObservedWorker";

const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/usage/callLogArtifactWriter.ts") &&
      specifier === "node:worker_threads"
    ) {
      return {
        url: new URL("../../helpers/persistence/artifactObservedWorker.ts", import.meta.url).href,
        shortCircuit: true,
      };
    }
    return next(specifier, context);
  },
});
const reader = await import("../../../src/lib/usage/callLogArtifacts");
const actor = await import("../../../src/lib/usage/callLogArtifactWriter");
const { MAX_CALL_LOG_DECODE_BYTES } =
  await import("../../../src/lib/usage/callLogArtifactProtocol");
const identity = {};
const encoded = new Map<string, Uint8Array | null>();
const errors = new Map<string, Error>();
let gets = 0;
const binding = {
  databaseIdentity: identity,
  assertReady() {},
} as unknown as PostgresControlPlaneBinding;
const oldRuntime = Object.getOwnPropertyDescriptor(globalThis, "__omniroutePersistenceRuntime");
const oldBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const oldLog = process.env.APP_LOG_TO_FILE;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.APP_LOG_TO_FILE = "false";
Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", {
  configurable: true,
  writable: true,
  value: {
    backend: "postgres",
    applicationIdentity: identity,
    phase: "ready",
    artifacts: {
      async get(reference: string) {
        gets++;
        if (errors.has(reference)) throw errors.get(reference);
        return encoded.get(reference) ?? null;
      },
    },
  },
});
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function owned<T>(run: () => Promise<T>): Promise<T> {
  return withPostgresControlPlane(binding, run);
}
function noMainDecode(t: test.TestContext) {
  let calls = 0;
  const parse = JSON.parse;
  t.mock.method(JSON, "parse", (...args: Parameters<typeof JSON.parse>) => {
    if (/callLogArtifacts\.(?:ts|js):/.test(new Error().stack ?? "")) {
      calls++;
      throw new Error("Unexpected parent-thread artifact JSON parse");
    }
    return Reflect.apply(parse, JSON, args);
  });
  return () => calls;
}
async function terminateIdle() {
  await observedWorker.last?.terminate();
  await yieldToEventLoop();
}

test.after(async () => {
  await actor.closeCallLogArtifactWriter();
  hooks.deregister();
  if (oldRuntime) Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", oldRuntime);
  else delete globalThis.__omniroutePersistenceRuntime;
  if (oldBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = oldBackend;
  if (oldLog === undefined) delete process.env.APP_LOG_TO_FILE;
  else process.env.APP_LOG_TO_FILE = oldLog;
});

test("native real artifact worker decodes exact UTF8/JSON while parent JSON parsing is forbidden", async (t) => {
  const expected = {
    schemaVersion: 5,
    requestBody: { content: "Unicode \u00e9 \u4e2d", large: "x".repeat(2 * 1024 * 1024) },
  };
  const bytes = new TextEncoder().encode(JSON.stringify(expected));
  encoded.set("ready", bytes);
  const copy = bytes.slice();
  const calls = noMainDecode(t);
  const result = await owned(() => reader.readCallArtifactAsync("ready"));
  assert.equal(result.state, "ready");
  assert.deepEqual(result.artifact, expected);
  assert.deepEqual(bytes, copy);
  assert.equal(calls(), 0);
});
test("native reader preserves missing/corrupt/integrity states and propagates storage unavailability", async (t) => {
  encoded.set("corrupt", new TextEncoder().encode("{invalid"));
  errors.set("integrity", new ArtifactStorageError("integrity"));
  const unavailable = new ArtifactStorageError("unavailable");
  errors.set("unavailable", unavailable);
  const calls = noMainDecode(t);
  await owned(async () => {
    assert.equal((await reader.readCallArtifactAsync("missing")).state, "missing");
    assert.equal((await reader.readCallArtifactAsync("corrupt")).state, "corrupt");
    assert.equal((await reader.readCallArtifactAsync("integrity")).state, "corrupt");
    await assert.rejects(
      reader.readCallArtifactAsync("unavailable"),
      (error) => error === unavailable
    );
    encoded.set("json-null", new TextEncoder().encode("null"));
    assert.deepEqual(await reader.readCallArtifactAsync("json-null"), {
      artifact: null,
      state: "ready",
    });
  });
  assert.equal(calls(), 0);
});
test("foreign authority and oversized decode fail before CPU queue admission", async () => {
  const before = gets;
  await withPostgresControlPlane(
    { databaseIdentity: {}, assertReady() {} } as unknown as PostgresControlPlaneBinding,
    async () => {
      await assert.rejects(
        reader.readCallArtifactAsync("ready"),
        UnsupportedPersistenceDomainError
      );
    }
  );
  assert.equal(gets, before);
  await owned(async () => {
    await assert.rejects(
      actor.deserializeCallArtifactAsync(new Uint8Array(MAX_CALL_LOG_DECODE_BYTES + 1)),
      actor.CallLogArtifactDeserializationError
    );
  });
});
test("actual worker fault rejects with sanitized unavailable error and never retries parse on the parent", async (t) => {
  await terminateIdle();
  actor.__setCallLogWorkerOverrideForTests({
    workerFile: fileURLToPath(
      new URL("../../helpers/persistence/artifactDecodeGateWorker.mjs", import.meta.url)
    ),
    execArgv: [],
  });
  const entered = barrier();
  observedWorker.entered = entered.resolve;
  const calls = noMainDecode(t);
  const reading = owned(() => reader.readCallArtifactAsync("ready"));
  await Promise.race([entered.promise, reading]);
  observedWorker.last!.postMessage({ fail: true });
  await assert.rejects(
    reading,
    (error) =>
      error instanceof actor.CallLogArtifactDeserializationError &&
      !error.message.includes("Private fixture")
  );
  assert.equal(calls(), 0);
  observedWorker.entered = undefined;
});
test("close rejects new decode admission and joins the already accepted worker result", async () => {
  const entered = barrier();
  observedWorker.entered = entered.resolve;
  const reading = owned(() => reader.readCallArtifactAsync("ready"));
  await Promise.race([entered.promise, reading]);
  let closed = false;
  const closing = actor.closeCallLogArtifactWriter().then(() => {
    closed = true;
  });
  await yieldToEventLoop();
  assert.equal(closed, false);
  await owned(async () => {
    await assert.rejects(
      actor.deserializeCallArtifactAsync(new Uint8Array()),
      actor.CallLogArtifactDeserializationError
    );
  });
  observedWorker.last!.postMessage({ release: true });
  assert.equal((await reading).state, "ready");
  await closing;
  assert.equal(closed, true);
  observedWorker.entered = undefined;
});
