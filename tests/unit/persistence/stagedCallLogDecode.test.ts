import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { registerHooks } from "node:module";
import test from "node:test";
import { encrypt } from "../../../src/lib/db/encryption.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type { StagedCallLogDecodeInput } from "../../../src/lib/usage/callLogArtifactProtocol.ts";
import { observedWorker } from "../../helpers/persistence/artifactObservedWorker.ts";

const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/usage/callLogArtifactWriter.ts") &&
      specifier === "node:worker_threads"
    )
      return {
        url: new URL("../../helpers/persistence/artifactObservedWorker.ts", import.meta.url).href,
        shortCircuit: true,
      };
    return next(specifier, context);
  },
});
const previous = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY", "APP_LOG_TO_FILE"].map((name) => [
    name,
    process.env[name],
  ])
);
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.STORAGE_ENCRYPTION_KEY = "a9".repeat(32);
process.env.APP_LOG_TO_FILE = "false";
const identity = {};
const runtime = Object.getOwnPropertyDescriptor(globalThis, "__omniroutePersistenceRuntime");
Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", {
  configurable: true,
  writable: true,
  value: { backend: "postgres", applicationIdentity: identity, phase: "ready", artifacts: {} },
});
const binding = {
  databaseIdentity: identity,
  assertReady() {},
} as unknown as PostgresControlPlaneBinding;
const actor = await import("../../../src/lib/usage/callLogArtifactWriter.ts");
const artifact = {
  schemaVersion: 5,
  summary: { id: "staged-call" },
  requestBody: { text: "private request" },
  responseBody: { text: "private response" },
  error: null,
  pipeline: { clientRequest: { content: "retained" } },
};
function input(value: unknown = artifact): StagedCallLogDecodeInput {
  const bytes = Buffer.from(JSON.stringify(value));
  return {
    ciphertext: encrypt(bytes.toString("base64")),
    sizeBytes: bytes.length,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    callId: "staged-call",
    expiresAt: Date.now() + 10000,
  };
}
function decode(value: StagedCallLogDecodeInput) {
  return withPostgresControlPlane(binding, () => actor.deserializeStagedCallArtifactAsync(value));
}
test.after(async () => {
  await actor.closeCallLogArtifactWriter();
  hooks.deregister();
  if (runtime) Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", runtime);
  else delete globalThis.__omniroutePersistenceRuntime;
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("real CPU actor decrypts, verifies and parses staging without parent-thread decoding", async (t) => {
  const value = input();
  const original = JSON.parse;
  t.mock.method(JSON, "parse", (...args: Parameters<typeof JSON.parse>) => {
    if (/stagedCallLogDecode\.(?:ts|js):/.test(new Error().stack ?? ""))
      assert.fail("Parent decoded staged detail");
    return Reflect.apply(original, JSON, args);
  });
  assert.deepEqual(await decode(value), { artifact, state: "ready" });
  assert.ok(observedWorker.last && observedWorker.last.threadId > 0);
});

test("each integrity and shape operand independently rejects the staged payload", async () => {
  const valid = input();
  const noncanonical = Buffer.from(JSON.stringify(artifact)).toString("base64") + "\n";
  for (const value of [
    { ...valid, sha256: "f".repeat(64) },
    { ...valid, sizeBytes: valid.sizeBytes + 1 },
    { ...valid, callId: "another-call" },
    { ...valid, ciphertext: "plaintext" },
    { ...valid, ciphertext: valid.ciphertext.slice(0, -2) + "xx" },
    { ...valid, ciphertext: encrypt(noncanonical) },
    input(null),
    input([]),
    input({ ...artifact, schemaVersion: 4 }),
    input({ ...artifact, summary: [] }),
    input({ ...artifact, pipeline: [] }),
    input({ schemaVersion: 5, summary: { id: "staged-call" } }),
  ])
    assert.deepEqual(await decode(value), { artifact: null, state: "corrupt" });
});

test("expiry and ciphertext admission limits reject before worker admission", async () => {
  await assert.rejects(
    decode({ ...input(), expiresAt: Date.now() - 1 }),
    actor.CallLogArtifactDeserializationError
  );
  await assert.rejects(
    decode({ ...input(), ciphertext: "x".repeat(22369721) }),
    actor.CallLogArtifactDeserializationError
  );
});

test("worker failure never retries staged decoding on the serving thread", async () => {
  await observedWorker.last?.terminate();
  actor.__setCallLogWorkerOverrideForTests({
    workerFile: new URL("../../helpers/persistence/artifactDecodeGateWorker.mjs", import.meta.url)
      .pathname,
    execArgv: [],
  });
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    entered = resolve;
  });
  observedWorker.entered = entered;
  const reading = decode(input());
  await Promise.race([gate, reading]);
  observedWorker.last!.postMessage({ fail: true });
  await assert.rejects(reading, actor.CallLogArtifactDeserializationError);
});
