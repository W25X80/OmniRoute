import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import test from "node:test";
import workerThreads from "node:worker_threads";

import type { NativeArtifactStore } from "../../src/domain/persistence/artifactStore.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import { installApplicationPersistence } from "../../src/lib/db/controlPlaneContext.ts";
import * as writer from "../../src/lib/usage/callLogArtifactWriter.ts";
import type { CallLogArtifact } from "../../src/lib/usage/callLogArtifacts.ts";
import "../../src/lib/usage/callLogArtifacts.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function until(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.fail("controlled artifact operation did not reach its expected state");
}

test("native artifact async resolver keeps exact source candidate priority", async () => {
  const context = {
    moduleDir: "/fixture/module",
    cwd: "/fixture/cwd",
    entryFile: "/fixture/dist/server.js",
  };
  const paths = [
    "/fixture/module/callLogArtifactWorker.js",
    "/fixture/dist/src/lib/usage/callLogArtifactWorker.js",
    "/fixture/cwd/src/lib/usage/callLogArtifactWorker.js",
    "/fixture/module/callLogArtifactWorker.ts",
    "/fixture/cwd/src/lib/usage/callLogArtifactWorker.ts",
  ];
  for (let selected = 0; selected < paths.length; selected++) {
    const probes: string[] = [];
    const result = await writer.resolveCallLogArtifactWorkerAsync({
      ...context,
      fileExists: async (file) => {
        probes.push(file);
        return file === paths[selected];
      },
    });
    assert.equal(result.workerFile, paths[selected]);
    assert.deepEqual(result.execArgv, selected >= 3 ? ["--import", "tsx/esm"] : []);
    assert.deepEqual(probes, paths.slice(0, selected + 1));
  }
  const absent = await writer.resolveCallLogArtifactWorkerAsync({
    ...context,
    fileExists: async () => false,
  });
  assert.equal(absent.workerFile, paths[1]);
});

test("native artifact public writer waits for metadata, remains FIFO, evicts and excludes late spawn", async (t) => {
  const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  const previousRuntime = globalThis.__omniroutePersistenceRuntime;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const identity = Object.freeze({});
  const uninstall = installApplicationPersistence({
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding);
  const stored: Uint8Array[] = [];
  const prepared: string[] = [];
  const preparation = deferred();
  const preparedFirst = deferred();
  const store: NativeArtifactStore = {
    async put(bytes, options) {
      const checksum = createHash("sha256").update(bytes).digest("hex");
      const artifact = {
        reference: `s3/${randomUUID()}_${checksum}.json`,
        sizeBytes: bytes.length,
        checksum,
      };
      assert.ok(options?.onPrepared, "native storage requires acknowledged caller preparation");
      await options.onPrepared({ ...artifact, storageScope: "a".repeat(64), singleAttempt: true });
      stored.push(bytes);
      return artifact;
    },
    async get() {
      return null;
    },
    async delete() {
      return true;
    },
    async close() {},
  };
  globalThis.__omniroutePersistenceRuntime = {
    backend: "postgres",
    phase: "ready",
    artifacts: store,
    applicationIdentity: identity,
    requests: new Set(),
    background: new Set(),
    tails: new Set(),
    owners: new AsyncLocalStorage(),
  };
  class FakeWorker extends EventEmitter {
    readonly messages: { id: number; kind: string; artifact: CallLogArtifact }[] = [];
    terminations = 0;
    postMessage(message: (typeof this.messages)[number]) {
      this.messages.push(message);
    }
    terminate() {
      this.terminations++;
      return Promise.resolve(0);
    }
  }
  const workers: FakeWorker[] = [];
  let gate = deferred();
  let probes = 0;
  const originalAccess = fsPromises.access.bind(fsPromises);
  t.mock.method(fsPromises, "access", async (file: unknown) => {
    if (!String(file).includes("callLogArtifactWorker."))
      return Reflect.apply(originalAccess, fsPromises, [file]);
    probes++;
    await gate.promise;
    if (!String(file).endsWith("/callLogArtifactWorker.ts"))
      throw Object.assign(new Error("absent"), { code: "ENOENT" });
  });
  t.mock.method(workerThreads, "Worker", function () {
    const worker = new FakeWorker();
    workers.push(worker);
    return worker;
  });
  let syncCount = 0;
  for (const name of ["existsSync", "statSync", "readFileSync"] as const) {
    const original = fs[name];
    t.mock.method(fs, name, (...args: unknown[]) => {
      syncCount++;
      return Reflect.apply(original, fs, args);
    });
  }
  syncBuiltinESMExports();
  const fixture = (id: string) => ({ summary: { id } }) as unknown as CallLogArtifact;
  // Controlled preparation observes the writer protocol; real SQL journaling is a separate test.
  const options = {
    async onPrepared(
      artifact: import("../../src/domain/persistence/artifactStore.ts").PreparedArtifact
    ) {
      prepared.push(artifact.reference);
      if (prepared.length === 1) {
        preparedFirst.resolve();
        await preparation.promise;
      }
    },
  };
  try {
    const first = writer.writeCallArtifactAsync(fixture("first"), options);
    const second = writer.writeCallArtifactAsync(fixture("second"), options);
    await until(() => probes === 1);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(workers.length, 0);
    gate.resolve();
    await until(() => workers[0]?.messages.length === 1);
    assert.equal(workers.length, 1);
    assert.equal(workers[0].messages[0].artifact.summary.id, "first");
    workers[0].emit("message", {
      kind: "serialize",
      id: workers[0].messages[0].id,
      bytes: new Uint8Array([1, 2]),
    });
    await preparedFirst.promise;
    assert.equal(stored.length, 0, "no bytes are accepted before preparation acknowledgement");
    preparation.resolve();
    await first;
    await until(() => workers[0].messages.length === 2);
    assert.equal(workers[0].messages[1].artifact.summary.id, "second");
    workers[0].emit("message", {
      kind: "serialize",
      id: workers[0].messages[1].id,
      bytes: new Uint8Array([3]),
    });
    await second;
    assert.deepEqual(
      stored.map((bytes) => Array.from(bytes)),
      [[1, 2], [3]]
    );
    assert.equal(syncCount, 0);

    // A completed native writer keeps the original 30 second idle eviction contract.
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const third = writer.writeCallArtifactAsync(fixture("third"), options);
    await until(() => workers[0].messages.length === 3);
    workers[0].emit("message", {
      kind: "serialize",
      id: workers[0].messages[2].id,
      bytes: new Uint8Array([4]),
    });
    await third;
    t.mock.timers.tick(30_000);
    assert.equal(workers[0].terminations, 1);

    gate = deferred();
    const previousProbes = probes;
    const fourth = writer.writeCallArtifactAsync(fixture("fourth"), options);
    const outcome = fourth.catch((error: unknown) => error);
    await until(() => probes > previousProbes);
    await assert.rejects(writer.closeCallLogArtifactWriter(0), /still draining/);
    assert.ok((await outcome) instanceof writer.CallLogArtifactSerializationError);
    gate.resolve();
    await until(() => probes >= previousProbes + 4);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(workers.length, 1, "metadata completion cannot construct a worker after close");
    assert.equal(stored.length, 3);
    assert.equal(prepared.length, 3, "rejected late work cannot prepare an object");
    assert.equal(syncCount, 0);
  } finally {
    preparation.resolve();
    gate.resolve();
    await writer.closeCallLogArtifactWriter(0).catch(() => undefined);
    uninstall();
    globalThis.__omniroutePersistenceRuntime = previousRuntime;
    t.mock.restoreAll();
    syncBuiltinESMExports();
    if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
  }
});
