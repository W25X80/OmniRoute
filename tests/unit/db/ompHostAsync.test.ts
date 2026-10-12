import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { fork, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  OmpHostActor,
  OmpHostMutationUnknownError,
  OmpHostUnavailableError,
  getNativeOmpHostActor,
  closeNativeOmpHostStorage,
  resolveOmpHostWorker,
} from "../../../src/lib/db/ompHostClient.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import {
  OMP_HOST_MAX_JOBS,
  OMP_HOST_MAX_PENDING_BYTES,
  type OmpHostRequest,
} from "../../../src/lib/db/ompHostProtocol.ts";
import {
  loadBetterSqlite3,
  BETTER_SQLITE3_SKIP_REASON,
} from "../_helpers/betterSqlite3Availability.ts";

const Native = loadBetterSqlite3();
const get = { kind: "get" as const, providerId: "omniroute" };
const save = {
  kind: "save" as const,
  providerId: "omniroute",
  apiKey: "private-test-key",
  baseUrl: "https://route.test/v1",
};
const fixedPath = path.join(os.tmpdir(), "omniroute-omp-actor-control.db");
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const tick = () => new Promise<void>((done) => setImmediate(done));
class ControlledChild extends EventEmitter {
  connected = true;
  requests: OmpHostRequest[] = [];
  killed = 0;
  autoClose = true;
  send(request: OmpHostRequest, callback?: (error: Error | null) => void): boolean {
    this.requests.push(request);
    callback?.(null);
    if (request.kind === "close" && this.autoClose) queueMicrotask(() => this.exit());
    return true;
  }
  ack(index: number): void {
    const request = this.requests[index];
    assert.equal(request.kind, "operation");
    if (request.kind === "operation")
      this.emit("message", {
        kind: "result",
        id: request.id,
        ok: true,
        ...(request.operation.kind === "get"
          ? { credentials: { hasOmniRoute: false, apiKey: null, baseUrl: null } }
          : {}),
      });
  }
  kill(): boolean {
    this.killed++;
    if (this.autoClose) queueMicrotask(() => this.exit());
    return true;
  }
  exit(): void {
    this.connected = false;
    this.emit("close", 0, null);
  }
  child(): ChildProcess {
    return this as unknown as ChildProcess;
  }
}

test("OMP actor bounds admission and dispatches only one accepted operation at a time", async () => {
  const child = new ControlledChild();
  let starts = 0;
  const actor = new OmpHostActor(fixedPath, {
    createChild: async () => {
      starts++;
      return child.child();
    },
  });
  const work = Array.from({ length: OMP_HOST_MAX_JOBS }, () => actor.execute(get));
  const all = Promise.all(work);
  try {
    await tick();
    assert.equal(child.requests.length, 1);
    assert.equal(actor.status().queued, 31);
    assert.ok(actor.status().bytes <= OMP_HOST_MAX_PENDING_BYTES);
    await assert.rejects(actor.execute(get), OmpHostUnavailableError);
    assert.equal(starts, 1);
    for (let index = 0; index < OMP_HOST_MAX_JOBS; index++) child.ack(index);
    await all;
    assert.equal(actor.status().bytes, 0);
    assert.equal(child.requests.length, OMP_HOST_MAX_JOBS);
  } finally {
    while (actor.status().active) child.ack(child.requests.length - 1);
    await all;
    await actor.close();
  }
});

test("OMP native oversized frames reject before child startup without clipping credentials", async () => {
  let starts = 0;
  const actor = new OmpHostActor(fixedPath, {
    createChild: async () => {
      starts++;
      return new ControlledChild().child();
    },
  });
  await assert.rejects(
    actor.execute({ ...save, apiKey: "x".repeat(64 * 1024) }),
    OmpHostUnavailableError
  );
  assert.equal(starts, 0);
  assert.equal(actor.status().bytes, 0);
  await actor.close();
});

test("queued OMP abort is known undispatched; accepted close drains the prior write", async () => {
  const child = new ControlledChild();
  const actor = new OmpHostActor(fixedPath, { createChild: async () => child.child() });
  const writing = actor.execute(save);
  await tick();
  const abort = new AbortController();
  const queued = actor.execute(get, abort.signal);
  abort.abort();
  await assert.rejects(queued, OmpHostUnavailableError);
  actor.beginClose();
  await assert.rejects(actor.execute(get), OmpHostUnavailableError);
  const closing = actor.close();
  child.ack(0);
  await writing;
  await closing;
  assert.deepEqual(
    child.requests.map((request) => request.kind),
    ["operation", "close"]
  );
  assert.equal(actor.status().bytes, 0);
});

test("postdispatch OMP cancellation is unknown and a successful kill call cannot authorize replacement", async () => {
  const child = new ControlledChild();
  child.autoClose = false;
  let starts = 0;
  const actor = new OmpHostActor(fixedPath, {
    createChild: async () => {
      starts++;
      return child.child();
    },
    timeoutMs: 30,
  });
  const abort = new AbortController();
  const writing = actor.execute(save, abort.signal);
  await tick();
  abort.abort();
  await assert.rejects(writing, OmpHostMutationUnknownError);
  assert.equal(child.killed, 1);
  assert.equal(actor.status().retiring, true);
  await assert.rejects(actor.execute(save), OmpHostUnavailableError);
  assert.equal(starts, 1);
  child.ack(0); // Late ACK is not authority for a retired operation or a replacement.
  await assert.rejects(actor.close(), OmpHostUnavailableError);
  child.exit();
  assert.equal(actor.status().retiring, false);
  assert.equal(child.requests.length, 1, "no mutation replay");
});

test("OMP startup TTL seals unresolved startup and late factory completion is retired", async () => {
  const start = barrier();
  const child = new ControlledChild();
  let startupSignal: AbortSignal | undefined;
  const actor = new OmpHostActor(fixedPath, {
    timeoutMs: 30,
    createChild: async (_path, signal) => {
      startupSignal = signal;
      await start.promise;
      return child.child();
    },
  });
  await assert.rejects(actor.execute(get), OmpHostUnavailableError);
  assert.equal(startupSignal?.aborted, true);
  await assert.rejects(actor.execute(get), OmpHostUnavailableError);
  await assert.rejects(actor.close(), OmpHostUnavailableError);
  start.resolve();
  await tick();
  assert.equal(child.requests.length, 0);
  assert.equal(child.killed, 1);
  assert.equal(actor.status().starting, false);
  assert.equal(actor.status().retiring, false);
});

test("OMP close deadline terminates queued authority even if actual child exit remains unverified", async () => {
  const child = new ControlledChild();
  child.autoClose = false;
  let starts = 0;
  const actor = new OmpHostActor(fixedPath, {
    createChild: async () => {
      starts++;
      return child.child();
    },
  });
  const writing = actor.execute(save);
  void writing.catch(() => {});
  await tick();
  const queued = actor.execute(get);
  void queued.catch(() => {});
  await assert.rejects(actor.close(), OmpHostUnavailableError);
  await assert.rejects(writing, OmpHostMutationUnknownError);
  await assert.rejects(queued, OmpHostUnavailableError);
  assert.equal(actor.status().bytes, 0);
  child.exit();
  await tick();
  assert.equal(starts, 1);
  assert.equal(child.requests.length, 1);
  await assert.rejects(actor.execute(get), OmpHostUnavailableError);
});

test("OMP facade rejects foreign or ended native authority before constructing a host actor", () => {
  const previous = globalThis.__omniroutePersistenceRuntime;
  const identity = {};
  let ended = false;
  const binding = {
    databaseIdentity: identity,
    assertReady: () => {
      if (ended) throw new Error("scope has ended");
    },
  } as unknown as PostgresControlPlaneBinding;
  try {
    globalThis.__omniroutePersistenceRuntime = {
      applicationIdentity: {},
      phase: "ready",
    } as NonNullable<typeof previous>;
    withPostgresControlPlane(binding, () => {
      assert.throws(getNativeOmpHostActor, /not been ported/);
      ended = true;
      assert.throws(getNativeOmpHostActor, /scope has ended/);
    });
  } finally {
    globalThis.__omniroutePersistenceRuntime = previous;
  }
});

// This models a held control-plane binding, not the real PostgreSQL request-entry/catalog.
// The production begin/drain and host getter/execute/close functions are the test subjects.
test(
  "OMP held caller can finish its deferred models read during closing before capability retirement",
  { skip: Native ? false : BETTER_SQLITE3_SKIP_REASON, timeout: 20000 },
  async (t) => {
    if (!Native) return;
    const previous = globalThis.__omniroutePersistenceRuntime;
    const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const directory = await mkdtemp(path.join(os.tmpdir(), "omp-held-request-"));
    const agent = path.join(directory, ".omp", "agent");
    await mkdir(agent, { recursive: true });
    const database = new Native(path.join(agent, "agent.db"));
    database.exec(
      "CREATE TABLE auth_credentials(provider TEXT,credential_type TEXT,data TEXT,disabled_cause TEXT,identity_key TEXT,created_at INTEGER,updated_at INTEGER)"
    );
    database.close();
    t.mock.method(os, "homedir", () => directory);
    const identity = {};
    const state = {
      backend: "postgres",
      phase: "ready",
      applicationIdentity: identity,
      requests: new Set<Promise<unknown>>(),
      background: new Set<Promise<unknown>>(),
      tails: new Set<Promise<unknown>>(),
      owners: new AsyncLocalStorage<{ active: boolean }>(),
    } as NonNullable<typeof previous>;
    globalThis.__omniroutePersistenceRuntime = state;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    const { beginPersistenceAdmissionClose, drainPersistenceAcceptedWork } =
      await import("../../../src/lib/db/persistenceRuntime.ts");
    const modelsRead = barrier();
    let ended = false;
    const held = {
      databaseIdentity: identity,
      assertReady: () => {
        if (ended) throw new Error("scope has ended");
      },
    } as unknown as PostgresControlPlaneBinding;
    const shutdown = {
      databaseIdentity: identity,
      assertReady: () => {},
    } as unknown as PostgresControlPlaneBinding;
    let request: Promise<void> | undefined;
    try {
      request = state.owners
        .run({ active: true }, () =>
          withPostgresControlPlane(held, async () => {
            await modelsRead.promise;
            const actor = getNativeOmpHostActor();
            assert.deepEqual(await actor.execute(get), {
              hasOmniRoute: false,
              apiKey: null,
              baseUrl: null,
            });
          })
        )
        .finally(() => {
          ended = true;
          state.requests.delete(request!);
        });
      state.requests.add(request);
      beginPersistenceAdmissionClose();
      assert.equal(state.phase, "closing");
      assert.equal(state.requests.size, 1);
      modelsRead.resolve();
      await drainPersistenceAcceptedWork();
      await request;
      assert.equal(state.requests.size, 0);
      await withPostgresControlPlane(shutdown, async () => {
        await closeNativeOmpHostStorage();
        assert.throws(getNativeOmpHostActor, OmpHostUnavailableError);
        state.phase = "ready";
        assert.throws(
          getNativeOmpHostActor,
          OmpHostUnavailableError,
          "the same closed owner cannot allocate a replacement"
        );
      });
    } finally {
      modelsRead.resolve();
      await request?.catch(() => {});
      await withPostgresControlPlane(shutdown, () => closeNativeOmpHostStorage()).catch(() => {});
      globalThis.__omniroutePersistenceRuntime = previous;
      if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
      t.mock.restoreAll();
      await rm(directory, { recursive: true, force: true });
    }
  }
);

test(
  "OMP no-actor resource close latches admission in an isolated process without resetting the holder",
  { timeout: 10000 },
  async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "omp-no-actor-close-"));
    const entry = path.join(directory, "probe.mjs");
    const clientUrl = new URL("../../../src/lib/db/ompHostClient.ts", import.meta.url).href;
    const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
    await writeFile(
      entry,
      `import assert from "node:assert/strict";\nimport {getNativeOmpHostActor,beginNativeOmpHostClose,closeNativeOmpHostStorage,OmpHostUnavailableError} from ${JSON.stringify(clientUrl)};\nimport {withPostgresControlPlane} from ${JSON.stringify(contextUrl)};\nawait closeNativeOmpHostStorage();\nglobalThis.__omniroutePersistenceRuntime={backend:"sqlite",phase:"ready"};\nbeginNativeOmpHostClose();await closeNativeOmpHostStorage();\nconst identity={};globalThis.__omniroutePersistenceRuntime={backend:"postgres",phase:"closing",applicationIdentity:identity};\nprocess.env.OMNIROUTE_DATABASE_BACKEND="postgres";\nawait withPostgresControlPlane({databaseIdentity:identity,assertReady(){}},async()=>{beginNativeOmpHostClose();await closeNativeOmpHostStorage();assert.throws(getNativeOmpHostActor,OmpHostUnavailableError);globalThis.__omniroutePersistenceRuntime.phase="ready";assert.throws(getNativeOmpHostActor,OmpHostUnavailableError);});\nprocess.send?.({kind:"no-actor-latched",ok:true},()=>process.disconnect());\n`
    );
    const imports: string[] = [];
    for (let index = 0; index < process.execArgv.length; index++) {
      if (process.execArgv[index] === "--import")
        imports.push("--import", process.execArgv[++index]);
    }
    const child = fork(entry, [], {
      execArgv: imports.length ? imports : ["--import", "tsx/esm"],
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
    let confirmed = false;
    try {
      child.on("message", (message: unknown) => {
        if (
          message &&
          typeof message === "object" &&
          (message as { kind?: unknown }).kind === "no-actor-latched"
        )
          confirmed = (message as { ok?: unknown }).ok === true;
      });
      await new Promise<void>((resolve, reject) => {
        child.once("error", () => reject(new Error("OMP close probe failed")));
        child.once("close", (code) => {
          if (code === 0 && confirmed) resolve();
          else reject(new Error("OMP close probe failed"));
        });
      });
      assert.equal(confirmed, true);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await rm(directory, { recursive: true, force: true });
    }
  }
);

test(
  "fixed OMP child preserves the real CLI table and a lost mutation ACK may already be committed",
  { skip: Native ? false : BETTER_SQLITE3_SKIP_REASON, timeout: 20000 },
  async () => {
    if (!Native) return;
    const directory = await mkdtemp(path.join(os.tmpdir(), "omniroute-omp-child-"));
    const databasePath = path.join(directory, "agent.db");
    const database = new Native(databasePath);
    database.exec(
      "CREATE TABLE auth_credentials(provider TEXT,credential_type TEXT,data TEXT,disabled_cause TEXT,identity_key TEXT,created_at INTEGER,updated_at INTEGER)"
    );
    database.close();
    const entry = await resolveOmpHostWorker();
    const env = {
      ...process.env,
      NODE_PATH: path.resolve(process.cwd(), "node_modules"),
      OMNIROUTE_DATABASE_BACKEND: "postgres",
    };
    const actor = new OmpHostActor(databasePath, {
      createChild: async (file, signal) => {
        if (signal.aborted) throw new OmpHostUnavailableError();
        return fork(entry.workerFile, [file], {
          execArgv: entry.execArgv,
          env,
          stdio: ["ignore", "ignore", "ignore", "ipc"],
        });
      },
    });
    let lost: OmpHostActor | undefined;
    try {
      assert.deepEqual(await actor.execute(get), {
        hasOmniRoute: false,
        apiKey: null,
        baseUrl: null,
      });
      await actor.execute(save);
      assert.deepEqual(await actor.execute(get), {
        hasOmniRoute: true,
        apiKey: save.apiKey,
        baseUrl: save.baseUrl,
      });
      await actor.execute({ ...save, apiKey: "replacement-key" });
      assert.equal((await actor.execute(get))?.apiKey, "replacement-key");
      await actor.execute({ kind: "delete", providerId: save.providerId });
      assert.equal((await actor.execute(get))?.hasOmniRoute, false);
      await actor.close();
      const wrapper = path.join(directory, "drop-omp-ack.mjs");
      await writeFile(
        wrapper,
        `const send = process.send?.bind(process);\nprocess.send = (reply, callback) => { if (reply?.kind === "result" && reply.ok === true) { process.disconnect(); return false; } return send?.(reply, callback); };\nawait import(${JSON.stringify(pathToFileURL(entry.workerFile).href)});\n`
      );
      lost = new OmpHostActor(databasePath, {
        createChild: async (file, signal) => {
          if (signal.aborted) throw new OmpHostUnavailableError();
          return fork(wrapper, [file], {
            execArgv: entry.execArgv,
            env,
            stdio: ["ignore", "ignore", "ignore", "ipc"],
          });
        },
      });
      await assert.rejects(lost.execute(save), OmpHostMutationUnknownError);
      const verification = new Native(databasePath, { readonly: true });
      try {
        const rows = verification
          .prepare("SELECT data FROM auth_credentials WHERE provider=?")
          .all(save.providerId) as { data: string }[];
        assert.equal(rows.length, 1);
        assert.equal(
          JSON.parse(rows[0].data).apiKey,
          save.apiKey,
          "lost ACK cannot be interpreted as no committed mutation"
        );
      } finally {
        verification.close();
      }
      await assert.rejects(access(path.join(directory, "storage.sqlite")));
    } finally {
      await actor.close().catch(() => {});
      await lost?.close().catch(() => {});
      await rm(directory, { recursive: true, force: true });
    }
  }
);
