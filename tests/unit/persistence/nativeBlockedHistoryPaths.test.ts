import assert from "node:assert/strict";
import fs from "node:fs";
import files from "node:fs/promises";
import { registerHooks } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import {
  withPostgresControlPlane,
  UnsupportedPersistenceDomainError,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext";
import { pathFixture } from "../../helpers/persistence/nativeBlockedHistoryPathFixture";

const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/src/lib/proxyHealth/blockedHistory.ts") &&
      specifier === "@/lib/dataPaths"
    ) {
      return {
        url: new URL(
          "../../helpers/persistence/nativeBlockedHistoryPathFixture.ts",
          import.meta.url
        ).href,
        shortCircuit: true,
      };
    }
    return next(specifier, context);
  },
});
const history = await import("../../../src/lib/proxyHealth/blockedHistory");
hooks.deregister();
const historySymbol = Symbol.for("omniroute.nativeBlockedHistory");
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function binding(identity: object): PostgresControlPlaneBinding {
  return { databaseIdentity: identity, assertReady() {} } as unknown as PostgresControlPlaneBinding;
}
function installApplication(identity: object): () => void {
  const runtime = Object.getOwnPropertyDescriptor(globalThis, "__omniroutePersistenceRuntime");
  const stores = Object.getOwnPropertyDescriptor(globalThis, historySymbol);
  Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", {
    configurable: true,
    writable: true,
    value: { backend: "postgres", applicationIdentity: identity },
  });
  Reflect.deleteProperty(globalThis, historySymbol);
  return () => {
    if (runtime) Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", runtime);
    else delete globalThis.__omniroutePersistenceRuntime;
    if (stores) Object.defineProperty(globalThis, historySymbol, stores);
    else Reflect.deleteProperty(globalThis, historySymbol);
  };
}

test("cold native flush/close and foreign calls do not allocate or resolve a path", async () => {
  const identity = {};
  const restore = installApplication(identity);
  pathFixture.calls = 0;
  try {
    await withPostgresControlPlane(binding(identity), async () => {
      await history.flushBlockedHistoryAsync();
      await history.closeBlockedHistoryAsync();
    });
    assert.equal(pathFixture.calls, 0);
    assert.equal(Reflect.get(globalThis, historySymbol), undefined);
    await withPostgresControlPlane(binding({}), async () => {
      for (const call of [
        () => history.getBlockedHistoriesAsync(["p"]),
        () => history.recordBlockedObservationAsync("p", "unproven", 403, 1),
        history.flushBlockedHistoryAsync,
        history.closeBlockedHistoryAsync,
      ]) {
        await assert.rejects(call(), UnsupportedPersistenceDomainError);
      }
    });
    assert.equal(pathFixture.calls, 0);
    assert.equal(Reflect.get(globalThis, historySymbol), undefined);
  } finally {
    restore();
  }
});

test("SQLite async facade retains its immediate synchronous history update", async () => {
  const directory = await files.mkdtemp(path.join(os.tmpdir(), "history-sqlite-"));
  const oldData = process.env.DATA_DIR;
  process.env.DATA_DIR = directory;
  try {
    history.clearBlockedHistoryForTesting();
    const pending = history.recordBlockedObservationAsync("p", "unproven", 403, 1);
    assert.equal(history.getBlockedHistory("p")?.count, 1);
    await pending;
    history.clearBlockedHistoryForTesting();
  } finally {
    if (oldData === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = oldData;
    await files.rm(directory, { recursive: true, force: true });
  }
});

for (const legacyExists of [true, false]) {
  test(`native absent DATA_DIR selects ${legacyExists ? "legacy" : "XDG"} asynchronously and shares a captured factory`, async (t) => {
    const directory = await files.mkdtemp(path.join(os.tmpdir(), "history-paths-"));
    const legacy = path.join(directory, ".omniroute");
    const xdg = path.join(directory, "xdg");
    if (legacyExists) await files.mkdir(legacy);
    const oldData = process.env.DATA_DIR;
    const oldXdg = process.env.XDG_CONFIG_HOME;
    delete process.env.DATA_DIR;
    process.env.XDG_CONFIG_HOME = xdg;
    t.mock.method(os, "homedir", () => directory);
    const identity = {};
    const restore = installApplication(identity);
    const entered = barrier();
    const release = barrier();
    pathFixture.calls = 0;
    pathFixture.entered = entered.resolve;
    pathFixture.release = release.promise;
    // Module loading is already complete; this catches the concrete legacy resolver regression.
    const exists = fs.existsSync;
    const stat = fs.statSync;
    const blocked = () => {
      throw new Error("Unexpected synchronous filesystem operation");
    };
    t.mock.method(fs, "existsSync", blocked);
    t.mock.method(fs, "statSync", blocked);
    try {
      await withPostgresControlPlane(binding(identity), async () => {
        const first = history.recordBlockedObservationAsync("p", "unproven", 403, 1);
        const second = history.recordBlockedObservationAsync("p", "target_refused", 429, 2);
        await Promise.race([entered.promise, first]);
        await yieldToEventLoop();
        assert.equal(pathFixture.calls, 1);
        const store = (
          Reflect.get(globalThis, historySymbol) as WeakMap<object, Promise<unknown>>
        ).get(identity);
        assert.ok(store instanceof Promise);
        // A later environment edit cannot redirect an identity whose file owner is being created.
        process.env.DATA_DIR = path.join(directory, "changed");
        release.resolve();
        await Promise.all([first, second]);
        assert.equal((await history.getBlockedHistoriesAsync(["p"])).p.count, 2);
        await history.closeBlockedHistoryAsync();
        assert.equal(pathFixture.calls, 1);
      });
      const expected = path.join(
        legacyExists ? legacy : path.join(xdg, "omniroute"),
        "blocked-history.json"
      );
      assert.equal(JSON.parse(await files.readFile(expected, "utf8")).p.firstSeen, 1);
      await assert.rejects(files.access(path.join(directory, "changed", "blocked-history.json")));
    } finally {
      release.resolve();
      pathFixture.entered = undefined;
      pathFixture.release = undefined;
      fs.existsSync = exists;
      fs.statSync = stat;
      t.mock.restoreAll();
      restore();
      if (oldData === undefined) delete process.env.DATA_DIR;
      else process.env.DATA_DIR = oldData;
      if (oldXdg === undefined) delete process.env.XDG_CONFIG_HOME;
      else process.env.XDG_CONFIG_HOME = oldXdg;
      await files.rm(directory, { recursive: true, force: true });
    }
  });
}
