import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

/** Real PostgreSQL/runtime admission; independent S3/bootstrap effects use the existing fixture. */
test(
  "blocked history owns native background dispatch and remains writable through accepted-work drain",
  {
    skip: !postgresTestsEnabled,
    timeout: 60000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        ) {
          return {
            url: new URL(
              "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
              import.meta.url
            ).href,
            shortCircuit: true,
          };
        }
        return next(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "DATA_DIR",
      "NODE_ENV",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = await fs.mkdtemp(join(tmpdir(), "omniroute-pg-blocked-history-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let foreign: PostgresControlPlane | undefined;
    let accepted: Promise<void> | undefined;
    const entered = barrier();
    const release = barrier();
    try {
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      const history = await import("../../src/lib/proxyHealth/blockedHistory.ts");
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      const { UnsupportedPersistenceDomainError } =
        await import("../../src/lib/db/controlPlaneContext.ts");
      foreign = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        runtime: {
          applyRuntimeSettings() {},
          scheduleCatalogRevalidationAfterInit() {},
        },
        logger: { warn() {} },
      });
      assert.equal(
        state.background.size,
        0,
        "free admission is required for the foreign-plane falsifier"
      );
      await foreign.run(async () => {
        await assert.rejects(
          history.recordBlockedObservationAsync("foreign", "unproven", 403, 1),
          UnsupportedPersistenceDomainError
        );
      });
      assert.equal(state.background.size, 0);
      t.mock.timers.enable({ apis: ["setTimeout"] });
      await runtime!.runPersistenceRequest(async () => {
        await history.recordBlockedObservationAsync("p", "unproven", 403, 1);
      });
      // Fire the debounce without inheriting an application request scope. Dispatch must acquire its own.
      await foreign.run(async () => {
        t.mock.timers.tick(1000);
      });
      assert.equal(state.background.size, 1);
      await Promise.all([...state.background]);
      assert.equal(
        JSON.parse(await fs.readFile(join(directory, "blocked-history.json"), "utf8")).p.count,
        1
      );
      accepted = runtime!.runPersistenceRequest(async () => {
        entered.resolve();
        await release.promise;
        await history.recordBlockedObservationAsync("p", "target_refused", 429, 2);
      });
      await entered.promise;
      runtime!.beginPersistenceAdmissionClose();
      let drained = false;
      const draining = runtime!.drainPersistenceAcceptedWork().then(() => {
        drained = true;
      });
      await yieldToEventLoop();
      assert.equal(drained, false, "accepted request must retain its history sink during drain");
      release.resolve();
      await accepted;
      await draining;
      await runtime!.runPersistenceShutdownWork(history.closeBlockedHistoryAsync);
      const stored = JSON.parse(await fs.readFile(join(directory, "blocked-history.json"), "utf8"));
      assert.equal(stored.p.count, 2);
      assert.equal(stored.p.firstSeen, 1);
      assert.equal(stored.p.lastStatus, 429);
      assert.equal(stored.foreign, undefined);
    } finally {
      t.mock.timers.reset();
      release.resolve();
      await accepted?.catch(() => {});
      if (runtime) {
        await runtime
          .runPersistenceShutdownWork(async () => {
            await (
              await import("../../src/lib/proxyHealth/blockedHistory.ts")
            ).closeBlockedHistoryAsync();
          })
          .catch(() => {});
        await runtime.closePersistenceRuntime();
      }
      await foreign?.close();
      hooks.deregister();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      await fs.rm(directory, { recursive: true, force: true });
    }
  }
);
