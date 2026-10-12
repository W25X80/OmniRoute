import assert from "node:assert/strict";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { nativeRuntimeFixture } from "../helpers/persistence/runtimeNativeIntegrationFixture.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "CC discovery hits retain native optional-work ownership without blocking their request",
  {
    skip: !postgresTestsEnabled,
    timeout: 60_000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const runtimeFixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const importFixtureUrl = new URL(
      "../helpers/persistence/ccDiscoveryCatalogImportFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (context.parentURL === runtimeUrl) {
          if (
            [
              "./postgres/environment",
              "./postgres/controlPlane",
              "../artifacts/s3ArtifactConfiguration",
              "../artifacts/s3ArtifactStore",
            ].includes(specifier)
          ) {
            return { url: runtimeFixtureUrl, shortCircuit: true };
          }
          if (specifier === "./postgres/catalogSnapshot") {
            return { url: importFixtureUrl, shortCircuit: true };
          }
        }
        return nextResolve(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "NODE_ENV",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "native-cc-metrics-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const database = await createTestPostgresDatabase();
    const counter = async () => {
      const result = await database.transaction((tx) =>
        tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='ccDiscoveryMetrics' AND key='discovery:hits'"
        )
      );
      return Number(result.rows[0]?.value ?? 0);
    };
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let standalone: PostgresControlPlane | undefined;
    const imported = barrier(),
      releaseImport = barrier();
    const pending = new Set<Promise<void>>();
    const warnings: string[] = [];
    t.mock.method(console, "warn", (...args: unknown[]) =>
      warnings.push(args.map(String).join(" "))
    );
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      standalone = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: {
          applyRuntimeSettings() {},
          scheduleCatalogRevalidationAfterInit() {},
        },
      });
      const metrics = await import("../../src/lib/db/ccDiscoveryMetrics.ts");

      await t.test(
        "an explicitly awaited standalone count never creates application runtime",
        async () => {
          const before = await counter();
          await standalone!.run(async () => {
            await metrics.incrementCcDiscoveryHitCountAsync();
            assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
          });
          assert.equal(await counter(), before + 1);
          assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
        }
      );

      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      assert.ok(state.applicationIdentity);
      globalThis.__ccDiscoveryCatalogImportGate = {
        entered: imported.resolve,
        release: releaseImport.promise,
      };

      await t.test(
        "admission precedes delayed module loading and survives originating scope revocation",
        async () => {
          const before = await counter();
          let expired: ReturnType<typeof getSelectedPostgresDatabase>;
          let metric!: Promise<void>;
          // Use the real application's request lease directly to keep the runtime's
          // catalog module cold. Its run() revocation is the same production scope owner.
          await nativeRuntimeFixture.plane!.run(() => {
            expired = getSelectedPostgresDatabase();
            metric = metrics.incrementCcDiscoveryHitCountAsync();
            pending.add(metric);
            assert.equal(state.background.size, 1, "owner exists before the async call returns");
          });
          await imported.promise;
          assert.throws(() => expired!.transaction(async () => {}), /scope has ended/);
          assert.equal(
            await counter(),
            before,
            "the request ended before module loading or the insert"
          );
          assert.equal(state.background.size, 1);
          releaseImport.resolve();
          await metric;
          pending.delete(metric);
          assert.equal(await counter(), before + 1);
          assert.equal(state.background.size, 0);
          assert.equal(warnings.length, 0);
        }
      );

      await t.test("native catalog counts each admitted Claude Code request once", async () => {
        const { prepareNativeProviders } = await import("../../open-sse/config/constants.ts");
        await prepareNativeProviders();
        const { updateSettings } = await import("../../src/lib/db/settings.ts");
        const { getUnifiedModelsResponse } = await import("../../src/app/api/v1/models/catalog.ts");
        const fetch = globalThis.fetch;
        globalThis.fetch = async () => {
          throw new Error("Unexpected external catalog request");
        };
        try {
          await runtime!.runPersistenceRequest(() =>
            updateSettings({
              requireAuthForModels: false,
              setupComplete: true,
              password: "fixture-configured-password",
            })
          );
          const before = await counter();
          const catalogRequest = (agent: string) =>
            runtime!.runPersistenceRequest(() =>
              getUnifiedModelsResponse(
                new Request("https://catalog.invalid/v1/models", {
                  headers: { "user-agent": agent },
                })
              )
            );
          const response = await catalogRequest("claude-cli/fixture");
          assert.equal(response.status, 200);
          await response.text();
          await runtime!.drainPersistenceAcceptedWork();
          assert.equal(await counter(), before + 1);
          const ordinary = await catalogRequest("ordinary-client");
          assert.equal(ordinary.status, 200);
          await ordinary.text();
          await runtime!.drainPersistenceAcceptedWork();
          assert.equal(await counter(), before + 1);
          await runtime!.runPersistenceRequest(() =>
            updateSettings({ requireAuthForModels: true })
          );
          const denied = await catalogRequest("claude-cli/fixture");
          assert.equal(denied.status, 401);
          await denied.text();
          await runtime!.drainPersistenceAcceptedWork();
          assert.equal(
            await counter(),
            before + 1,
            "rejected authentication is never a discovery hit"
          );
        } finally {
          globalThis.fetch = fetch;
        }
      });

      await t.test(
        "a saturated application gate refuses best-effort and performs no increment",
        async () => {
          const before = await counter(),
            messagesBefore = warnings.length;
          const slots = Array.from({ length: 64 }, () => barrier());
          for (const slot of slots) state.background.add(slot.promise);
          try {
            await runtime!.runPersistenceRequest(async () => {
              await metrics.incrementCcDiscoveryHitCountAsync();
            });
            assert.equal(state.background.size, 64);
            assert.equal(await counter(), before);
            assert.equal(warnings.length, messagesBefore + 1);
            assert.match(
              warnings.at(-1)!,
              /Discovery-hit persistence failed.*admission is unavailable/
            );
            assert.doesNotMatch(warnings.at(-1)!, /at \/|STORAGE_ENCRYPTION_KEY/);
          } finally {
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "a foreign authority is awaited directly even when application admission is full",
        async () => {
          const before = await counter(),
            messagesBefore = warnings.length;
          const slots = Array.from({ length: 64 }, () => barrier());
          for (const slot of slots) state.background.add(slot.promise);
          try {
            await standalone!.run(async (binding) => {
              assert.equal(
                runtime!.isApplicationPersistenceIdentity(binding.databaseIdentity),
                false
              );
              await metrics.incrementCcDiscoveryHitCountAsync();
              assert.equal(state.background.size, 64);
            });
            assert.equal(await counter(), before + 1);
            assert.equal(warnings.length, messagesBefore);
          } finally {
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "authority drift is reported and does not write under a replacement identity",
        async () => {
          const before = await counter(),
            messagesBefore = warnings.length;
          const entered = barrier(),
            releaseRead = barrier();
          const identity = state.applicationIdentity;
          globalThis.__ccDiscoveryCatalogReadGate = {
            entered: entered.resolve,
            release: releaseRead.promise,
          };
          let metric!: Promise<void>;
          try {
            await nativeRuntimeFixture.plane!.run(() => {
              metric = metrics.incrementCcDiscoveryHitCountAsync();
              pending.add(metric);
              assert.equal(state.background.size, 1);
            });
            await entered.promise;
            state.applicationIdentity = Object.freeze({});
            releaseRead.resolve();
            await metric;
            pending.delete(metric);
            assert.equal(await counter(), before);
            assert.equal(warnings.length, messagesBefore + 1);
            assert.match(warnings.at(-1)!, /authority changed before execution/);
          } finally {
            state.applicationIdentity = identity;
            releaseRead.resolve();
            delete globalThis.__ccDiscoveryCatalogReadGate;
          }
        }
      );

      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      await runtime.closePersistenceRuntime();
      assert.equal(state.background.size, 0);
    } finally {
      releaseImport.resolve();
      delete globalThis.__ccDiscoveryCatalogImportGate;
      delete globalThis.__ccDiscoveryCatalogReadGate;
      await Promise.allSettled([...pending]);
      await runtime?.closePersistenceRuntime().catch(() => {});
      await standalone?.close().catch(() => {});
      await database.close();
      hooks.deregister();
      t.mock.restoreAll();
      delete globalThis.__omniroutePersistenceRuntime;
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
