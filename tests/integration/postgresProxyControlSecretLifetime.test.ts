import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { getSelectedControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import { proxyControlSecretProbe } from "../helpers/persistence/proxyControlSecretProbe.ts";

test(
  "real PostgreSQL deferred default-core secret lookup owns a fresh application scope after its request ends",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const reloadUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/proxySubscription/reloadSecret.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const probeUrl = new URL("../helpers/persistence/proxyControlSecretProbe.ts", import.meta.url)
      .href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        if (context.parentURL === reloadUrl && specifier === "../db/encryption")
          return { url: probeUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "OMNIROUTE_PROXY_CORE_RELOAD_COMMAND",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-control-secret-"));
    const id = `native-control-secret-${randomUUID()}`;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    let core: typeof import("../../src/lib/proxySubscription/coreConfig/reload.ts") | undefined;
    let resource: AsyncResource | undefined;
    try {
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "5c".repeat(32);
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_PROXY_CORE_RELOAD_COMMAND = "external";
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = (await import(
        runtimeUrl
      )) as typeof import("../../src/lib/db/persistenceRuntime.ts");
      await runtime.ensurePersistenceReady();
      database = await createTestPostgresDatabase();
      const service: typeof import("../../src/lib/proxySubscription/reloadSecret.ts") =
        await import(reloadUrl);
      core = await import("../../src/lib/proxySubscription/coreConfig/reload.ts");
      const renderers = await import("../../src/lib/proxySubscription/coreConfig/renderers.ts");
      assert.equal(
        renderers.RENDERERS[renderers.DEFAULT_CORE].apiReload,
        false,
        "the actual default renderer does not expose a control HTTP reload"
      );
      await database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,10)");
        await tx.query(
          "INSERT INTO omniroute.proxy_subscriptions(id,name,url,control_secret_enc,created_at,updated_at) VALUES($1,'native lifetime fixture','https://example.invalid/fixture',$2,$3,$3)",
          [id, encrypt("native-stored-control-secret"), new Date().toISOString()]
        );
      });
      proxyControlSecretProbe.reset();
      const warnings: string[] = [];
      t.mock.method(console, "warn", (message: unknown) => warnings.push(String(message)));
      let now = 1_000_000;
      t.mock.method(Date, "now", () => now);
      assert.equal(
        await runtime.runPersistenceRequest(() =>
          service.reloadAfterReplace(id, null, "fixture.json")
        ),
        null
      );
      now += 59_980;
      await runtime.runPersistenceRequest(async () => {
        resource = new AsyncResource("expired-native-proxy-control-request");
        assert.equal(await service.reloadAfterReplace(id, null, "fixture.json"), null);
      });
      assert.throws(() => resource!.runInAsyncScope(getSelectedControlPlane), /scope has ended/);
      const deadline = performance.now() + 10000;
      while (proxyControlSecretProbe.values.length < 3 && performance.now() < deadline)
        await delay(5);
      assert.equal(
        proxyControlSecretProbe.values.length,
        3,
        "the real delayed callback must complete a fresh native lookup"
      );
      const state = globalThis.__omniroutePersistenceRuntime!;
      await Promise.allSettled([...state.background]);
      assert.deepEqual(
        proxyControlSecretProbe.values,
        Array(3).fill("native-stored-control-secret")
      );
      assert.notEqual(proxyControlSecretProbe.bindings[2], proxyControlSecretProbe.bindings[1]);
      assert.equal(proxyControlSecretProbe.bindings[2].databaseIdentity, state.applicationIdentity);
      assert.throws(() => proxyControlSecretProbe.bindings[1].assertReady(), /scope has ended/);
      assert.deepEqual(warnings, []);
      assert.equal(state.requests.size, 0);
      assert.equal(state.background.size, 0);
      const idle = await database.transaction((tx) =>
        tx.query<{ count: string }>(
          "SELECT COUNT(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"
        )
      );
      assert.equal(idle.rows[0].count, "0");
    } finally {
      core?.clearCoreReloadState(id);
      resource?.emitDestroy();
      try {
        try {
          await runtime?.closePersistenceRuntime();
        } finally {
          if (database)
            await database.transaction((tx) =>
              tx.query("DELETE FROM omniroute.proxy_subscriptions WHERE id=$1", [id])
            );
        }
      } finally {
        try {
          await database?.close();
        } finally {
          hooks.deregister();
          proxyControlSecretProbe.reset();
          delete globalThis.__omniroutePersistenceRuntime;
          for (const [name, value] of previous) {
            if (value === undefined) delete process.env[name];
            else process.env[name] = value;
          }
          rmSync(directory, { recursive: true, force: true });
        }
      }
    }
  }
);
