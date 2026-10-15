import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  postgresTestsEnabled,
  createTestPostgresDatabase,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "LKGP helpers admit before suspension and keep native authority through parent completion and shutdown",
  {
    skip: !postgresTestsEnabled,
    timeout: 60000,
  },
  async () => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
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
        )
          return { url: fixtureUrl, shortCircuit: true };
        return next(specifier, context);
      },
    });
    const previous = new Map(
      ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY", "JWT_SECRET", "API_KEY_SECRET"].map(
        (name) => [name, process.env[name]]
      )
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") = await import(
      runtimeUrl
    );
    const pins = await import("../../src/lib/db/settings/lkgp.ts");
    const { recordLkgpPin } = await import("../../open-sse/services/combo/recordLkgpPin.ts");
    const { prepareLkgpClear } = await import("../../open-sse/services/combo/staleLkgpClear.ts");
    const comboName = randomUUID(),
      warnings: unknown[] = [];
    const log = {
      warn(...args: unknown[]) {
        warnings.push(args);
      },
    };
    const gate = Promise.withResolvers<void>(),
      entered = Promise.withResolvers<void>();
    try {
      await runtime.ensurePersistenceReady();
      let recorded!: Promise<void>;
      await runtime.runPersistenceRequest(() => {
        recorded = recordLkgpPin({
          comboName,
          comboId: "id",
          executionKey: "execution",
          provider: "anthropic",
          connectionId: "account",
          log,
        });
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      });
      await recorded;
      let cleared!: Promise<void>;
      await runtime.runPersistenceRequest(async () => {
        const clear = await prepareLkgpClear(comboName, log);
        assert.ok(await pins.getLKGP(comboName, "id"));
        cleared = clear(comboName, "execution", "id", log, "COMBO", undefined, {
          provider: "anthropic",
          connectionId: "account",
        });
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      });
      await cleared;
      await runtime.runPersistenceRequest(async () => {
        assert.equal(await pins.getLKGP(comboName, "id"), null);
        assert.equal(await pins.getLKGP(comboName, "execution"), null);
      });
      let draining!: Promise<void>;
      await runtime.runPersistenceRequest(() => {
        draining = recordLkgpPin({
          comboName,
          comboId: "id",
          executionKey: "execution",
          provider: "openai",
          connectionId: "new",
          log,
          async setLKGP(...args) {
            entered.resolve();
            await gate.promise;
            await pins.setLKGP(...args);
          },
        });
      });
      await entered.promise;
      const closed = runtime.closePersistenceRuntime();
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      gate.resolve();
      await draining;
      await closed;
      assert.deepEqual(warnings, []);
      const verification = await createTestPostgresDatabase();
      try {
        const rows = await verification.transaction((tx) =>
          tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='lkgp' AND starts_with(key,$1)",
            [comboName + ":"]
          )
        );
        assert.equal(rows.rowCount, 2);
        assert.ok(rows.rows.every((row) => JSON.parse(row.value).provider === "openai"));
      } finally {
        await verification.close();
      }
    } finally {
      gate.resolve();
      await runtime.closePersistenceRuntime();
      hooks.deregister();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
