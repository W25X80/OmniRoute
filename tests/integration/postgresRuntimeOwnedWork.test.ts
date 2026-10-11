import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import { nativeRuntimeFixture } from "../helpers/persistence/runtimeNativeIntegrationFixture.ts";
import type { PersistenceTailPermit } from "../../src/lib/db/persistenceRuntime.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native stream tail commits during drain and pool closure revokes its scoped authority",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async () => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
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
        ) {
          return { url: fixtureUrl, shortCircuit: true };
        }
        return nextResolve(specifier, context);
      },
    });
    const previous = new Map(
      ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY", "JWT_SECRET", "API_KEY_SECRET"].map(
        (key) => [key, process.env[key]]
      )
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts");
    const key = randomUUID();
    const external = new AsyncResource("native-stream-detached-callback");
    const entered = barrier(),
      commit = barrier();
    let permit: PersistenceTailPermit | undefined;
    let scoped: ReturnType<typeof getSelectedPostgresDatabase>;
    let write: Promise<string> | undefined;
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      await runtime.runPersistenceRequest(async () => {
        permit = await runtime.acquirePersistenceTail();
      });
      write = external.runInAsyncScope(() =>
        permit!.run(async () => {
          scoped = getSelectedPostgresDatabase()!;
          entered.resolve();
          await commit.promise;
          await scoped.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('runtime_owned_work_test',$1,$2)",
              [key, "committed-after-admission-close"]
            )
          );
          return "committed";
        })
      );
      void write.catch(() => {});
      await entered.promise;
      const closing = runtime.closePersistenceRuntime();
      void closing.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      await assert.rejects(
        runtime.runPersistenceRequest(() => {}),
        /unavailable/
      );
      assert.equal(nativeRuntimeFixture.plane?.state, "ready");
      commit.resolve();
      assert.equal(await write, "committed");
      await permit!.release();
      await closing;
      assert.equal(nativeRuntimeFixture.plane?.state, "closed");
      assert.equal(nativeRuntimeFixture.artifactsClosed, 1);
      assert.throws(() => scoped!.transaction(async () => {}), /scope has ended/);
      const verification = await createTestPostgresDatabase();
      try {
        const result = await verification.transaction((tx) =>
          tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='runtime_owned_work_test' AND key=$1",
            [key]
          )
        );
        assert.equal(result.rows[0]?.value, "committed-after-admission-close");
      } finally {
        await verification.close();
      }
    } finally {
      commit.resolve();
      await write?.catch(() => {});
      await permit?.release().catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      external.emitDestroy();
      try {
        const cleanup = await createTestPostgresDatabase();
        try {
          await cleanup.transaction((tx) =>
            tx.query(
              "DELETE FROM omniroute.key_value WHERE namespace='runtime_owned_work_test' AND key=$1",
              [key]
            )
          );
        } finally {
          await cleanup.close();
        }
      } finally {
        delete globalThis.__omniroutePersistenceRuntime;
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
