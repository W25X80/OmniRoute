import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { createPostgresMemoryRepository } from "../../src/lib/db/postgres/memoryRepository.ts";
import { PostgresMemoryVectorStore } from "../../src/lib/db/postgres/memoryVectorRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { MemoryType } from "../../src/lib/memory/types.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { reindexResolution } from "../helpers/persistence/nativeMemoryReindexFixture.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native reindex preserves work queued after its committed vector upsert",
  { skip: !postgresTestsEnabled, timeout: 30_000 },
  async (t) => {
    const database = await createTestPostgresDatabase();
    const fixtureUrl = new URL(
      "../helpers/persistence/nativeMemoryReindexFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL?.endsWith("/src/lib/memory/reindex.ts") &&
          (specifier === "./embedding" || specifier === "./settings")
        ) {
          return { url: fixtureUrl, shortCircuit: true };
        }
        return next(specifier, context);
      },
    });
    const savedQuantization = process.env.MEMORY_VEC_QUANTIZATION;
    delete process.env.MEMORY_VEC_QUANTIZATION;
    try {
      await migratePostgresSchema(database);
      const { runReindexBatch } = await import("../../src/lib/memory/reindex.ts");
      const memory = createPostgresMemoryRepository(database);
      const binding = {
        database,
        databaseIdentity: Object.freeze({}),
        assertReady() {},
      } as unknown as PostgresControlPlaneBinding;

      for (const change of ["signature", "content"] as const) {
        await t.test(change, async (subtest) => {
          await database.transaction((tx) => tx.query("TRUNCATE omniroute.memories CASCADE"));
          const stored = await memory.create({
            apiKeyId: "reindex-race-owner",
            sessionId: "session",
            type: MemoryType.FACTUAL,
            key: "race",
            content: "original content",
            metadata: {},
            expiresAt: null,
          });
          const committed = barrier();
          const resume = barrier();
          const upsert = PostgresMemoryVectorStore.prototype.upsertVector;
          subtest.mock.method(
            PostgresMemoryVectorStore.prototype,
            "upsertVector",
            async function (this: PostgresMemoryVectorStore, ...args: Parameters<typeof upsert>) {
              await upsert.apply(this, args);
              if (args[0] === stored.id) {
                committed.resolve();
                await resume.promise;
              }
            }
          );

          const batch = withPostgresControlPlane(binding, () => runReindexBatch(1));
          try {
            await Promise.race([
              committed.promise,
              batch.then(() => {
                throw new Error("Batch did not reach its committed vector upsert");
              }),
            ]);
            assert.equal(await memory.reindexCount(), 0);
            const current = new PostgresMemoryVectorStore(database);
            if (change === "signature") {
              await current.ensureReady({
                ...reindexResolution,
                model: "replacement",
                signature: "static:replacement:2",
              });
            } else {
              await memory.update(stored.id, { content: "replacement content" });
              await current.ensureReady(reindexResolution);
            }
            assert.equal(await memory.reindexCount(), 1);
            resume.resolve();
            assert.deepEqual(await batch, { processed: 1, errors: 0 });
            assert.deepEqual(
              (await memory.reindexQueue(10)).map((item) => item.id),
              [stored.id],
              "late batch continuation must not acknowledge a newer invalidation"
            );
            assert.deepEqual(
              await current.searchVector(new Float32Array([1, 0]), 10, stored.apiKeyId),
              [],
              "invalidated vector stays absent while fresh work remains pending"
            );
          } finally {
            resume.resolve();
            await batch.catch(() => {});
            subtest.mock.restoreAll();
          }
        });
      }
    } finally {
      if (savedQuantization === undefined) delete process.env.MEMORY_VEC_QUANTIZATION;
      else process.env.MEMORY_VEC_QUANTIZATION = savedQuantization;
      hooks.deregister();
      await database.close();
    }
  }
);
