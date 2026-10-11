import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { hookFixture } from "../../helpers/persistence/reasoningHookFixture.ts";
import { sequentialFixture } from "../../helpers/persistence/reasoningSequentialFixture.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const serviceUrl = new URL("../../../open-sse/services/reasoningCache.ts", import.meta.url).href;
const fixtureUrl = new URL(
  "../../helpers/persistence/reasoningSequentialFixture.ts",
  import.meta.url
).href;
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (
      context.parentURL === serviceUrl &&
      (specifier.includes("src/lib/db/reasoningCache") ||
        specifier.includes("src/shared/utils/featureFlags") ||
        specifier === "../utils/logger.ts")
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const previousJwt = process.env.JWT_SECRET;
const previousKey = process.env.API_KEY_SECRET;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
const service = await import("../../../open-sse/services/reasoningCache.ts");
function message(keys: string[]) {
  return {
    role: "assistant",
    reasoning_content: "retained reasoning",
    tool_calls: keys.map((id) => ({ id })),
  };
}
async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  hookFixture.reset();
  sequentialFixture.reset();
  try {
    await body();
  } finally {
    fixture.releaseBarriers();
    await runtime.closePersistenceRuntime().catch(() => {});
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [name, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", previousBackend],
    ["JWT_SECRET", previousJwt],
    ["API_KEY_SECRET", previousKey],
  ]) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test(
  "native multi-tool capture owns one draining job and never overlaps SQL writes",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const keys = Array.from({ length: 6 }, (_, i) => `sequential-held-${i}`);
      const started = keys.map(() => barrier()),
        finished = keys.map(() => barrier());
      keys.forEach((key, i) => {
        sequentialFixture.entered.set(key, started[i].resolve);
        sequentialFixture.pending.set(key, finished[i].promise);
      });
      let capture!: Promise<number>;
      await runtime.runPersistenceRequest(() => {
        capture = service.cacheReasoningFromAssistantMessageAsync(
          message(keys),
          "deepseek",
          "deepseek-chat"
        );
      });
      const closing = runtime.closePersistenceRuntime();
      try {
        for (let i = 0; i < keys.length; i++) {
          await started[i].promise;
          assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
          assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
          assert.equal(sequentialFixture.active, 1);
          assert.equal(sequentialFixture.peak, 1);
          assert.deepEqual(
            hookFixture.writes.map((write) => write.key),
            keys.slice(0, i + 1)
          );
          assert.equal(fixture.poolCloses, 0);
          finished[i].resolve();
        }
        assert.equal(await capture, keys.length);
        await closing;
        assert.equal(sequentialFixture.active, 0);
        assert.equal(fixture.poolCloses, 1);
      } finally {
        fixture.releaseBarriers();
        await capture.catch(() => {});
        await closing.catch(() => {});
      }
    })
);

test(
  "native optional failure preserves logical count, hot reasoning and later independent writes",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const keys = ["sequential-failing", "sequential-good", "sequential-failing"];
      sequentialFixture.failures.add(keys[0]);
      await runtime.runPersistenceRequest(async () => {
        assert.equal(
          await service.cacheReasoningFromAssistantMessageAsync(
            message(keys),
            "deepseek",
            "deepseek-chat"
          ),
          3
        );
        assert.deepEqual(
          hookFixture.writes.map((write) => write.key),
          keys
        );
        assert.equal(service.lookupReasoning(keys[0]), "retained reasoning");
        assert.equal(service.lookupReasoning(keys[1]), "retained reasoning");
      });
      assert.equal(sequentialFixture.peak, 1);
      assert.equal(sequentialFixture.active, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test("SQLite captures keep immediate writes and both public key counts", { timeout: 10000 }, () =>
  scenario(async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    try {
      const keys = ["sequential-sqlite-a", "sequential-sqlite-b", "sequential-sqlite-a"];
      assert.equal(
        service.cacheReasoningFromAssistantMessage(message(keys), "deepseek", "deepseek-chat"),
        3
      );
      assert.deepEqual(hookFixture.sqliteWrites, keys);
      const pending = service.cacheReasoningFromAssistantMessageAsync(
        message(keys),
        "deepseek",
        "deepseek-chat"
      );
      assert.deepEqual(hookFixture.sqliteWrites, [...keys, ...keys]);
      assert.equal(await pending, 3);
      assert.equal(hookFixture.writes.length, 0);
      assert.equal(sequentialFixture.peak, 0);
    } finally {
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    }
  })
);

test(
  "disabled replay retains the source logical count without admitting a native job",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      hookFixture.enabled = false;
      await runtime.runPersistenceRequest(async () => {
        assert.equal(
          await service.cacheReasoningFromAssistantMessageAsync(
            message(["sequential-disabled-a", "sequential-disabled-b"]),
            "deepseek",
            "deepseek-chat"
          ),
          2
        );
      });
      assert.equal(hookFixture.writes.length, 0);
      assert.equal(sequentialFixture.peak, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test(
  "native batch admission rejection preserves logical count and emits one fixed diagnostic",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const blocked = barrier();
      const admitted: Promise<void>[] = [];
      try {
        await runtime.runPersistenceRequest(async () => {
          for (let i = 0; i < 64; i++) {
            const work = runtime.runPersistenceBackgroundWork(() => blocked.promise);
            void work.catch(() => {});
            admitted.push(work);
          }
          assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 64);
          assert.equal(
            await service.cacheReasoningFromAssistantMessageAsync(
              message(["sequential-not-admitted-a", "sequential-not-admitted-b"]),
              "deepseek",
              "deepseek-chat"
            ),
            2
          );
          assert.equal(service.lookupReasoning("sequential-not-admitted-a"), "retained reasoning");
          assert.deepEqual(sequentialFixture.warnings, [
            "Optional reasoning capture persistence unavailable",
          ]);
          assert.equal(hookFixture.writes.length, 0);
          assert.equal(sequentialFixture.peak, 0);
        });
      } finally {
        blocked.resolve();
        await Promise.all(admitted);
      }
    })
);
