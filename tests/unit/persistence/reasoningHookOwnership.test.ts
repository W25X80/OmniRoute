import assert from "node:assert/strict";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { hookFixture } from "../../helpers/persistence/reasoningHookFixture.ts";

const rootRuntimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const serviceUrl = new URL("../../../open-sse/services/reasoningCache.ts", import.meta.url).href;
const streamUrl = new URL(
  "../../../open-sse/handlers/chatCore/streamReasoningCapture.ts",
  import.meta.url
).href;
const fixtureUrl = new URL("../../helpers/persistence/reasoningHookFixture.ts", import.meta.url)
  .href;
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
    if (context.parentURL === rootRuntimeUrl && runtimeDependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (
      context.parentURL === serviceUrl &&
      (specifier.includes("src/lib/db/reasoningCache") ||
        specifier.includes("src/shared/utils/featureFlags"))
    )
      return { url: fixtureUrl, shortCircuit: true };
    if (
      context.parentURL === streamUrl &&
      [
        "../../translator/index.ts",
        "../../translator/response/openai-responses/toolSchemas.ts",
        "../responseTranslator.ts",
      ].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const previousJwt = process.env.JWT_SECRET;
const previousKey = process.env.API_KEY_SECRET;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  rootRuntimeUrl
);
const service = await import("../../../open-sse/services/reasoningCache.ts");
const stream = await import("../../../open-sse/handlers/chatCore/streamReasoningCapture.ts");
const message = {
  role: "assistant",
  content: "result",
  reasoning_content: "retained reasoning",
  tool_calls: [{ id: "owned-replay-tool" }],
};
const input = {
  streamResponseBody: { choices: [{ message }] },
  clientResponseFormat: "openai",
  responseToolNameMap: null,
  providerRequestBody: {},
  translatedBody: { messages: [{ role: "user", content: "request" }] },
  reasoningReplayHistory: [],
  provider: "deepseek",
  model: "deepseek-chat",
  reasoningCacheScope: "owner",
  videoTranscriptSensitive: false,
};
const observe = <T>(work: Promise<T>) => {
  void work.catch(() => {});
  return work;
};
async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  hookFixture.reset();
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
  "async replay capture admits native work before its request ends and awaits delayed fresh execution",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const catalog = barrier(),
        writeStarted = barrier(),
        writeDone = barrier();
      hookFixture.finishWrite = writeDone.promise;
      hookFixture.writeEntered = writeStarted.resolve;
      let capture!: Promise<number>;
      await runtime.runPersistenceRequest(() => {
        hookFixture.delayNextCatalog = catalog.promise;
        capture = observe(
          service.cacheReasoningFromAssistantMessageAsync(message, "deepseek", "deepseek-chat")
        );
      });
      assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      assert.equal(hookFixture.writes.length, 0);
      const closing = observe(runtime.closePersistenceRuntime());
      catalog.resolve();
      await writeStarted.promise;
      assert.equal(hookFixture.writes[0].authority, fixture.authority);
      assert.equal(hookFixture.writes[0].generation, 2);
      assert.equal(fixture.poolCloses, 0);
      writeDone.resolve();
      assert.equal(await capture, 1);
      await closing;
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test(
  "stream Async sibling returns the actual native completion while legacy hook stays void",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const entered = barrier(),
        done = barrier();
      hookFixture.finishWrite = done.promise;
      hookFixture.writeEntered = entered.resolve;
      let finished = false;
      const capture = observe(
        runtime.runPersistenceRequest(() => stream.captureStreamReasoningForReplayAsync(input))
      );
      void capture.then(
        () => {
          finished = true;
        },
        () => {}
      );
      await entered.promise;
      assert.equal(finished, false);
      assert.equal(hookFixture.writes.length, 1);
      done.resolve();
      await capture;
      assert.equal(finished, true);
      hookFixture.finishWrite = undefined;
      const result = await runtime.runPersistenceRequest(() =>
        stream.captureStreamReasoningForReplay(input)
      );
      assert.equal(result, undefined);
    })
);

test(
  "optional persistence failure settles the capture without changing the client hook outcome",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      hookFixture.writeFailure = new Error("optional-replay-write-failed");
      await runtime.runPersistenceRequest(() => stream.captureStreamReasoningForReplayAsync(input));
      assert.equal(hookFixture.writes.length, 1);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test(
  "sensitive transcript and internal placeholder produce no native job",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      await runtime.runPersistenceRequest(async () => {
        await stream.captureStreamReasoningForReplayAsync({
          ...input,
          videoTranscriptSensitive: true,
        });
        assert.equal(
          await service.cacheReasoningFromAssistantMessageAsync(
            { ...message, reasoning_content: "(prior reasoning summary unavailable)" },
            "deepseek",
            "deepseek-chat"
          ),
          0
        );
      });
      assert.equal(hookFixture.writes.length, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test(
  "SQLite synchronous capture preserves return values and writes immediately",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
      try {
        assert.equal(
          service.cacheReasoningFromAssistantMessage(message, "deepseek", "deepseek-chat"),
          1
        );
        assert.equal(
          service.cacheReasoning(
            "sync-replay-tool",
            "deepseek",
            "deepseek-chat",
            "retained reasoning"
          ),
          undefined
        );
        assert.deepEqual(hookFixture.sqliteWrites, ["owned-replay-tool", "sync-replay-tool"]);
        assert.equal(
          await service.cacheReasoningFromAssistantMessageAsync(
            message,
            "deepseek",
            "deepseek-chat"
          ),
          1
        );
        assert.equal(hookFixture.writes.length, 0);
      } finally {
        process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      }
    })
);
