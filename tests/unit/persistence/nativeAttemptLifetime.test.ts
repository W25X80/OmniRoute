import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { registerHooks } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import {
  getSelectedPersistenceIdentity,
  getSelectedPostgresDatabase,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { controlledOwnerModule } from "../../helpers/persistence/controlledOwnerModule.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
  .href;
const dependencies = new Set([
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
    if (context.parentURL === runtimeUrl && dependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const previous = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET", "DATA_DIR"].map((key) => [
    key,
    process.env[key],
  ])
);
const directory = mkdtempSync(join(tmpdir(), "omni-native-attempt-"));
process.env.DATA_DIR = directory;
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const { buildTargetTimeoutRunner } =
  await import("../../../open-sse/services/combo/targetTimeoutRunner.ts");
const { executeWithUpstreamStartTimeout } =
  await import("../../../open-sse/handlers/chatCore/upstreamTimeouts.ts");
const log = { warn() {}, info() {}, error() {}, debug() {} };

test("a failed streaming dispatch joins direct usage while call logging keeps its background owner", () =>
  scenario(async () => {
    const logEntered = barrier(),
      finishLog = barrier(),
      usageEntered = barrier(),
      finishUsage = barrier();
    const identities: Array<object | undefined> = [];
    const errors = await import("../../../open-sse/utils/error.ts");
    const streamErrors = await import("../../../open-sse/handlers/chatCore/streamErrorResult.ts");
    const { HTTP_STATUS, ANTIGRAVITY_PRE_RESPONSE_TIMEOUT_CODE } =
      await import("../../../open-sse/config/constants.ts");
    const { projectFailureUsageErrorCode } =
      await import("../../../open-sse/handlers/chatCore/failureUsage.ts");
    type Leg = typeof import("../../../open-sse/handlers/chatCore/streamingResponse.ts");
    const noop = () => {};
    let dispatched = false;
    const leg = await controlledOwnerModule<Leg>(
      new URL("../../../open-sse/handlers/chatCore/streamingResponse.ts", import.meta.url),
      {},
      {
        ...errors,
        ...streamErrors,
        HTTP_STATUS,
        ANTIGRAVITY_PRE_RESPONSE_TIMEOUT_CODE,
        projectFailureUsageErrorCode,
        getSelectedPostgresDatabase,
        runProviderExecutionPipeline: async () => {
          dispatched = true;
          throw new Error("Upstream unavailable");
        },
        trackPendingRequest: noop,
        appendRequestLog: async () => {},
        getProviderCredentials: noop,
        updateFromHeaders: noop,
        updateFromResponseBody: noop,
        writeTerminalStatus: noop,
        updateProviderConnection: noop,
        lockModel: noop,
        recordCoreOwnedAntigravityQuotaState: noop,
        shouldIsolateProbeFailures: noop,
        isLocalStreamLifecycleError: () => false,
        localLimiterErrors: {
          getClientSafeLocalRateLimitError: () => undefined,
          markTrustedLocalRateLimitResponse: noop,
        },
        COLORS: { red: "", reset: "" },
      },
      (source) => source.slice(source.indexOf("export async function runStreamingResponse("))
    );
    const pending: Promise<void>[] = [];
    const persist =
      (entered: ReturnType<typeof barrier>, finish: ReturnType<typeof barrier>) => () => {
        const work = (async () => {
          entered.resolve();
          await finish.promise;
          identities.push(getSelectedPersistenceIdentity());
        })();
        void work.catch(() => {});
        pending.push(work);
        return work;
      };
    let settled = false;
    const response = runtime.runPersistenceRequest(() =>
      runtime.runPersistenceAttempt(() =>
        leg.runStreamingResponse({
          provider: "test",
          model: "test",
          stream: true,
          getCurrentConnectionId: () => "connection",
          getManagedLeaseFenceErrorCode: () => null,
          persistAttemptLogs: () =>
            runtime.runPersistenceBackgroundWork(persist(logEntered, finishLog)),
          persistFailureUsage: persist(usageEntered, finishUsage),
          translatedBody: {},
          triedModels: new Set(),
          streamController: { handleError: noop },
        })
      )
    );
    void response.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      }
    );
    try {
      await logEntered.promise;
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(dispatched, true);
      assert.equal(settled, false, "the failed response must not orphan its direct usage");
      await usageEntered.promise;
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(settled, false, "the failed response must not orphan its usage");
      finishUsage.resolve();
      const result = await response;
      assert.equal(result.response.status, 502);
      assert.deepEqual(identities, [fixture.authority]);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      finishLog.resolve();
      await runtime.drainPersistenceAcceptedWork();
      assert.deepEqual(identities, [fixture.authority, fixture.authority]);
    } finally {
      finishLog.resolve();
      finishUsage.resolve();
      await response.catch(() => {});
      await Promise.allSettled(pending);
    }
  }));

test("a shutdown cancellation tick after pool teardown is harmless", () =>
  scenario(async () => {
    await runtime.closePersistenceRuntime();
    assert.doesNotThrow(() => runtime.cancelPersistenceAttempts());
  }));

async function scenario(body: () => Promise<void>) {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  try {
    await runtime.ensurePersistenceReady();
    await body();
  } finally {
    fixture.releaseBarriers();
    await runtime.closePersistenceRuntime();
  }
}
test.after(() => {
  hooks.deregister();
  rmSync(directory, { recursive: true, force: true });
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [key, value] of previous) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test(
  "timeout response does not retire the actual native preparation or cancellation cleanup",
  { timeout: 5000 },
  () =>
    scenario(async () => {
      const finish = barrier();
      let inner!: Promise<void>;
      let identity: object | undefined;
      let cancelled = false;
      const runner = buildTargetTimeoutRunner({
        comboTargetTimeoutMs: 10,
        log,
        handleSingleModel: async (_body, _model, target) => {
          inner = finish.promise.then(() => {
            identity = getSelectedPersistenceIdentity();
            cancelled = target?.modelAbortSignal?.aborted === true;
          });
          await inner;
          return new Response("late", { status: 200 });
        },
      });
      const response = await runtime.runPersistenceRequest(() => runner({}, "slow"));
      assert.equal(response.status, 504);
      let drained = false;
      const closing = runtime.drainPersistenceAcceptedWork().then(() => {
        drained = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(drained, false, "the winner must not release the pending loser");
      finish.resolve();
      await inner;
      await closing;
      assert.equal(identity, fixture.authority);
      assert.equal(cancelled, true);
    })
);

test(
  "cancelled preparation keeps cleanup authority but cannot dispatch with disabled upstream timeout",
  { timeout: 5000 },
  () =>
    scenario(async () => {
      const prepared = barrier();
      let dispatched = false,
        cleaned = false;
      const runner = buildTargetTimeoutRunner({
        comboTargetTimeoutMs: 10,
        log,
        handleSingleModel: async (_body, _model, target) => {
          await prepared.promise;
          try {
            return await executeWithUpstreamStartTimeout({
              executor: { getTimeoutMs: () => 0 },
              provider: "test",
              model: "test",
              signal: target!.modelAbortSignal!,
              execute: async () => {
                dispatched = true;
                return new Response("unexpected");
              },
            });
          } finally {
            assert.equal(getSelectedPersistenceIdentity(), fixture.authority);
            cleaned = true;
          }
        },
      });
      assert.equal((await runtime.runPersistenceRequest(() => runner({}, "test"))).status, 504);
      prepared.resolve();
      await runtime.drainPersistenceAcceptedWork();
      assert.equal(cleaned, true);
      assert.equal(dispatched, false);
    })
);

test(
  "speculative winner returns while the losing task retains independent cleanup ownership",
  { timeout: 5000 },
  () =>
    scenario(async () => {
      const loser = barrier();
      let cleaned = false;
      type Dispatch =
        typeof import("../../../open-sse/services/combo/comboAttemptLoop.ts").dispatchWithCooldownRetry;
      const loop = await controlledOwnerModule<{ dispatchWithCooldownRetry: Dispatch }>(
        new URL("../../../open-sse/services/combo/comboAttemptLoop.ts", import.meta.url),
        {},
        {
          Response,
          AbortController,
          setTimeout,
          clearTimeout,
          runPersistenceAttempt: runtime.runPersistenceAttempt,
          COMBO_LOOP_SAFETY_TIMEOUT_MS: 1000,
          isBodySmallEnoughToHedge: () => true,
          getStrategyTraits: () => ({ honorsFallbackOnlyTargets: false }),
          requestScopedReplayKey: (key: string) => key,
          resolveDelayMs: () => 1,
          hasIdenticalModelErrorStreak: () => false,
          finalizeComboTrace() {},
          finishComboTrace() {},
        },
        (source) => source.slice(source.indexOf("export async function dispatchWithCooldownRetry"))
      );
      const state = {
        orderedTargets: [{ modelStr: "slow" }, { modelStr: "fast" }],
        abortControllers: new Map<number, AbortController>(),
      };
      const options = {
        state,
        deps: {
          log,
          config: { hedging: true, zeroLatencyOptimizationsEnabled: true },
          body: {},
          strategy: "priority",
        },
        extra: {
          maxSetRetries: 0,
          comboTimeoutMs: 1000,
          comboStartTime: Date.now(),
          evaluateGates: async () => ({ kind: "execute" }),
          executeAttempt: async ({ index }: { index: number }) => {
            if (index === 1) return { ok: true, response: new Response("winner") };
            await loser.promise;
            assert.equal(getSelectedPersistenceIdentity(), fixture.authority);
            cleaned = true;
            return { ok: false };
          },
        },
      } as unknown as Parameters<Dispatch>[0];
      const result = await runtime.runPersistenceRequest(() =>
        loop.dispatchWithCooldownRetry(options)
      );
      assert.equal(await result.text(), "winner");
      assert.equal(cleaned, false);
      assert.equal(state.abortControllers.get(0)?.signal.aborted, true);
      assert.equal(runtime.getPersistenceRuntimeStatus().activeAttempts, 1);
      loser.resolve();
      await runtime.drainPersistenceAcceptedWork();
      assert.equal(cleaned, true);
    })
);

test("attempt admission is bounded and rejects a detached ended request", { timeout: 5000 }, () =>
  scenario(async () => {
    const finish = barrier();
    const work: Promise<unknown>[] = [];
    let stale!: AsyncResource;
    await runtime.runPersistenceRequest(async () => {
      stale = new AsyncResource("ended-native-request");
      for (let i = 0; i < 64; i++)
        work.push(runtime.runPersistenceAttempt(() => finish.promise, { independent: true }));
      await assert.rejects(
        runtime.runPersistenceAttempt(async () => {}, { independent: true }),
        runtime.isPersistenceStreamCapacityError
      );
    });
    finish.resolve();
    await Promise.all(work);
    await assert.rejects(
      stale.runInAsyncScope(() => runtime.runPersistenceAttempt(async () => {})),
      /ended|admission/
    );
    stale.emitDestroy();
    assert.equal(runtime.getPersistenceRuntimeStatus().activeAttempts, 0);
  })
);

test(
  "sixty nested raced attempts retain sixty credits and reuse prepared catalogs",
  { timeout: 5000 },
  () =>
    scenario(async () => {
      const finish = barrier(),
        started = barrier();
      let count = 0;
      const work = Array.from({ length: 60 }, () =>
        runtime.runPersistenceRequest(() =>
          runtime.runPersistenceAttempt(() =>
            runtime.runPersistenceAttempt(
              async () => {
                count++;
                if (count === 60) started.resolve();
                await finish.promise;
                assert.equal(getSelectedPersistenceIdentity(), fixture.authority);
              },
              { independent: true }
            )
          )
        )
      );
      await started.promise;
      assert.equal(runtime.getPersistenceRuntimeStatus().activeAttempts, 60);
      assert.equal(runtime.getPersistenceRuntimeStatus().attemptOperations, 120);
      assert.equal(fixture.generation, 60, "nested scopes reuse the request's catalog snapshot");
      finish.resolve();
      await Promise.all(work);
      assert.equal(runtime.getPersistenceRuntimeStatus().activeAttempts, 0);
    })
);

test(
  "a late rotating-token refresh retains CAS and persistence after its shared timeout",
  { timeout: 5000 },
  () =>
    scenario(async () => {
      const entered = barrier(),
        finish = barrier(),
        casDone = barrier();
      const identities: Array<object | undefined> = [];
      type Refresh = typeof import("../../../open-sse/services/tokenRefresh.ts");
      const module = await controlledOwnerModule<Pick<Refresh, "getAccessToken">>(
        new URL("../../../open-sse/services/tokenRefresh.ts", import.meta.url),
        {},
        {
          runPersistenceAttempt: runtime.runPersistenceAttempt,
          isApplicationPersistenceIdentity: runtime.isApplicationPersistenceIdentity,
          getSelectedPersistenceIdentity,
          connectionRefreshMutex: new Map(),
          refreshMutexMaxMs: 5,
          getActiveOnPersist: () => undefined,
          setTimeout,
          clearTimeout,
          _getAccessTokenWithStalenessCheck: async () => {
            entered.resolve();
            await finish.promise;
            return { accessToken: "rotated-access", refreshToken: "rotated-refresh" };
          },
          casGuardShouldSkipPersist: async () => {
            try {
              identities.push(getSelectedPersistenceIdentity());
              return false;
            } finally {
              casDone.resolve();
            }
          },
        },
        (source) => {
          const helper = source.indexOf("function runNativeRefreshWork");
          const start = source.indexOf("export async function getAccessToken(");
          const end = source.indexOf("async function _getAccessTokenWithStalenessCheck", start);
          assert.ok(start >= 0 && end > start);
          return (
            (helper >= 0 ? source.slice(helper, source.indexOf("/**", helper)) : "") +
            source.slice(start, end)
          );
        }
      );
      const keepAlive = setTimeout(() => {}, 1000);
      try {
        const response = runtime.runPersistenceRequest(() =>
          runtime.runPersistenceAttempt(() =>
            module.getAccessToken(
              "codex",
              { connectionId: "late-refresh", refreshToken: "original" },
              log,
              null,
              async () => {
                identities.push(getSelectedPersistenceIdentity());
              }
            )
          )
        );
        await entered.promise;
        assert.equal(await response, null, "the existing shared timeout remains bounded");
        let drained = false;
        const closing = runtime.closePersistenceRuntime().then(() => {
          drained = true;
        });
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(drained, false, "timeout must not hide the underlying refresh from drain");
        finish.resolve();
        await closing;
        assert.deepEqual(identities, [fixture.authority, fixture.authority]);
      } finally {
        clearTimeout(keepAlive);
        finish.resolve();
        await casDone.promise;
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    })
);

for (const compressed of [false, true]) {
  test(`Responses WS preparation joins ${compressed ? "compressed" : "skipped"} analytics`, () =>
    scenario(async () => {
      const entered = barrier(),
        finish = barrier();
      let identity: object | undefined;
      const work: Promise<unknown>[] = [];
      const write = () => {
        const pending = (async () => {
          entered.resolve();
          await finish.promise;
          identity = getSelectedPersistenceIdentity();
        })();
        void pending.catch(() => {});
        work.push(pending);
        return pending;
      };
      const module = await controlledOwnerModule<{
        persistAndRestore(...args: unknown[]): Promise<object>;
      }>(
        new URL("../../../src/app/api/internal/codex-responses-ws/compression.ts", import.meta.url),
        {},
        {
          writeCompressionSkip: write,
          writeCompressionAnalytics: write,
          log,
        },
        (source) =>
          source
            .slice(source.indexOf("async function persistAndRestore("))
            .replace(
              "async function persistAndRestore(",
              "export async function persistAndRestore("
            )
      );
      const original = { input: "original" },
        restored = { input: "compressed" };
      let settled = false;
      const response = runtime.runPersistenceRequest(() =>
        module.persistAndRestore(
          { stats: {}, compressed, body: restored },
          { restore: (body: object) => body },
          original,
          "lite",
          {},
          { provider: "test", model: "test", requestId: "compression-lifetime" }
        )
      );
      void response.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        }
      );
      try {
        await entered.promise;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(settled, false, "preparation must not abandon the writer's returned promise");
        finish.resolve();
        assert.deepEqual(await response, compressed ? restored : original);
        assert.equal(identity, fixture.authority);
      } finally {
        finish.resolve();
        await response.catch(() => {});
        await Promise.allSettled(work);
      }
    }));
}
