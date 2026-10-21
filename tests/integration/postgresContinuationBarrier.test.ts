import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { Pool, type PoolClient } from "pg";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { streamingNativeFixture } from "../helpers/persistence/streamingNativeIntegrationFixture.ts";
import { closeNativeStreamingFixture } from "../helpers/persistence/nativeStreamingFixtureShutdown.ts";
import { observeNativeStreamingResources } from "../helpers/persistence/nativeStreamingResourceObserver.ts";
import { continuationNativeFixture } from "../helpers/persistence/continuationNativeIntegrationFixture.ts";
import type { StreamCompletionClock } from "../../open-sse/handlers/chatCore/streamingCompletion.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
  PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";

const STREAM_CASE_COUNT = 10;

function persistenceFailureTag(error: unknown) {
  const knownKinds = new Set([
    "unavailable",
    "overloaded",
    "closed",
    "query_failed",
    "transaction_failed",
    "commit_unknown",
    "invalid_scope",
    "transaction_timeout",
  ]);
  const record =
    error && typeof error === "object"
      ? (error as { kind?: unknown; sqlState?: unknown; code?: unknown })
      : {};
  const kind =
    typeof record.kind === "string" && knownKinds.has(record.kind) ? record.kind : "unclassified";
  const code = record.sqlState ?? record.code;
  const sqlState = typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : null;
  return { kind, sqlState };
}

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
type ContinuationReadGate = {
  responseId: string;
  pauseAfter: "empty-ready-row" | "privacy-read" | "observe-only";
  entered: ReturnType<typeof barrier>;
  release: ReturnType<typeof barrier>;
  pauseBeforeSecondKeyPolicyLock?: boolean;
  freshPrivacyEntered?: ReturnType<typeof barrier>;
  releaseFreshPrivacy?: ReturnType<typeof barrier>;
  keyPolicySharedLocks: number;
  readyReads: number;
  privacyReads: number;
  firstReadyRowCount?: number;
};
function makeContinuationReadGate(
  responseId: string,
  pauseAfter: ContinuationReadGate["pauseAfter"]
): ContinuationReadGate {
  return {
    responseId,
    pauseAfter,
    entered: barrier(),
    release: barrier(),
    keyPolicySharedLocks: 0,
    readyReads: 0,
    privacyReads: 0,
  };
}
async function withDeadline<T>(work: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Native continuation oracle deadline exceeded")),
          timeoutMs
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
async function eventually(condition: () => Promise<boolean> | boolean, timeoutMs = 6000) {
  const deadline = performance.now() + timeoutMs;
  while (!(await condition())) {
    if (performance.now() >= deadline) throw new Error("Native stream oracle deadline exceeded");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

// Runs actual engine/runtime/domain owners and actual private HTTP, not Next route/auth acceptance.
// S3 transport is explicitly excluded by the in-memory artifact fixture.
test(
  "native HTTP continuation is available before EOF and artifact upload, with current privacy and cancellation",
  {
    skip: !postgresTestsEnabled,
    timeout: 45000,
  },
  async (t) => {
    const resources = observeNativeStreamingResources();
    t.after(() => resources.report(t));
    const tag = randomUUID().replaceAll("-", "");
    const model = `streaming-${tag}`;
    const faultModel = `${model}-fault`;
    const plugin = `streaming-oracle-${tag}`;
    const directory = mkdtempSync(join(tmpdir(), "omni-native-stream-"));
    const keysToRestore = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "OMNIROUTE_BUILDING",
      "NEXT_PHASE",
      "APP_LOG_TO_FILE",
    ];
    const previous = new Map(keysToRestore.map((key) => [key, process.env[key]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.JWT_SECRET = `private-stream-jwt-${tag}`;
    process.env.API_KEY_SECRET = `private-stream-api-key-${tag}`;
    process.env.DATA_DIR = directory;
    process.env.APP_LOG_TO_FILE = "false";
    delete process.env.OMNIROUTE_BUILDING;
    delete process.env.NEXT_PHASE;
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const contextUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/controlPlaneContext.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/continuationNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const probeUrl = new URL("../helpers/persistence/streamingSqliteGuardProbe.ts", import.meta.url)
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
        ) {
          return { url: fixtureUrl, shortCircuit: true };
        }
        const resolved = nextResolve(specifier, context);
        if (resolved.url === contextUrl && context.parentURL !== probeUrl)
          return { url: probeUrl, shortCircuit: true };
        return resolved;
      },
    });
    // Observe genuine node-pg leases. No query or transaction result is substituted.
    const connect = Pool.prototype.connect;
    let observedPool: Pool | undefined;
    const diagnosedClients = new WeakSet<PoolClient>();
    let fixtureStage = "setup";
    let primaryFailure: { error: unknown } | undefined;
    const fixtureStartedAt = performance.now();
    type GatewayPhase =
      | "client_started"
      | "gateway_entered"
      | "tail_import_started"
      | "tail_import_ready"
      | "helper_imports_ready"
      | "request_scope_entered"
      | "logger_ready"
      | "upstream_fetch_started"
      | "upstream_headers_received"
      | "stream_tail_ready"
      | "gateway_ready"
      | "client_headers_received"
      | "client_terminal_received"
      | "client_body_finished";
    const phaseHistory = new Map<number, { phase: GatewayPhase; elapsedMs: number }[]>();
    const firstFailures = new Map<
      string,
      {
        owner: "client" | "handler";
        index: number | null;
        phase: GatewayPhase | null;
        elapsedMs: number;
        name: string;
        kind: string;
        sqlState: string | null;
      }
    >();
    let phaseHistoryTruncated = false;
    let bodyFailureElapsedMs: number | undefined;
    const elapsed = () => Math.round((performance.now() - fixtureStartedAt) * 1000) / 1000;
    const recordPhase = (index: number, phase: GatewayPhase) => {
      if (!Number.isInteger(index) || index < 0 || index >= STREAM_CASE_COUNT) return;
      const history = phaseHistory.get(index) ?? [];
      if (history.length >= 16) {
        phaseHistoryTruncated = true;
        return;
      }
      history.push({ phase, elapsedMs: elapsed() });
      phaseHistory.set(index, history);
    };
    const recordFailure = (owner: "client" | "handler", index: number | null, error: unknown) => {
      const key = `${owner}:${index ?? "none"}`;
      if (firstFailures.has(key)) return;
      const allowedNames = new Set([
        "Error",
        "TypeError",
        "AbortError",
        "TimeoutError",
        "AssertionError",
        "AggregateError",
      ]);
      const name = error instanceof Error && allowedNames.has(error.name) ? error.name : "other";
      firstFailures.set(key, {
        owner,
        index,
        phase: index === null ? null : (phaseHistory.get(index)?.at(-1)?.phase ?? null),
        elapsedMs: elapsed(),
        name,
        ...persistenceFailureTag(error),
      });
    };
    t.after(() => {
      if (primaryFailure)
        t.diagnostic(
          `Native fixture phase failure: ${JSON.stringify({
            bodyFailureElapsedMs,
            phaseHistory: [...phaseHistory.entries()].map(([index, phases]) => ({ index, phases })),
            firstFailures: [...firstFailures.values()],
            phaseHistoryTruncated,
            maxPhasesPerIndex: 16,
            maxIndices: STREAM_CASE_COUNT,
          })}`
        );
    });
    const diagnosePersistenceFailure = (owner: string, error: unknown) => {
      t.diagnostic(
        `Fixture persistence failure: ${JSON.stringify({
          owner,
          stage: fixtureStage,
          ...persistenceFailureTag(error),
        })}`
      );
    };
    let active = 0,
      peak = 0,
      tlsLeases = 0,
      totalLeases = 0;
    t.mock.method(Pool.prototype, "connect", function (this: Pool) {
      observedPool ??= this;
      const connection = (Reflect.apply(connect, this, []) as Promise<PoolClient>).then(
        (client) => {
          if (!diagnosedClients.has(client)) {
            diagnosedClients.add(client);
            const owner = this === observedPool ? "runtime-client" : "verification-client";
            client.on("error", (error: unknown) => diagnosePersistenceFailure(owner, error));
          }
          return client;
        }
      );
      if (this !== observedPool) return connection;
      return connection.then((client) => {
        active++;
        totalLeases++;
        peak = Math.max(peak, active);
        const transport = client as unknown as { connection: { stream: { encrypted?: boolean } } };
        if (transport.connection.stream.encrypted === true) tlsLeases++;
        const release = client.release.bind(client);
        let released = false;
        client.release = (destroy) => {
          if (!released) {
            released = true;
            active--;
          }
          release(destroy);
        };
        return client;
      });
    });
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let verification: PostgresDatabase | undefined;
    let apiKeys: typeof import("../../src/lib/db/apiKeys.ts") | undefined;
    let providers: typeof import("../../src/lib/db/providers.ts") | undefined;
    let pricing: typeof import("../../src/lib/db/settings/pricing.ts") | undefined;
    let pluginHooks: typeof import("../../src/lib/plugins/hooks.ts") | undefined;
    const keyIds: string[] = [];
    let connectionId: string | undefined;
    let mainKey = "",
      faultKey = "",
      deletedKey = "",
      incompleteKey = "",
      completedKey = "",
      freshPrivacyKey = "",
      freshDeleteKey = "";
    const terminals = Array.from({ length: STREAM_CASE_COUNT }, barrier);
    const upstreamEofs = Array.from({ length: STREAM_CASE_COUNT }, barrier);
    const preparationEntered = Array.from({ length: STREAM_CASE_COUNT }, barrier);
    const preparations = new Map<number, boolean>();
    const principals = new Map<number, string>();
    const readyIndices = new Set<number>();
    const optional = barrier(),
      policyLocked = barrier(),
      releasePolicy = barrier(),
      cancelReadLocked = barrier(),
      releaseCancelReadPolicy = barrier();
    let policyHold: Promise<void> | undefined;
    let cancelReadPolicyHold: Promise<void> | undefined;
    let originalPipelineSetting: string | undefined;
    let pipelineSettingCaptured = false;
    const clocks = new Map<number, StreamCompletionClock>();
    const starts = new Map<number, number>();
    const terminalSeen = new Set<number>();
    const abortedIndices = new Set<number>();
    const upstreamCancelled = new Set<number>();
    const pipelineControllers = new Map<
      number,
      ReturnType<typeof import("../../open-sse/utils/streamHandler.ts").createStreamController>
    >();
    const clientControllers = Array.from(
      { length: STREAM_CASE_COUNT },
      () => new AbortController()
    );
    const clients: Promise<string>[] = [];
    const handlerWork = new Set<Promise<void>>();
    const continuationReaders: Promise<unknown>[] = [];
    let ready = 0,
      optionalStarted = 0;
    const optionalRequestIds = new Set<string>();
    let continuationReadGate: ContinuationReadGate | undefined;
    const originalFetch = globalThis.fetch;
    let baseUrl = "";
    let shutdown: Promise<void> | undefined;
    const server = createServer((incoming, outgoing) => {
      const work = handle(incoming, outgoing);
      handlerWork.add(work);
      void work
        .catch((error) => {
          const candidateIndex = Number((incoming.url ?? "").split("/").at(-1));
          const index =
            Number.isInteger(candidateIndex) &&
            candidateIndex >= 0 &&
            candidateIndex < STREAM_CASE_COUNT
              ? candidateIndex
              : null;
          recordFailure("handler", index, error);
          return outgoing.destroy();
        })
        .finally(() => handlerWork.delete(work));
    });
    async function handle(incoming: IncomingMessage, outgoing: ServerResponse): Promise<void> {
      const path = incoming.url ?? "";
      if (path.startsWith("/continue/")) {
        const index = Number(path.split("/").at(-1));
        let input = "";
        for await (const chunk of incoming) input += chunk;
        const {
          previous_response_id: previousId,
          mutate,
          holdHandoff,
        } = JSON.parse(input) as {
          previous_response_id: string;
          mutate?: boolean;
          holdHandoff?: boolean;
        };
        // Independent module instance models separate Next server bundles in one process.
        const { resolvePreviousResponseStateAsync } = await import(
          new URL(
            "../../src/lib/db/responsesContinuationStore.ts?continuation-consumer",
            import.meta.url
          ).href
        );
        const originalStore = await import("../../src/lib/db/responsesContinuationStore.ts");
        assert.notEqual(
          resolvePreviousResponseStateAsync,
          originalStore.resolvePreviousResponseStateAsync
        );
        const state = await runtime!.runPersistenceRequest(async () => {
          const apiKeyId = principals.get(index);
          if (!holdHandoff || continuationReadGate?.responseId !== previousId)
            return resolvePreviousResponseStateAsync(previousId, apiKeyId);
          const { getSelectedControlPlane, withPostgresControlPlane } =
            await import("../../src/lib/db/controlPlaneContext.ts");
          const binding = getSelectedControlPlane();
          assert.ok(binding?.database);
          const gate = continuationReadGate;
          // Wrap real transaction queries and pause only after their actual result returns.
          const intercepted = new Proxy(binding.database, {
            get(target, property, receiver) {
              if (property !== "transaction") return Reflect.get(target, property, receiver);
              return <T>(
                body: (transaction: PostgresTransaction) => Promise<T>,
                options?: PostgresTransactionOptions
              ) =>
                target.transaction(
                  (transaction) =>
                    body({
                      noteAdvisoryLock: (owner, mode) =>
                        transaction.noteAdvisoryLock?.(owner, mode),
                      query: async <Row = Record<string, unknown>>(
                        sql: string,
                        values?: unknown[]
                      ) => {
                        const isCurrentGate =
                          continuationReadGate === gate && previousId === gate.responseId;
                        const isSharedKeyPolicyLock =
                          sql.includes("pg_advisory_xact_lock_shared(18765,$1)") &&
                          values?.[0] === 5;
                        if (isCurrentGate && isSharedKeyPolicyLock) {
                          gate.keyPolicySharedLocks++;
                          if (
                            gate.pauseBeforeSecondKeyPolicyLock &&
                            gate.keyPolicySharedLocks === 2
                          ) {
                            gate.freshPrivacyEntered?.resolve();
                            await gate.releaseFreshPrivacy?.promise;
                          }
                        }
                        const result = await transaction.query<Row>(sql, values);
                        if (!isCurrentGate) return result;
                        const isReadyLookup =
                          sql.includes("FROM omniroute.call_logs") &&
                          sql.includes("detail_state='ready'") &&
                          values?.[0] === previousId;
                        if (isReadyLookup) {
                          gate.readyReads++;
                          if (gate.readyReads === 1) gate.firstReadyRowCount = result.rows.length;
                          if (gate.pauseAfter === "empty-ready-row" && gate.readyReads === 1) {
                            gate.entered.resolve();
                            await gate.release.promise;
                          }
                        }
                        const isPrivacyRead =
                          sql.includes("SELECT no_log FROM omniroute.api_keys") &&
                          values?.[0] === apiKeyId;
                        if (isPrivacyRead) {
                          gate.privacyReads++;
                          if (gate.pauseAfter === "privacy-read" && gate.privacyReads === 1) {
                            gate.entered.resolve();
                            await gate.release.promise;
                          }
                        }
                        return result;
                      },
                    }),
                  options
                );
            },
          }) as PostgresDatabase;
          return withPostgresControlPlane({ ...binding, database: intercepted }, () =>
            resolvePreviousResponseStateAsync(previousId, apiKeyId)
          );
        });
        if (mutate && state) {
          (state.input[0] as { content: string }).content = "changed-by-reader";
          const item = state.output[0] as { content: { text: string }[] };
          item.content[0].text = "changed-by-reader";
          state.output.push({ type: "message", role: "assistant", content: [] });
        }
        outgoing.setHeader("Content-Type", "application/json");
        outgoing.end(
          JSON.stringify({
            found: state !== null,
            input: state?.input,
            output: state?.output,
            handoff:
              holdHandoff && continuationReadGate?.responseId === previousId
                ? {
                    readyReads: continuationReadGate.readyReads,
                    privacyReads: continuationReadGate.privacyReads,
                    keyPolicySharedLocks: continuationReadGate.keyPolicySharedLocks,
                    firstReadyRowCount: continuationReadGate.firstReadyRowCount,
                  }
                : undefined,
          })
        );
        return;
      }
      if (path.startsWith("/optional")) {
        optionalStarted++;
        await optional.promise;
        if (!outgoing.destroyed && !outgoing.writableEnded) outgoing.end("released");
        return;
      }
      const index = Number(path.split("/").at(-1));
      assert.ok(Number.isInteger(index) && index >= 0 && index < STREAM_CASE_COUNT);
      const responseId = `resp_${tag}_${index}`;
      const selectedModel = index === 6 ? faultModel : model;
      if (path.startsWith("/upstream/")) {
        outgoing.once("close", () => {
          if (!outgoing.writableEnded) upstreamCancelled.add(index);
        });
        outgoing.writeHead(200, { "Content-Type": "text/event-stream" });
        const send = (type: string, payload: Record<string, unknown>) =>
          outgoing.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...payload })}\n\n`);
        send("response.created", {
          response: {
            id: responseId,
            object: "response",
            status: "in_progress",
            model: selectedModel,
            output: [],
          },
        });
        await terminals[index].promise;
        const item = {
          id: `msg_${tag}_${index}`,
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text: `answer-${index}`, annotations: [] }],
        };
        send("response.output_item.added", { output_index: 0, item });
        send("response.output_text.delta", {
          item_id: item.id,
          output_index: 0,
          content_index: 0,
          delta: `answer-${index}`,
        });
        send("response.output_item.done", { output_index: 0, item });
        send(index === 2 ? "response.incomplete" : "response.completed", {
          response: {
            id: responseId,
            object: "response",
            status: index === 2 ? "incomplete" : "completed",
            ...(index === 2 ? { incomplete_details: { reason: "max_output_tokens" } } : {}),
            model: selectedModel,
            service_tier: "standard",
            output: [item],
            usage: { input_tokens: 100 + index, output_tokens: 10, total_tokens: 110 + index },
          },
        });
        await upstreamEofs[index].promise;
        outgoing.end();
        return;
      }
      assert.ok(path.startsWith("/gateway/"));
      recordPhase(index, "gateway_entered");
      const clientAbort = new AbortController();
      outgoing.once("close", () => {
        if (!outgoing.writableEnded) {
          clientAbort.abort();
          abortedIndices.add(index);
        }
      });
      recordPhase(index, "tail_import_started");
      const { runStreamingTail } =
        await import("../../open-sse/handlers/chatCore/streamingTail.ts");
      recordPhase(index, "tail_import_ready");
      const { createRequestLogger } = await import("../../open-sse/utils/requestLogger.ts");
      const { createStreamController } = await import("../../open-sse/utils/streamHandler.ts");
      const { persistAttemptLogs } =
        await import("../../open-sse/handlers/chatCore/attemptLogging.ts");
      const { buildFailureUsageRecord } =
        await import("../../open-sse/handlers/chatCore/failureUsage.ts");
      const { saveRequestUsage, trackPendingRequest } =
        await import("../../src/lib/usage/usageHistory.ts");
      const { resolveReportedServiceTier } =
        await import("../../open-sse/handlers/chatCore/serviceTier.ts");
      recordPhase(index, "helper_imports_ready");
      const apiKeyId = principals.get(index)!;
      let carry: Awaited<ReturnType<typeof runStreamingTail>>["carry"] | undefined;
      const outcome = await runtime!.runPersistenceRequest(async () => {
        recordPhase(index, "request_scope_entered");
        const startTime = Date.now();
        starts.set(index, startTime);
        const body = {
          model: selectedModel,
          stream: true,
          input: [{ role: "user", content: `question-${index}` }],
        };
        const request = new Request(`${baseUrl}/v1/responses`, {
          method: "POST",
          body: JSON.stringify(body),
          signal: clientAbort.signal,
        });
        const logger = await createRequestLogger(
          "openai-responses",
          "openai-responses",
          selectedModel,
          { enabled: true, captureStreamChunks: false }
        );
        recordPhase(index, "logger_ready");
        logger.logClientRawRequest("/v1/responses", body, request.headers, body.input);
        const pendingRequestId = trackPendingRequest(selectedModel, "openai", connectionId!, true)!;
        const streamController = createStreamController({
          provider: "openai",
          model: selectedModel,
          connectionId,
          pendingRequestId,
          clientResponseFormat: "openai-responses",
          clientAbortSignal: clientAbort.signal,
          onDisconnect: (event) => carry?.onClientDisconnectFinalize?.(event) ?? false,
          onError: (event) => carry?.onPipelineStreamError?.(event) ?? false,
        });
        pipelineControllers.set(index, streamController);
        recordPhase(index, "upstream_fetch_started");
        const providerResponse = await fetch(`${baseUrl}/upstream/${index}`, {
          signal: streamController.signal,
        });
        recordPhase(index, "upstream_headers_received");
        const traceId = `${tag}-${index}`;
        const context = {
          traceId,
          provider: "openai",
          connectionId,
          model: selectedModel,
          skillRequestId: traceId,
          detailedLoggingEnabled: true,
          reqLogger: { getPipelinePayloads: () => logger.getPipelinePayloads() ?? undefined },
          pendingRequestId,
          clientRawRequest: { endpoint: "/v1/responses" },
          requestedModel: selectedModel,
          credentials: { connectionId },
          startTime,
          body,
          sourceFormat: "openai-responses",
          targetFormat: "openai-responses",
          comboName: null,
          comboStepId: null,
          comboExecutionKey: null,
          tokensCompressed: 0,
          apiKeyInfo: { id: apiKeyId, name: "private-stream-oracle" },
          noLogEnabled: false,
          correlationId: traceId,
        };
        const tail = await runStreamingTail({
          agentGoalPolicy: { detected: false },
          apiKeyInfo: context.apiKeyInfo,
          attachCompressionUsageReceiptAfterAnalytics: () => {},
          body,
          bodyForCacheWrite: body,
          calibrationEstimatedInputTokens: 100 + index,
          clientRawRequest: request,
          clientResponseFormat: "openai-responses",
          comboStrategy: null,
          connectionId,
          contextEditingEnabled: false,
          credentials: context.credentials,
          currentModel: selectedModel,
          effectiveModel: selectedModel,
          effectiveServiceTier: "standard",
          endpointPath: "/v1/responses",
          executeProviderRequest: async () => {
            throw new Error("Unexpected fixture retry");
          },
          executor: { getTimeoutMs: () => 10000 },
          fallbackAttempts: 0,
          getCurrentConnectionId: () => connectionId,
          isCombo: false,
          isDroidCLI: false,
          isResponsesEndpoint: true,
          memorySettings: { enabled: false },
          model: selectedModel,
          modelInfo: { model: selectedModel },
          pendingConnId: connectionId,
          pendingRequestId,
          persistAttemptLogs: (args: Parameters<typeof persistAttemptLogs>[0]) => {
            if (args.completionClock) clocks.set(index, args.completionClock);
            return persistAttemptLogs(args, context);
          },
          persistFailureUsage: (
            status: number,
            code?: string,
            aggregate?: Parameters<typeof buildFailureUsageRecord>[0]["aggregate"],
            clock?: StreamCompletionClock
          ) =>
            saveRequestUsage(
              buildFailureUsageRecord({
                provider: "openai",
                model: selectedModel,
                connectionId,
                apiKeyInfo: context.apiKeyInfo,
                effectiveServiceTier: "standard",
                isCombo: false,
                comboStrategy: null,
                statusCode: status,
                errorCode: code,
                latencyMs: clock?.latencyMs ?? Date.now() - startTime,
                timestamp: clock?.timestamp,
                aggregate,
              })
            ),
          prepareStreamContinuation: async (payload: unknown, signal: AbortSignal) => {
            preparationEntered[index].resolve();
            const { prepareResponsesContinuationBeforeTerminal } =
              await import("../../src/lib/usage/prepareResponsesContinuation.ts");
            const seed = await prepareResponsesContinuationBeforeTerminal(
              payload,
              {
                apiKeyId,
                noLog: false,
                detailedLoggingEnabled: true,
                videoContentRemoved: false,
                getPipelinePayloads: () => logger.getPipelinePayloads(),
              },
              signal
            );
            preparations.set(index, seed !== undefined);
            return seed;
          },
          pipelineSessionId: traceId,
          provider: "openai",
          providerHeaders: providerResponse.headers,
          providerRequestCapture: { body() {} },
          providerUrl: `${baseUrl}/upstream/${index}`,
          providerResponse,
          reasoningReplayHistory: [],
          releaseTurnExecution() {},
          reqLogger: logger,
          resolveReportedServiceTier: (payload: unknown) =>
            resolveReportedServiceTier("openai", payload),
          semanticCacheEnabled: false,
          skillRequestId: traceId,
          startTime,
          stream: true,
          streamController,
          targetFormat: "openai-responses",
          traceId,
          translatedBody: body,
          finalBody: body,
          videoBridgeObserved: false,
        });
        recordPhase(index, "stream_tail_ready");
        carry = tail.carry;
        return tail.result;
      });
      assert.equal(outcome.success, true);
      assert.ok(outcome.response);
      outgoing.writeHead(outcome.response.status, Object.fromEntries(outcome.response.headers));
      outgoing.flushHeaders();
      ready++;
      readyIndices.add(index);
      recordPhase(index, "gateway_ready");
      const reader = outcome.response.body!.getReader();
      try {
        while (!outgoing.destroyed) {
          const chunk = await reader.read();
          if (chunk.done) break;
          outgoing.write(chunk.value);
        }
        if (!outgoing.destroyed) outgoing.end();
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
    }
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const configuration =
        await import("../helpers/persistence/streamingNativeIntegrationFixture.ts").then((module) =>
          module.readPostgresEnvironment()
        );
      assert.equal(configuration.maxConnections, 16);
      assert.equal(configuration.maxQueuedOperations, 256);
      verification = await createTestPostgresDatabase();
      apiKeys = await import("../../src/lib/db/apiKeys.ts");
      providers = await import("../../src/lib/db/providers.ts");
      pricing = await import("../../src/lib/db/settings/pricing.ts");
      pluginHooks = await import("../../src/lib/plugins/hooks.ts");
      await runtime.runPersistenceRequest(async () => {
        const key = await apiKeys!.createApiKey(`stream-main-${tag}`, "private-stream-fixture");
        mainKey = String(key.id);
        keyIds.push(mainKey);
        const fault = await apiKeys!.createApiKey(`stream-fault-${tag}`, "private-stream-fixture");
        faultKey = String(fault.id);
        keyIds.push(faultKey);
        const deleted = await apiKeys!.createApiKey(
          `stream-delete-${tag}`,
          "private-stream-fixture"
        );
        deletedKey = String(deleted.id);
        keyIds.push(deletedKey);
        const incomplete = await apiKeys!.createApiKey(
          `stream-incomplete-${tag}`,
          "private-stream-fixture"
        );
        incompleteKey = String(incomplete.id);
        keyIds.push(incompleteKey);
        const completed = await apiKeys!.createApiKey(
          `stream-completed-${tag}`,
          "private-stream-fixture"
        );
        completedKey = String(completed.id);
        keyIds.push(completedKey);
        const freshPrivacy = await apiKeys!.createApiKey(
          `stream-fresh-privacy-${tag}`,
          "private-stream-fixture"
        );
        freshPrivacyKey = String(freshPrivacy.id);
        keyIds.push(freshPrivacyKey);
        const freshDelete = await apiKeys!.createApiKey(
          `stream-fresh-delete-${tag}`,
          "private-stream-fixture"
        );
        freshDeleteKey = String(freshDelete.id);
        keyIds.push(freshDeleteKey);
        principals.set(0, mainKey);
        principals.set(1, deletedKey);
        principals.set(2, incompleteKey);
        principals.set(3, mainKey);
        principals.set(4, mainKey);
        principals.set(5, completedKey);
        principals.set(6, faultKey);
        principals.set(7, mainKey);
        principals.set(8, freshPrivacyKey);
        principals.set(9, freshDeleteKey);
        const connection = await providers!.createProviderConnection({
          provider: "openai",
          name: `stream-${tag}`,
          authType: "apikey",
          apiKey: "fixture-only-credential",
          isActive: true,
        });
        connectionId = String(connection.id);
        await pricing!.updatePricing({
          openai: { [model]: { input: 1, output: 2 }, [faultModel]: { input: 1, output: 2 } },
        });
      });
      originalPipelineSetting = (
        await verification.transaction((tx) =>
          tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='call_log_pipeline_enabled'"
          )
        )
      ).rows[0]?.value;
      pipelineSettingCaptured = true;
      await runtime.runPersistenceRequest(async () => {
        const { updateSettings } = await import("../../src/lib/db/settings.ts");
        await updateSettings({ call_log_pipeline_enabled: true });
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      baseUrl = `http://127.0.0.1:${address.port}`;
      globalThis.fetch = (input, options) => {
        const url = new URL(
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url
        );
        if (url.origin !== baseUrl)
          return Promise.reject(new Error("External HTTP is outside the private stream oracle"));
        return originalFetch(input, options);
      };
      await runtime.runPersistenceRequest(() =>
        pluginHooks!.registerHook("onStreamComplete", plugin, async (payload) => {
          const data = payload as { model?: string; requestId?: string };
          if (data.model === model) {
            // Accounting owners, including call-log publication and seed retirement, run first.
            if (typeof data.requestId === "string") optionalRequestIds.add(data.requestId);
            await fetch(`${baseUrl}/optional`, { signal: AbortSignal.timeout(12000) });
          }
        })
      );
      async function client(index: number): Promise<string> {
        const response = await fetch(`${baseUrl}/gateway/${index}`, {
          signal: clientControllers[index].signal,
        });
        assert.equal(response.status, 200);
        recordPhase(index, "client_headers_received");
        const reader = response.body!.getReader();
        let text = "";
        try {
          while (true) {
            const chunk = await reader.read();
            if (chunk.done) break;
            text += new TextDecoder().decode(chunk.value);
            if (text.includes("response.completed") || text.includes("response.incomplete")) {
              if (!terminalSeen.has(index)) recordPhase(index, "client_terminal_received");
              terminalSeen.add(index);
            }
          }
          assert.ok(text.includes(`resp_${tag}_${index}`));
          recordPhase(index, "client_body_finished");
          return "complete";
        } catch (error) {
          if (clientControllers[index].signal.aborted) return "cancelled";
          throw error;
        } finally {
          reader.releaseLock();
        }
      }
      async function continueFrom(
        index: number,
        previousId = `resp_${tag}_${index}`,
        mutate = false,
        holdHandoff = false
      ) {
        const response = await fetch(`${baseUrl}/continue/${index}`, {
          method: "POST",
          body: JSON.stringify({ previous_response_id: previousId, mutate, holdHandoff }),
          headers: { "Content-Type": "application/json" },
        });
        assert.equal(response.status, 200);
        return response.json() as Promise<{
          found: boolean;
          input?: unknown[];
          output?: unknown[];
          handoff?: {
            readyReads: number;
            privacyReads: number;
            keyPolicySharedLocks: number;
            firstReadyRowCount?: number;
          };
        }>;
      }
      function observeContinuationReader<T>(reader: Promise<T>): Promise<T> {
        continuationReaders.push(reader);
        // Attach immediately to prevent a barrier-time rejection becoming unhandled.
        // The original promise is still awaited normally at each oracle.
        void reader.catch(() => {});
        return reader;
      }
      function startClient(index: number) {
        recordPhase(index, "client_started");
        const work = client(index);
        clients.push(work);
        void work.catch((error) => recordFailure("client", index, error));
        return work;
      }
      async function assertFreshPrivacyFallbackDenial(
        index: number,
        apiKeyId: string,
        label: string,
        commitPolicyMutation: () => Promise<void>
      ) {
        fixtureStage = `index${index}_fresh_privacy_fallback`;
        startClient(index);
        await eventually(() => readyIndices.has(index));
        terminals[index].resolve();
        await eventually(() => terminalSeen.has(index));
        assert.equal(preparations.get(index), true);
        const responseId = `resp_${tag}_${index}`;
        const gate = makeContinuationReadGate(responseId, "privacy-read");
        gate.pauseBeforeSecondKeyPolicyLock = true;
        gate.freshPrivacyEntered = barrier();
        gate.releaseFreshPrivacy = barrier();
        continuationReadGate = gate;
        const reader = observeContinuationReader(continueFrom(index, responseId, false, true));
        await withDeadline(gate.entered.promise, 6000);
        assert.equal(gate.firstReadyRowCount, 0);
        assert.equal(gate.readyReads, 1);
        assert.equal(gate.privacyReads, 1);
        assert.equal(gate.keyPolicySharedLocks, 1);
        upstreamEofs[index].resolve();
        await eventually(
          async () =>
            (
              await verification!.transaction((tx) =>
                tx.query<{ detail_state: string }>(
                  "SELECT detail_state FROM omniroute.call_logs WHERE response_id=$1",
                  [responseId]
                )
              )
            ).rows[0]?.detail_state === "ready"
        );
        await eventually(() => optionalRequestIds.has(`${tag}-${index}`));
        gate.release.resolve();
        assert.ok(gate.freshPrivacyEntered);
        await withDeadline(gate.freshPrivacyEntered.promise, 6000);
        assert.equal(gate.readyReads, 2);
        assert.equal(gate.privacyReads, 1);
        assert.equal(gate.keyPolicySharedLocks, 2);
        const publishedArtifact = await verification!.transaction((tx) =>
          tx.query<{
            api_key_id: string | null;
            artifact_relpath: string | null;
            detail_state: string;
          }>(
            "SELECT api_key_id,artifact_relpath,detail_state FROM omniroute.call_logs WHERE response_id=$1",
            [responseId]
          )
        );
        assert.equal(publishedArtifact.rows[0]?.api_key_id, apiKeyId);
        assert.equal(publishedArtifact.rows[0]?.detail_state, "ready");
        const publishedReference = publishedArtifact.rows[0]?.artifact_relpath;
        assert.ok(publishedReference);
        assert.ok(continuationNativeFixture.references.has(publishedReference));
        await commitPolicyMutation();
        assert.ok(gate.releaseFreshPrivacy);
        gate.releaseFreshPrivacy.resolve();
        const result = await withDeadline(reader, 6000);
        assert.equal(result.found, false, `${label} denies fallback artifact after artifact IO`);
        assert.deepEqual(result.handoff, {
          readyReads: 2,
          privacyReads: 2,
          keyPolicySharedLocks: 2,
          firstReadyRowCount: 0,
        });
        continuationReadGate = undefined;
      }
      startClient(0);
      await eventually(() => ready === 1);
      terminals[0].resolve();
      await eventually(() => terminalSeen.has(0));
      assert.equal(preparations.get(0), true);
      const early = await continueFrom(0);
      assert.equal(early.found, true);
      assert.equal(early.input?.length, 1);
      assert.equal(early.output?.length, 1);
      const changed = await continueFrom(0, `resp_${tag}_0`, true);
      assert.equal(changed.output?.length, 2);
      const reread = await continueFrom(0);
      assert.equal((reread.input?.[0] as { content: string }).content, "question-0");
      assert.equal(reread.output?.length, 1);
      assert.equal(
        (reread.output?.[0] as { content: { text: string }[] }).content[0].text,
        "answer-0"
      );
      const noWitnessGate = {
        responseId: `resp_${tag}_0`,
        pauseAfter: "observe-only" as const,
        entered: barrier(),
        release: barrier(),
        keyPolicySharedLocks: 0,
        readyReads: 0,
        privacyReads: 0,
        firstReadyRowCount: undefined as number | undefined,
      };
      continuationReadGate = noWitnessGate;
      const wrongPrincipal = await continueFrom(6, noWitnessGate.responseId, false, true);
      assert.equal(wrongPrincipal.found, false);
      assert.deepEqual(wrongPrincipal.handoff, {
        readyReads: 1,
        privacyReads: 1,
        keyPolicySharedLocks: 1,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;
      assert.equal((await continueFrom(0)).found, true);
      assert.equal(clocks.size, 0);
      assert.equal(continuationNativeFixture.putCalls, 0);
      assert.equal(optionalStarted, 0);
      const beforeEof = await verification.transaction(async (tx) => ({
        usage: (
          await tx.query("SELECT id FROM omniroute.usage_history WHERE api_key_id=$1", [mainKey])
        ).rowCount,
        logs: (await tx.query("SELECT id FROM omniroute.call_logs WHERE api_key_id=$1", [mainKey]))
          .rowCount,
      }));
      assert.deepEqual(beforeEof, { usage: 0, logs: 0 });

      startClient(2);
      await eventually(() => readyIndices.has(2));
      terminals[2].resolve();
      await eventually(() => terminalSeen.has(2));
      assert.equal(preparations.get(2), true);
      const partial = await continueFrom(2);
      assert.equal(partial.found, true);
      assert.equal((partial.input?.[0] as { content: string }).content, "question-2");
      assert.equal(
        (partial.output?.[0] as { content: { text: string }[] }).content[0].text,
        "answer-2"
      );
      assert.equal(clocks.has(2), false);
      assert.equal(continuationNativeFixture.putCalls, 0);
      assert.equal(optionalStarted, 0);
      const partialBeforeEof = await verification.transaction(async (tx) => ({
        usage: (
          await tx.query("SELECT id FROM omniroute.usage_history WHERE api_key_id=$1", [
            incompleteKey,
          ])
        ).rowCount,
        logs: (
          await tx.query("SELECT id FROM omniroute.call_logs WHERE api_key_id=$1", [incompleteKey])
        ).rowCount,
      }));
      assert.deepEqual(partialBeforeEof, { usage: 0, logs: 0 });
      await runtime.runPersistenceRequest(() =>
        apiKeys!.updateApiKeyPermissions(incompleteKey, { noLog: true })
      );
      assert.equal((await continueFrom(2)).found, false);

      upstreamEofs[0].resolve();
      await withDeadline(continuationNativeFixture.putEntered, 6000);
      assert.equal((await continueFrom(0)).found, true);
      assert.equal(optionalStarted, 0);

      startClient(6);
      await eventually(() => readyIndices.has(6));
      await runtime.runPersistenceRequest(() =>
        apiKeys!.updateApiKeyPermissions(faultKey, { noLog: true })
      );
      terminals[6].resolve();
      await eventually(() => terminalSeen.has(6));
      assert.equal(preparations.get(6), false);
      assert.equal((await continueFrom(6)).found, false);

      startClient(1);
      await eventually(() => readyIndices.has(1));
      await runtime.runPersistenceRequest(() => apiKeys!.deleteApiKey(deletedKey));
      terminals[1].resolve();
      await eventually(() => terminalSeen.has(1));
      assert.equal(preparations.get(1), false);
      assert.equal((await continueFrom(1)).found, false);

      await runtime.runPersistenceRequest(() =>
        apiKeys!.updateApiKeyPermissions(mainKey, { noLog: true })
      );
      assert.equal((await continueFrom(0)).found, false);
      await runtime.runPersistenceRequest(() =>
        apiKeys!.updateApiKeyPermissions(mainKey, { noLog: false })
      );
      assert.equal(
        (await continueFrom(0)).found,
        false,
        "denied pending seed does not reappear after re-enable"
      );
      continuationNativeFixture.releasePut();
      await eventually(() => optionalStarted >= 1);
      assert.equal(
        (await continueFrom(0)).found,
        false,
        "revoked pending content cannot reappear after no-log is disabled"
      );

      const revokedIntent = await verification.transaction((tx) =>
        tx.query<{
          publication_state: string;
          staged_payload: string | null;
          artifact_relpath: string | null;
        }>(
          `SELECT q.publication_state,q.staged_payload,c.artifact_relpath FROM omniroute.call_logs c
         LEFT JOIN omniroute.artifact_deletion_queue q ON q.call_log_id=c.id AND q.call_log_order=c.insertion_order
         WHERE c.response_id=$1`,
          [`resp_${tag}_0`]
        )
      );
      assert.equal(revokedIntent.rows.length, 1);
      assert.equal(revokedIntent.rows[0].artifact_relpath, null);
      assert.equal(revokedIntent.rows[0].staged_payload, null);
      assert.ok(
        revokedIntent.rows[0].publication_state === null ||
          revokedIntent.rows[0].publication_state === "delete_pending"
      );

      fixtureStage = "index4_prepare";
      startClient(4);
      await eventually(() => readyIndices.has(4));
      terminals[4].resolve();
      await eventually(() => terminalSeen.has(4));
      assert.equal(preparations.get(4), true);
      assert.equal((await continueFrom(4)).found, true);
      fixtureStage = "index4_policy_lock";
      cancelReadPolicyHold = verification.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
        cancelReadLocked.resolve();
        await releaseCancelReadPolicy.promise;
      });
      void cancelReadPolicyHold.catch((error) =>
        diagnosePersistenceFailure("index4-held-transaction", error)
      );
      await withDeadline(cancelReadLocked.promise, 6000);
      const waitingRead = observeContinuationReader(continueFrom(4));
      await eventually(
        async () =>
          (
            await verification!.transaction((tx) =>
              tx.query<{ waiting: boolean }>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.datname=current_database() AND l.locktype='advisory' AND l.classid=18765 AND l.objid=5 AND l.mode='ShareLock' AND NOT l.granted) AS waiting"
              )
            )
          ).rows[0].waiting
      );
      // A completed HTTP disconnect is not a pipeline abort; abort the real signal here.
      const cancelledPipeline = pipelineControllers.get(4);
      assert.ok(cancelledPipeline);
      assert.equal(cancelledPipeline.signal.aborted, false);
      fixtureStage = "index4_pipeline_abort";
      cancelledPipeline.abort();
      assert.equal(cancelledPipeline.signal.aborted, true);
      clientControllers[4].abort();
      assert.equal(clientControllers[4].signal.aborted, true);
      fixtureStage = "index4_policy_release";
      releaseCancelReadPolicy.resolve();
      await withDeadline(cancelReadPolicyHold, 6000);
      assert.equal((await withDeadline(waitingRead, 6000)).found, false);
      assert.equal((await continueFrom(4)).found, false);

      fixtureStage = "index5_completed_disconnect";
      startClient(5);
      await eventually(() => readyIndices.has(5));
      terminals[5].resolve();
      await eventually(() => terminalSeen.has(5));
      assert.equal(preparations.get(5), true);
      const firstHandoff = {
        responseId: `resp_${tag}_5`,
        pauseAfter: "empty-ready-row" as const,
        entered: barrier(),
        release: barrier(),
        keyPolicySharedLocks: 0,
        readyReads: 0,
        privacyReads: 0,
        firstReadyRowCount: undefined as number | undefined,
      };
      continuationReadGate = firstHandoff;
      const beforeSeedCaptureRead = observeContinuationReader(
        continueFrom(5, firstHandoff.responseId, false, true)
      );
      await withDeadline(firstHandoff.entered.promise, 6000);
      assert.equal(firstHandoff.firstReadyRowCount, 0);
      assert.equal(firstHandoff.readyReads, 1);
      const completedPipeline = pipelineControllers.get(5);
      assert.ok(completedPipeline);
      assert.equal(completedPipeline.signal.aborted, false);
      clientControllers[5].abort();
      await eventually(() => abortedIndices.has(5));
      assert.equal(completedPipeline.signal.aborted, false);
      await eventually(() => upstreamCancelled.has(5));
      assert.equal(upstreamCancelled.has(5), true);
      // The upstream EOF is still held: cancellation must finish the actual SSE
      // accounting owner, not synthesize a 499 after the disconnect grace period.
      await eventually(async () => {
        const counts = await verification!.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.request_cost_ledger WHERE api_key_id=$1",
            [completedKey]
          )
        );
        return counts.rows[0].count === "1";
      });
      const terminalAccounting = await verification!.transaction(async (tx) => ({
        usage: (
          await tx.query(
            "SELECT status,success,tokens_input,tokens_output FROM omniroute.usage_history WHERE api_key_id=$1",
            [completedKey]
          )
        ).rows,
        logs: (
          await tx.query(
            "SELECT status,tokens_in,tokens_out FROM omniroute.call_logs WHERE api_key_id=$1",
            [completedKey]
          )
        ).rows,
      }));
      await eventually(
        async () =>
          (
            await verification!.transaction((tx) =>
              tx.query<{ detail_state: string }>(
                "SELECT detail_state FROM omniroute.call_logs WHERE response_id=$1",
                [`resp_${tag}_5`]
              )
            )
          ).rows[0]?.detail_state === "ready"
      );
      await eventually(() => optionalRequestIds.has(`${tag}-5`));
      firstHandoff.release.resolve();
      const beforeSeedCapture = await withDeadline(beforeSeedCaptureRead, 6000);
      assert.equal(
        beforeSeedCapture.found,
        true,
        "durable continuation survives publication before seed capture"
      );
      assert.deepEqual(beforeSeedCapture.handoff, {
        readyReads: 2,
        privacyReads: 2,
        keyPolicySharedLocks: 2,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;
      assert.equal(
        (await continueFrom(5)).found,
        true,
        "completed response remains readable after client disconnect"
      );
      assert.equal(
        (await continueFrom(5)).found,
        true,
        "authorized completed artifact remains readable"
      );
      assert.equal(terminalAccounting.usage.length, 1);
      assert.equal(String(terminalAccounting.usage[0].status), "200");
      assert.equal(Number(terminalAccounting.usage[0].success), 1);
      assert.equal(Number(terminalAccounting.usage[0].tokens_input), 105);
      assert.equal(Number(terminalAccounting.usage[0].tokens_output), 10);
      assert.equal(terminalAccounting.logs.length, 1);
      assert.equal(Number(terminalAccounting.logs[0].status), 200);
      assert.equal(Number(terminalAccounting.logs[0].tokens_in), 105);
      assert.equal(Number(terminalAccounting.logs[0].tokens_out), 10);
      await runtime!.runPersistenceRequest(() =>
        apiKeys!.updateApiKeyPermissions(completedKey, { noLog: true })
      );
      assert.equal((await continueFrom(5)).found, false);

      fixtureStage = "index7_privacy_handoff";
      startClient(7);
      await eventually(() => readyIndices.has(7));
      terminals[7].resolve();
      await eventually(() => terminalSeen.has(7));
      assert.equal(preparations.get(7), true);
      const afterSeedCaptureGate = {
        responseId: `resp_${tag}_7`,
        pauseAfter: "privacy-read" as const,
        entered: barrier(),
        release: barrier(),
        keyPolicySharedLocks: 0,
        readyReads: 0,
        privacyReads: 0,
        firstReadyRowCount: undefined as number | undefined,
      };
      continuationReadGate = afterSeedCaptureGate;
      const afterSeedCaptureRead = observeContinuationReader(
        continueFrom(7, afterSeedCaptureGate.responseId, false, true)
      );
      await withDeadline(afterSeedCaptureGate.entered.promise, 6000);
      assert.equal(afterSeedCaptureGate.firstReadyRowCount, 0);
      assert.equal(afterSeedCaptureGate.readyReads, 1);
      assert.equal(afterSeedCaptureGate.privacyReads, 1);
      upstreamEofs[7].resolve();
      await eventually(
        async () =>
          (
            await verification!.transaction((tx) =>
              tx.query<{ detail_state: string }>(
                "SELECT detail_state FROM omniroute.call_logs WHERE response_id=$1",
                [afterSeedCaptureGate.responseId]
              )
            )
          ).rows[0]?.detail_state === "ready"
      );
      await eventually(() => optionalRequestIds.has(`${tag}-7`));
      afterSeedCaptureGate.release.resolve();
      const afterSeedCapture = await withDeadline(afterSeedCaptureRead, 6000);
      assert.equal(
        afterSeedCapture.found,
        true,
        "durable continuation survives publication after seed capture"
      );
      assert.deepEqual(afterSeedCapture.handoff, {
        readyReads: 2,
        privacyReads: 2,
        keyPolicySharedLocks: 2,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;

      await assertFreshPrivacyFallbackDenial(8, freshPrivacyKey, "fresh no_log", async () => {
        await runtime!.runPersistenceRequest(() =>
          apiKeys!.updateApiKeyPermissions(freshPrivacyKey, { noLog: true })
        );
        const changedPrivacy = await verification!.transaction((tx) =>
          tx.query<{ no_log: number }>("SELECT no_log FROM omniroute.api_keys WHERE id=$1", [
            freshPrivacyKey,
          ])
        );
        assert.equal(changedPrivacy.rows[0]?.no_log, 1);
      });
      await assertFreshPrivacyFallbackDenial(9, freshDeleteKey, "deleted principal", async () => {
        const deleted = await runtime!.runPersistenceRequest(() =>
          apiKeys!.deleteApiKey(freshDeleteKey)
        );
        assert.equal(deleted, true);
        const principal = await verification!.transaction((tx) =>
          tx.query("SELECT id FROM omniroute.api_keys WHERE id=$1", [freshDeleteKey])
        );
        assert.equal(principal.rowCount, 0);
      });

      const { clearOwnedPendingContinuationState, seedOwnedPendingContinuationState } =
        await import("../../src/lib/db/responsesContinuationStore.ts");
      const pendingPipeline = (label: string) => ({
        clientRawRequest: { effectiveInput: [{ role: "user", content: `input-${label}` }] },
        clientResponse: {
          output: [
            {
              type: "message",
              role: "assistant",
              content: [{ type: "output_text", text: `output-${label}` }],
            },
          ],
        },
      });
      const seedPending = (responseId: string, label: string) =>
        runtime!.runPersistenceRequest(() =>
          seedOwnedPendingContinuationState(responseId, mainKey, pendingPipeline(label), false)
        );

      const replacementId = `resp_${tag}_replacement`;
      const replacementSeedA = await seedPending(replacementId, "A");
      assert.ok(replacementSeedA);
      const replacementGate = makeContinuationReadGate(replacementId, "privacy-read");
      continuationReadGate = replacementGate;
      const replacementReaderA = observeContinuationReader(
        continueFrom(7, replacementId, false, true)
      );
      await withDeadline(replacementGate.entered.promise, 6000);
      assert.equal(replacementGate.firstReadyRowCount, 0);
      assert.equal(replacementGate.readyReads, 1);
      assert.equal(replacementGate.privacyReads, 1);
      const replacementSeedB = await seedPending(replacementId, "B");
      assert.ok(replacementSeedB);
      assert.notEqual(replacementSeedA.nonce, replacementSeedB.nonce);
      replacementGate.release.resolve();
      const staleReaderResult = await withDeadline(replacementReaderA, 6000);
      assert.equal(staleReaderResult.found, false, "reader A cannot return replacement seed B");
      assert.deepEqual(staleReaderResult.handoff, {
        readyReads: 2,
        privacyReads: 1,
        keyPolicySharedLocks: 1,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;
      const replacementReaderB = await continueFrom(7, replacementId);
      assert.equal(replacementReaderB.found, true, "reader A cleanup preserves replacement seed B");
      assert.equal((replacementReaderB.input?.[0] as { content: string }).content, "input-B");
      assert.equal(
        (replacementReaderB.output?.[0] as { content: { text: string }[] }).content[0].text,
        "output-B"
      );
      await runtime!.runPersistenceRequest(() =>
        clearOwnedPendingContinuationState(replacementSeedB)
      );

      const expiredId = `resp_${tag}_expired`;
      const { getSelectedControlPlane, withPostgresControlPlane } =
        await import("../../src/lib/db/controlPlaneContext.ts");
      const selectedBinding = getSelectedControlPlane();
      assert.ok(selectedBinding?.database);
      const actualDateNow = Date.now;
      const backdatedNow = actualDateNow() - 120_000;
      let expiredSeed: Awaited<ReturnType<typeof seedOwnedPendingContinuationState>>;
      Date.now = () => backdatedNow;
      try {
        expiredSeed = withPostgresControlPlane(selectedBinding, () =>
          seedOwnedPendingContinuationState(expiredId, mainKey, pendingPipeline("expired"), false)
        );
      } finally {
        Date.now = actualDateNow;
      }
      assert.ok(expiredSeed);
      const expiredGate = makeContinuationReadGate(expiredId, "observe-only");
      continuationReadGate = expiredGate;
      const expiredReader = observeContinuationReader(continueFrom(7, expiredId, false, true));
      const expiredResult = await withDeadline(expiredReader, 6000);
      assert.equal(expiredResult.found, false, "expired seed cannot be revived");
      assert.deepEqual(expiredResult.handoff, {
        readyReads: 1,
        privacyReads: 1,
        keyPolicySharedLocks: 1,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;
      if (expiredSeed)
        await runtime!.runPersistenceRequest(() => clearOwnedPendingContinuationState(expiredSeed));

      const cancelledId = `resp_${tag}_cancelled`;
      const cancelledSeed = await seedPending(cancelledId, "cancelled");
      assert.ok(cancelledSeed);
      const cancelledGate = makeContinuationReadGate(cancelledId, "privacy-read");
      continuationReadGate = cancelledGate;
      const cancelledReader = observeContinuationReader(continueFrom(7, cancelledId, false, true));
      await withDeadline(cancelledGate.entered.promise, 6000);
      assert.equal(cancelledGate.firstReadyRowCount, 0);
      assert.equal(cancelledGate.readyReads, 1);
      assert.equal(cancelledGate.privacyReads, 1);
      await runtime!.runPersistenceRequest(() => clearOwnedPendingContinuationState(cancelledSeed));
      cancelledGate.release.resolve();
      const cancelledResult = await withDeadline(cancelledReader, 6000);
      assert.equal(
        cancelledResult.found,
        false,
        "cancelled seed without ready artifact stays absent"
      );
      assert.deepEqual(cancelledResult.handoff, {
        readyReads: 2,
        privacyReads: 1,
        keyPolicySharedLocks: 1,
        firstReadyRowCount: 0,
      });
      continuationReadGate = undefined;
      assert.equal((await continueFrom(7, cancelledId)).found, false);

      fixtureStage = "index3_preterminal_abort";
      startClient(3);
      await eventually(() => readyIndices.has(3));
      policyHold = verification.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
        policyLocked.resolve();
        await releasePolicy.promise;
      });
      void policyHold.catch((error) =>
        diagnosePersistenceFailure("index3-held-transaction", error)
      );
      await withDeadline(policyLocked.promise, 6000);
      terminals[3].resolve();
      await withDeadline(preparationEntered[3].promise, 6000);
      await eventually(
        async () =>
          (
            await verification!.transaction((tx) =>
              tx.query<{ waiting: boolean }>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.datname=current_database() AND l.locktype='advisory' AND l.classid=18765 AND l.objid=5 AND l.mode='ShareLock' AND NOT l.granted) AS waiting"
              )
            )
          ).rows[0].waiting
      );
      clientControllers[3].abort();
      await eventually(() => abortedIndices.has(3));
      releasePolicy.resolve();
      await withDeadline(policyHold, 6000);
      await eventually(() => preparations.has(3));
      assert.equal(preparations.get(3), false);
      assert.equal((await continueFrom(3)).found, false);

      for (const eof of upstreamEofs) eof.resolve();
      shutdown = closeNativeStreamingFixture(runtime);
      void shutdown.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      assert.equal(streamingNativeFixture.plane?.state, "ready");
      optional.resolve();
      await withDeadline(Promise.all(clients), 15000);
      await withDeadline(shutdown, 15000);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      for (const stack of streamingNativeFixture.sqliteAttemptStacks)
        t.diagnostic(`Observed legacy SQLite attempt before assert-zero:\n${stack}`);
      assert.equal(streamingNativeFixture.sqliteAttempts, 0);
      assert.equal(streamingNativeFixture.artifactsClosed, 1);
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      assert.ok(peak > 0 && peak <= 16);
      assert.equal(tlsLeases, totalLeases);
      assert.equal(active, 0);
    } catch (error) {
      primaryFailure = { error };
      bodyFailureElapsedMs = elapsed();
      t.diagnostic(
        `Original native streaming failure before fixture cleanup:\n${
          error instanceof Error ? (error.stack ?? error.message) : String(error)
        }`
      );
      for (const stack of streamingNativeFixture.sqliteAttemptStacks)
        t.diagnostic(`Observed legacy SQLite attempt before fixture cleanup:\n${stack}`);
      throw error;
    } finally {
      fixtureStage = "fixture_cleanup";
      try {
        try {
          continuationReadGate?.release.resolve();
          continuationReadGate?.releaseFreshPrivacy?.resolve();
          for (const terminal of terminals) terminal.resolve();
          for (const eof of upstreamEofs) eof.resolve();
          releasePolicy.resolve();
          releaseCancelReadPolicy.resolve();
          continuationNativeFixture.releasePut();
          optional.resolve();
          for (const controller of clientControllers) controller.abort();
          try {
            server.closeAllConnections();
            if (server.listening)
              await withDeadline(
                new Promise<void>((resolve) => server.close(() => resolve())),
                2000
              );
          } finally {
            await withDeadline(Promise.allSettled(continuationReaders), 15000);
          }
          await withDeadline(
            Promise.all([
              Promise.allSettled(clients),
              policyHold,
              cancelReadPolicyHold,
              Promise.allSettled([...handlerWork]),
            ]),
            15000
          );
        } finally {
          try {
            await withDeadline((shutdown ??= closeNativeStreamingFixture(runtime)), 15000);
          } finally {
            pipelineControllers.clear();
            globalThis.fetch = originalFetch;
            try {
              if (verification) {
                try {
                  await verification.transaction(async (tx) => {
                    for (const table of [
                      "usage_history",
                      "call_logs",
                      "request_cost_ledger",
                      "domain_cost_receipts",
                      "domain_cost_history",
                      "api_key_quota_counters",
                    ])
                      await tx.query(
                        `DELETE FROM omniroute.${table} WHERE api_key_id=ANY($1::text[])`,
                        [keyIds]
                      );
                    await tx.query(
                      "DELETE FROM omniroute.api_key_token_limits WHERE api_key_id=ANY($1::text[])",
                      [keyIds]
                    );
                    await tx.query(
                      "UPDATE omniroute.key_value SET value=(value::jsonb-$1::text-$2::text)::text WHERE namespace='pricing' AND key='openai'",
                      [model, faultModel]
                    );
                    await tx.query(
                      "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=ANY($1::text[])",
                      [[...continuationNativeFixture.references]]
                    );
                    if (pipelineSettingCaptured && originalPipelineSetting === undefined)
                      await tx.query(
                        "DELETE FROM omniroute.key_value WHERE namespace='settings' AND key='call_log_pipeline_enabled'"
                      );
                    else if (pipelineSettingCaptured)
                      await tx.query(
                        "UPDATE omniroute.key_value SET value=$1 WHERE namespace='settings' AND key='call_log_pipeline_enabled'",
                        [originalPipelineSetting]
                      );
                    await tx.query("DELETE FROM omniroute.api_keys WHERE id=ANY($1::text[])", [
                      keyIds,
                    ]);
                    if (connectionId)
                      await tx.query("DELETE FROM omniroute.provider_connections WHERE id=$1", [
                        connectionId,
                      ]);
                  });
                } finally {
                  await verification.close();
                }
              }
            } finally {
              hooks.deregister();
              t.mock.restoreAll();
              delete globalThis.__omniroutePersistenceRuntime;
              for (const [key, value] of previous) {
                if (value === undefined) delete process.env[key];
                else process.env[key] = value;
              }
              rmSync(directory, { recursive: true, force: true });
            }
          }
        }
      } catch (cleanupError) {
        diagnosePersistenceFailure("cleanup", cleanupError);
        if (primaryFailure) {
          throw new AggregateError(
            [primaryFailure.error, cleanupError],
            "Native continuation oracle failed and fixture cleanup also failed",
            { cause: primaryFailure.error }
          );
        }
        throw cleanupError;
      }
    }
  }
);
