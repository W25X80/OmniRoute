import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { createServer, type ServerResponse } from "node:http";
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
import type { StreamCompletionClock } from "../../open-sse/handlers/chatCore/streamingCompletion.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function withDeadline<T>(work: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Native stream teardown deadline exceeded")),
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
  "six native HTTP stream completions retain paid accounting during optional network stall and drain",
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
      "../helpers/persistence/streamingNativeIntegrationFixture.ts",
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
    let active = 0,
      peak = 0,
      tlsLeases = 0,
      totalLeases = 0;
    t.mock.method(Pool.prototype, "connect", function (this: Pool) {
      observedPool ??= this;
      const connection = Reflect.apply(connect, this, []) as Promise<PoolClient>;
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
      tokenLimitId = "";
    const terminal = barrier(),
      optional = barrier();
    const clocks = new Map<number, StreamCompletionClock>();
    const starts = new Map<number, number>();
    const terminalSeen = new Set<number>();
    const clientControllers = Array.from({ length: 7 }, () => new AbortController());
    const clients: Promise<string>[] = [];
    const handlerWork = new Set<Promise<void>>();
    let ready = 0,
      optionalStarted = 0;
    const originalFetch = globalThis.fetch;
    let baseUrl = "";
    let shutdown: Promise<void> | undefined;
    const server = createServer((incoming, outgoing) => {
      const work = handle(incoming.url ?? "", outgoing);
      handlerWork.add(work);
      void work.catch(() => outgoing.destroy()).finally(() => handlerWork.delete(work));
    });
    async function handle(path: string, outgoing: ServerResponse): Promise<void> {
      if (path.startsWith("/optional")) {
        optionalStarted++;
        await optional.promise;
        if (!outgoing.destroyed && !outgoing.writableEnded) outgoing.end("released");
        return;
      }
      const index = Number(path.split("/").at(-1));
      assert.ok(Number.isInteger(index) && index >= 0 && index < 7);
      const responseId = `resp_${tag}_${index}`;
      const selectedModel = index === 6 ? faultModel : model;
      if (path.startsWith("/upstream/")) {
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
        if (index !== 6) await terminal.promise;
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
        send("response.completed", {
          response: {
            id: responseId,
            object: "response",
            status: "completed",
            model: selectedModel,
            service_tier: "standard",
            output: [item],
            usage: { input_tokens: 100 + index, output_tokens: 10, total_tokens: 110 + index },
          },
        });
        outgoing.end();
        return;
      }
      assert.ok(path.startsWith("/gateway/"));
      const clientAbort = new AbortController();
      outgoing.once("close", () => {
        if (!outgoing.writableEnded) clientAbort.abort();
      });
      const { runStreamingTail } =
        await import("../../open-sse/handlers/chatCore/streamingTail.ts");
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
      const apiKeyId = index === 6 ? faultKey : mainKey;
      let carry: Awaited<ReturnType<typeof runStreamingTail>>["carry"] | undefined;
      const outcome = await runtime!.runPersistenceRequest(async () => {
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
        const providerResponse = await fetch(`${baseUrl}/upstream/${index}`, {
          signal: streamController.signal,
        });
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
        carry = tail.carry;
        return tail.result;
      });
      assert.equal(outcome.success, true);
      assert.ok(outcome.response);
      outgoing.writeHead(outcome.response.status, Object.fromEntries(outcome.response.headers));
      outgoing.flushHeaders();
      if (index !== 6) ready++;
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
        const fault = await apiKeys!.createApiKey(`stream-fault-${tag}`, "private-stream-fixture");
        mainKey = String(key.id);
        faultKey = String(fault.id);
        keyIds.push(mainKey, faultKey);
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
      const { PostgresTokenLimitRepository } =
        await import("../../src/lib/db/postgres/tokenLimitRepository.ts");
      const tokenLimit = await new PostgresTokenLimitRepository(verification).upsert({
        apiKeyId: mainKey,
        scopeType: "global",
        tokenLimit: 100000,
      });
      tokenLimitId = tokenLimit.id;
      const functionName = `stream_oracle_${tag}`;
      const triggerName = `stream_delay_${tag}`;
      assert.ok(keyIds.every((id) => /^[a-f0-9-]{36}$/i.test(id)));
      await verification.transaction(async (tx) => {
        await tx.query(`CREATE FUNCTION omniroute.${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.api_key_id = ANY(string_to_array(TG_ARGV[0],',')) THEN
          IF TG_TABLE_NAME='usage_history' AND NEW.model=TG_ARGV[1] THEN RAISE EXCEPTION 'injected accounting fault' USING ERRCODE='23514'; END IF;
          PERFORM pg_sleep(0.03);
        END IF; RETURN NEW; END $$`);
        for (const table of ["usage_history", "call_logs", "request_cost_ledger"])
          await tx.query(
            `CREATE TRIGGER ${triggerName} BEFORE INSERT ON omniroute.${table} FOR EACH ROW EXECUTE FUNCTION omniroute.${functionName}('${keyIds.join(",")}','${faultModel}')`
          );
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
          const data = payload as { model?: string };
          if (data.model === model)
            await fetch(`${baseUrl}/optional`, { signal: AbortSignal.timeout(12000) });
        })
      );
      async function client(index: number): Promise<string> {
        const response = await fetch(`${baseUrl}/gateway/${index}`, {
          signal: clientControllers[index].signal,
        });
        assert.equal(response.status, 200);
        const reader = response.body!.getReader();
        let text = "";
        try {
          while (true) {
            const chunk = await reader.read();
            if (chunk.done) break;
            text += new TextDecoder().decode(chunk.value);
            if (text.includes("response.completed")) terminalSeen.add(index);
          }
          assert.ok(text.includes(`resp_${tag}_${index}`));
          return "complete";
        } catch (error) {
          if (clientControllers[index].signal.aborted) return "cancelled";
          throw error;
        } finally {
          reader.releaseLock();
        }
      }
      // Deliberate owner rejection must not skip independent log and financial owners.
      assert.equal(await client(6), "complete");
      const failureRows = await verification.transaction(async (tx) => ({
        usage: (
          await tx.query("SELECT id FROM omniroute.usage_history WHERE api_key_id=$1", [faultKey])
        ).rowCount,
        logs: (await tx.query("SELECT id FROM omniroute.call_logs WHERE api_key_id=$1", [faultKey]))
          .rowCount,
        ledger: (
          await tx.query("SELECT id FROM omniroute.request_cost_ledger WHERE api_key_id=$1", [
            faultKey,
          ])
        ).rowCount,
      }));
      assert.deepEqual(failureRows, { usage: 0, logs: 1, ledger: 1 });
      for (let index = 0; index < 6; index++) {
        const work = client(index);
        clients.push(work);
        void work.catch(() => {});
      }
      await eventually(() => ready === 6);
      terminal.resolve();
      await eventually(() => optionalStarted === 6 && clocks.size === 7);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 6);
      const rows = await verification.transaction(async (tx) => ({
        usage: (
          await tx.query<{
            timestamp: string;
            latency_ms: number;
            tokens_input: string;
            tokens_output: string;
          }>(
            "SELECT timestamp,latency_ms,tokens_input,tokens_output FROM omniroute.usage_history WHERE api_key_id=$1 ORDER BY tokens_input",
            [mainKey]
          )
        ).rows,
        logs: (
          await tx.query<{
            timestamp: string;
            duration: number;
            response_id: string;
            detail_state: string;
          }>(
            "SELECT timestamp,duration,response_id,detail_state FROM omniroute.call_logs WHERE api_key_id=$1 ORDER BY response_id",
            [mainKey]
          )
        ).rows,
        ledger: (
          await tx.query<{ timestamp: string; request_id: string; amount_usd: number }>(
            "SELECT timestamp,request_id,amount_usd FROM omniroute.request_cost_ledger WHERE api_key_id=$1 ORDER BY request_id",
            [mainKey]
          )
        ).rows,
      }));
      assert.equal(rows.usage.length, 6);
      assert.equal(rows.logs.length, 6);
      assert.equal(rows.ledger.length, 6);
      const counters = await verification.transaction(async (tx) => ({
        tokens: (
          await tx.query<{ total: string }>(
            "SELECT COALESCE(SUM(tokens_used),0) AS total FROM omniroute.api_key_token_counters WHERE limit_id=$1",
            [tokenLimitId]
          )
        ).rows[0].total,
        receipts: (
          await tx.query(
            "SELECT usage_history_id FROM omniroute.api_key_token_usage_receipts WHERE limit_id=$1",
            [tokenLimitId]
          )
        ).rowCount,
        quota: (
          await tx.query<{ dimension_key: string; total: number }>(
            "SELECT dimension_key,SUM(consumed) AS total FROM omniroute.api_key_quota_counters WHERE api_key_id=$1 GROUP BY dimension_key",
            [mainKey]
          )
        ).rows,
      }));
      assert.equal(Number(counters.tokens), 675);
      assert.equal(counters.receipts, 6);
      assert.equal(
        Number(counters.quota.find((row) => row.dimension_key === "key-quota:tpm")?.total),
        675
      );
      assert.equal(
        Number(counters.quota.find((row) => row.dimension_key === "key-quota:rpm")?.total),
        6
      );
      for (let index = 0; index < 6; index++) {
        const clock = clocks.get(index)!;
        assert.equal(clock.completedAt - starts.get(index)!, clock.latencyMs);
        assert.equal(clock.timestamp, new Date(clock.completedAt).toISOString());
        assert.equal(Number(rows.usage[index].tokens_input), 100 + index);
        assert.equal(Number(rows.usage[index].tokens_output), 10);
        assert.equal(rows.usage[index].timestamp, clock.timestamp);
        assert.equal(Number(rows.usage[index].latency_ms), clock.latencyMs);
        assert.equal(rows.logs[index].response_id, `resp_${tag}_${index}`);
        assert.equal(rows.logs[index].detail_state, "ready");
        assert.equal(rows.logs[index].timestamp, clock.timestamp);
        assert.equal(Number(rows.logs[index].duration), clock.latencyMs);
        assert.equal(rows.ledger[index].timestamp, clock.timestamp);
        assert.equal(rows.ledger[index].request_id, `${tag}-${index}`);
        assert.ok(
          Math.abs(Number(rows.ledger[index].amount_usd) - (120 + index) / 1_000_000) < 1e-12
        );
      }
      await runtime.runPersistenceRequest(async () => {
        const { resolvePreviousResponseStateAsync } =
          await import("../../src/lib/db/responsesContinuationStore.ts");
        for (let index = 0; index < 6; index++) {
          const state = await resolvePreviousResponseStateAsync(`resp_${tag}_${index}`, mainKey);
          assert.ok(state);
          assert.equal(state.output.length, 1);
        }
        const spend = await import("../../src/lib/spend/batchWriter.ts");
        const flushed = await spend.flushSpendBatchWriter();
        assert.equal(flushed.requeued, false);
        await spend.stopSpendBatchWriter();
        pluginHooks!.unregisterHooks(plugin);
      });
      const receipts = await verification.transaction((tx) =>
        tx.query<{ timestamp: string; cost: number }>(
          "SELECT timestamp,cost FROM omniroute.domain_cost_receipts WHERE api_key_id=$1 ORDER BY cost",
          [mainKey]
        )
      );
      assert.equal(receipts.rows.length, 6);
      for (let index = 0; index < 6; index++)
        assert.equal(Number(receipts.rows[index].timestamp), clocks.get(index)!.completedAt);
      await eventually(() => terminalSeen.size === 7);
      clientControllers[0].abort();
      shutdown = closeNativeStreamingFixture(runtime);
      void shutdown.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      assert.equal(streamingNativeFixture.plane?.state, "ready");
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 6);
      optional.resolve();
      const outcomes = await Promise.all(clients);
      assert.equal(outcomes[0], "cancelled");
      assert.ok(outcomes.slice(1).every((outcome) => outcome === "complete"));
      await shutdown;
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      assert.equal(streamingNativeFixture.artifactsClosed, 1);
      for (const stack of streamingNativeFixture.sqliteAttemptStacks)
        t.diagnostic(`Observed legacy SQLite attempt before assert-zero:\n${stack}`);
      assert.equal(streamingNativeFixture.sqliteAttempts, 0);
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      assert.ok(peak >= 2 && peak <= 16);
      assert.ok(totalLeases > 0);
      assert.equal(tlsLeases, totalLeases);
      assert.equal(active, 0);
    } catch (error) {
      t.diagnostic(
        `Original native streaming failure before fixture cleanup:\n${
          error instanceof Error ? (error.stack ?? error.message) : String(error)
        }`
      );
      for (const stack of streamingNativeFixture.sqliteAttemptStacks)
        t.diagnostic(`Observed legacy SQLite attempt before fixture cleanup:\n${stack}`);
      throw error;
    } finally {
      try {
        terminal.resolve();
        optional.resolve();
        for (const controller of clientControllers) controller.abort();
        server.closeAllConnections();
        if (server.listening)
          await withDeadline(new Promise<void>((resolve) => server.close(() => resolve())), 2000);
        await withDeadline(
          Promise.all([Promise.allSettled(clients), Promise.allSettled([...handlerWork])]),
          15000
        );
      } finally {
        try {
          await withDeadline((shutdown ??= closeNativeStreamingFixture(runtime)), 15000);
        } finally {
          globalThis.fetch = originalFetch;
          try {
            if (verification) {
              try {
                await verification.transaction(async (tx) => {
                  for (const table of ["usage_history", "call_logs", "request_cost_ledger"])
                    await tx.query(
                      `DROP TRIGGER IF EXISTS stream_delay_${tag} ON omniroute.${table}`
                    );
                  await tx.query(`DROP FUNCTION IF EXISTS omniroute.stream_oracle_${tag}()`);
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
    }
  }
);
