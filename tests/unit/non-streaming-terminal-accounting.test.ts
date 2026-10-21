import assert from "node:assert/strict";
import test from "node:test";
import * as errors from "../../open-sse/utils/error.ts";
import * as deadlines from "../../open-sse/utils/requestDeadline.ts";
import * as finalization from "../../open-sse/handlers/chatCore/nonStreamingFinalization.ts";
import { toFailureUsageAggregate } from "../../open-sse/handlers/chatCore/failureUsage.ts";
import { runNonStreamingProviderLeg } from "../../open-sse/handlers/chatCore/nonStreamingProviderLeg.ts";
import { baseInput, makeExecutorResult } from "./_helpers/nonStreamingProviderLegFixtures.ts";
import { controlledOwnerModule } from "../helpers/persistence/controlledOwnerModule.ts";
import type {
  NonStreamingProviderLegResult,
  ProviderLegUsage,
} from "../../src/lib/skills/toolLoopTypes.ts";

function deadline() {
  const error = new Error("Configured ingress deadline");
  error.name = "TimeoutError";
  Object.defineProperty(error, Symbol.for("omniroute.request-ingress-deadline.v1"), {
    value: true,
  });
  return error;
}

test("parsed empty response retains its known usage when cancellation prevents fallback", async () => {
  const abort = new AbortController();
  let wires = 0;
  const result = await runNonStreamingProviderLeg(
    baseInput({
      signal: abort.signal,
      provider: "gemini",
      model: "gemini-3-pro",
      setRequestWireState() {
        if (++wires === 2) abort.abort(deadline());
      },
      executeProviderRequest: async () =>
        makeExecutorResult({
          choices: [{ message: { role: "assistant", content: "" }, finish_reason: "stop" }],
          usage: { prompt_tokens: 12, completion_tokens: 0, total_tokens: 12 },
        }),
    })
  );
  assert.equal(result.kind, "error");
  if (result.kind !== "error") return;
  assert.equal(result.result.status, 504);
  assert.equal(result.result.errorCode, deadlines.INGRESS_REQUEST_DEADLINE_CODE);
  assert.equal(result.usage?.prompt_tokens, 12);
});

test("parsed ClinePass usage survives cancellation during retry delay", async () => {
  const abort = new AbortController();
  let sends = 0;
  const result = await runNonStreamingProviderLeg(
    baseInput({
      signal: abort.signal,
      provider: "clinepass",
      sleep: async () => {
        abort.abort(deadline());
      },
      executeProviderRequest: async () => {
        sends++;
        return makeExecutorResult({
          success: false,
          error: "empty content",
          usage: { prompt_tokens: 9, completion_tokens: 0, total_tokens: 9 },
        });
      },
    })
  );
  assert.equal(result.kind, "error");
  if (result.kind !== "error") return;
  assert.equal(sends, 1);
  assert.equal(result.result.status, 504);
  assert.equal(result.usage?.prompt_tokens, 9);
});

const known: ProviderLegUsage = { prompt_tokens: 12, completion_tokens: 3, total_tokens: 15 };
const cumulative: ProviderLegUsage = { prompt_tokens: 24, completion_tokens: 7, total_tokens: 31 };

async function scenario(
  options: {
    cancelInLoop?: boolean;
    cancelInGuardrail?: boolean;
    cancelInWrite?: boolean;
    cancelInCost?: boolean;
    cancelInSuccess?: boolean;
    clientAbort?: boolean;
    errorLeg?: boolean;
    cancelAtClassifierEntry?: boolean;
  } = {}
) {
  const abort = new AbortController();
  const history: Array<{ status: number; usage: unknown }> = [];
  const attempts: number[] = [];
  const effects: string[] = [];
  let pending = 0;
  const body = {
    choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
    usage: known,
  };
  const successfulLeg: NonStreamingProviderLegResult = {
    kind: "ok",
    response: body,
    responseForMemoryExtraction: body,
    providerBody: body,
    providerRequest: {},
    usage: known,
    responsePayloadFormat: "openai",
    looksLikeSSE: false,
    connectionId: "conn",
    headers: new Headers(),
    receipt: {
      index: 0,
      connectionId: "conn",
      provider: "openai",
      model: "model",
      startedAt: "",
      endedAt: "",
      latencyMs: 1,
      httpStatus: 200,
      errorType: null,
      usage: known,
      serviceTier: null,
      computedCostUsd: null,
      toolCalls: [],
      termination: "completed",
      clientVisible: true,
    },
  };
  const leg: NonStreamingProviderLegResult = options.errorLeg
    ? {
        kind: "error",
        result: errors.createErrorResult(429, "Rate limited"),
        usage: known,
        receipt: successfulLeg.receipt,
      }
    : successfulLeg;
  const cancel = () =>
    abort.abort(
      options.clientAbort ? new DOMException("Client disconnected", "AbortError") : deadline()
    );
  const noop = () => {};
  const asyncNoop = async () => {};
  const owner = await controlledOwnerModule<{
    runNonStreamingResponse(
      deps: Record<string, unknown>
    ): Promise<{ response: { status?: number; success?: boolean } }>;
  }>(
    new URL("../../open-sse/handlers/chatCore/nonStreamingResponse.ts", import.meta.url),
    {},
    {
      Error,
      Response,
      Headers,
      ...errors,
      ...deadlines,
      ...finalization,
      toFailureUsageAggregate,
      getSelectedControlPlane: () => undefined,
      getSelectedPersistenceIdentity: () => undefined,
      runNonStreamingProviderLeg: async () => leg,
      applyServerOwnedToolLoopIfNeeded: async () => {
        if (options.cancelInLoop) cancel();
        return {
          kind: "ok",
          leg: { ...leg, usage: cumulative },
          usage: cumulative,
          loop: {
            kind: "ok",
            cumulativeUsage: cumulative,
            receipts: [leg.receipt],
            totalCostUsd: 0.01,
            followUps: 1,
            termination: "completed",
          },
        };
      },
      derivePostInjectionRequestIdentity: () => "identity",
      buildClaudePromptCacheLogMeta: () => null,
      maybeSyncClaudeExtraUsageState: asyncNoop,
      recordFinalInputCalibration: noop,
      runMemoryExtractionGate: noop,
      extractFacts: noop,
      buildPostCallGuardrailContext: () => ({}),
      hasTrustedEmptyTurn: () => false,
      storeSemanticCacheResponseAsync: async () => {
        effects.push("cache");
      },
      MEMORY_BUILTIN_TOOL_NAMES: [],
      readCpaAuthIndex: () => null,
      meteredBudgetCost: (_provider: string, cost: number) => cost,
      buildNonStreamingResponseHeaders: () => ({}),
      maybeWrapForcedNonStreamingResponsesJson: () => new Response("{}"),
      inheritEmptyTurnPolicy: (_original: Response, response: Response) => response,
      createRoutingEvent: (value: unknown) => value,
      emitRoutingEvent: asyncNoop,
      routingFinishReason: () => "stop",
      CONNECTION_SCOPE_DENIED_CODE: "connection_scope_denied",
      isAntigravityProvider: () => false,
      toAntigravityDiagnosticPayload: () => null,
    },
    (source) => source.slice(source.indexOf("export async function runNonStreamingResponse("))
  );
  const outcome = await owner.runNonStreamingResponse({
    body: { messages: [] },
    buildErrorBody: errors.buildErrorBody,
    translatedBody: {},
    currentModel: "model",
    effectiveModel: "model",
    model: "model",
    provider: "openai",
    clientRawRequest: { signal: abort.signal, headers: {} },
    credentials: {},
    injectionResult: { builtinToolNames: [], injectedCustomSkillNames: [] },
    memorySettings: {},
    webFetchFallbackPlan: {},
    webSearchFallbackPlan: {},
    providerRequestCapture: { latest: () => null, body: (value: unknown) => value },
    getCurrentConnectionId: () => "conn",
    isServerOwnedToolLoopEnabled: () => false,
    getSkillsModelIdForFormat: () => "model",
    syncExecuteTranslatedBody: noop,
    triedModels: new Set(),
    reqLogger: {
      logTargetRequest: noop,
      logProviderResponse: noop,
      logConvertedResponse: noop,
      logError: noop,
    },
    applyProviderFailureClassification: async () => {
      // The production classifier checks cancellation before its internal try/catch.
      if (options.cancelAtClassifierEntry) cancel();
      deadlines.throwIfRequestCancelled(abort.signal);
      effects.push("provider-classification");
    },
    normalizeHeaders: () => ({}),
    resolveReportedServiceTier: () => null,
    onRequestSuccess: async () => {
      if (options.cancelInSuccess) {
        cancel();
        throw abort.signal.reason;
      }
      effects.push("account-success");
    },
    extractUsageFromResponse: () => known,
    buildCacheUsageLogMeta: () => null,
    attachCompressionUsageReceiptAfterAnalytics: noop,
    runMemoryExtractionGate: noop,
    recordContextEditingTelemetryHook: noop,
    appendRequestLog: asyncNoop,
    recordNonStreamingUsageStats: async (usage: unknown) => {
      history.push({ status: 200, usage });
      if (options.cancelInWrite) cancel();
    },
    guardrailRegistry: {
      runPostCallHooks: async (response: unknown) => {
        if (options.cancelInGuardrail) cancel();
        return { response, blocked: false };
      },
    },
    isJsonRecord: (value: unknown) => !!value && typeof value === "object",
    normalizeUsage: (value: unknown) => value,
    calculateCost: async () => {
      if (options.cancelInCost) cancel();
      return 0.01;
    },
    buildCostCtx: () => ({}),
    detectMalformedNonStream: () => null,
    saveIdempotency: asyncNoop,
    persistAttemptLogs: (input: { status: number }) => {
      attempts.push(input.status);
    },
    persistFailureUsage: async (status: number, _code: string, usage: unknown) => {
      history.push({ status, usage });
    },
    recordChatCallCost: asyncNoop,
    scheduleQuotaShareConsumption: asyncNoop,
    emitRequestGamificationEvent: asyncNoop,
    finalizePendingScope: () => {
      pending++;
    },
    trackPendingRequest: () => {
      pending++;
    },
    echoModelInObject: noop,
    runPluginOnResponseHook: asyncNoop,
    isSemaphoreCapacityError: () => false,
    getSafeErrorMetadata: () => ({}),
    getManagedLeaseFenceErrorCode: () => null,
    isLocalStreamLifecycleError: (error: unknown) =>
      error instanceof Error && error.name === "AbortError",
  });
  return { outcome, history, attempts, effects, pending };
}

test("deadline after a tool await finalizes known cumulative usage once as failure", async () => {
  const result = await scenario({ cancelInLoop: true });
  assert.equal(result.outcome.response.status, 504);
  assert.deepEqual(result.history, [{ status: 504, usage: toFailureUsageAggregate(cumulative) }]);
  assert.deepEqual(result.attempts, [504]);
  assert.equal(result.pending, 1);
  assert.deepEqual(result.effects, []);
});

test("successful multi-leg flow records cumulative usage once", async () => {
  const result = await scenario();
  assert.equal(result.outcome.response.success, true);
  assert.deepEqual(result.history, [{ status: 200, usage: cumulative }]);
  assert.deepEqual(result.attempts, [200]);
  assert.equal(result.pending, 1);
});

test("deadline at the classifier entry preserves known error-leg usage once", async () => {
  const result = await scenario({ errorLeg: true, cancelAtClassifierEntry: true });
  assert.equal(result.outcome.response.status, 504);
  assert.deepEqual(result.history, [{ status: 504, usage: toFailureUsageAggregate(known) }]);
  assert.deepEqual(result.attempts, [504]);
  assert.equal(result.pending, 1);
  assert.deepEqual(result.effects, []);
});

test("a live error leg preserves provider classification and original accounting", async () => {
  const result = await scenario({ errorLeg: true });
  assert.equal(result.outcome.response.status, 429);
  assert.deepEqual(result.history, [{ status: 429, usage: toFailureUsageAggregate(known) }]);
  assert.deepEqual(result.attempts, [429]);
  assert.equal(result.pending, 1);
  assert.deepEqual(result.effects, ["provider-classification"]);
});

test("cancellation before finalization preserves 499 for a real client abort", async () => {
  const result = await scenario({ cancelInGuardrail: true, clientAbort: true });
  assert.equal(result.outcome.response.status, 499);
  assert.deepEqual(result.history, [{ status: 499, usage: toFailureUsageAggregate(cumulative) }]);
  assert.deepEqual(result.attempts, [499]);
});

test("deadline during durable finalization does not compensate or write a second outcome", async () => {
  const result = await scenario({ cancelInWrite: true });
  assert.deepEqual(result.history, [{ status: 200, usage: cumulative }]);
  assert.deepEqual(result.attempts, [200]);
});

test("deadline during cost preparation selects failure before any success side effect", async () => {
  const result = await scenario({ cancelInCost: true });
  assert.equal(result.outcome.response.status, 504);
  assert.deepEqual(result.history, [{ status: 504, usage: toFailureUsageAggregate(cumulative) }]);
  assert.deepEqual(result.attempts, [504]);
  assert.deepEqual(result.effects, []);
  assert.equal(result.pending, 1);
});

test("cancellation in an observer after the durable claim does not lose or replay accounting", async () => {
  const result = await scenario({ cancelInSuccess: true });
  assert.deepEqual(result.history, [{ status: 200, usage: cumulative }]);
  assert.deepEqual(result.attempts, [200]);
  assert.equal(result.pending, 1);
});

test("a string client cancellation preserves parsed usage and remains 499", async () => {
  const abort = new AbortController();
  let wires = 0;
  const result = await runNonStreamingProviderLeg(
    baseInput({
      signal: abort.signal,
      provider: "gemini",
      model: "gemini-3-pro",
      setRequestWireState() {
        if (++wires === 2) abort.abort("caller cancelled");
      },
      executeProviderRequest: async () =>
        makeExecutorResult({
          choices: [{ message: { role: "assistant", content: "" }, finish_reason: "stop" }],
          usage: { prompt_tokens: 12, completion_tokens: 0, total_tokens: 12 },
        }),
    })
  );
  assert.equal(result.kind, "error");
  if (result.kind !== "error") return;
  assert.equal(result.result.status, 499);
  assert.equal(result.usage?.prompt_tokens, 12);
});
