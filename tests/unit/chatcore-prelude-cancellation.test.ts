import assert from "node:assert/strict";
import test from "node:test";
import {
  isPersistenceStreamCapacityError,
  PersistenceStreamCapacityError,
  PERSISTENCE_STREAM_CAPACITY_CODE,
} from "../../src/lib/db/persistenceRuntime.ts";
import { resolveNonStreamingCancellation } from "../../open-sse/handlers/chatCore/nonStreamingFinalization.ts";
import { createErrorResult } from "../../open-sse/utils/error.ts";
import {
  INGRESS_REQUEST_DEADLINE_CODE,
  isIngressRequestDeadline,
  throwIfRequestCancelled,
} from "../../open-sse/utils/requestDeadline.ts";
import { controlledOwnerModule } from "../helpers/persistence/controlledOwnerModule.ts";
import {
  clearPendingRequests,
  getPendingById,
  getPendingRequests,
  getPendingRetainedBytes,
  trackPendingRequest,
} from "../../src/lib/usage/usageHistory.ts";

interface Args {
  clientRawRequest?: { signal?: AbortSignal };
}

async function fixture(
  options: {
    signal?: AbortSignal;
    shutdownSignal?: AbortSignal;
    admissionError?: unknown;
    prelude?: (args: Args) => Promise<void>;
  } = {}
) {
  const effects: string[] = [];
  let effectiveSignal: AbortSignal | undefined;
  const success = { success: true };
  const owner = await controlledOwnerModule<{
    handleChatCore(args: Args): Promise<ReturnType<typeof createErrorResult> | typeof success>;
  }>(
    new URL("../../open-sse/handlers/chatCore.ts", import.meta.url),
    {},
    {
      AbortSignal,
      Object,
      getPersistenceAttemptCancellationSignal: () => options.shutdownSignal,
      runPersistenceAttempt: async (work: () => Promise<unknown>) => {
        if (options.admissionError !== undefined) throw options.admissionError;
        return work();
      },
      withResilienceActionsContext: ([args]: Args[], work: (args: Args) => Promise<unknown>) =>
        work(args),
      handleChatCoreInner: async (args: Args) => {
        effectiveSignal = args.clientRawRequest?.signal;
        // Match the actual inner entry guard and the next guard after awaited preparation.
        throwIfRequestCancelled(effectiveSignal);
        await options.prelude?.(args);
        throwIfRequestCancelled(effectiveSignal);
        effects.push("provider");
        return success;
      },
      isPersistenceStreamCapacityError,
      PERSISTENCE_STREAM_CAPACITY_CODE,
      createErrorResult,
      resolveNonStreamingCancellation,
      isIngressRequestDeadline,
      HTTP_STATUS: { SERVICE_UNAVAILABLE: 503 },
    },
    (source) =>
      source.slice(
        source.indexOf("export async function handleChatCore("),
        source.indexOf("async function handleChatCoreInner(")
      )
  );
  return {
    invoke: () => owner.handleChatCore({ clientRawRequest: { signal: options.signal } }),
    effects,
    effectiveSignal: () => effectiveSignal,
    success,
  };
}

test("a pre-aborted request resolves 499 before provider work", async () => {
  const abort = new AbortController();
  abort.abort();
  const owner = await fixture({ signal: abort.signal });
  const result = await owner.invoke();
  assert.equal("status" in result && result.status, 499);
  assert.equal("originalError" in result && result.originalError, abort.signal.reason);
  assert.deepEqual(owner.effects, []);
});

test("cancellation after awaited preparation resolves 499 without dispatch", async () => {
  const abort = new AbortController();
  const owner = await fixture({
    signal: abort.signal,
    prelude: async () => {
      await Promise.resolve();
      abort.abort("caller disconnected during preparation");
    },
  });
  const result = await owner.invoke();
  assert.equal("status" in result && result.status, 499);
  assert.deepEqual(owner.effects, []);
});

test("shutdown cancellation uses the effective composite signal reason", async () => {
  const request = new AbortController();
  const shutdown = new AbortController();
  const owner = await fixture({
    signal: request.signal,
    shutdownSignal: shutdown.signal,
    prelude: async () => {
      await Promise.resolve();
      shutdown.abort(new Error("Runtime draining"));
    },
  });
  const result = await owner.invoke();
  assert.equal("status" in result && result.status, 499);
  assert.equal("originalError" in result && result.originalError, shutdown.signal.reason);
  assert.notEqual(owner.effectiveSignal(), request.signal);
  assert.equal(request.signal.aborted, false);
  assert.deepEqual(owner.effects, []);
});

test("a trusted ingress deadline before dispatch resolves 504", async () => {
  const deadline = new Error("Configured ingress deadline");
  Object.defineProperty(deadline, Symbol.for("omniroute.request-ingress-deadline.v1"), {
    value: true,
  });
  const owner = await fixture({
    prelude: async () => {
      throw deadline;
    },
  });
  const result = await owner.invoke();
  assert.equal("status" in result && result.status, 504);
  assert.equal("errorCode" in result && result.errorCode, INGRESS_REQUEST_DEADLINE_CODE);
  assert.equal("originalError" in result && result.originalError, deadline);
  assert.deepEqual(owner.effects, []);
});

test("an unrelated preparation error is not hidden by concurrent cancellation", async () => {
  for (const error of [
    Object.assign(new Error("SQL operation failed"), { code: "55P03" }),
    Object.assign(new Error("Upstream timeout"), { code: INGRESS_REQUEST_DEADLINE_CODE }),
    new DOMException("Unrelated operation aborted", "AbortError"),
  ]) {
    const abort = new AbortController();
    const owner = await fixture({
      signal: abort.signal,
      prelude: async () => {
        abort.abort();
        throw error;
      },
    });
    await assert.rejects(owner.invoke, (actual) => actual === error);
    assert.deepEqual(owner.effects, []);
  }
});

test("matching public timeout text is not trusted lifecycle provenance", async () => {
  const error = Object.assign(new Error("request_ingress_deadline_exceeded"), {
    name: "TimeoutError",
    code: INGRESS_REQUEST_DEADLINE_CODE,
  });
  const owner = await fixture({
    prelude: async () => {
      throw error;
    },
  });
  await assert.rejects(owner.invoke, (actual) => actual === error);
});

test("capacity refusal remains 503 even if a request has concurrently aborted", async () => {
  const abort = new AbortController();
  abort.abort();
  const owner = await fixture({
    signal: abort.signal,
    admissionError: new PersistenceStreamCapacityError(),
  });
  const result = await owner.invoke();
  assert.equal("status" in result && result.status, 503);
  assert.equal("errorCode" in result && result.errorCode, PERSISTENCE_STREAM_CAPACITY_CODE);
  assert.deepEqual(owner.effects, []);
});

test("a live request retains its original successful result", async () => {
  const owner = await fixture({ shutdownSignal: new AbortController().signal });
  assert.equal(await owner.invoke(), owner.success);
  assert.deepEqual(owner.effects, ["provider"]);
});

async function pendingPrelude(stream: boolean, prepare: () => Promise<void>, promote: () => void) {
  return controlledOwnerModule<{
    runPendingPrelude(clientRawRequest: { signal?: AbortSignal }): Promise<string>;
  }>(
    new URL("../../open-sse/handlers/chatCore.ts", import.meta.url),
    {},
    {
      trackPendingRequest,
      throwIfRequestCancelled,
      promoteRequestToStreaming: promote,
      prepare,
      model: "prelude-model",
      provider: "openai",
      connectionId: "prelude-connection",
      credentials: {},
      body: { stream, messages: [{ role: "user", content: "cancelled request payload" }] },
      effectiveModel: "prelude-model",
      videoBridgeObserved: false,
      redactPendingBody: (value: unknown) => value,
      initialPendingBody: (value: unknown) => value,
      correlationId: null,
      conversationId: null,
      generateRequestId: () => "unexpected-missing-pending-id",
      nativeCodexPassthrough: false,
      resolveStreamFlag: (explicit: boolean) => explicit,
      acceptHeader: "application/json",
      sourceFormat: "openai",
      streamUserAgent: "fixture",
      apiKeyInfo: null,
    },
    (source) => {
      const entryStart = source.indexOf(
        "  throwIfRequestCancelled(clientRawRequest?.signal);",
        source.indexOf("async function handleChatCoreInner(")
      );
      const registrationStart = source.indexOf("  const pendingConnId = connectionId");
      const registrationEnd = source.indexOf(
        "  // Initialize rate limit settings",
        registrationStart
      );
      const guardStart = source.indexOf("  const stream =", registrationEnd);
      const guardEnd = source.indexOf("  // `settings` is already consolidated", guardStart);
      assert.ok(entryStart > 0 && registrationStart > entryStart);
      assert.ok(registrationEnd > registrationStart && guardEnd > guardStart);
      return `export async function runPendingPrelude(clientRawRequest) {
${source.slice(entryStart, source.indexOf("\n", entryStart))}
${source.slice(registrationStart, registrationEnd)}
await prepare();
${source.slice(guardStart, guardEnd)}
return pendingRequestId;
}`;
    }
  );
}

for (const mode of ["client-abort", "promotion-deadline", "live", "pre-aborted"] as const) {
  test(`registered prelude ownership is conserved for ${mode}`, async () => {
    clearPendingRequests();
    try {
      const model = "prelude-model";
      const provider = "openai";
      const connection = "prelude-connection";
      const modelKey = `${model} (${provider})`;
      const siblingId = trackPendingRequest(model, provider, connection, true, {
        clientRequest: { messages: [{ role: "user", content: "living sibling payload" }] },
      });
      assert.ok(siblingId);
      const sibling = structuredClone(getPendingById().get(siblingId));
      const siblingBytes = getPendingRetainedBytes();
      const abort = new AbortController();
      if (mode === "pre-aborted") abort.abort();
      let ownId: string | undefined;
      let registeredBytes = 0;
      let promotions = 0;
      const deadline = new Error("Configured promotion deadline");
      Object.defineProperty(deadline, Symbol.for("omniroute.request-ingress-deadline.v1"), {
        value: true,
      });
      const guard = await pendingPrelude(
        mode === "promotion-deadline",
        async () => {
          await Promise.resolve();
          ownId = [...getPendingById().keys()].find((id) => id !== siblingId);
          assert.ok(ownId);
          assert.equal(getPendingRequests().byModel[modelKey], 2);
          assert.equal(getPendingRequests().byAccount[connection]?.[modelKey], 2);
          registeredBytes = getPendingRetainedBytes();
          assert.ok(registeredBytes > siblingBytes);
          if (mode === "client-abort") abort.abort();
        },
        () => {
          promotions++;
          throw deadline;
        }
      );
      const owner = await fixture({
        signal: abort.signal,
        prelude: async (args) => {
          await guard.runPendingPrelude(args.clientRawRequest ?? {});
        },
      });
      const result = await owner.invoke();
      assert.deepEqual(getPendingById().get(siblingId), sibling);
      if (mode === "live") {
        assert.equal(result, owner.success);
        assert.ok(ownId && getPendingById().has(ownId));
        assert.equal(getPendingRequests().byModel[modelKey], 2);
        assert.equal(getPendingRequests().byAccount[connection]?.[modelKey], 2);
        assert.equal(getPendingRetainedBytes(), registeredBytes);
        assert.deepEqual(owner.effects, ["provider"]);
      } else {
        assert.equal(
          "status" in result && result.status,
          mode === "promotion-deadline" ? 504 : 499
        );
        assert.equal(getPendingById().size, 1, "only the living sibling remains");
        if (ownId) assert.equal(getPendingById().has(ownId), false);
        assert.equal(getPendingRequests().byModel[modelKey], 1);
        assert.equal(getPendingRequests().byAccount[connection]?.[modelKey], 1);
        assert.equal(
          getPendingRetainedBytes(),
          siblingBytes,
          "cancelled payload bytes are released"
        );
        assert.deepEqual(owner.effects, []);
      }
      assert.equal(promotions, mode === "promotion-deadline" ? 1 : 0);
      if (mode === "pre-aborted") assert.equal(ownId, undefined);
    } finally {
      clearPendingRequests();
    }
  });
}
