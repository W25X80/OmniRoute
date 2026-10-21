import assert from "node:assert/strict";
import test from "node:test";
import {
  createStreamFailureFinalizers,
  type StreamCompletionPayload,
  type StreamFailurePayload,
} from "../../open-sse/utils/streamFailureFinalization.ts";

const failure = { status: 502, message: "Upstream stream failed", code: "stream_pipeline_error" };

test("an owned ingress deadline preserves local provenance rather than a provider failure", () => {
  let completed = false;
  const seen: StreamFailurePayload[] = [];
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => completed,
    isFailureCompletionRecorded: () => completed,
    onStreamComplete: () => {
      completed = true;
    },
    persistFailureUsage: () => {},
    onStreamFailure: (value) => {
      seen.push(value);
    },
  });
  const error = new Error("Gateway request exceeded its configured ingress deadline");
  error.name = "TimeoutError";
  Object.defineProperty(error, Symbol.for("omniroute.request-ingress-deadline.v1"), {
    value: true,
  });
  finalizers.onPipelineStreamError({ error, message: error.message, statusCode: 504 });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].code, "request_ingress_deadline_exceeded");
  assert.equal(seen[0].type, "request_deadline");
  assert.equal(seen[0].status, 504);
});

test("a similarly named upstream timeout cannot claim trusted ingress provenance", () => {
  let completed = false;
  const seen: StreamFailurePayload[] = [];
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => completed,
    isFailureCompletionRecorded: () => completed,
    onStreamComplete: () => {
      completed = true;
    },
    persistFailureUsage: () => {},
    onStreamFailure: (value) => {
      seen.push(value);
    },
  });
  const error = new Error("Gateway request exceeded its configured ingress deadline");
  error.name = "TimeoutError";
  finalizers.onPipelineStreamError({ error, message: error.message, statusCode: 504 });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].code, "stream_pipeline_error");
  assert.equal(seen[0].type, "stream_error");
});

test("undefined callback results retain order and exactly one dispatch", () => {
  const calls: string[] = [];
  let completed = false;
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => completed,
    isFailureCompletionRecorded: () => completed,
    onStreamComplete: () => {
      completed = true;
      calls.push("completion");
    },
    persistFailureUsage: () => {
      calls.push("usage");
    },
    onStreamFailure: () => {
      calls.push("provider");
    },
  });
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.deepEqual(calls, ["completion", "usage", "provider"]);
});

test("void callbacks may return incidental truthy values without skipping the next callback", () => {
  const calls: string[] = [];
  let completed = false;
  // Contextually void callbacks can return a value whose meaning the caller ignores.
  const completion: (payload: StreamCompletionPayload) => void = () => {
    completed = true;
    return calls.push("completion");
  };
  const usage: () => void = () => calls.push("usage");
  const provider: (payload: StreamFailurePayload) => void = () => calls.push("provider");
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => completed,
    isFailureCompletionRecorded: () => completed,
    onStreamComplete: completion,
    persistFailureUsage: usage,
    onStreamFailure: provider,
  });
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.deepEqual(calls, ["completion", "usage", "provider"]);
});

test("rejected async callbacks are observed once using only their error name", async (t) => {
  const warnings: unknown[][] = [];
  t.mock.method(console, "warn", (...args: unknown[]) => warnings.push(args));
  const calls: string[] = [];
  let completed = false;
  const rejected: Promise<void>[] = [];
  const reject = (label: string) => {
    calls.push(label);
    const work = Promise.reject<void>(new Error("Bearer SECRET /srv/private.ts:42"));
    rejected.push(work);
    return work;
  };
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => completed,
    isFailureCompletionRecorded: () => completed,
    onStreamComplete: () => {
      completed = true;
      return reject("completion");
    },
    persistFailureUsage: () => reject("usage"),
    onStreamFailure: () => reject("provider"),
  });
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.equal(finalizers.handleStreamFailure(failure), true);
  await Promise.allSettled(rejected);
  assert.deepEqual(calls, ["completion", "usage", "provider"]);
  assert.deepEqual(warnings, [
    ["[STREAM] Completion callback failed:", "Error"],
    ["[STREAM] Failure usage callback failed:", "Error"],
    ["[STREAM] Provider failure callback failed:", "Error"],
  ]);
});

test("completion and usage synchronous throws keep their existing caller boundaries", () => {
  for (const stage of ["completion", "usage"]) {
    const calls: string[] = [];
    const error = new Error(stage);
    const finalizers = createStreamFailureFinalizers({
      isStreamCompletionRecorded: () => false,
      isFailureCompletionRecorded: () => false,
      onStreamComplete: () => {
        calls.push("completion");
        if (stage === "completion") throw error;
      },
      persistFailureUsage: () => {
        calls.push("usage");
        if (stage === "usage") throw error;
      },
      onStreamFailure: () => {
        calls.push("provider");
      },
    });
    assert.throws(
      () => finalizers.handleStreamFailure(failure),
      (actual) => actual === error
    );
    assert.deepEqual(calls, stage === "completion" ? ["completion"] : ["completion", "usage"]);
  }
});

test("provider failure synchronous exceptions remain best effort", () => {
  const calls: string[] = [];
  const finalizers = createStreamFailureFinalizers({
    isStreamCompletionRecorded: () => false,
    isFailureCompletionRecorded: () => false,
    onStreamComplete: () => {
      calls.push("completion");
    },
    persistFailureUsage: () => {
      calls.push("usage");
    },
    onStreamFailure: () => {
      calls.push("provider");
      throw new Error("provider callback");
    },
  });
  assert.equal(finalizers.handleStreamFailure(failure), true);
  assert.deepEqual(calls, ["completion", "usage", "provider"]);
});
