import assert from "node:assert/strict";
import test from "node:test";
import type { PersistenceTailPermit } from "../../../src/lib/db/persistenceRuntime.ts";
import {
  createStreamingCompletionCallback,
  releaseFailedStreamingHandoff,
} from "../../../open-sse/handlers/chatCore/streamingCompletion.ts";
import {
  createClientDisconnectGraceHandler,
  createStreamFailureFinalizers,
} from "../../../open-sse/utils/streamFailureFinalization.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((finish) => {
    resolve = finish;
  });
  return { promise, resolve };
}
class AdmittedPermit implements PersistenceTailPermit {
  runs = 0;
  releases = 0;
  released = false;
  private closing = false;
  private pending = new Set<Promise<unknown>>();
  private releaseWork?: Promise<void>;
  constructor(private readonly snapshot: Promise<void> = Promise.resolve()) {}
  run<T>(body: () => Promise<T>): Promise<T> {
    if (this.closing) return Promise.reject(new Error("authority ended"));
    this.runs++;
    const work = (async () => {
      await this.snapshot;
      return body();
    })();
    this.pending.add(work);
    void work.then(
      () => this.pending.delete(work),
      () => this.pending.delete(work)
    );
    return work;
  }
  release(): Promise<void> {
    return (this.releaseWork ??= (async () => {
      this.closing = true;
      this.releases++;
      while (this.pending.size) await Promise.allSettled([...this.pending]);
      this.released = true;
    })());
  }
}
async function tick() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

test("SQLite completion stays synchronous, ordered and returns void", () => {
  const seen: string[] = [];
  let claimed = false;
  const complete = createStreamingCompletionCallback({
    claim: () => {
      if (claimed) return false;
      claimed = true;
      seen.push("claim");
      return true;
    },
    complete: (_, track) => {
      seen.push("write");
      track(() => Promise.resolve());
      seen.push("after");
    },
  });
  assert.equal(complete({}), undefined);
  assert.deepEqual(seen, ["claim", "write", "after"]);
  assert.equal(complete({}), undefined);
  assert.deepEqual(seen, ["claim", "write", "after"]);
});

test("completion claims before snapshot await and retains its authority through delayed writes", async () => {
  const snapshot = deferred(),
    accounting = deferred();
  const permit = new AdmittedPermit(snapshot.promise);
  let claimed = false,
    writes = 0;
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      if (claimed) return false;
      claimed = true;
      return true;
    },
    complete: (_, track) => {
      writes++;
      track(() => accounting.promise);
    },
  });
  const work = complete({});
  assert.ok(work);
  assert.equal(claimed, true);
  assert.equal(writes, 0);
  assert.equal(complete({}), work);
  assert.equal(permit.runs, 1);
  snapshot.resolve();
  await tick();
  assert.equal(writes, 1);
  assert.equal(permit.released, false);
  accounting.resolve();
  await work;
  assert.equal(permit.released, true);
  assert.equal(permit.releases, 1);
});

test("rejected accounting waits already started siblings before release and propagates failure", async () => {
  const sibling = deferred(),
    permit = new AdmittedPermit();
  let claimed = false;
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      if (claimed) return false;
      claimed = true;
      return true;
    },
    complete: (_, track) => {
      track(() => Promise.reject(new Error("usage unavailable")));
      track(() => sibling.promise);
    },
  });
  const work = complete({});
  assert.ok(work);
  const rejection = assert.rejects(work, AggregateError);
  await tick();
  assert.equal(permit.released, false);
  sibling.resolve();
  await rejection;
  assert.equal(permit.released, true);
});

test("real failure finalizer admits all three callbacks before terminal release", async () => {
  const usage = deferred(),
    provider = deferred(),
    permit = new AdmittedPermit();
  let completeClaimed = false,
    failed = false,
    completion: void | Promise<void> = undefined;
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      if (completeClaimed) return false;
      completeClaimed = true;
      failed = true;
      return true;
    },
    complete: (_, track) => {
      track(() => Promise.resolve());
    },
  });
  const finalizers = createStreamFailureFinalizers({
    isFailureCompletionRecorded: () => failed,
    isStreamCompletionRecorded: () => completeClaimed,
    onStreamComplete: (payload) => {
      completion = complete(payload);
      return completion;
    },
    persistFailureUsage: () =>
      permit.run(async () => {
        await usage.promise;
      }),
    onStreamFailure: () =>
      permit.run(async () => {
        await provider.promise;
      }),
  });
  assert.equal(finalizers.handleStreamFailure({ status: 502, message: "failed" }), true);
  assert.equal(permit.runs, 3);
  await tick();
  assert.equal(permit.released, false);
  usage.resolve();
  await tick();
  assert.equal(permit.released, false);
  provider.resolve();
  await completion;
  assert.equal(permit.released, true);
  assert.equal(finalizers.handleStreamFailure({ status: 502, message: "late" }), true);
  assert.equal(permit.runs, 3);
});

test("disconnect grace sees a synchronous success claim before its delayed snapshot", async () => {
  const snapshot = deferred(),
    permit = new AdmittedPermit(snapshot.promise);
  const timers: Array<() => void> = [];
  let claimed = false,
    failures = 0;
  const disconnect = createClientDisconnectGraceHandler({
    isStreamCompletionRecorded: () => claimed,
    gracePeriodMs: 1000,
    finalize: () => {
      failures++;
    },
    setTimeoutFn: (callback) => {
      timers.push(callback);
    },
  });
  assert.equal(disconnect({ reason: "cancel", duration: 1 }), true);
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      if (claimed) return false;
      claimed = true;
      return true;
    },
    complete: () => {},
  });
  const work = complete({});
  assert.ok(work);
  timers.shift()!();
  assert.equal(failures, 0);
  assert.equal(permit.released, false);
  snapshot.resolve();
  await work;
  assert.equal(permit.released, true);
});

test("disconnect expiry keeps the permit until its terminal failure accounting settles", async (context) => {
  let now = 0;
  context.mock.method(Date, "now", () => now);
  const accounting = deferred(),
    permit = new AdmittedPermit(),
    timers: Array<() => void> = [];
  let claimed = false,
    completion: void | Promise<void> = undefined;
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      if (claimed) return false;
      claimed = true;
      return true;
    },
    complete: (_, track) => {
      track(() => accounting.promise);
    },
  });
  const disconnect = createClientDisconnectGraceHandler({
    isStreamCompletionRecorded: () => claimed,
    gracePeriodMs: 1000,
    finalize: () => {
      completion = complete({});
    },
    setTimeoutFn: (callback) => {
      timers.push(callback);
    },
  });
  disconnect({ reason: "cancel", duration: 1 });
  assert.equal(permit.releases, 0);
  now = 1001;
  timers.shift()!();
  assert.equal(claimed, true);
  await tick();
  assert.equal(permit.released, false);
  accounting.resolve();
  await completion;
  assert.equal(permit.released, true);
});

test("construction cleanup releases even when cancel rejects", async () => {
  const permit = new AdmittedPermit();
  await assert.rejects(
    releaseFailedStreamingHandoff(permit, () => {
      throw new Error("cancel failed");
    }),
    /cancel failed/
  );
  assert.equal(permit.released, true);
  assert.equal(permit.releases, 1);
});

test("a synchronous projection error still attempts and drains its registered owners", async () => {
  const write = deferred(),
    permit = new AdmittedPermit();
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => true,
    complete: (_, track) => {
      track(() => write.promise);
      throw new Error("projection failed");
    },
  });
  const work = complete({});
  assert.ok(work);
  const rejected = assert.rejects(work, AggregateError);
  await tick();
  assert.equal(permit.released, false);
  write.resolve();
  await rejected;
  assert.equal(permit.released, true);
});

test("failed handoff cannot close authority before an already admitted callback settles", async () => {
  const write = deferred(),
    permit = new AdmittedPermit();
  const admitted = permit.run(async () => {
    await write.promise;
  });
  const cleanup = releaseFailedStreamingHandoff(permit, () => {
    throw new Error("abort failed");
  });
  const rejected = assert.rejects(cleanup, /abort failed/);
  await tick();
  assert.equal(permit.released, false);
  write.resolve();
  await rejected;
  await admitted;
  assert.equal(permit.released, true);
  assert.equal(permit.releases, 1);
});

test("native streaming cost awaits the real recorder and preserves nonpositive/NaN skip", async () => {
  const { recordStreamingCostAsync } =
    await import("../../../open-sse/handlers/chatCore/streamingCost.ts");
  const commit = deferred();
  let recorded = 0;
  let finished = false;
  const base = {
    apiKeyId: "key-a",
    provider: "openai",
    model: "model-a",
    streamUsage: { prompt_tokens: 1 },
    calculateCost: async () => 0.5,
    recordCost: async () => {
      recorded++;
      await commit.promise;
    },
  };
  const work = recordStreamingCostAsync(base).then(() => {
    finished = true;
  });
  await tick();
  assert.equal(recorded, 1);
  assert.equal(finished, false);
  commit.resolve();
  await work;
  for (const cost of [0, -1, NaN]) {
    await recordStreamingCostAsync({ ...base, calculateCost: async () => cost });
  }
  assert.equal(recorded, 1);
});

test("native streaming cost reports pricing/record failures and never invents missing attribution", async () => {
  const { recordStreamingCostAsync } =
    await import("../../../open-sse/handlers/chatCore/streamingCost.ts");
  let calculated = 0;
  const base = {
    apiKeyId: "key-a",
    provider: "openai",
    model: "model-a",
    streamUsage: { prompt_tokens: 1 },
    calculateCost: async () => {
      calculated++;
      return 0.5;
    },
    recordCost: async () => {},
  };
  await assert.rejects(recordStreamingCostAsync({ ...base, provider: null }), /attribution/);
  assert.equal(calculated, 0);
  await assert.rejects(
    recordStreamingCostAsync({
      ...base,
      calculateCost: async () => {
        throw new Error("pricing unavailable");
      },
    }),
    /pricing unavailable/
  );
  await assert.rejects(
    recordStreamingCostAsync({
      ...base,
      recordCost: async () => {
        throw new Error("recorder unavailable");
      },
    }),
    /recorder unavailable/
  );
});
