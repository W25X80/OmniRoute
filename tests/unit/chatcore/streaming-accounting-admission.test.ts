import assert from "node:assert/strict";
import test from "node:test";
import type { PersistenceTailPermit } from "../../../src/lib/db/persistenceRuntime.ts";
import {
  captureStreamCompletionClock,
  createStreamingCompletionCallback,
} from "../../../open-sse/handlers/chatCore/streamingCompletion.ts";
import { buildStreamLedgerDetails } from "../../../open-sse/handlers/chatCore/streamingCost.ts";
import { buildFailureUsageRecord } from "../../../open-sse/handlers/chatCore/failureUsage.ts";

class SlowSqlAdmission {
  active = 0;
  maximum = 0;
  rejected = 0;
  readonly committed: string[] = [];

  async query(label: string): Promise<void> {
    if (this.active === 16) {
      this.rejected++;
      throw new Error("SQL admission full");
    }
    this.active++;
    this.maximum = Math.max(this.maximum, this.active);
    try {
      await new Promise<void>((resolve) => setTimeout(resolve, 2));
      this.committed.push(label);
    } finally {
      this.active--;
    }
  }
}

function admittedPermit(sql: SlowSqlAdmission, id: number) {
  let closed = false;
  let released = false;
  let releaseWork: Promise<void> | undefined;
  const pending = new Set<Promise<unknown>>();
  const permit: PersistenceTailPermit = {
    run<T>(body: () => Promise<T>): Promise<T> {
      if (closed) return Promise.reject(new Error("tail closed"));
      const work = (async () => {
        await sql.query(`snapshot-${id}`);
        return body();
      })();
      pending.add(work);
      void work.then(
        () => pending.delete(work),
        () => pending.delete(work)
      );
      return work;
    },
    release(): Promise<void> {
      return (releaseWork ??= (async () => {
        closed = true;
        while (pending.size) await Promise.allSettled([...pending]);
        released = true;
      })());
    },
  };
  return { permit, released: () => released };
}

test("six paid completions keep every independent owner within sixteen slow SQL slots", async () => {
  const sql = new SlowSqlAdmission();
  const held = Array.from({ length: 6 }, (_, id) => admittedPermit(sql, id));
  const claimed = new Set<number>();
  const completions = held.map(({ permit }, id) =>
    createStreamingCompletionCallback({
      permit,
      claim: () => {
        if (claimed.has(id)) return false;
        claimed.add(id);
        return true;
      },
      complete: (_, track) => {
        for (const owner of ["usage-receipt", "token-counter", "call-log", "ledger"]) {
          track(() => sql.query(`${owner}-${id}`), "accounting");
        }
      },
    })
  );
  const pending = completions.map((complete) => complete({ status: 200 }));
  assert.equal(claimed.size, 6);
  for (const [id, complete] of completions.entries()) {
    assert.equal(complete({ status: 200 }), pending[id]);
  }
  await Promise.all(pending);
  assert.equal(sql.rejected, 0);
  assert.ok(sql.maximum <= 6);
  assert.equal(sql.committed.length, 30);
  for (let id = 0; id < 6; id++) {
    assert.deepEqual(
      sql.committed.filter((label) => label.endsWith(`-${id}`)),
      [
        `snapshot-${id}`,
        `usage-receipt-${id}`,
        `token-counter-${id}`,
        `call-log-${id}`,
        `ledger-${id}`,
      ]
    );
  }
  assert.ok(held.every((item) => item.released()));
});

test("an early failed owner does not skip later usage, log and financial owners", async () => {
  const seen: string[] = [];
  const permit = admittedPermit(new SlowSqlAdmission(), 0);
  const complete = createStreamingCompletionCallback({
    permit: permit.permit,
    claim: () => true,
    complete: (_, track) => {
      track(() => {
        seen.push("optional-start");
        throw new Error("optional failed");
      });
      track(async () => {
        seen.push("usage");
      });
      track(async () => {
        seen.push("call-log");
      });
      track(async () => {
        seen.push("ledger");
      });
    },
  });
  await assert.rejects(complete({}) as Promise<void>, AggregateError);
  assert.deepEqual(seen, ["optional-start", "usage", "call-log", "ledger"]);
  assert.equal(permit.released(), true);
});

test("SQLite invokes each owner synchronously and retains its original throw boundary", () => {
  const seen: string[] = [];
  const complete = createStreamingCompletionCallback({
    claim: () => true,
    complete: (_, track) => {
      track(() => {
        seen.push("first");
        return Promise.resolve();
      });
      seen.push("between");
      track(() => {
        seen.push("second");
      }, "accounting");
    },
  });
  assert.equal(complete({}), undefined);
  assert.deepEqual(seen, ["first", "between", "second"]);
  const fail = createStreamingCompletionCallback({
    claim: () => true,
    complete: (_, track) => {
      track(() => {
        throw new Error("synchronous owner failed");
      });
      track(() => {
        seen.push("must-not-run");
      });
    },
  });
  assert.throws(() => fail({}), /synchronous owner failed/);
  assert.equal(seen.includes("must-not-run"), false);
});

test("terminal time stays before midnight despite snapshot and optional-owner delays", async () => {
  const beforeMidnight = Date.parse("2026-10-10T23:59:59.900Z");
  let now = beforeMidnight;
  let clock: ReturnType<typeof captureStreamCompletionClock> | undefined;
  let ledger: ReturnType<typeof buildStreamLedgerDetails> | undefined;
  let releaseSnapshot!: () => void;
  const snapshot = new Promise<void>((resolve) => {
    releaseSnapshot = resolve;
  });
  const observed: Array<{ timestamp: string; latencyMs: number }> = [];
  const permit: PersistenceTailPermit = {
    run: async (body) => {
      await snapshot;
      return body();
    },
    release: async () => {},
  };
  const complete = createStreamingCompletionCallback({
    permit,
    claim: () => {
      clock = captureStreamCompletionClock(beforeMidnight - 400, now);
      ledger = buildStreamLedgerDetails("standard", true, "request-a", clock.completedAt);
      return true;
    },
    complete: (_, track) => {
      track(async () => {
        now += 30_000;
      });
      track(() => {
        const failure = buildFailureUsageRecord({
          provider: "openai",
          model: "model-a",
          connectionId: "connection-a",
          apiKeyInfo: { id: "key-a" },
          effectiveServiceTier: "standard",
          isCombo: false,
          comboStrategy: null,
          statusCode: 500,
          errorCode: "upstream_error",
          latencyMs: clock!.latencyMs,
          timestamp: clock!.timestamp,
        });
        observed.push({ timestamp: failure.timestamp, latencyMs: failure.latencyMs });
      });
    },
  });
  const work = complete({});
  assert.equal(clock!.completedAt, beforeMidnight);
  now += 10_000;
  releaseSnapshot();
  await work;
  assert.equal(now, beforeMidnight + 40_000);
  assert.deepEqual(observed, [{ timestamp: "2026-10-10T23:59:59.900Z", latencyMs: 400 }]);
  assert.equal(ledger!.timestamp, "2026-10-10T23:59:59.900Z");
  assert.equal(ledger!.completedAt, beforeMidnight);
});

test("the actual pending finalizer uses the terminal clock for id and fallback paths", async () => {
  const usage = await import("../../../src/lib/usage/usageHistory.ts");
  const { finalizeStreamRequestLog } =
    await import("../../../open-sse/utils/streamFailureFinalization.ts");
  usage.clearPendingRequests();
  try {
    for (const useId of [true, false]) {
      const id = usage.trackPendingRequest("model-a", "provider-a", "connection-a", true);
      assert.ok(id);
      const startedAt = usage.getPendingById().get(id)!.startedAt;
      finalizeStreamRequestLog({
        pendingRequestId: useId ? id : "missing-id",
        model: "model-a",
        provider: "provider-a",
        connectionId: "connection-a",
        status: 200,
        completedAt: startedAt + 1234,
      });
      const completed = usage.getCompletedDetails().get(id);
      assert.equal(completed?.completedAt, startedAt + 1234);
      assert.equal(completed?.durationMs, 1234);
      assert.equal(usage.getPendingById().has(id), false);
    }
  } finally {
    usage.clearPendingRequests();
  }
});

test("a stalled optional network owner cannot delay six paid accounting tails", async () => {
  const sql = new SlowSqlAdmission();
  let finishOptional!: () => void;
  const optionalNetwork = new Promise<void>((resolve) => {
    finishOptional = resolve;
  });
  let optionalStarted = 0;
  const held = Array.from({ length: 6 }, (_, id) => admittedPermit(sql, id));
  const work = held.map(({ permit }, id) => {
    const complete = createStreamingCompletionCallback({
      permit,
      claim: () => true,
      complete: (_, track) => {
        // Native changes only scheduling; original source registration may put this first.
        track(async () => {
          optionalStarted++;
          await optionalNetwork;
        });
        track(() => sql.query(`usage-receipt-${id}`), "accounting");
        track(() => sql.query(`call-log-${id}`), "accounting");
        track(() => sql.query(`ledger-${id}`), "accounting");
      },
    });
    return complete({ status: 200 });
  });
  const deadline = Date.now() + 1000;
  try {
    while (optionalStarted !== 6 && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, 2));
    }
    assert.equal(optionalStarted, 6);
    assert.equal(sql.committed.filter((label) => !label.startsWith("snapshot-")).length, 18);
    assert.equal(sql.rejected, 0);
    assert.ok(held.every((item) => !item.released()));
  } finally {
    finishOptional();
    await Promise.allSettled(work);
  }
  assert.ok(held.every((item) => item.released()));
});

test("native accounting failures preserve later owner attempts and release after optional settlement", async () => {
  const seen: string[] = [];
  let finishOptional!: () => void;
  const optionalNetwork = new Promise<void>((resolve) => {
    finishOptional = resolve;
  });
  const held = admittedPermit(new SlowSqlAdmission(), 0);
  const complete = createStreamingCompletionCallback({
    permit: held.permit,
    claim: () => true,
    complete: (_, track) => {
      track(async () => {
        seen.push("optional");
        await optionalNetwork;
      });
      track(() => {
        seen.push("usage-failed");
        throw new Error("usage unavailable");
      }, "accounting");
      track(async () => {
        seen.push("call-log");
      }, "accounting");
      track(async () => {
        seen.push("ledger");
      }, "accounting");
    },
  });
  const work = complete({});
  assert.ok(work);
  const rejected = assert.rejects(work, AggregateError);
  const deadline = Date.now() + 1000;
  try {
    while (!seen.includes("optional") && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, 2));
    }
    assert.deepEqual(seen, ["usage-failed", "call-log", "ledger", "optional"]);
    assert.equal(held.released(), false);
  } finally {
    finishOptional();
    await rejected;
  }
  assert.equal(held.released(), true);
});
