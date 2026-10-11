import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { PostgresQuotaStore } from "../../src/lib/quota/postgresQuotaStore.ts";
import { RedisQuotaStore } from "../../src/lib/quota/redisQuotaStore.ts";
import { validateQuotaEventTimeMs } from "../../src/lib/quota/eventTime.ts";

const COMPLETED_AT = Date.UTC(2026, 8, 21, 10, 59, 59, 900);
const HOUR_MS = 3_600_000;
const dimension = { poolId: "event-time", unit: "requests" as const, window: "hourly" as const };

test("native event time rejects invalid instants without sampling a replacement clock", () => {
  for (const invalid of [null, "123", {}]) {
    assert.throws(
      () => validateQuotaEventTimeMs(invalid as unknown as number),
      /Quota completion time is invalid/
    );
  }
  assert.equal(validateQuotaEventTimeMs(0), 0);
  assert.equal(validateQuotaEventTimeMs(COMPLETED_AT), COMPLETED_AT);
  assert.equal(validateQuotaEventTimeMs(8_640_000_000_000_000), 8_640_000_000_000_000);
  for (const invalid of [-1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER, 8_640_000_000_000_001]) {
    assert.throws(() => validateQuotaEventTimeMs(invalid), /Quota completion time is invalid/);
  }
});

test("PostgreSQL consume retains one event bucket across deferred repository completion", async () => {
  const originalClock = Date.now;
  const store = new PostgresQuotaStore({} as PostgresDatabase);
  const calls: unknown[][] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  Reflect.set(store, "repository", {
    async consume(...args: unknown[]) {
      calls.push(args);
      await gate;
      return { curr: 5, prev: 20 };
    },
  });
  const pending = store.consume("key", dimension, 5, COMPLETED_AT);
  assert.deepEqual(calls, [
    ["key", "event-time:requests:hourly", Math.floor(COMPLETED_AT / HOUR_MS), 5, COMPLETED_AT],
  ]);
  release();
  assert.equal(await pending, 5 + 20 * (1 - (COMPLETED_AT % HOUR_MS) / HOUR_MS));
  assert.equal(Date.now, originalClock);
  await assert.rejects(store.consume("key", dimension, 5, NaN), RangeError);
  await assert.rejects(store.consume("key", dimension, 5, null as unknown as number), RangeError);
  assert.equal(calls.length, 1);
});

test("omitted PostgreSQL event time keeps operation-entry clock behavior", async () => {
  const originalClock = Date.now;
  const store = new PostgresQuotaStore({} as PostgresDatabase);
  let captured = -1;
  Reflect.set(store, "repository", {
    async consume(_key: string, _dimension: string, _bucket: number, _cost: number, nowMs: number) {
      captured = nowMs;
      return { curr: 1, prev: 0 };
    },
  });
  const before = Date.now();
  assert.equal(await store.consume("key", dimension, 1), 1);
  const after = Date.now();
  assert(captured >= before && captured <= after);
  assert.equal(Date.now, originalClock);
});

test("configured Redis authority receives the same captured bucket before awaiting its client", async () => {
  const originalClock = Date.now;
  const store = new RedisQuotaStore("redis://unused-in-test");
  const calls: Array<{ kind: string; key: string }> = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const client = {
    async incrbyfloat(key: string) {
      calls.push({ kind: "increment", key });
      return "5";
    },
    async expire(key: string) {
      calls.push({ kind: "expire", key });
      return 1;
    },
    async mget(key: string) {
      calls.push({ kind: "read", key });
      return ["20"];
    },
  };
  Reflect.set(store, "client", async () => {
    await gate;
    return client;
  });
  const pending = store.consume("key", dimension, 5, COMPLETED_AT);
  assert.deepEqual(calls, []);
  release();
  assert.equal(await pending, 5 + 20 * (1 - (COMPLETED_AT % HOUR_MS) / HOUR_MS));
  const bucket = Math.floor(COMPLETED_AT / HOUR_MS);
  assert(
    calls
      .filter((call) => call.kind === "increment")
      .every((call) => call.key.endsWith(`:${bucket}`))
  );
  assert(
    calls
      .filter((call) => call.kind === "read")
      .every((call) => call.key.endsWith(`:${bucket - 1}`))
  );
  assert.equal(calls.length, 4);
  await assert.rejects(store.consume("key", dimension, 5, -1), RangeError);
  await assert.rejects(store.consume("key", dimension, 5, null as unknown as number), RangeError);
  assert.equal(calls.length, 4);
  assert.equal(Date.now, originalClock);
});
