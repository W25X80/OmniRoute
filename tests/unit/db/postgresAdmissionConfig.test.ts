import assert from "node:assert/strict";
import test from "node:test";
import {
  parsePostgresConfiguration,
  postgresPoolConfiguration,
  type PostgresOptions,
} from "../../../src/lib/db/postgres/config.ts";
import { readPostgresEnvironment } from "../../../src/lib/db/postgres/environment.ts";

const options: PostgresOptions = {
  host: "localhost",
  database: "test",
  user: "test",
  password: "private-test-value",
  tls: { mode: "verify-full" },
};
const environment = {
  OMNIROUTE_POSTGRES_HOST: "localhost",
  OMNIROUTE_POSTGRES_DATABASE: "test",
  OMNIROUTE_POSTGRES_USER: "test",
  OMNIROUTE_POSTGRES_PASSWORD: "private-test-value",
};

test("factory defaults keep fail-fast admission and derive wait from the connection timeout", () => {
  const defaults = parsePostgresConfiguration(options);
  assert.equal(defaults.maxQueuedOperations, 0);
  assert.equal(defaults.admissionWaitTimeoutMs, 3000);
  assert.equal(
    parsePostgresConfiguration({ ...options, connectionTimeoutMs: 750 }).admissionWaitTimeoutMs,
    750
  );
  const explicit = parsePostgresConfiguration({
    ...options,
    connectionTimeoutMs: 750,
    admissionWaitTimeoutMs: 1000,
    maxQueuedOperations: 8,
  });
  assert.equal(explicit.admissionWaitTimeoutMs, 1000);
  assert.equal(explicit.maxQueuedOperations, 8);
  const pool = postgresPoolConfiguration(explicit);
  assert.equal(pool.max, 4);
  assert.equal(pool.connectionTimeoutMillis, 750);
  assert.equal(Object.hasOwn(pool, "maxQueuedOperations"), false);
  assert.equal(Object.hasOwn(pool, "admissionWaitTimeoutMs"), false);
});

test("queue limits remain strict, finite and sanitized", () => {
  for (const patch of [
    { maxQueuedOperations: -1 },
    { maxQueuedOperations: 1025 },
    { maxQueuedOperations: 0.5 },
    { admissionWaitTimeoutMs: 0 },
    { admissionWaitTimeoutMs: 300001 },
    { admissionWaitTimeoutMs: Infinity },
    { unrecognizedAdmissionOption: "private-test-value" },
  ])
    assert.throws(
      () => parsePostgresConfiguration({ ...options, ...patch } as PostgresOptions),
      (error) => {
        assert.ok(error instanceof Error);
        assert.equal(error.message, "Invalid PostgreSQL configuration");
        assert.equal(error.cause, undefined);
        assert.doesNotMatch(JSON.stringify(error), /private-test-value/);
        return true;
      }
    );
  assert.equal(
    parsePostgresConfiguration({
      ...options,
      maxQueuedOperations: 1024,
      admissionWaitTimeoutMs: 300000,
    }).maxQueuedOperations,
    1024
  );
});

test("server profile keeps sixteen connections and admits a bounded waiting burst", async () => {
  const defaults = await readPostgresEnvironment(environment);
  assert.equal(defaults.maxConnections, 16);
  assert.equal(defaults.maxQueuedOperations, 256);
  assert.equal(defaults.admissionWaitTimeoutMs, 3000);
  const derived = await readPostgresEnvironment({
    ...environment,
    OMNIROUTE_POSTGRES_CONNECTION_TIMEOUT_MS: "750",
  });
  assert.equal(derived.admissionWaitTimeoutMs, 750);
  const explicit = await readPostgresEnvironment({
    ...environment,
    OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS: "0",
    OMNIROUTE_POSTGRES_ADMISSION_WAIT_TIMEOUT_MS: "1200",
  });
  assert.equal(explicit.maxQueuedOperations, 0);
  assert.equal(explicit.admissionWaitTimeoutMs, 1200);
});

test("invalid environment queue input fails without exposing configured values", async () => {
  for (const patch of [
    { OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS: "1025" },
    { OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS: "-1" },
    { OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS: "256suffix" },
    { OMNIROUTE_POSTGRES_ADMISSION_WAIT_TIMEOUT_MS: "0" },
    { OMNIROUTE_POSTGRES_ADMISSION_WAIT_TIMEOUT_MS: "300001" },
  ])
    await assert.rejects(readPostgresEnvironment({ ...environment, ...patch }), (error) => {
      assert.ok(error instanceof Error);
      assert.doesNotMatch(error.message, /private-test-value|256suffix/);
      return true;
    });
});
