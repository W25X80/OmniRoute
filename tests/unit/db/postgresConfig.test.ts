import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";
import {
  parsePostgresConfiguration,
  postgresPoolConfiguration,
  type PostgresOptions,
} from "../../../src/lib/db/postgres/config.ts";
import { createPostgresDatabase } from "../../../src/lib/db/postgres/connect.ts";

const options: PostgresOptions = {
  host: "database.example.com",
  database: "omniroute",
  user: "omniroute",
  password: "secret-sentinel",
  tls: { mode: "verify-full" },
};

test("importing the optional factory does not load pg", () => {
  const require = createRequire(import.meta.url);
  assert.equal(typeof createPostgresDatabase, "function");
  assert.equal(
    Object.keys(require.cache).some((path) =>
      path.replaceAll("\\", "/").includes("/node_modules/pg/")
    ),
    false
  );
});

test("pool options explicitly configure verified TLS and finite limits", () => {
  const config = parsePostgresConfiguration(options);
  const pool = postgresPoolConfiguration(config);
  assert.deepEqual(pool.ssl, { rejectUnauthorized: true });
  assert.equal(pool.connectionString, undefined);
  assert.equal(pool.max, 4);
  assert.equal(pool.connectionTimeoutMillis, 3_000);
  assert.equal(pool.statement_timeout, 5_000);
  assert.equal(pool.query_timeout, 6_000);
  assert.equal(pool.lock_timeout, 1_000);
  assert.match(pool.options!, /transaction_timeout=15000/);
  assert.match(pool.options!, /timezone=UTC/);
});

test("a supplied RDS CA retains certificate and hostname verification", () => {
  const pool = postgresPoolConfiguration(
    parsePostgresConfiguration({
      ...options,
      tls: { mode: "verify-full", ca: "test-ca" },
    })
  );
  assert.deepEqual(pool.ssl, { ca: "test-ca", rejectUnauthorized: true });
});

test("plaintext is an explicit loopback-IP-only choice", () => {
  for (const host of ["127.0.0.1", "::1"]) {
    assert.equal(
      postgresPoolConfiguration(
        parsePostgresConfiguration({
          ...options,
          host,
          tls: { mode: "disable-local" },
        })
      ).ssl,
      false
    );
  }
  for (const host of ["database.example.com", "localhost", "/tmp"]) {
    assert.throws(
      () =>
        parsePostgresConfiguration({
          ...options,
          host,
          tls: { mode: "disable-local" },
        }),
      /Invalid PostgreSQL configuration/
    );
  }
});

test("rejects unsafe configuration without exposing input or a nested cause", () => {
  const rejected = [
    { connectionString: "postgres://secret-sentinel@host/db?sslmode=disable" },
    { password: "" },
    { host: "" },
    { port: 0 },
    { maxConnections: 0 },
    { connectionTimeoutMs: 0 },
    { queryTimeoutMs: 4_000 },
    { lockTimeoutMs: 5_000 },
    { transactionTimeoutMs: 6_000 },
    { idleInTransactionTimeoutMs: 20_000 },
    { tls: { mode: "verify-full", rejectUnauthorized: false } },
  ];
  for (const input of rejected) {
    assert.throws(
      () => parsePostgresConfiguration({ ...options, ...input } as PostgresOptions),
      (error: Error) => {
        assert.equal(error.message, "Invalid PostgreSQL configuration");
        assert.equal(error.cause, undefined);
        assert.doesNotMatch(JSON.stringify(error), /secret-sentinel/);
        return true;
      }
    );
  }
});
