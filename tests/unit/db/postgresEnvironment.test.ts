import assert from "node:assert/strict";
import { test } from "node:test";
import { readPostgresEnvironment } from "../../../src/lib/db/postgres/environment.ts";

const environment = {
  OMNIROUTE_POSTGRES_HOST: "127.0.0.1",
  OMNIROUTE_POSTGRES_DATABASE: "test",
  OMNIROUTE_POSTGRES_USER: "test",
  OMNIROUTE_POSTGRES_PASSWORD: "private-value",
  OMNIROUTE_POSTGRES_TLS: "disable-local",
};

test("boot configuration retains explicit credentials and bounds the server pool", async () => {
  const config = await readPostgresEnvironment(environment);
  assert.equal(config.maxConnections, 16);
  assert.equal(config.password, environment.OMNIROUTE_POSTGRES_PASSWORD);
  assert.equal(config.tls.mode, "disable-local");
  assert.equal(
    (await readPostgresEnvironment({ ...environment, OMNIROUTE_POSTGRES_MAX_CONNECTIONS: "8" }))
      .maxConnections,
    8
  );
});

test("invalid inputs do not leak values or fall back to a local authority", async () => {
  for (const patch of [
    { OMNIROUTE_POSTGRES_PORT: "5432suffix" },
    { OMNIROUTE_POSTGRES_MAX_CONNECTIONS: "0" },
    { OMNIROUTE_POSTGRES_TLS: "disable" },
    { OMNIROUTE_POSTGRES_HOST: "rds.example", OMNIROUTE_POSTGRES_TLS: "disable-local" },
    { OMNIROUTE_POSTGRES_CA_FILE: "/missing-private-ca-file" },
    { OMNIROUTE_POSTGRES_PASSWORD: "" },
    { OMNIROUTE_POSTGRES_LOCK_TIMEOUT_MS: "9000" },
  ]) {
    await assert.rejects(readPostgresEnvironment({ ...environment, ...patch }), (error: Error) => {
      assert.ok(!error.message.includes(environment.OMNIROUTE_POSTGRES_PASSWORD));
      assert.ok(!error.message.includes("missing-private-ca-file"));
      return true;
    });
  }
});
