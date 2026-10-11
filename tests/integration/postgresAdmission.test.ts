import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { readPostgresEnvironment } from "../../src/lib/db/postgres/environment.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

for (const count of [6, 60])
  test(
    `server admission profile completes ${count} concurrent SQL transactions without replay`,
    { skip: !postgresTestsEnabled, timeout: 30000 },
    async () => {
      const fixture = await testPostgresOptions();
      const profile = await readPostgresEnvironment({
        OMNIROUTE_POSTGRES_HOST: fixture.host,
        OMNIROUTE_POSTGRES_PORT: String(fixture.port),
        OMNIROUTE_POSTGRES_DATABASE: fixture.database,
        OMNIROUTE_POSTGRES_USER: fixture.user,
        OMNIROUTE_POSTGRES_PASSWORD: fixture.password,
        OMNIROUTE_POSTGRES_TLS: "verify-full",
        OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
      });
      assert.equal(profile.maxConnections, 16);
      assert.equal(profile.maxQueuedOperations, 256);
      assert.equal(profile.admissionWaitTimeoutMs, 3000);
      const database = await createPostgresDatabase(profile, { warn() {} });
      const table = `admission_${randomUUID().replaceAll("-", "")}`;
      const executions = Array.from({ length: count }, () => 0);
      const backends = new Set<number>();
      try {
        await database.transaction((tx) =>
          tx.query(`CREATE TABLE ${table}(id integer PRIMARY KEY)`)
        );
        await Promise.all(
          executions.map((_, index) =>
            database.transaction(async (tx) => {
              executions[index]++;
              // A server wait supplies genuine overlap; it is not a client-side fixed sleep.
              const result = await tx.query<{ pid: number }>(
                "SELECT pg_backend_pid() AS pid, pg_sleep(0.03)"
              );
              backends.add(result.rows[0].pid);
              await tx.query(`INSERT INTO ${table}(id) VALUES($1)`, [index]);
            })
          )
        );
        assert.deepEqual(
          executions,
          Array.from({ length: count }, () => 1)
        );
        assert.ok(backends.size <= 16);
        const receipt = await database.transaction((tx) =>
          tx.query<{ id: number }>(`SELECT id FROM ${table} ORDER BY id`)
        );
        assert.deepEqual(
          receipt.rows.map((row) => row.id),
          Array.from({ length: count }, (_, index) => index)
        );
      } finally {
        await database.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
        await database.close();
      }
    }
  );
