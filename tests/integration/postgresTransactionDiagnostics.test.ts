import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "server idle termination is attributed to the callback gap and the client is replaced",
  { skip: !postgresTestsEnabled },
  async () => {
    const diagnostics: Record<string, unknown>[] = [];
    const database = await createPostgresDatabase(
      await testPostgresOptions({ idleInTransactionTimeoutMs: 100 }),
      {
        warn(fields: Record<string, unknown>) {
          diagnostics.push(fields);
        },
      }
    );
    try {
      let originalPid: number | undefined;
      await assert.rejects(
        database.transaction(
          async (tx) => {
            originalPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
              .rows[0].pid;
            // Deliberately violate the SQL-only callback contract. The server owns this timeout.
            await delay(500);
            await tx.query("SELECT 1");
          },
          { operation: "test.idle_callback" }
        ),
        (error: unknown) => error instanceof PostgresError && error.sqlState === "25P03"
      );
      const failure = diagnostics.find((entry) => entry.operation === "test.idle_callback");
      assert.equal(failure?.phase, "body");
      assert.equal(failure?.sqlState, "25P03");
      assert.ok(Number(failure?.callbackGapMs) >= 100);
      assert.equal(failure?.queryCount, 1);
      const next = await database.transaction((tx) =>
        tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")
      );
      assert.notEqual(next.rows[0].pid, originalPid);
    } finally {
      await database.close();
    }
  }
);
