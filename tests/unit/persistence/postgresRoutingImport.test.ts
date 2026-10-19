import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import {
  PostgresDatabase,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
} from "../../../src/lib/db/postgres/database.ts";
import {
  importRoutingSnapshot,
  type RoutingSnapshot,
} from "../../../src/lib/db/postgres/routingImport.ts";

type StoredReceipt = { digest: string; rowCount: number };

function fixture() {
  const calls: { text: string; values?: unknown[] }[] = [];
  const events = new EventEmitter();
  let storedReceipt: StoredReceipt | undefined;
  let targetSeedAvailable = true;
  const client: PostgresClient = {
    async query<Row>(request: {
      text: string;
      values?: unknown[];
      queryMode: "extended";
    }): Promise<PostgresResult<Row>> {
      calls.push({ text: request.text, values: request.values });
      if (request.text === "BEGIN" || request.text === "COMMIT" || request.text === "ROLLBACK")
        return { command: request.text, rows: [], rowCount: 0 } as PostgresResult<Row>;
      if (request.text.includes("SELECT digest,row_count FROM omniroute.routing_imports")) {
        const rows = storedReceipt
          ? [{ digest: storedReceipt.digest, row_count: storedReceipt.rowCount }]
          : [];
        return { command: "SELECT", rows, rowCount: rows.length } as PostgresResult<Row>;
      }
      if (request.text.includes("SELECT namespace,key,value FROM omniroute.key_value LIMIT 2")) {
        const rows = targetSeedAvailable
          ? [
              {
                namespace: "settings",
                key: "managementAuthGeneration",
                value: JSON.stringify("00000000-0000-4000-8000-000000000010"),
              },
            ]
          : [];
        return { command: "SELECT", rows, rowCount: rows.length } as PostgresResult<Row>;
      }
      if (request.text.includes("SELECT EXISTS (SELECT 1 FROM omniroute.")) {
        return {
          command: "SELECT",
          rows: [{ occupied: false }],
          rowCount: 1,
        } as PostgresResult<Row>;
      }
      if (request.text.startsWith("INSERT INTO omniroute.routing_imports")) {
        assert.equal(request.values?.length, 2);
        storedReceipt = {
          digest: String(request.values?.[0]),
          rowCount: Number(request.values?.[1]),
        };
      }
      return { command: "SELECT", rows: [], rowCount: 0 } as PostgresResult<Row>;
    },
    release() {},
    on: events.on.bind(events),
    removeListener: events.removeListener.bind(events),
  };
  const pool: PostgresPool = {
    async connect() {
      return client;
    },
    async end() {},
    on: events.on.bind(events),
  };
  const database = new PostgresDatabase(
    pool,
    { maxConnections: 1, transactionTimeoutMs: 500 },
    { warn() {} }
  );
  return {
    database,
    calls,
    setTargetSeedAvailable(value: boolean) {
      targetSeedAvailable = value;
    },
  };
}

test("matching routing receipt reconciles before inspecting a later target epoch", async () => {
  const f = fixture();
  const snapshot: RoutingSnapshot = {
    format: "omniroute-routing-v1",
    proxyAssignmentSequence: 0,
    tables: {
      combos: [],
      model_combo_mappings: [],
      key_value: [
        {
          namespace: "settings",
          key: "managementAuthGeneration",
          value: JSON.stringify("00000000-0000-4000-8000-000000000020"),
        },
      ],
      proxy_registry: [],
      proxy_assignments: [],
      proxy_scope_rotation: [],
    },
  };
  try {
    const imported = await importRoutingSnapshot(f.database, snapshot);
    assert.equal(imported.alreadyImported, false);
    assert.equal(imported.rowCount, 1);
    f.setTargetSeedAvailable(false);
    const start = f.calls.length;
    assert.deepEqual(await importRoutingSnapshot(f.database, snapshot), {
      ...imported,
      alreadyImported: true,
    });
    const replaySql = f.calls
      .slice(start)
      .map(({ text }) => text)
      .filter((text) => text !== "BEGIN" && text !== "COMMIT" && text !== "ROLLBACK");
    assert.deepEqual(replaySql, [
      "SELECT pg_advisory_xact_lock(18765,$1)",
      "SELECT digest,row_count FROM omniroute.routing_imports",
    ]);
  } finally {
    await f.database.close();
  }
});
