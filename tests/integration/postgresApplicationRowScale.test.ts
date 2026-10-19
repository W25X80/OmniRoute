import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { spawn } from "node:child_process";
import Database from "better-sqlite3";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
const enabled = postgresTestsEnabled && process.env.RUN_PG_IMPORT_MAX_ROWS === "1";

type ImportReceipt = {
  rowCount: number;
  tableCounts: Record<string, number>;
  transformations: {
    table: string;
    column: string;
    row: number;
    source: unknown;
    target: unknown;
  }[];
  alreadyImported: boolean;
  planDigest: string;
};

test(
  "offline CLI preserves 2600000 history rows with bounded carriers and reconciles the receipt",
  { skip: !enabled, timeout: 3600000 },
  async (t) => {
    const root = await mkdtemp(path.join(os.tmpdir(), "omniroute-row-scale-"));
    const source = path.join(root, "source.sqlite"),
      bundle = path.join(root, "bundle", "bundle.json");
    const raw = new Database(source);
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    let completed = false;
    try {
      raw.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,parent_id TEXT,role TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',last_correlation_id TEXT,first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<2600000)
INSERT INTO conversation_turn_nodes SELECT printf('%064d',v),printf('%064d',1),printf('%064d',v-1),'assistant',printf('%064d',v),'request-correlation','2026-10-12T12:00:00.000Z','2026-10-12T12:00:00.000Z' FROM n;`);
      raw.close();
      database = await createTestPostgresDatabase({ transactionTimeoutMs: 1800000 });
      await migratePostgresSchema(database);
      const generationRows = await database.transaction((tx) =>
        tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
        )
      );
      assert.equal(generationRows.rows.length, 1);
      const targetGeneration = generationRows.rows[0].value;
      await import("node:fs/promises").then((fs) =>
        fs.mkdir(path.dirname(bundle), { mode: 0o700 })
      );
      const env = {
        ...process.env,
        STORAGE_ENCRYPTION_KEY: "isolated-scale-encryption",
        OMNIROUTE_POSTGRES_HOST: "localhost",
        OMNIROUTE_POSTGRES_PORT: process.env.OMNIROUTE_TEST_POSTGRES_PORT,
        OMNIROUTE_POSTGRES_DATABASE: "omniroute_test",
        OMNIROUTE_POSTGRES_USER: "omniroute_test",
        OMNIROUTE_POSTGRES_PASSWORD: "omniroute_test",
        OMNIROUTE_POSTGRES_TLS: "verify-full",
        OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
        OMNIROUTE_POSTGRES_STATEMENT_TIMEOUT_MS: "60000",
        OMNIROUTE_POSTGRES_QUERY_TIMEOUT_MS: "90000",
        OMNIROUTE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS: "90000",
        OMNIROUTE_POSTGRES_TRANSACTION_TIMEOUT_MS: "1800000",
        NODE_OPTIONS: "--max-old-space-size=4096",
      };
      async function cli(action: "capture"): Promise<unknown>;
      async function cli(action: "import" | "verify"): Promise<ImportReceipt>;
      async function cli(action: "capture" | "import" | "verify"): Promise<unknown> {
        const args = [
          "--import",
          "tsx/esm",
          "scripts/ops/postgres-migrate-storage.ts",
          "--action",
          action,
          "--offline",
          "--large-rows",
          "--bundle",
          bundle,
        ];
        if (action === "capture") args.push("--source", source);
        let output = "";
        const start = Date.now();
        await new Promise<void>((resolve, reject) => {
          let failure: Error | undefined;
          let escalation: ReturnType<typeof setTimeout> | undefined;
          const child = spawn(process.execPath, args, { env, stdio: ["ignore", "pipe", "pipe"] });
          const stop = () => {
            child.kill("SIGTERM");
            escalation ??= setTimeout(() => child.kill("SIGKILL"), 1000);
          };
          const abort = () => {
            failure ??= new Error("scale witness cancelled");
            stop();
          };
          t.signal.addEventListener("abort", abort, { once: true });
          if (t.signal.aborted) abort();
          const deadline = setTimeout(() => {
            failure ??= new Error("scale command deadline");
            stop();
          }, 1900000);
          for (const stream of [child.stdout, child.stderr])
            stream.on("data", (chunk) => {
              if (Buffer.byteLength(output) + chunk.byteLength <= 100000) output += chunk;
              else {
                failure ??= new Error("output budget");
                stop();
              }
            });
          child.once("error", (e) => {
            failure ??= e;
          });
          child.once("close", (code) => {
            clearTimeout(deadline);
            clearTimeout(escalation);
            t.signal.removeEventListener("abort", abort);
            if (failure) reject(failure);
            else if (code === 0) resolve();
            else reject(Error(output));
          });
        });
        console.log(JSON.stringify({ scaleAction: action, milliseconds: Date.now() - start }));
        const lines = output.trim().split("\n");
        const receipt = lines.at(-1);
        if (!receipt) throw new Error("scale command returned no JSON result");
        return JSON.parse(receipt);
      }
      await cli("capture");
      const initial = await cli("import");
      assert.equal(initial.rowCount, 2600001);
      assert.equal(initial.tableCounts.conversation_turn_nodes, 2600000);
      assert.equal(initial.tableCounts.key_value, 1);
      assert.equal(
        initial.rowCount,
        Object.values(initial.tableCounts).reduce((sum, count) => sum + count, 0)
      );
      assert.deepEqual(
        initial.transformations.filter(
          (change) => change.table === "key_value" && change.column === "value"
        ),
        [
          {
            table: "key_value",
            row: 0,
            column: "value",
            source: null,
            target: targetGeneration,
          },
        ]
      );
      assert.deepEqual(
        (
          await database.transaction((tx) =>
            tx.query<{ namespace: string; key: string; value: string }>(
              "SELECT namespace,key,value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
            )
          )
        ).rows,
        [
          {
            namespace: "settings",
            key: "managementAuthGeneration",
            value: targetGeneration,
          },
        ]
      );
      assert.equal(initial.alreadyImported, false);
      const again = await cli("import");
      assert.equal(again.alreadyImported, true);
      assert.equal(again.planDigest, initial.planDigest);
      const verified = await cli("verify");
      assert.equal(verified.planDigest, initial.planDigest);
      console.log(JSON.stringify({ scaleRows: 2600000, allActionsAcknowledged: true }));
      completed = true;
    } finally {
      if (raw.open) raw.close();
      await database?.close();
      await rm(root, { recursive: true, force: true });
    }
    assert.ok(completed);
  }
);
