import assert from "node:assert/strict";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { isNoLogAsync, isNoLog, setNoLog } from "../../src/lib/compliance/noLog.ts";
import {
  logToolCall,
  queryAuditEntries,
  closeAuditDb,
  __setAuditCallerIdResolverForTests,
} from "../../open-sse/mcp-server/audit.ts";

test("PostgreSQL audit selection bypasses native SQLite and preserves durable privacy reads", async (t) => {
  const statements: string[] = [];
  let noLog = 1;
  let fail = false;
  const transaction: PostgresTransaction = {
    async query<Row>(sql: string) {
      statements.push(sql);
      if (fail) throw new Error("database unavailable");
      const rows = sql.includes("SELECT no_log") ? [{ no_log: noLog }] : [];
      return {
        rows: rows as Row[],
        rowCount: 1,
        command: sql.includes("INSERT") ? "INSERT" : "SELECT",
      };
    },
  };
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      return body(transaction);
    },
  } as unknown as PostgresDatabase;
  const binding = { database, assertReady() {} } as unknown as PostgresControlPlaneBinding;
  let sqliteTouched = false;
  globalThis.__omnirouteMcpAuditDb = {
    prepare() {
      sqliteTouched = true;
      throw new Error("unexpected SQLite audit access");
    },
    pragma() {
      sqliteTouched = true;
    },
    close() {
      sqliteTouched = true;
    },
    open: true,
  };
  __setAuditCallerIdResolverForTests(async () => "privacy-key");
  t.after(() => {
    globalThis.__omnirouteMcpAuditDb = undefined;
    __setAuditCallerIdResolverForTests(null);
  });
  await withPostgresControlPlane(binding, async () => {
    setNoLog("privacy-key", false);
    assert.equal(await isNoLogAsync("privacy-key"), true);
    assert.throws(() => isNoLog("privacy-key"), { name: "UnsupportedPersistenceDomainError" });
    noLog = 0;
    assert.equal(await isNoLogAsync("privacy-key"), false);
    // Administrative/MCP audit is intentionally unaffected by the traffic noLog option.
    noLog = 1;
    await logToolCall("omniroute_get_health", { rawPrompt: "not stored" }, "ok", 1, true);
    assert.ok(statements.some((sql) => sql.includes("INSERT INTO omniroute.mcp_tool_audit")));
    assert.equal(closeAuditDb(), false);
    fail = true;
    await assert.rejects(isNoLogAsync("privacy-key"));
    await assert.rejects(queryAuditEntries());
  });
  assert.equal(sqliteTouched, false);
});
