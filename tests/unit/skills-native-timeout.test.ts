import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import "../_setup/isolateDataDir.ts";

import {
  PostgresDatabase,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
} from "../../src/lib/db/postgres/database.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { SkillStatus } from "../../src/lib/skills/types.ts";

let skillExecutor: typeof import("../../src/lib/skills/executor.ts").skillExecutor;
let skillRegistry: typeof import("../../src/lib/skills/registry.ts").skillRegistry;
let getSkillExecutionRowAsync: typeof import("../../src/lib/db/skillStorage.ts").getSkillExecutionRowAsync;

type ExecutionRow = Record<string, unknown> & { id: string; status: string };

/** Controlled COMMIT acknowledgments exercise the real native DB owners, without a SQL server. */
function nativeFixture() {
  const events = new EventEmitter();
  let committed = new Map<string, ExecutionRow>();
  let working = new Map<string, ExecutionRow>();
  const statuses: string[] = [];
  const sql: string[] = [];
  let failure: "insert" | "update" | undefined;
  let pendingStatus: string | undefined;
  const client: PostgresClient = {
    async query<Row>(request: Parameters<PostgresClient["query"]>[0]) {
      sql.push(request.text);
      const values = request.values ?? [];
      let rows: unknown[] = [];
      if (request.text === "BEGIN") {
        working = structuredClone(committed);
      } else if (request.text === "COMMIT") {
        committed = working;
        if (pendingStatus) statuses.push(pendingStatus);
        pendingStatus = undefined;
      } else if (request.text === "ROLLBACK") {
        pendingStatus = undefined;
      } else if (request.text.startsWith("INSERT INTO omniroute.skill_executions")) {
        if (failure === "insert") throw new Error("controlled insert rejection");
        working.set(String(values[0]), {
          id: String(values[0]),
          skill_id: values[1],
          api_key_id: values[2],
          session_id: values[3],
          input: values[4],
          status: String(values[5]),
          created_at: values[6],
        });
        pendingStatus = String(values[5]);
      } else if (request.text.startsWith("UPDATE omniroute.skill_executions")) {
        if (failure === "update") throw new Error("controlled finalization rejection");
        const row = working.get(String(values[0]));
        assert.ok(row);
        const fields = request.text.split(" SET ")[1].split(" WHERE ")[0].split(",");
        fields.forEach((field, index) => {
          row[field.split("=")[0]] = values[index + 1];
        });
        pendingStatus = row.status;
      } else if (request.text.includes("FROM omniroute.skill_executions WHERE id=$1")) {
        const row = working.get(String(values[0]));
        if (row) rows = [{ ...row }];
      } else {
        assert.ok(
          request.text.startsWith("SELECT pg_advisory_xact_lock") ||
            request.text.startsWith("INSERT INTO omniroute.skills"),
          `unexpected native statement: ${request.text}`
        );
      }
      return {
        command: ["BEGIN", "COMMIT", "ROLLBACK"].includes(request.text) ? request.text : "SELECT",
        rows,
        rowCount: rows.length,
      } as PostgresResult<Row>;
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
    { maxConnections: 1, transactionTimeoutMs: 2000 },
    { warn() {} }
  );
  const binding = {
    database,
    databaseIdentity: {},
    settings: {
      async get() {
        return { skillsEnabled: true };
      },
    },
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  return {
    database,
    binding,
    statuses,
    sql,
    fail(kind: "insert" | "update") {
      failure = kind;
    },
    running() {
      return [...committed.values()].some((row) => row.status === "running");
    },
  };
}

async function withNativeSkill(
  handler: import("../../src/lib/skills/types.ts").SkillHandler,
  body: (fixture: ReturnType<typeof nativeFixture>, name: string) => Promise<void>
) {
  const fixture = nativeFixture();
  const name = "native-timeout-fixture";
  try {
    await withPostgresControlPlane(fixture.binding, async () => {
      [{ skillExecutor }, { skillRegistry }, { getSkillExecutionRowAsync }] = await Promise.all([
        import("../../src/lib/skills/executor.ts"),
        import("../../src/lib/skills/registry.ts"),
        import("../../src/lib/db/skillStorage.ts"),
      ]);
      skillExecutor.registerHandler(name, handler);
      await skillRegistry.register({
        name,
        schema: { input: {}, output: {} },
        handler: name,
        enabled: true,
        apiKeyId: "native-timeout-owner",
      });
      await body(fixture, name);
    });
  } finally {
    skillExecutor?.setTimeout(30000);
    await fixture.database.close();
  }
}

for (const cooperative of [false, true]) {
  test(`native expired deadline persists timeout with ${cooperative ? "cooperative" : "pending"} handler`, async () => {
    let invocations = 0;
    let aborted = false;
    await withNativeSkill(
      (_input, context) => {
        invocations++;
        return new Promise<Record<string, unknown>>((_resolve, reject) => {
          context.abortSignal?.addEventListener(
            "abort",
            () => {
              aborted = true;
              if (cooperative) reject(new DOMException("handler cancelled", "AbortError"));
            },
            { once: true }
          );
        });
      },
      async (fixture, name) => {
        skillExecutor.setTimeout(5);
        const run = await skillExecutor.execute(name, {}, { apiKeyId: "native-timeout-owner" });
        assert.equal(run.status, SkillStatus.TIMEOUT);
        assert.equal(run.errorMessage, "Skill execution timed out");
        const stored = await getSkillExecutionRowAsync(run.id);
        assert.equal(stored?.status, SkillStatus.TIMEOUT);
        assert.deepEqual(fixture.statuses, ["running", "timeout"]);
        assert.equal(invocations, 1);
        assert.equal(aborted, true);
        assert.ok(fixture.sql.some((sql) => sql.includes("omniroute.skill_executions")));
      }
    );
  });
}

test("native independent AbortError before the deadline remains a handler error", async () => {
  await withNativeSkill(
    async () => {
      throw new DOMException("handler stopped", "AbortError");
    },
    async (fixture, name) => {
      skillExecutor.setTimeout(30000);
      const run = await skillExecutor.execute(name, {}, { apiKeyId: "native-timeout-owner" });
      assert.equal(run.status, SkillStatus.ERROR);
      assert.equal((await getSkillExecutionRowAsync(run.id))?.status, SkillStatus.ERROR);
      assert.deepEqual(fixture.statuses, ["running", "error"]);
    }
  );
});

test("native successful handler clears its losing deadline timer", async () => {
  await withNativeSkill(
    async () => ({ ok: true }),
    async (fixture, name) => {
      const before = process.getActiveResourcesInfo().filter((value) => value === "Timeout").length;
      skillExecutor.setTimeout(30000);
      const run = await skillExecutor.execute(name, {}, { apiKeyId: "native-timeout-owner" });
      assert.equal(run.status, SkillStatus.SUCCESS);
      assert.equal((await getSkillExecutionRowAsync(run.id))?.status, SkillStatus.SUCCESS);
      assert.deepEqual(fixture.statuses, ["running", "success"]);
      assert.ok(
        process.getActiveResourcesInfo().filter((value) => value === "Timeout").length <= before,
        "the settled handler must not retain its 30-second deadline"
      );
    }
  );
});

test("native insert rejection prevents invocation and finalization rejection cannot acknowledge success", async () => {
  for (const failure of ["insert", "update"] as const) {
    let invocations = 0;
    await withNativeSkill(
      async () => {
        invocations++;
        return { ok: true };
      },
      async (fixture, name) => {
        fixture.fail(failure);
        await assert.rejects(skillExecutor.execute(name, {}, { apiKeyId: "native-timeout-owner" }));
        assert.equal(invocations, failure === "insert" ? 0 : 1);
        assert.equal(fixture.running(), failure === "update");
        assert.deepEqual(fixture.statuses, failure === "insert" ? [] : ["running"]);
      }
    );
  }
});
