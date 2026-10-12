import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import {
  A2ATaskManager,
  getTaskManager,
  type A2APersistence,
} from "../../src/lib/a2a/taskManager.ts";
import { runWithServerToolFence } from "../../src/lib/skills/toolExecutionFence.ts";
import {
  registerHook,
  getActiveEvents,
  resetHooks,
  runOnRequest,
} from "../../src/lib/plugins/hooks.ts";
import { withPluginExecutionGuard } from "../../src/lib/plugins/executionGuard.ts";
import { createValidationPluginFence } from "../../src/lib/modelValidation/pluginFence.ts";

function binding(database: PostgresDatabase): PostgresControlPlaneBinding {
  return { database, databaseIdentity: {}, assertReady() {} } as PostgresControlPlaneBinding;
}
function databaseWithQuery(query: PostgresTransaction["query"]): PostgresDatabase {
  return {
    transaction: async <T>(body: (tx: PostgresTransaction) => Promise<T>) => body({ query }),
  } as PostgresDatabase;
}

test("successful tool handler is never finalized twice after unknown commit", async () => {
  let handlerCalls = 0,
    finalizations = 0;
  const database = databaseWithQuery(async <Row>(sql: string) => {
    if (sql.startsWith("UPDATE omniroute.server_tool_executions")) {
      finalizations++;
      throw new PostgresError("commit_unknown");
    }
    return { rows: [{ id: "claimed" }] as Row[], rowCount: 1, command: "INSERT" };
  });
  await withPostgresControlPlane(binding(database), async () => {
    await assert.rejects(
      runWithServerToolFence({
        apiKeyId: "owner",
        requestIdentity: randomUUID(),
        toolCallId: "call",
        toolName: "write",
        arguments: { value: 1 },
        leaseDurationMs: 1000,
        async execute() {
          handlerCalls++;
          return { success: true };
        },
      }),
      { kind: "commit_unknown" }
    );
  });
  assert.equal(handlerCalls, 1);
  assert.equal(finalizations, 1);
});

test("native task persistence freezes transition rows and barriers stop cursor acknowledgment", async () => {
  const rows: string[] = [];
  let release!: () => void;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  const failure = new PostgresError("commit_unknown");
  const persistence: A2APersistence = {
    upsert() {},
    appendEvent() {},
    purge() {
      return 0;
    },
    async persist(row) {
      rows.push(row.state);
      if (rows.length === 1) await pending;
      else throw failure;
    },
  };
  const database = databaseWithQuery(async () => {
    throw new Error("unused");
  });
  await withPostgresControlPlane(binding(database), async () => {
    const manager = new A2ATaskManager(5, persistence);
    try {
      const task = manager.createTask({ skill: "smart-routing", messages: [] });
      manager.updateTask(task.id, "working");
      assert.deepEqual(rows, ["submitted"]);
      release();
      await assert.rejects(manager.flushPersistence(), (err) => err === failure);
      assert.deepEqual(rows, ["submitted", "working"]);
      await assert.rejects(manager.flushPersistence(), (err) => err === failure);
    } finally {
      manager.destroy();
    }
  });
});

test("unrelated planes do not share live task or hook state", () => {
  const database = databaseWithQuery(async () => {
    throw new Error("unused");
  });
  const a = binding(database),
    b = binding(database);
  let managerA!: A2ATaskManager, managerB!: A2ATaskManager;
  try {
    withPostgresControlPlane(a, () => {
      managerA = getTaskManager();
      registerHook("onRequest", "plane-a", () => {});
      assert.deepEqual(getActiveEvents(), ["onRequest"]);
    });
    withPostgresControlPlane(b, () => {
      managerB = getTaskManager();
      assert.deepEqual(getActiveEvents(), []);
    });
    assert.notEqual(managerA, managerB);
    withPostgresControlPlane(a, () => assert.deepEqual(getActiveEvents(), ["onRequest"]));
  } finally {
    withPostgresControlPlane(a, resetHooks);
    withPostgresControlPlane(b, resetHooks);
    managerA?.destroy();
    managerB?.destroy();
  }
});

test("a failed native task does not poison unrelated tasks or hide failures from other waiters", async () => {
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const failure = new PostgresError("overloaded");
  const saved: string[] = [];
  let failedId: string | undefined;
  const persistence: A2APersistence = {
    upsert() {},
    appendEvent() {},
    purge() {
      return 0;
    },
    async persist(row) {
      if (!failedId) failedId = row.id;
      if (row.id === failedId) {
        await blocked;
        throw failure;
      }
      saved.push(row.id);
    },
  };
  const database = databaseWithQuery(async () => {
    throw new Error("unused");
  });
  await withPostgresControlPlane(binding(database), async () => {
    const manager = new A2ATaskManager(5, persistence);
    try {
      const bad = manager.createTask({ skill: "smart-routing", messages: [] });
      const first = assert.rejects(manager.flushPersistence(bad.id), (error) => error === failure);
      const second = assert.rejects(manager.flushPersistence(bad.id), (error) => error === failure);
      const good = manager.createTask({ skill: "smart-routing", messages: [] });
      const healthy = manager.flushPersistence(good.id);
      release();
      await Promise.all([first, second, healthy]);
      assert.deepEqual(saved, [good.id]);
      manager.updateTask(bad.id, "working");
      await assert.rejects(manager.flushPersistence(bad.id), (error) => error === failure);
      assert.deepEqual(saved, [good.id], "failed/uncertain task transition is not replayed");
      manager.updateTask(good.id, "working");
      await manager.flushPersistence(good.id);
      assert.deepEqual(saved, [good.id, good.id]);
    } finally {
      release();
      manager.destroy();
    }
  });
});

test("strict plugin policy awaits a live native read before dispatch", async () => {
  let release!: () => void,
    reads = 0,
    dispatches = 0;
  const delay = new Promise<void>((resolve) => {
    release = resolve;
  });
  const database = databaseWithQuery(async <Row>(sql: string) => {
    if (sql.includes("FROM omniroute.plugins")) {
      reads++;
      await delay;
      return { rows: [{ name: "newly-active" }] as Row[], rowCount: 1, command: "SELECT" };
    }
    return { rows: [] as Row[], rowCount: 1, command: "SELECT" };
  });
  await withPostgresControlPlane(binding(database), async () => {
    const fence = createValidationPluginFence();
    const request = withPluginExecutionGuard(fence, async () => {
      await runOnRequest({
        requestId: "strict",
        body: {},
        model: "m",
        provider: "p",
        metadata: {},
      });
      dispatches++;
    });
    await Promise.resolve();
    assert.equal(dispatches, 0);
    release();
    await assert.rejects(request, { code: "VALIDATION_PLUGINS_UNSUPPORTED" });
    assert.equal(reads, 1);
    assert.equal(dispatches, 0);
    await assert.rejects(fence.assertIdleAsync(), { code: "VALIDATION_PLUGINS_UNSUPPORTED" });
  });
});

test("unknown terminal task commit cannot produce a second failed-state write", async () => {
  const { executeA2ATaskWithState } = await import("../../src/lib/a2a/taskExecution.ts");
  const states: string[] = [];
  const failure = new PostgresError("commit_unknown");
  const database = databaseWithQuery(async () => {
    throw new Error("unused");
  });
  const persistence: A2APersistence = {
    upsert() {},
    appendEvent() {},
    purge() {
      return 0;
    },
    async persist(row) {
      states.push(row.state);
      if (row.state === "completed") throw failure;
    },
  };
  await withPostgresControlPlane(binding(database), async () => {
    const manager = new A2ATaskManager(5, persistence);
    try {
      const task = manager.createTask({ skill: "smart-routing", messages: [] });
      manager.updateTask(task.id, "working");
      await manager.flushPersistence();
      await assert.rejects(
        executeA2ATaskWithState(manager, task, async () => ({ artifacts: [], metadata: {} })),
        (err) => err === failure
      );
      assert.deepEqual(states, ["submitted", "working", "completed"]);
    } finally {
      manager.destroy();
    }
  });
});

test("native plugin bootstrap failure cannot become an empty-policy request", async () => {
  const database = databaseWithQuery(async <Row>(sql: string) => {
    if (sql.includes("FROM omniroute.plugins")) throw new PostgresError("unavailable");
    return { rows: [] as Row[], rowCount: 0, command: "SELECT" };
  });
  await withPostgresControlPlane(binding(database), async () => {
    await assert.rejects(
      runOnRequest({ requestId: "native-boot", body: {}, model: "m", provider: "p", metadata: {} }),
      { kind: "unavailable" }
    );
  });
});

test("runtime task shutdown waits for admitted writes and removes the interval owner", async () => {
  const { shutdownA2ATaskManagerRuntime } = await import("../../src/lib/a2a/taskManager.ts");
  let entered!: () => void, release!: () => void;
  const writing = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  const database = databaseWithQuery(async <Row>(sql: string) => {
    if (sql.startsWith("INSERT INTO omniroute.a2a_tasks")) {
      entered();
      await pending;
    }
    return { rows: [] as Row[], rowCount: 1, command: "INSERT" };
  });
  await withPostgresControlPlane(binding(database), async () => {
    const manager = getTaskManager();
    manager.createTask({ skill: "smart-routing", messages: [] });
    await writing;
    let stopped = false;
    const shutdown = shutdownA2ATaskManagerRuntime().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    assert.equal(stopped, false);
    release();
    await shutdown;
    const replacement = getTaskManager();
    assert.notEqual(replacement, manager);
    replacement.destroy();
  });
});

test("runtime plugin shutdown awaits every child and clears scoped hooks", async () => {
  const { pluginManager, shutdownPluginRuntime } = await import("../../src/lib/plugins/manager.ts");
  const database = databaseWithQuery(async () => {
    throw new Error("unused");
  });
  await withPostgresControlPlane(binding(database), async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    const loaded = (
      pluginManager as unknown as {
        loadedPlugins: Map<string, { cleanup: () => void; cleanupAsync: () => Promise<void> }>;
      }
    ).loadedPlugins;
    loaded.set("delayed-child", { cleanup() {}, cleanupAsync: () => pending });
    registerHook("onRequest", "delayed-child", () => {});
    let stopped = false;
    const shutdown = shutdownPluginRuntime().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    assert.equal(stopped, false);
    release();
    await shutdown;
    assert.deepEqual(getActiveEvents(), []);
    assert.equal(loaded.size, 0);
  });
});
