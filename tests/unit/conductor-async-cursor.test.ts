import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers/promises";
import { A2ATaskManager } from "../../src/lib/a2a/taskManager.ts";
import { createConductorBridge } from "../../src/lib/conductor/bridge.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";

const frames = new TextEncoder().encode(
  'id: 1\nevent: task.created\ndata: {"payload":{"task_id":"one"}}\n\n' +
    'id: 2\nevent: task.completed\ndata: {"payload":{"task_id":"one"}}\n\n'
);
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function manager() {
  return new A2ATaskManager(5, {
    upsert() {},
    appendEvent() {},
    purge() {
      return 0;
    },
  });
}

test("SSE awaits durable task history and each cursor write before processing the next frame", async () => {
  const history = deferred();
  const cursorWrite = deferred();
  const taskManager = manager();
  let historyCalls = 0;
  taskManager.flushPersistence = () => (++historyCalls === 1 ? history.promise : Promise.resolve());
  const cursors: string[] = [];
  const bridge = createConductorBridge({
    hubUrl: "https://fixture.invalid",
    token: "fixture",
    tm: taskManager,
    cursor: {
      async get() {
        return "0";
      },
      async set(value) {
        cursors.push(value);
        if (value === "1") await cursorWrite.promise;
      },
    },
    async fetchImpl() {
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(frames);
          },
        })
      );
    },
    log() {},
  });
  try {
    bridge.start();
    await setImmediate();
    assert.equal(taskManager.listTasks()[0].state, "submitted");
    assert.deepEqual(cursors, []);
    history.resolve();
    await setImmediate();
    assert.deepEqual(cursors, ["1"]);
    assert.equal(taskManager.listTasks()[0].state, "submitted");
    cursorWrite.resolve();
    await setImmediate();
    assert.deepEqual(cursors, ["1", "2"]);
    assert.equal(taskManager.listTasks()[0].state, "completed");
  } finally {
    bridge.stop();
    taskManager.destroy();
  }
});

test("unknown cursor commit stops the SSE bridge without replay or later cursor advancement", async () => {
  const taskManager = manager();
  let fetches = 0;
  let canceled = 0;
  const cursors: string[] = [];
  const bridge = createConductorBridge({
    hubUrl: "https://fixture.invalid",
    token: "fixture",
    tm: taskManager,
    cursor: {
      get() {
        return "0";
      },
      async set(value) {
        cursors.push(value);
        throw new PostgresError("commit_unknown");
      },
    },
    async fetchImpl() {
      fetches++;
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(frames);
          },
          cancel() {
            canceled++;
          },
        })
      );
    },
    log() {},
    backoffBaseMs: 1,
  });
  try {
    bridge.start();
    await setImmediate();
    await setImmediate();
    assert.equal(bridge.state(), "stopped");
    assert.deepEqual(cursors, ["1"]);
    assert.equal(fetches, 1);
    assert.equal(canceled, 1);
    assert.equal(taskManager.listTasks()[0].state, "submitted");
  } finally {
    bridge.stop();
    taskManager.destroy();
  }
});
