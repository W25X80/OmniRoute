import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import asyncFs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  runNativeMitmCommand,
  withNativeMitmCancellation,
} from "../../../src/mitm/nativeHostIo.ts";

test("native MITM publication awaits async file I/O while other callbacks progress", async (context) => {
  const { writeBypassJsonAsync } = await import("../../../src/mitm/manager.ts");
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  let release!: () => void;
  let entered!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const writing = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const realWrite = asyncFs.writeFile;
  const mock = context.mock.method(
    asyncFs,
    "writeFile",
    async (...args: Parameters<typeof realWrite>) => {
      entered();
      await blocked;
      return realWrite(...args);
    }
  );
  for (const name of ["writeFileSync", "mkdirSync", "existsSync"] as const)
    context.mock.method(fs, name, () => {
      throw new Error(`Native MITM called ${name}`);
    });
  syncBuiltinESMExports();
  try {
    let settled = false;
    const work = writeBypassJsonAsync(["example.test"]).then(() => {
      settled = true;
    });
    await writing;
    await nextTurn();
    assert.equal(settled, false);
    release();
    await work;
    const payload = JSON.parse(
      await asyncFs.readFile(path.join(process.env.DATA_DIR!, "mitm/bypass.json"), "utf8")
    );
    assert.deepEqual(payload.patterns, ["example.test"]);
    assert.equal(mock.mock.callCount(), 1);
  } finally {
    release();
    context.mock.restoreAll();
    syncBuiltinESMExports();
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});

test("native host subprocess yields and returns only after close on timeout and abort", async () => {
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    let settled = false;
    const command = runNativeMitmCommand(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      timeoutMs: 100,
    });
    void command.then(() => {
      settled = true;
    });
    await nextTurn();
    assert.equal(settled, false);
    const timedOut = await command;
    assert.equal(timedOut.code, null);
    assert.ok(timedOut.error);
    const controller = new AbortController();
    const aborted = withNativeMitmCancellation(controller.signal, () =>
      runNativeMitmCommand(process.execPath, ["-e", "setInterval(() => {}, 1000)"])
    );
    const rejection = assert.rejects(aborted, { name: "AbortError" });
    await nextTurn();
    controller.abort();
    await rejection;
    const healthy = await runNativeMitmCommand(process.execPath, [
      "-e",
      "process.stdout.write('ok')",
    ]);
    assert.equal(healthy.code, 0);
    assert.equal(healthy.stdout, "ok");
  } finally {
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});

test("native agent status uses async detectors rather than synchronous host probes", async (context) => {
  const { getAllAgentsStatusAsync } = await import("../../../src/mitm/manager.ts");
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  let release!: () => void;
  let entered!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const probing = new Promise<void>((resolve) => {
    entered = resolve;
  });
  context.mock.method(asyncFs, "access", async () => {
    entered();
    await blocked;
  });
  context.mock.method(asyncFs, "readdir", async () => ["github.copilot-fixture"]);
  context.mock.method(fs, "existsSync", () => {
    throw new Error("Sync detector called");
  });
  context.mock.method(fs, "readdirSync", () => {
    throw new Error("Sync detector called");
  });
  syncBuiltinESMExports();
  try {
    let settled = false;
    const status = getAllAgentsStatusAsync().then((value) => {
      settled = true;
      return value;
    });
    await probing;
    await nextTurn();
    assert.equal(settled, false);
    release();
    const agents = await status;
    assert.ok(agents.length > 0);
    assert.ok(agents.some((agent) => agent.id === "copilot" && agent.detection.installed));
    assert.ok(
      agents.filter((agent) => agent.id !== "trae").every((agent) => agent.detection.installed)
    );
  } finally {
    release();
    context.mock.restoreAll();
    syncBuiltinESMExports();
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});
