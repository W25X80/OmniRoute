import assert from "node:assert/strict";
import { spawn as actualSpawn } from "node:child_process";
import { EventEmitter } from "node:events";
import {
  appendFileSync,
  existsSync,
  readFileSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { registerHooks } from "node:module";
import { dirname, isAbsolute, join, relative, sep } from "node:path";
import { PassThrough } from "node:stream";
import { readNativeScratchDataDir } from "../../../src/lib/db/backendSelection.ts";

const runner = new URL("../../../scripts/test/postgres-runtime.mjs", import.meta.url).href;
const scratchRoot = process.env.OMNIROUTE_RUNNER_FIXTURE_ROOT;
const report = process.env.OMNIROUTE_RUNNER_FIXTURE_REPORT;
const events = process.env.OMNIROUTE_RUNNER_FIXTURE_EVENTS;
const container = process.env.OMNIROUTE_RUNNER_FIXTURE_CONTAINER;
const mode = process.env.OMNIROUTE_RUNNER_FIXTURE_MODE;
const phase = process.env.OMNIROUTE_RUNNER_FIXTURE_PHASE;
assert(scratchRoot && report);
const previous = [];
let pendingClose = 0;
let readinessCalls = 0;
const record = (event) => {
  if (events) appendFileSync(events, JSON.stringify(event) + "\n");
};

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runner && specifier === "node:child_process") {
      return { url: import.meta.url, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

// Replace external process ports only. The actual runner and filesystem lifecycle execute.
function commandResult(command, args) {
  assert.equal(pendingClose, 0, "the previous external child must close before another stage");
  assert(
    command === "openssl" ||
      (command === "docker" && ["create", "cp", "start", "exec", "port", "rm"].includes(args[0]))
  );
  record({ event: "command", command, args });
  if (command === "openssl" && mode === "command-output-overflow")
    return { status: 0, stdout: "x".repeat(1024 * 1024 + 1), stderr: "" };
  if (command === "docker" && args.includes("pg_isready")) {
    readinessCalls++;
    if (mode === "readiness-spawn-error" && readinessCalls === 1)
      return {
        status: null,
        stdout: "",
        stderr: "",
        error: new Error("fixture readiness spawn failure"),
      };
  }
  if (command === "docker" && args[0] === "create" && container) writeFileSync(container, args[2]);
  if (command === "docker" && args[0] === "rm") {
    for (const prior of previous) assert.equal(existsSync(prior), false);
    if (container && existsSync(container))
      assert.equal(args.at(-1), readFileSync(container, "utf8"));
    if (mode?.endsWith("cleanup-error")) throw new Error("fixture Docker removal failure");
    if (container && existsSync(container)) unlinkSync(container);
  }
  return { status: 0, stdout: args[0] === "port" ? "127.0.0.1:5432\n" : "", stderr: "" };
}

// Retained for the baseline runner and signal-regression mutation control.
export function spawnSync(command, args) {
  return commandResult(command, args);
}

function suspendedChild(label, dataDir) {
  pendingClose++;
  const child = actualSpawn(
    process.execPath,
    [
      "-e",
      `
      const stop = signal => {
        process.send({ signal });
        if (process.env.OMNIROUTE_RUNNER_FIXTURE_IGNORE_SIGNAL !== "1")
          setTimeout(() => process.exit(1), 150);
      };
      process.on("SIGINT", () => stop("SIGINT"));
      process.on("SIGTERM", () => stop("SIGTERM"));
      process.send({ ready: true });
      setTimeout(() => process.exit(2), 15000);
    `,
    ],
    { stdio: ["ignore", "pipe", "pipe", "ipc"], env: process.env }
  );
  const kill = child.kill.bind(child);
  child.kill = (signal) => {
    record({ event: "kill", pid: child.pid, signal });
    return kill(signal);
  };
  child.on("message", (message) => {
    record({ event: message.ready ? "ready" : "signal", phase: label, pid: child.pid, ...message });
  });
  child.once("close", (code, signal) => {
    pendingClose--;
    if (dataDir) assert.equal(existsSync(dataDir), true, "scratch must survive until child close");
    record({ event: "close", phase: label, pid: child.pid, code, signal, dataDir });
  });
  record({ event: "spawn", phase: label, pid: child.pid, dataDir });
  return child;
}

function completedChild(result, dataDir) {
  pendingClose++;
  const child = new EventEmitter();
  child.kill = () => true;
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  setImmediate(() => {
    if (result.error) child.emit("error", result.error);
    child.stdout.end(result.stdout);
    child.stderr.end(result.stderr);
    child.emit("exit", result.status);
    // Exit is not close: the owner must keep scratch through this independent event.
    setImmediate(() => {
      if (dataDir) assert.equal(existsSync(dataDir), true, "scratch retired before child close");
      pendingClose--;
      child.emit("close", result.status);
    });
  });
  return child;
}

export function spawn(command, args, options = {}) {
  assert.equal(pendingClose, 0, "the previous external child must close before another stage");
  if (command !== process.execPath) {
    const result = commandResult(command, args);
    const label =
      command === "openssl"
        ? "preparation"
        : args[0] === "create"
          ? "create"
          : args.includes("pg_isready")
            ? "readiness"
            : undefined;
    if (label && phase === label) return suspendedChild(label);
    return completedChild(result);
  }
  assert.equal(command, process.execPath);
  assert(args.includes("--test"));
  // Use the production contract, which remains strict when DATA_DIR is absent.
  const dataDir = readNativeScratchDataDir(options.env);
  const within = relative(scratchRoot, dataDir);
  assert(within && !isAbsolute(within) && within !== ".." && !within.startsWith(".." + sep));
  assert.notEqual(dataDir, process.env.DATA_DIR);
  assert.equal(statSync(dataDir).isDirectory(), true);
  assert.equal(dirname(dataDir), dirname(previous[0] ?? dataDir));
  for (const prior of previous) {
    assert.notEqual(dataDir, prior);
    assert.equal(existsSync(prior), false, "the preceding suite scratch must be retired");
  }
  writeFileSync(join(dataDir, "suite-owned"), "scratch is writable");
  previous.push(dataDir);
  appendFileSync(report, JSON.stringify({ dataDir, test: args.at(-1) }) + "\n");
  if (phase === "suite") return suspendedChild("suite", dataDir);
  return completedChild(
    {
      status: mode?.startsWith("spawn-error") ? -1 : mode?.startsWith("nonzero") ? 1 : 0,
      stdout: "",
      stderr: "",
      ...(mode?.startsWith("spawn-error") ? { error: new Error("fixture spawn failure") } : {}),
    },
    dataDir
  );
}
