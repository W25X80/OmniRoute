import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { appendFileSync, existsSync, statSync, writeFileSync } from "node:fs";
import { registerHooks } from "node:module";
import { dirname, isAbsolute, join, relative, sep } from "node:path";
import { readNativeScratchDataDir } from "../../../src/lib/db/backendSelection.ts";

const runner = new URL("../../../scripts/test/postgres-runtime.mjs", import.meta.url).href;
const scratchRoot = process.env.OMNIROUTE_RUNNER_FIXTURE_ROOT;
const report = process.env.OMNIROUTE_RUNNER_FIXTURE_REPORT;
assert(scratchRoot && report);
const previous = [];

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runner && specifier === "node:child_process") {
      return { url: import.meta.url, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

// Replace external process ports only. The actual runner and filesystem lifecycle execute.
export function spawnSync(command, args) {
  assert(
    command === "openssl" ||
      (command === "docker" && ["create", "cp", "start", "exec", "port", "rm"].includes(args[0]))
  );
  if (command === "docker" && args[0] === "rm") {
    for (const prior of previous) assert.equal(existsSync(prior), false);
    if (process.env.OMNIROUTE_RUNNER_FIXTURE_MODE === "cleanup-error")
      throw new Error("fixture Docker removal failure");
  }
  return { status: 0, stdout: args[0] === "port" ? "127.0.0.1:5432\n" : "", stderr: "" };
}

export function spawn(command, args, options) {
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
  const child = new EventEmitter();
  setImmediate(() => {
    if (process.env.OMNIROUTE_RUNNER_FIXTURE_MODE === "spawn-error") {
      child.emit("error", new Error("fixture spawn failure"));
      child.emit("close", -1);
    } else {
      const code = process.env.OMNIROUTE_RUNNER_FIXTURE_MODE === "nonzero" ? 1 : 0;
      child.emit("exit", code);
      child.emit("close", code);
    }
  });
  return child;
}
