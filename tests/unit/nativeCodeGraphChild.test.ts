import test from "node:test";
import assert from "node:assert/strict";
import { createRequire, syncBuiltinESMExports } from "node:module";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  writeFileSync,
  cpSync,
  rmSync,
  existsSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { fork, type ChildProcess } from "node:child_process";
import { buildSync } from "esbuild";
import {
  bundleCodeGraphWorker,
  colocateCodeGraphDependencies,
} from "../../scripts/build/colocate-standalone.mjs";
import {
  APP_STAGING_ALLOWED_EXACT_PATHS,
  PACK_ARTIFACT_ALLOWED_EXACT_PATHS,
  PACK_ARTIFACT_REQUIRED_PATHS,
  findUnexpectedArtifactPaths,
} from "../../scripts/build/pack-artifact-policy";
import { EXTRA_MODULE_ENTRIES } from "../../scripts/build/assembleStandalone.mjs";
import { getCopilotTool } from "../../src/lib/copilot/tools";
import {
  setCodeGraphPathForTest,
  closeCodeGraphWorkerDatabase,
} from "../../src/lib/copilot/codegraphKnowledge";
import {
  closeNativeCodeGraphQueries,
  resolveCodeGraphQueryWorker,
} from "../../src/lib/copilot/codegraphQueryClient";

const require = createRequire(import.meta.resolve("better-sqlite3"));
const Database = require("better-sqlite3") as typeof import("better-sqlite3");
const directory = mkdtempSync(join(tmpdir(), "omniroute-codegraph-artifact-"));
const originalCwd = process.cwd();
const originalEnv = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  dataDir: process.env.DATA_DIR,
};
const originalGlobalRequire = (globalThis as { require?: NodeRequire }).require;
const projectRoot = resolve(dirname(require.resolve("better-sqlite3/package.json")), "../..");
const workerFile = join(directory, "src/lib/copilot/codegraphQueryWorker.cjs");
function makeDatabase(file: string, rows = 35): void {
  const db = new Database(file);
  try {
    db.exec(`CREATE TABLE nodes (id TEXT, kind TEXT, name TEXT, file_path TEXT, language TEXT, start_line INTEGER, payload BLOB);
      CREATE TABLE edges (id TEXT, kind TEXT, source TEXT, target TEXT, line INTEGER, col INTEGER);
      CREATE TABLE files (path TEXT, language TEXT);
      CREATE VIRTUAL TABLE nodes_fts USING fts5(id UNINDEXED, name);`);
    const node = db.prepare("INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?)");
    const fts = db.prepare("INSERT INTO nodes_fts VALUES (?, ?)");
    for (let index = 0; index < rows; index += 1) {
      node.run(
        String(index),
        "function",
        `symbol${index}`,
        "src/demo.ts",
        "typescript",
        index,
        Buffer.from([1, 2, 3])
      );
      fts.run(String(index), `symbol${index}`);
    }
    db.exec(
      "INSERT INTO edges VALUES ('e1', 'call', '0', '1', 5, 2); INSERT INTO files VALUES ('src/demo.ts', 'typescript');"
    );
  } finally {
    db.close();
  }
}
function hash(file: string): string {
  return createHash("sha256").update(readFileSync(file)).digest("hex");
}
function waitForReply(child: ChildProcess, request: object): Promise<string> {
  return new Promise((resolveOutput, reject) => {
    child.once("error", reject);
    child.once("message", (reply: { output?: string; failed?: boolean }) => {
      if (reply.failed) reject(new Error("readonly child query failed"));
      else resolveOutput(reply.output!);
    });
    child.send(request);
  });
}
test.after(async () => {
  await closeNativeCodeGraphQueries();
  closeCodeGraphWorkerDatabase();
  process.chdir(originalCwd);
  for (const [name, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", originalEnv.backend],
    ["DATA_DIR", originalEnv.dataDir],
  ] as const) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  if (originalGlobalRequire === undefined) delete (globalThis as { require?: NodeRequire }).require;
  else (globalThis as { require?: NodeRequire }).require = originalGlobalRequire;
  rmSync(directory, { recursive: true, force: true });
});

test("actual CJS readonly child preserves all six tool projections and closes without main SQLite", async () => {
  const staged = join(directory, "stage");
  const files = [
    "src/lib/copilot/codegraphQueryWorker.ts",
    "src/lib/copilot/codegraphKnowledge.ts",
    "src/lib/copilot/codegraphResult.ts",
    "src/lib/copilot/codegraphQueryProtocol.ts",
    "src/lib/buildPhase.ts",
  ];
  for (const path of files) {
    const source = fileURLToPath(import.meta.resolve(`../../${path}`));
    const destination = join(staged, path);
    mkdirSync(dirname(destination), { recursive: true });
    cpSync(source, destination, { dereference: true });
  }
  writeFileSync(join(staged, "package.json"), '{"type":"module"}\n');
  bundleCodeGraphWorker(workerFile, (tool: string, packageName: string, argv: string[]) => {
    assert.equal(tool, "esbuild");
    assert.equal(packageName, "esbuild");
    assert.ok(argv.includes("--format=cjs") && argv.includes("--packages=external"));
    buildSync({
      entryPoints: [join(staged, files[0])],
      bundle: true,
      platform: "node",
      packages: "external",
      format: "cjs",
      outfile: workerFile,
    });
  });
  // Give the packaged child a real independent dependency tree; no symlinks, installed mutation or loaders.
  const targetModules = join(directory, "node_modules");
  mkdirSync(join(targetModules, "better-sqlite3"), { recursive: true });
  colocateCodeGraphDependencies(join(projectRoot, "node_modules"), targetModules);
  for (const name of ["better-sqlite3", "bindings", "file-uri-to-path"]) {
    assert.ok(
      EXTRA_MODULE_ENTRIES.some(
        (entry: { dest: string[] }) => entry.dest.join("/") === `node_modules/${name}`
      )
    );
    assert.ok(existsSync(join(targetModules, name, "package.json")));
  }
  assert.deepEqual(
    findUnexpectedArtifactPaths(["src/lib/copilot/codegraphQueryWorker.cjs"], {
      exactPaths: APP_STAGING_ALLOWED_EXACT_PATHS,
    }),
    []
  );
  assert.deepEqual(
    findUnexpectedArtifactPaths(["dist/src/lib/copilot/codegraphQueryWorker.cjs"], {
      exactPaths: PACK_ARTIFACT_ALLOWED_EXACT_PATHS,
    }),
    []
  );
  assert.ok(PACK_ARTIFACT_REQUIRED_PATHS.includes("dist/src/lib/copilot/codegraphQueryWorker.cjs"));
  const file = join(directory, "graph.db");
  makeDatabase(file);
  const before = hash(file);
  (globalThis as { require?: NodeRequire }).require = require;
  delete process.env.OMNIROUTE_DATABASE_BACKEND;
  setCodeGraphPathForTest(file);
  const cases: Array<[string, Record<string, unknown>]> = [
    ["searchCodeGraph", { query: "symbol1" }],
    ["findCallers", { symbol: "symbol1" }],
    ["findCallees", { symbol: "symbol0" }],
    ["getFileContext", { filePath: "demo.ts" }],
    ["listCodeGraphFiles", { language: "typescript" }],
    ["codeGraphStats", {}],
  ];
  const expected = [];
  for (const [name, args] of cases) expected.push(await getCopilotTool(name)!.handler(args));
  assert.match(expected[3], /"type": "Buffer"/);
  assert.match(expected[3], /and 5 more/);
  closeCodeGraphWorkerDatabase();

  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = join(directory, "main-control-plane-must-stay-empty");
  process.chdir(directory);
  const resolved = await resolveCodeGraphQueryWorker();
  assert.equal(realpathSync(resolved.workerFile), realpathSync(workerFile));
  assert.deepEqual(resolved.execArgv, [], "real packaged CJS uses no tsx loader");
  const loader = require("node:module") as {
    _load: (name: string, parent: unknown, isMain: boolean) => unknown;
  };
  const originalLoad = loader._load;
  let parentOpens = 0;
  loader._load = function (name, parent, isMain) {
    if (name === "better-sqlite3") {
      parentOpens += 1;
      throw new Error("parent SQLite forbidden");
    }
    return originalLoad.call(this, name, parent, isMain);
  };
  const childProcess = require("node:child_process") as typeof import("node:child_process");
  const originalFork = childProcess.fork;
  const children: ChildProcess[] = [];
  let failedClose: Promise<void> | null = null;
  childProcess.fork = ((...args: Parameters<typeof fork>) => {
    const child =
      children.length === 0
        ? originalFork(args[0], args[1], { ...args[2], execPath: join(directory, "missing-node") })
        : originalFork(...args);
    if (children.length === 0)
      failedClose = new Promise((done) => child.once("close", () => done()));
    children.push(child);
    return child;
  }) as typeof fork;
  syncBuiltinESMExports();
  let ticks = 0;
  const interval = setInterval(() => {
    ticks += 1;
  }, 1);
  try {
    await assert.rejects(
      getCopilotTool("codeGraphStats")!.handler({}),
      /CodeGraph worker is unavailable/
    );
    await failedClose;
    for (let index = 0; index < cases.length; index += 1) {
      const [name, args] = cases[index];
      assert.equal(await getCopilotTool(name)!.handler(args), expected[index]);
    }
    assert.equal(
      children.length,
      2,
      "one failed spawn is followed by one persistent readonly child"
    );
    assert.equal(children[0].pid, undefined);
    assert.ok(children[1].pid !== process.pid);
    assert.ok(ticks > 0, "the parent event loop advances during actual child work");
    assert.equal(parentOpens, 0);
    assert.equal(existsSync(join(process.env.DATA_DIR!, "storage.sqlite")), false);
    assert.equal(hash(file), before, "readonly operations preserve index bytes");
    setCodeGraphPathForTest(null);
    assert.match(
      await getCopilotTool("codeGraphStats")!.handler({}),
      /^CodeGraph not available: CodeGraph DB not found\./
    );
    const future = join(directory, "future.db");
    setCodeGraphPathForTest(future);
    assert.match(
      await getCopilotTool("codeGraphStats")!.handler({}),
      /better-sqlite3 not available/
    );
    makeDatabase(future, 3);
    assert.match(await getCopilotTool("codeGraphStats")!.handler({}), /"value": 3/);
    assert.equal(parentOpens, 0, "a missing index retries only inside the child");
    const relativeDirectory = join(directory, "relative");
    mkdirSync(relativeDirectory);
    makeDatabase(join(relativeDirectory, "index.db"), 4);
    process.chdir(relativeDirectory);
    setCodeGraphPathForTest("index.db");
    assert.match(await getCopilotTool("codeGraphStats")!.handler({}), /"value": 4/);
    process.chdir(directory);
    const pid = children[1].pid!;
    await closeNativeCodeGraphQueries();
    assert.throws(() => process.kill(pid, 0), /ESRCH/);
    await assert.rejects(
      getCopilotTool("codeGraphStats")!.handler({}),
      /CodeGraph worker is unavailable/
    );
  } finally {
    clearInterval(interval);
    loader._load = originalLoad;
    childProcess.fork = originalFork;
    syncBuiltinESMExports();
  }
});

test("source/dev TS child uses the same readonly queries with worker-only createRequire", async () => {
  const sourceEntry = join(directory, "stage/src/lib/copilot/codegraphQueryWorker.ts");
  const fixture = join(directory, "dev.db");
  makeDatabase(fixture, 2);
  const child = fork(sourceEntry, [], {
    cwd: projectRoot,
    execArgv: ["--import", "tsx/esm"],
    stdio: ["ignore", "ignore", "pipe", "ipc"],
    env: { ...process.env, OMNIROUTE_DATABASE_BACKEND: "postgres" },
  });
  let stderr = "";
  child.stderr?.on("data", (value) => {
    stderr += String(value);
  });
  try {
    const output = await waitForReply(child, {
      kind: "query",
      id: 1,
      operation: "getCodeGraphStats",
      args: [],
      context: {
        revision: 1,
        cwd: directory,
        moduleDirectory: dirname(sourceEntry),
        pathOverride: fixture,
      },
    });
    assert.match(output, /"value": 2/, stderr);
    await new Promise<void>((done) => {
      child.once("exit", () => done());
      child.send({ kind: "close" });
    });
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>((done) => child.once("exit", () => done()));
      child.kill("SIGKILL");
      await exited;
    }
  }
});

test("native Copilot CLI lookup is asynchronous and retains the source read-only command gate", async () => {
  const originalPath = process.env.PATH;
  const childProcess = require("node:child_process") as typeof import("node:child_process");
  const originalSync = childProcess.execSync;
  const cli = join(directory, "omniroute");
  const launched = join(directory, "cli-launched");
  writeFileSync(
    cli,
    `#!${process.execPath}\nrequire("node:fs").appendFileSync(${JSON.stringify(launched)}, "run"); console.log("healthy");\n`,
    { mode: 0o700 }
  );
  writeFileSync(
    join(directory, "which"),
    `#!${process.execPath}\nsetTimeout(() => console.log(${JSON.stringify(cli)}), 60);\n`,
    { mode: 0o700 }
  );
  process.env.PATH = directory;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  childProcess.execSync = (() => {
    throw new Error("native synchronous CLI lookup forbidden");
  }) as typeof childProcess.execSync;
  syncBuiltinESMExports();
  let ticks = 0;
  const interval = setInterval(() => {
    ticks += 1;
  }, 1);
  try {
    const tool = getCopilotTool("runOmniRouteCli")!;
    assert.equal(await tool.handler({ command: "health" }), "```\nhealthy\n```");
    assert.ok(ticks > 0);
    const before = readFileSync(launched, "utf8");
    assert.match(await tool.handler({ command: "set-budget 10" }), /blocked/);
    assert.equal(
      readFileSync(launched, "utf8"),
      before,
      "mutating commands remain rejected before CLI execution"
    );
  } finally {
    clearInterval(interval);
    childProcess.execSync = originalSync;
    syncBuiltinESMExports();
    if (originalPath === undefined) delete process.env.PATH;
    else process.env.PATH = originalPath;
  }
});
