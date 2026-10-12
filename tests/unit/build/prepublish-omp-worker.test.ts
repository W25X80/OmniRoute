import assert from "node:assert/strict";
import { execFileSync, fork } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import Database from "better-sqlite3";
import ts from "typescript";
import { colocateCodeGraphDependencies } from "../../../scripts/build/colocate-standalone.mjs";

// Exercise the actual unconditional prepublish call with a cold stage, then execute its
// generated carrier without tsx or source fallback. The broad Next build is a separate gate.
test("cold prepublish staging emits an executable OMP carrier", { timeout: 20_000 }, async () => {
  const source = ts.createSourceFile(
    "prepublish.ts",
    readFileSync(new URL("../../../scripts/build/prepublish.ts", import.meta.url), "utf8"),
    ts.ScriptTarget.ES2022,
    true
  );
  const calls = source.statements.filter(
    (statement): statement is ts.ExpressionStatement =>
      ts.isExpressionStatement(statement) &&
      ts.isCallExpression(statement.expression) &&
      ts.isIdentifier(statement.expression.expression) &&
      statement.expression.expression.text === "bundleOmpHostWorker"
  );
  assert.equal(calls.length, 1, "prepublish must stage OMP without relying on npm postbuild");
  const call = calls[0];
  const assembly = source.statements.find(
    (statement) =>
      ts.isExpressionStatement(statement) &&
      ts.isCallExpression(statement.expression) &&
      ts.isIdentifier(statement.expression.expression) &&
      statement.expression.expression.text === "assembleStandalone"
  );
  const prune = source.statements.find(
    (statement) =>
      ts.isVariableStatement(statement) &&
      statement.declarationList.declarations.some(
        (declaration) => declaration.name.getText(source) === "stagedFiles"
      )
  );
  assert(assembly && prune && assembly.end < call.pos && call.end < prune.pos);
  const stage = mkdtempSync(join(tmpdir(), "omniroute-omp-package-"));
  const dist = join(stage, "dist");
  let child: ReturnType<typeof fork> | undefined;
  try {
    mkdirSync(dist);
    writeFileSync(join(dist, "package.json"), '{"type":"commonjs"}\n');
    const worker = join(dist, "src/lib/db/ompHostWorker.cjs");
    assert.equal(existsSync(worker), false);
    const entry = join(stage, "stage-worker.mjs");
    const owner = new URL("../../../scripts/build/colocate-standalone.mjs", import.meta.url).href;
    writeFileSync(
      entry,
      `import { bundleOmpHostWorker } from ${JSON.stringify(owner)};\n` +
        `import { join } from "node:path";\n` +
        `const DIST_DIR = ${JSON.stringify(dist)};\n${call.getText(source)}\n`
    );
    execFileSync(process.execPath, [entry], { cwd: stage, stdio: "pipe" });
    assert.ok(existsSync(worker));
    colocateCodeGraphDependencies(join(process.cwd(), "node_modules"), join(dist, "node_modules"));
    const databasePath = join(stage, "agent.db");
    const db = new Database(databasePath);
    try {
      db.exec("CREATE TABLE auth_credentials(provider TEXT, credential_type TEXT, data TEXT)");
      db.prepare("INSERT INTO auth_credentials VALUES (?, ?, ?)").run(
        "fixture",
        "api_key",
        JSON.stringify({ apiKey: "fixture-key", baseUrl: "https://fixture.invalid" })
      );
    } finally {
      db.close();
    }
    child = fork(worker, [databasePath], {
      cwd: dist,
      execArgv: [],
      stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    const message = once(child, "message", { signal: AbortSignal.timeout(5_000) });
    child.send({ kind: "operation", id: 1, operation: { kind: "get", providerId: "fixture" } });
    assert.deepEqual((await message)[0], {
      kind: "result",
      id: 1,
      ok: true,
      credentials: {
        hasOmniRoute: true,
        apiKey: "fixture-key",
        baseUrl: "https://fixture.invalid",
      },
    });
  } finally {
    if (child && child.exitCode === null && child.signalCode === null) {
      const closed = once(child, "close");
      child.kill();
      await closed;
    }
    rmSync(stage, { recursive: true, force: true });
  }
});
