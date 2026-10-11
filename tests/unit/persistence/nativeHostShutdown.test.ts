import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { shutdownFixture } from "../../helpers/persistence/shutdownOrderingFixture.ts";
import {
  probeNativeHostCapability,
  stopNativeHostProbes,
} from "../../../src/lib/nativeHostProbes.ts";
import {
  runCodeGraphToolAsync,
  closeNativeCodeGraphQueries,
} from "../../../src/lib/copilot/codegraphQueryClient.ts";

const runtimeUrl = import.meta.resolve("../../../src/lib/db/persistenceRuntime.ts");
const shutdownUrl = import.meta.resolve("../../../src/lib/startup/postgresShutdown.ts");
const runtimeFixtureUrl = import.meta
  .resolve("../../helpers/persistence/runtimeOwnedWorkFixture.ts");
const shutdownFixtureUrl = import.meta
  .resolve("../../helpers/persistence/nativeHostShutdownFixture.ts");
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL?.split("?")[0] === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: runtimeFixtureUrl, shortCircuit: true };
    if (
      context.parentURL?.split("?")[0] === shutdownUrl &&
      specifier !== "@/lib/db/persistenceRuntime" &&
      specifier !== "./postgresBootstrap"
    )
      return { url: shutdownFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
const shutdown = await import("../../../src/lib/startup/postgresShutdown.ts");
const turn = () => new Promise<void>((done) => setImmediate(done));
async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Controlled child did not reach its barrier");
    await new Promise<void>((done) => setTimeout(done, 5));
  }
}

// Runtime authority and shutdown coordinator are real. SQL/catalog transports and unrelated
// producers are controlled; both capability clients use real child processes and close events.
test(
  "accepted host work settles before capability teardown, which joins actual child exit",
  {
    timeout: 15000,
  },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-host-shutdown-"));
    const cwd = process.cwd();
    const previous = Object.fromEntries(
      ["OMNIROUTE_DATABASE_BACKEND", "DATA_DIR", "JWT_SECRET", "API_KEY_SECRET"].map((name) => [
        name,
        process.env[name],
      ])
    );
    const ownerSymbol = Symbol.for("omniroute.postgresStartupOwner");
    const ownerGlobals = globalThis as typeof globalThis & {
      [ownerSymbol]?: unknown;
    };
    const owner = ownerGlobals[ownerSymbol];
    const requestGate = join(directory, "request-gate");
    const probeReady = join(directory, "probe-ready");
    const graphReady = join(directory, "graph-ready");
    const graphClosing = join(directory, "graph-closing");
    const exitGate = join(directory, "exit-gate");
    const probeFile = join(directory, "probe.cjs");
    const workerFile = join(directory, "src/lib/copilot/codegraphQueryWorker.cjs");
    mkdirSync(join(directory, "src/lib/copilot"), { recursive: true });
    writeFileSync(
      probeFile,
      `const fs = require("node:fs");
    fs.writeFileSync(${JSON.stringify(probeReady)}, String(process.pid));
    const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(requestGate)})) {
      clearInterval(timer); process.stdout.write(${JSON.stringify("agent v1.2.3\n")});
    } }, 5);`
    );
    // The worker transport deliberately holds a query and its final close at separate causal
    // barriers. Its SQL projection is already covered by the independent actual-artifact suite.
    writeFileSync(
      workerFile,
      `const fs = require("node:fs");
    process.on("message", (message) => {
      if (message.kind === "query") {
        fs.writeFileSync(${JSON.stringify(graphReady)}, String(process.pid));
        const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(requestGate)})) {
          clearInterval(timer); process.send({kind:"result", id:message.id, output:"readonly result"});
        } }, 5);
      } else if (message.kind === "close") {
        fs.writeFileSync(${JSON.stringify(graphClosing)}, "closing");
        const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(exitGate)})) {
          clearInterval(timer); process.exit(0);
        } }, 5);
      }
    });`
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = join(directory, "main-control-plane-must-stay-empty");
    delete globalThis.__omniroutePersistenceRuntime;
    delete ownerGlobals[ownerSymbol];
    fixture.reset();
    shutdownFixture.reset();
    process.chdir(directory);
    let request: Promise<void> | undefined;
    let closing: Promise<void> | undefined;
    let probePid: number | undefined;
    let graphPid: number | undefined;
    try {
      await runtime.ensurePersistenceReady();
      request = runtime.runPersistenceRequest(async () => {
        const [probe, graph] = await Promise.all([
          probeNativeHostCapability(process.execPath, [probeFile]),
          runCodeGraphToolAsync("getCodeGraphStats", []),
        ]);
        assert.deepEqual(probe, { success: true, stdout: "agent v1.2.3\n" });
        assert.equal(graph, "readonly result");
        shutdownFixture.events.push("accepted:complete");
      });
      let requestFailure: unknown;
      void request.catch((error) => {
        requestFailure = error;
      });
      await waitFor(() => !!requestFailure || (existsSync(probeReady) && existsSync(graphReady)));
      if (requestFailure) throw requestFailure;
      const fs = await import("node:fs/promises");
      probePid = Number(await fs.readFile(probeReady, "utf8"));
      graphPid = Number(await fs.readFile(graphReady, "utf8"));
      let settled = false;
      closing = shutdown.shutdownPostgresApplication().then(() => {
        settled = true;
      });
      void closing.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      await assert.rejects(
        runtime.runPersistenceRequest(async () => {}),
        /unavailable|draining/
      );
      await waitFor(() => shutdownFixture.events.includes("producer:stop"));
      await turn();
      assert.equal(
        shutdownFixture.events.includes("probe:closing"),
        false,
        "shutdown must not kill a probe owned by an already accepted request"
      );
      assert.equal(
        shutdownFixture.events.includes("graph:closing"),
        false,
        "shutdown must not reject an accepted readonly child query"
      );
      assert.doesNotThrow(() => process.kill(probePid!, 0));
      assert.doesNotThrow(() => process.kill(graphPid!, 0));
      assert.equal(fixture.poolCloses, 0);
      assert.equal(settled, false);
      writeFileSync(requestGate, "complete accepted request");
      await request;
      await waitFor(() => existsSync(graphClosing));
      assert.ok(
        shutdownFixture.events.indexOf("accepted:complete") <
          shutdownFixture.events.indexOf("probe:closing")
      );
      assert.ok(
        shutdownFixture.events.indexOf("accepted:complete") <
          shutdownFixture.events.indexOf("graph:closing")
      );
      assert.equal(
        shutdownFixture.events.includes("graph:closed"),
        false,
        "a close request is not an exit acknowledgement"
      );
      assert.doesNotThrow(() => process.kill(graphPid!, 0));
      assert.equal(fixture.poolCloses, 0);
      assert.equal(settled, false);
      writeFileSync(exitGate, "release actual child exit");
      await closing;
      assert.ok(shutdownFixture.events.includes("graph:closed"));
      assert.throws(() => process.kill(probePid!, 0), /ESRCH/);
      assert.throws(() => process.kill(graphPid!, 0), /ESRCH/);
      assert.equal(fixture.poolCloses, 1);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
      assert.equal(existsSync(join(process.env.DATA_DIR!, "storage.sqlite")), false);
    } finally {
      writeFileSync(requestGate, "cleanup");
      writeFileSync(exitGate, "cleanup");
      await request?.catch(() => {});
      await closing?.catch(() => {});
      await stopNativeHostProbes();
      await closeNativeCodeGraphQueries();
      await runtime.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      process.chdir(cwd);
      delete globalThis.__omniroutePersistenceRuntime;
      if (owner === undefined) delete ownerGlobals[ownerSymbol];
      else ownerGlobals[ownerSymbol] = owner;
      for (const [name, value] of Object.entries(previous)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
