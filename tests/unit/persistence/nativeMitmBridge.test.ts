import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import { fork, type ChildProcess } from "node:child_process";
import { registerHooks } from "node:module";
import { realpathSync as resolveFile } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import { once, EventEmitter } from "node:events";
import { setImmediate as nextTurn } from "node:timers/promises";
import { fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import {
  resetAliasFixture,
  aliasAdmission,
} from "../../helpers/persistence/nativeMitmAliasFixture.ts";

const root = fileURLToPath(new URL("../../../", import.meta.url));
async function freePort() {
  const server = net.createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}
function request(port: number): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        host: "127.0.0.1",
        port,
        method: "POST",
        agent: false,
        path: "/v1beta/models/source:generateContent",
        rejectUnauthorized: false,
        headers: { host: "daily-cloudcode-pa.googleapis.com", "Content-Type": "application/json" },
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk: string) => {
          body += chunk;
        });
        res.on("end", () => resolve({ status: res.statusCode!, body }));
        res.on("error", reject);
      }
    );
    req.on("error", reject);
    req.end(JSON.stringify({ model: "source", messages: [{ role: "user", content: "hello" }] }));
  });
}

test(
  "real native CJS bridge reads changed mappings in fresh scopes and fails closed",
  { timeout: 20000 },
  async () => {
    const runtimeUrl = pathToFileURL(
      resolveFile(path.join(root, "src/lib/db/persistenceRuntime.ts"))
    ).href;
    const aliasUrl = pathToFileURL(
      resolveFile(path.join(root, "src/lib/db/models/mitmAlias.ts"))
    ).href;
    const boot = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
      .href;
    const catalog = new URL("../../helpers/persistence/nativeMitmAliasFixture.ts", import.meta.url)
      .href;
    const bootDependencies = new Set([
      "./postgres/environment",
      "./postgres/controlPlane",
      "../artifacts/s3ArtifactConfiguration",
      "../artifacts/s3ArtifactStore",
      "@/shared/utils/circuitBreaker",
      "@/domain/quotaCache",
    ]);
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (context.parentURL === runtimeUrl && bootDependencies.has(specifier))
          return { url: boot, shortCircuit: true };
        if (
          (context.parentURL === runtimeUrl && specifier === "./postgres/catalogSnapshot") ||
          (context.parentURL === aliasUrl &&
            ["../postgres/catalogSnapshot", "../core", "../backup"].includes(specifier))
        )
          return { url: catalog, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const previous = new Map(
      ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY"].map((key) => [key, process.env[key]])
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "5a".repeat(32);
    const scratch = await fs.mkdtemp(path.join(os.tmpdir(), "mitm-native-child-"));
    let child: ChildProcess | undefined;
    let bridge: { close(): Promise<void> } | undefined;
    let runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") | undefined;
    const forwarded: Array<{ model: string; reasoningEffortOverride?: string }> = [];
    const forbidden: unknown[] = [];
    const router = http.createServer((req, res) => {
      let body = "";
      req.setEncoding("utf8");
      req.on("data", (chunk: string) => {
        body += chunk;
      });
      req.on("end", () => {
        forwarded.push(JSON.parse(body));
        res.end("data: [DONE]\n\n");
      });
    });
    try {
      fixture.reset();
      resetAliasFixture();
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const { setMitmAliasAll, getMitmAlias } = await import(aliasUrl);
      const { attachNativeAliasBridge } = await import("../../../src/mitm/nativeAliasBridge.ts");
      await runtime.runPersistenceRequest(() =>
        setMitmAliasAll("antigravity", { source: "provider/first" })
      );
      const { default: selfsigned } = await import("selfsigned");
      const pair = await selfsigned.generate([{ name: "commonName", value: "localhost" }], {
        keySize: 2048,
      });
      await fs.mkdir(path.join(scratch, "mitm"));
      await fs.writeFile(path.join(scratch, "mitm/server.key"), pair.private);
      await fs.writeFile(path.join(scratch, "mitm/server.crt"), pair.cert);
      // A native consumer must ignore stale application files even when they exist.
      await fs.writeFile(
        path.join(scratch, "db.json"),
        JSON.stringify({ mitmAlias: { antigravity: { source: "stale" } } })
      );
      router.listen(0, "127.0.0.1");
      await once(router, "listening");
      const routerPort = (router.address() as net.AddressInfo).port;
      const port = await freePort();
      await runtime.runPersistenceRequest(async () => {
        child = fork(path.join(root, "src/mitm/server.cjs"), [], {
          execArgv: [
            "--require",
            path.join(root, "tests/helpers/persistence/nativeMitmIoGuard.cjs"),
          ],
          env: {
            ...process.env,
            DATA_DIR: scratch,
            ROUTER_API_KEY: "fixture",
            MITM_LOCAL_PORT: String(port),
            OMNIROUTE_BASE_URL: `http://127.0.0.1:${routerPort}`,
            MITM_CERT_MODE: "legacy",
            INSPECTOR_INTERNAL_INGEST_TOKEN: "",
          },
          stdio: ["ignore", "pipe", "pipe", "ipc"],
        });
        bridge = attachNativeAliasBridge(child);
        child.on("message", (message) => {
          if (
            message &&
            typeof message === "object" &&
            "fixture" in message &&
            message.fixture === "sync-io"
          )
            forbidden.push(message);
        });
      });
      // The child IPC resource was created in the now-ended request; the handler must re-admit.
      await assert.rejects(
        fixture.resources.at(-1)!.runInAsyncScope(() => getMitmAlias("antigravity")),
        /scope ended/
      );
      await new Promise<void>((resolve, reject) => {
        let output = "";
        child!.stdout!.on("data", (chunk: Buffer) => {
          output += chunk.toString();
          if (output.includes("MITM ready")) resolve();
        });
        child!.once("error", reject);
        child!.once("exit", (code) => reject(new Error(`Native MITM exited ${code}`)));
      });
      assert.equal((await request(port)).status, 200);
      assert.deepEqual(
        forwarded.map((entry) => entry.model),
        ["provider/first"]
      );
      await runtime.runPersistenceRequest(() =>
        setMitmAliasAll("antigravity", {
          source: { model: "provider/second", reasoningEffort: "extra" },
        })
      );
      assert.equal((await request(port)).status, 200);
      assert.equal(forwarded[1].model, "provider/second");
      assert.equal(forwarded[1].reasoningEffortOverride, "xhigh");
      fixture.catalogFailure = new Error("credential-value /private/app/secret.ts\nstack");
      const failure = await request(port);
      assert.equal(failure.status, 503);
      assert.deepEqual(JSON.parse(failure.body), {
        error: { message: "MITM alias lookup is unavailable" },
      });
      assert.equal(forwarded.length, 2);
      assert.equal((await request(port)).status, 200);
      await nextTurn();
      assert.deepEqual(forbidden, []);
      const exit = once(child!, "exit");
      child!.kill("SIGTERM");
      await exit;
      await bridge.close();
      const stats = JSON.parse(await fs.readFile(path.join(scratch, "mitm/stats.json"), "utf8"));
      assert.equal(stats.totalRequests, 4);
      assert.equal(stats.interceptedRequests, 3);
      assert.equal(stats.activeConnections, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);

      // Close joins an already accepted lookup and SQL teardown cannot overtake it.
      const localChild = new EventEmitter() as EventEmitter & {
        connected: boolean;
        send(message: unknown, callback: () => void): void;
      };
      const replies: unknown[] = [];
      localChild.connected = true;
      localChild.send = (message, callback) => {
        replies.push(message);
        callback();
      };
      const admitted = attachNativeAliasBridge(localChild as unknown as ChildProcess);
      let entered!: () => void;
      let release!: () => void;
      const entering = new Promise<void>((resolve) => {
        entered = resolve;
      });
      aliasAdmission.gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      aliasAdmission.entered = entered;
      localChild.emit("message", {
        channel: "omniroute-mitm-alias.v1",
        kind: "lookup",
        id: 1,
        model: "source",
        aliasKey: "antigravity",
      });
      await entering;
      let bridgeClosed = false;
      let runtimeClosed = false;
      const stopping = admitted.close().then(() => {
        bridgeClosed = true;
      });
      const shuttingDown = runtime.closePersistenceRuntime().then(() => {
        runtimeClosed = true;
      });
      await nextTurn();
      assert.equal(bridgeClosed, false);
      assert.equal(runtimeClosed, false);
      release();
      await Promise.all([stopping, shuttingDown]);
      assert.deepEqual(replies, [
        {
          channel: "omniroute-mitm-alias.v1",
          kind: "result",
          id: 1,
          ok: true,
          override: { model: "provider/second", reasoningEffort: "xhigh" },
        },
      ]);
      assert.equal(fixture.poolCloses, 1);
    } finally {
      child?.kill("SIGKILL");
      await bridge?.close();
      await runtime?.closePersistenceRuntime().catch(() => {});
      router.closeAllConnections();
      await new Promise<void>((resolve) => router.close(() => resolve()));
      hooks.deregister();
      fixture.reset();
      resetAliasFixture();
      delete globalThis.__omniroutePersistenceRuntime;
      await fs.rm(scratch, { recursive: true, force: true });
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }
);

test(
  "native alias IPC bounds capacity, times out and rejects pending lookups on disconnect",
  { timeout: 10000 },
  async () => {
    const child = fork(
      path.join(root, "tests/helpers/persistence/nativeMitmAliasClientFixture.cjs"),
      [],
      {
        execArgv: [],
        stdio: ["ignore", "pipe", "pipe", "ipc"],
      }
    );
    let closedResults = "";
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      closedResults += chunk;
    });
    const results = new Map<string, Record<string, unknown>>();
    const requests: Array<{ id: number }> = [];
    child.on("message", (value) => {
      const message = value as Record<string, unknown>;
      if (message.fixture === "result") results.set(String(message.label), message);
      if (message.kind === "lookup") requests.push(message as unknown as { id: number });
    });
    async function result(label: string) {
      while (!results.has(label)) await once(child, "message");
      return results.get(label)!;
    }
    try {
      child.send({ fixture: "lookup", label: "first", model: "source" });
      while (!requests.length) await once(child, "message");
      child.send({ fixture: "lookup", label: "over-capacity", model: "source" });
      assert.equal((await result("over-capacity")).error, "MITM alias lookup is unavailable");
      assert.equal((await result("first")).error, "MITM alias lookup is unavailable");
      child.send({ fixture: "lookup", label: "next", model: "source" });
      while (requests.length < 2) await once(child, "message");
      child.send({
        channel: "omniroute-mitm-alias.v1",
        kind: "result",
        id: requests[1].id,
        ok: true,
        override: null,
      });
      assert.equal((await result("next")).override, null);
      child.send({ fixture: "lookup", label: "disconnect", model: "source" });
      while (requests.length < 3) await once(child, "message");
      const exit = once(child, "exit");
      child.disconnect();
      assert.equal((await exit)[0], 0);
      assert.equal(
        JSON.parse(closedResults).find((entry: { label: string }) => entry.label === "disconnect")
          .error,
        "MITM alias lookup is unavailable"
      );
    } finally {
      child.kill("SIGKILL");
    }
  }
);
