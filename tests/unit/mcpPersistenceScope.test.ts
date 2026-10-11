import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { registerHooks } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { z } from "zod";
import { getSelectedControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import {
  scopeFixture,
  type ScopeFixtureBinding,
} from "../helpers/persistence/mcpRuntimeScopeFixture.ts";

const serverUrl = new URL("../../open-sse/mcp-server/server.ts", import.meta.url).href;
const heartbeatUrl = new URL("../../open-sse/mcp-server/runtimeHeartbeat.ts", import.meta.url).href;
const fixtureUrl = new URL("../helpers/persistence/mcpRuntimeScopeFixture.ts", import.meta.url)
  .href;
const lifetimeHeartbeatUrl = new URL(
  "../helpers/persistence/mcpHeartbeatLifetimeFixture.ts",
  import.meta.url
).href;
const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    // Redirect only the entry's lifetime dependencies; heartbeat tick tests import its real owner.
    if (
      (context.parentURL === serverUrl || context.parentURL === heartbeatUrl) &&
      new URL(specifier, context.parentURL).href === runtimeUrl
    ) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (context.parentURL === serverUrl && new URL(specifier, serverUrl).href === heartbeatUrl) {
      return { url: lifetimeHeartbeatUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
let serverModule: typeof import("../../open-sse/mcp-server/server.ts");
let heartbeatModule: typeof import("../../open-sse/mcp-server/runtimeHeartbeat.ts");
try {
  serverModule = await import("../../open-sse/mcp-server/server.ts");
  heartbeatModule = await import("../../open-sse/mcp-server/runtimeHeartbeat.ts");
} finally {
  hooks.deregister();
}

test("SDK tool callbacks create fresh persistence scopes after constructor scope ends", async () => {
  const detached = new AsyncResource("mcp-independent-input");
  const server = await serverModule.createMcpServerAsync();
  const constructorScope = scopeFixture.resources[0];
  const constructorGeneration = scopeFixture.generation;
  let bodyCalls = 0;
  server.registerTool(
    "persistence_scope_probe",
    { inputSchema: z.object({ nonce: z.string() }) },
    async (args, extra) => {
      bodyCalls++;
      const binding = getSelectedControlPlane() as ScopeFixtureBinding;
      binding.assertReady();
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              generation: binding.catalogGeneration,
              nonce: args.nonce,
              caller: extra.authInfo?.clientId,
            }),
          },
        ],
      };
    }
  );
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const send = clientTransport.send.bind(clientTransport);
  let callerScopes = ["manage"];
  clientTransport.send = (message, options) =>
    send(message, {
      ...options,
      authInfo: { token: "test-key", clientId: "principal-one", scopes: callerScopes },
    });
  const client = new Client({ name: "persistence-scope-test", version: "1.0.0" });
  try {
    await server.connect(serverTransport);
    await client.connect(clientTransport);
    assert.equal(getSelectedControlPlane(), undefined);
    // This is the actual counterexample to relying on constructor ALS inheritance.
    assert.throws(
      () => constructorScope.runInAsyncScope(() => getSelectedControlPlane()),
      /scope has ended/
    );
    const first = await detached.runInAsyncScope(() =>
      client.callTool({
        name: "persistence_scope_probe",
        arguments: { nonce: "detached" },
      })
    );
    const second = await constructorScope.runInAsyncScope(() =>
      client.callTool({
        name: "persistence_scope_probe",
        arguments: { nonce: "old-constructor" },
      })
    );
    const text = (result: unknown) => {
      const entry = (result as { content: Array<{ text: string }> }).content[0];
      return JSON.parse(entry.text) as { generation: number; nonce: string; caller: string };
    };
    const one = text(first);
    const two = text(second);
    assert.equal(one.generation, constructorGeneration + 1);
    assert.equal(two.generation, constructorGeneration + 2);
    assert.equal(one.caller, "principal-one");
    assert.equal(two.nonce, "old-constructor");
    assert.equal(bodyCalls, 2);
    callerScopes = ["mcp:connect"];
    const denied = await detached.runInAsyncScope(() =>
      client.callTool({
        name: "omniroute_get_health",
        arguments: {},
      })
    );
    assert.equal(denied.isError, true);
    assert.match(
      (denied as { content: Array<{ text: string }> }).content[0].text,
      /Insufficient MCP scopes/
    );
    assert.ok(
      scopeFixture.statements.some(
        (entry) =>
          entry.generation === constructorGeneration + 3 &&
          entry.sql.includes("INSERT INTO omniroute.mcp_tool_audit")
      )
    );
    scopeFixture.closed = true;
    const rejected = await client.callTool({
      name: "persistence_scope_probe",
      arguments: { nonce: "closed" },
    });
    assert.equal(rejected.isError, true);
    assert.equal(bodyCalls, 2);
  } finally {
    await client.close();
    await server.close();
    scopeFixture.closed = false;
    detached.emitDestroy();
    for (const resource of scopeFixture.resources) resource.emitDestroy();
  }
});

test("heartbeat ticks read dynamic flags inside fresh scopes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-mcp-heartbeat-scope-"));
  const previousData = process.env.DATA_DIR;
  process.env.DATA_DIR = directory;
  let reads = 0;
  let enabled = false;
  const observed: number[] = [];
  const stop = heartbeatModule.startMcpHeartbeat({
    version: "test",
    allowedScopes: [],
    toolCount: 1,
    intervalMs: 10,
    scopesEnforced: () => {
      const binding = getSelectedControlPlane() as ScopeFixtureBinding;
      binding.assertReady();
      observed.push(binding.catalogGeneration);
      reads++;
      return enabled;
    },
  });
  try {
    const deadline = Date.now() + 5000;
    while (reads < 1 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    enabled = true;
    let snapshot: { scopesEnforced: boolean } | undefined;
    while (Date.now() < deadline) {
      try {
        snapshot = JSON.parse(
          await readFile(heartbeatModule.resolveMcpHeartbeatPath(), "utf8")
        ) as { scopesEnforced: boolean };
      } catch {
        /* The first asynchronous write may not have completed yet. */
      }
      if (reads >= 3 && snapshot?.scopesEnforced === true) break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    await stop();
    assert.ok(reads >= 3);
    assert.equal(new Set(observed).size, observed.length);
    assert.equal(snapshot?.scopesEnforced, true);
  } finally {
    await stop();
    if (previousData === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = previousData;
    await rm(directory, { recursive: true, force: true });
  }
});

test("heartbeat drops overlapping ticks and stop drains its admitted write", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-mcp-heartbeat-drain-"));
  const previousData = process.env.DATA_DIR;
  process.env.DATA_DIR = directory;
  let release!: () => void;
  scopeFixture.barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  const generation = scopeFixture.generation;
  let reads = 0;
  const stop = heartbeatModule.startMcpHeartbeat({
    version: "test",
    allowedScopes: [],
    toolCount: 1,
    intervalMs: 5,
    scopesEnforced: () => {
      reads++;
      return true;
    },
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(scopeFixture.generation, generation + 1);
    const stopped = stop();
    const drained = await Promise.race([
      stopped.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 10)),
    ]);
    assert.equal(drained, false);
    release();
    await stopped;
    assert.equal(reads, 1);
    const snapshot = JSON.parse(
      await readFile(heartbeatModule.resolveMcpHeartbeatPath(), "utf8")
    ) as { scopesEnforced: boolean };
    assert.equal(snapshot.scopesEnforced, true);
  } finally {
    release();
    await stop();
    scopeFixture.barrier = null;
    if (previousData === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = previousData;
    await rm(directory, { recursive: true, force: true });
  }
});

test("stdio stays alive until stdin EOF and closes its owned runtime once", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-mcp-stdio-lifetime-"));
  const previousData = process.env.DATA_DIR;
  const descriptor = Object.getOwnPropertyDescriptor(process, "stdin")!;
  const input = new PassThrough();
  process.env.DATA_DIR = directory;
  Object.defineProperty(process, "stdin", { configurable: true, value: input });
  const closeCalls = scopeFixture.closeCalls;
  const heartbeatStops = scopeFixture.heartbeatStops;
  const started = serverModule.startMcpStdio();
  try {
    const deadline = Date.now() + 2000;
    while (input.listenerCount("data") === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.ok(input.listenerCount("data") > 0, "SDK must actually connect to stdin");
    const returned = await Promise.race([
      started.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 30)),
    ]);
    assert.equal(returned, false, "connect completion must not terminate the stdio lifetime");
    assert.equal(scopeFixture.closeCalls, closeCalls);
    assert.equal(scopeFixture.heartbeatStops, heartbeatStops);
    input.end();
    await started;
    assert.equal(scopeFixture.closeCalls, closeCalls + 1);
    assert.equal(scopeFixture.heartbeatStops, heartbeatStops + 1);
    assert.equal(input.listenerCount("data"), 0);
  } finally {
    input.end();
    await started;
    scopeFixture.closed = false;
    Object.defineProperty(process, "stdin", descriptor);
    if (previousData === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = previousData;
    await rm(directory, { recursive: true, force: true });
  }
});
