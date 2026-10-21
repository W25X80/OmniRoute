import * as requestDeadline from "../../../open-sse/utils/requestDeadline.ts";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, get, request as httpRequest } from "node:http";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { controlledOwnerModule } from "../../helpers/persistence/controlledOwnerModule.ts";
import { wrapRequestListenerWithPersistence } from "../../../scripts/dev/persistence-request-boundary.mjs";
import { pipeToNodeResponse } from "next/dist/server/pipe-readable.js";

const previous = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET", "DATA_DIR"].map((key) => [
    key,
    process.env[key],
  ])
);
const directory = mkdtempSync(join(tmpdir(), "omni-http-handler-lifetime-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
  .href;
const dependencies = new Set([
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
    if (context.parentURL === runtimeUrl && dependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const { withEarlyStreamKeepalive } =
  await import("../../../open-sse/utils/earlyStreamKeepalive.ts");
const chatAdmission = await import("../../../src/sse/handlers/chatAdmission.ts");
const { errorResponse } = await import("../../../open-sse/utils/error.ts");
const { HTTP_STATUS } = await import("../../../open-sse/config/constants.ts");

test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  rmSync(directory, { recursive: true, force: true });
  for (const [key, value] of previous) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

async function eventually(predicate: () => boolean) {
  const deadline = performance.now() + 3000;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error("HTTP owner oracle timed out");
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

test("an owned JSON deadline retains native cleanup authority and rejects a late Next writer", async () => {
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  const finish = barrier();
  const entered = barrier();
  const cleanupDone = barrier();
  const previousTimeout = process.env.OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS;
  const oldHook = globalThis.__omniroutePersistenceRequest;
  process.env.OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS = "50";
  globalThis.__omniroutePersistenceRequest = (run, lifetime) =>
    runtime.runPersistenceRequest(async () => {
      await run();
      await lifetime;
    });
  let selected: object | undefined;
  let cancellation: unknown;
  const server = createServer(
    wrapRequestListenerWithPersistence(
      async (_req, res) => {
        await runtime.runPersistenceAttempt(async () => {
          const signal = requestDeadline.getRequestLifecycleSignal();
          entered.resolve();
          await finish.promise;
          selected = getSelectedPersistenceIdentity();
          cancellation = signal?.reason;
          const late = new Response('{"late":true}', {
            headers: { "content-type": "application/json" },
          });
          res.setHeader("content-type", "application/json");
          await pipeToNodeResponse(late.body!, res);
          cleanupDone.resolve();
        });
      },
      { basePath: "", experimental: { caseSensitiveRoutes: false } }
    )
  );
  try {
    await runtime.ensurePersistenceReady();
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const response = new Promise<{ status: number; body: string }>((resolve, reject) => {
      const client = httpRequest(
        `http://127.0.0.1:${address.port}/v1/messages`,
        { method: "POST" },
        (res) => {
          let body = "";
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            body += chunk;
          });
          res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
          res.on("error", reject);
        }
      );
      client.on("error", reject);
      client.end("{}");
    });
    await entered.promise;
    const received = await response;
    assert.equal(received.status, 504);
    assert.equal(JSON.parse(received.body).error.code, "request_ingress_deadline_exceeded");
    assert.equal(runtime.getPersistenceRuntimeStatus().activeAttempts, 1);
    let drained = false;
    const closing = runtime.closePersistenceRuntime().then(() => {
      drained = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(drained, false);
    finish.resolve();
    await cleanupDone.promise;
    await closing;
    assert.equal(selected, fixture.authority);
    assert.equal(requestDeadline.isIngressRequestDeadline(cancellation), true);
  } finally {
    finish.resolve();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await runtime.closePersistenceRuntime();
    globalThis.__omniroutePersistenceRequest = oldHook;
    if (previousTimeout === undefined) delete process.env.OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS;
    else process.env.OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS = previousTimeout;
    delete globalThis.__omniroutePersistenceRuntime;
  }
});

test(
  "early keepalive cancellation retains chat preparation through the actual Next response pipe",
  { timeout: 10000 },
  async () => {
    delete globalThis.__omniroutePersistenceRuntime;
    fixture.reset();
    const prepared = barrier(),
      finish = barrier(),
      handlerDone = barrier();
    let selected: object | undefined;
    const chat = await controlledOwnerModule<{ handleChat(request: Request): Promise<Response> }>(
      new URL("../../../src/sse/handlers/chat.ts", import.meta.url),
      {},
      {
        chatAdmission,
        ...requestDeadline,
        errorResponse,
        HTTP_STATUS,
        runPersistenceAttempt: runtime.runPersistenceAttempt,
        isPersistenceStreamCapacityError: runtime.isPersistenceStreamCapacityError,
        PERSISTENCE_STREAM_CAPACITY_CODE: runtime.PERSISTENCE_STREAM_CAPACITY_CODE,
        handleChatImplementation: async () => {
          prepared.resolve();
          try {
            await finish.promise;
            selected = getSelectedPersistenceIdentity();
            return new Response("data: completed\n\n");
          } finally {
            handlerDone.resolve();
          }
        },
      },
      (source) => {
        const marker = source.includes("const handleChatWithAdmission =")
          ? "const handleChatWithAdmission ="
          : "export const handleChat =";
        const start = source.indexOf(marker);
        const end = source.indexOf("/** Handle one resolved model", start);
        assert.ok(start >= 0 && end > start);
        return source.slice(start, end);
      }
    );
    const oldHook = globalThis.__omniroutePersistenceRequest;
    globalThis.__omniroutePersistenceRequest = (run, lifetime) =>
      runtime.runPersistenceRequest(async () => {
        await run();
        await lifetime;
      });
    const server = createServer(
      wrapRequestListenerWithPersistence(
        async (_req, res) => {
          const clientAbort = new AbortController();
          res.once("close", () => clientAbort.abort());
          const response = await withEarlyStreamKeepalive(
            chat.handleChat(
              new Request("http://localhost/v1/messages", { signal: clientAbort.signal })
            ),
            { thresholdMs: 1, signal: clientAbort.signal, slowPathDeadlineMs: 0 }
          );
          assert.ok(response.body);
          await pipeToNodeResponse(response.body, res);
        },
        { basePath: "", experimental: { caseSensitiveRoutes: false } }
      )
    );
    let client: ReturnType<typeof get> | undefined;
    try {
      await runtime.ensurePersistenceReady();
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      assert.ok(address && typeof address === "object");
      await new Promise<void>((resolve, reject) => {
        client = get(`http://127.0.0.1:${address.port}/v1/messages`, (response) => {
          response.once("error", () => {});
          response.once("data", () => {
            response.destroy();
            resolve();
          });
        });
        client.once("error", reject);
      });
      await prepared.promise;
      await eventually(() => runtime.getPersistenceRuntimeStatus().activeRequests === 0);
      assert.equal(
        runtime.getPersistenceRuntimeStatus().activeAttempts,
        1,
        "the HTTP listener can finish while pre-core work still needs an owner"
      );
      let drained = false;
      const closing = runtime.closePersistenceRuntime().then(() => {
        drained = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(drained, false);
      finish.resolve();
      await handlerDone.promise;
      await closing;
      assert.equal(selected, fixture.authority);
    } finally {
      finish.resolve();
      client?.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await runtime.closePersistenceRuntime();
      globalThis.__omniroutePersistenceRequest = oldHook;
      delete globalThis.__omniroutePersistenceRuntime;
    }
  }
);

test(
  "A2A skill completion retains its owner after the Next response pipe is cancelled",
  { timeout: 10000 },
  async () => {
    delete globalThis.__omniroutePersistenceRuntime;
    fixture.reset();
    const entered = barrier(),
      finish = barrier(),
      skillDone = barrier();
    let selected: object | undefined;
    const { createA2AStream, SSE_HEADERS } = await import("../../../src/lib/a2a/streaming.ts");
    const route = await controlledOwnerModule<{
      streamResponse(task: object, manager: object, request: Request): Promise<Response>;
    }>(
      new URL("../../../src/app/a2a/route.ts", import.meta.url),
      {},
      {
        Response,
        createA2AStream,
        SSE_HEADERS,
        runPersistenceAttempt: runtime.runPersistenceAttempt,
        A2A_SKILL_HANDLERS: { fixture: async () => {} },
        toMessageArray: (value: unknown) => value,
        executeA2ATaskWithState: async () => {
          entered.resolve();
          try {
            await finish.promise;
            selected = getSelectedPersistenceIdentity();
            return { artifacts: [], metadata: {} };
          } finally {
            skillDone.resolve();
          }
        },
      },
      (source) => {
        const start = source.indexOf('case "message/stream": {');
        const end = source.indexOf("// ── tasks/get", start);
        assert.ok(start >= 0 && end > start);
        return `export async function streamResponse(task, tm, req) {
          const params = { skill: "fixture", messages: [{ role: "user", content: "test" }] };
          const id = 1, callerOwner = "fixture-owner";
          switch ("message/stream") { ${source.slice(start, end)} }
        }`;
      }
    );
    const oldHook = globalThis.__omniroutePersistenceRequest;
    globalThis.__omniroutePersistenceRequest = (run, lifetime) =>
      runtime.runPersistenceRequest(async () => {
        await run();
        await lifetime;
      });
    const server = createServer(
      wrapRequestListenerWithPersistence(
        async (_req, res) => {
          const signal = new AbortController();
          res.once("close", () => signal.abort());
          const response = await route.streamResponse(
            { id: "cancelled-a2a" },
            {
              createTask: () => ({ id: "cancelled-a2a" }),
              async flushPersistence() {},
              updateTask() {},
              beginStream() {},
              endStream() {},
            },
            new Request("http://localhost/a2a", { signal: signal.signal })
          );
          assert.ok(response.body);
          await pipeToNodeResponse(response.body, res);
        },
        { basePath: "", experimental: { caseSensitiveRoutes: false } }
      )
    );
    let client: ReturnType<typeof get> | undefined;
    try {
      await runtime.ensurePersistenceReady();
      await runtime.runPersistenceRequest(async () => {
        const release = barrier();
        const work = Array.from({ length: 64 }, () =>
          runtime.runPersistenceAttempt(() => release.promise, { independent: true })
        );
        let created = false;
        try {
          await assert.rejects(
            route.streamResponse(
              {},
              {
                createTask() {
                  created = true;
                },
              },
              new Request("http://localhost/a2a")
            ),
            runtime.isPersistenceStreamCapacityError
          );
          assert.equal(created, false, "capacity refusal must precede durable task creation");
        } finally {
          release.resolve();
          await Promise.allSettled(work);
        }
      });
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      assert.ok(address && typeof address === "object");
      client = get(`http://127.0.0.1:${address.port}/a2a`);
      client.on("error", () => {});
      await entered.promise;
      client.destroy();
      await eventually(() => runtime.getPersistenceRuntimeStatus().activeRequests === 0);
      assert.equal(
        runtime.getPersistenceRuntimeStatus().activeAttempts,
        1,
        "the skill must remain admitted after HTTP closes"
      );
      let drained = false;
      const closing = runtime.closePersistenceRuntime().then(() => {
        drained = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(drained, false);
      finish.resolve();
      await skillDone.promise;
      await closing;
      assert.equal(selected, fixture.authority);
    } finally {
      finish.resolve();
      client?.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await runtime.closePersistenceRuntime();
      globalThis.__omniroutePersistenceRequest = oldHook;
      delete globalThis.__omniroutePersistenceRuntime;
    }
  }
);
