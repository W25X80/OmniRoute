import assert from "node:assert/strict";
import http from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import v8 from "node:v8";
import vm from "node:vm";
import { Readable } from "node:stream";
import { readNonStreamingResponseBody } from "../../open-sse/handlers/chatCore/nonStreamingResponseBody.ts";
import {
  resolveIngressRequestTimeoutMs,
  wrapRequestListenerWithDeadline,
} from "../../scripts/dev/request-deadline.mjs";
import {
  getRequestLifecycleSignal,
  isIngressRequestDeadline,
  promoteRequestToStreaming,
  readRequestChunk,
  throwIfRequestCancelled,
} from "../../src/lib/requestDeadline.ts";

const routing = { basePath: "", experimental: { caseSensitiveRoutes: false } };

async function serve(listener: http.RequestListener) {
  const server = http.createServer(listener);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function fixture(
  listener: (req: http.IncomingMessage, res: http.ServerResponse) => Promise<void>,
  timeoutMs = 100,
  config = routing
) {
  const errors: unknown[] = [];
  const wrapped = wrapRequestListenerWithDeadline(listener, config, {
    OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS: String(timeoutMs),
  });
  const target = await serve((req, res) => {
    void Promise.resolve(wrapped(req, res)).catch((error) => errors.push(error));
  });
  const proxy = await serve((req, res) => {
    const upstream = http.request(
      target.url + req.url,
      {
        method: req.method,
        headers: req.headers,
      },
      (response) => {
        res.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(res);
      }
    );
    upstream.setTimeout(700, () => {
      upstream.destroy();
      if (!res.headersSent) res.writeHead(504, { "content-type": "application/json" });
      res.end('{"source":"idle-proxy"}');
    });
    upstream.on("error", () => {
      if (res.writableEnded || res.destroyed) return;
      if (!res.headersSent) res.writeHead(502);
      res.end();
    });
    req.pipe(upstream);
    res.on("close", () => upstream.destroy());
  });
  return {
    url: proxy.url,
    errors,
    close: async () => {
      await proxy.close();
      await target.close();
    },
  };
}

test("ingress timeout is opt-in and rejects invalid or overflowing values", () => {
  assert.equal(resolveIngressRequestTimeoutMs({}), 0);
  assert.equal(resolveIngressRequestTimeoutMs({ OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS: "0" }), 0);
  assert.equal(
    resolveIngressRequestTimeoutMs({ OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS: "55000" }),
    55_000
  );
  for (const value of ["-1", "1.5", "Infinity", "2147483648", "garbage"]) {
    assert.throws(() =>
      resolveIngressRequestTimeoutMs({ OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS: value })
    );
  }
});

test("actual HTTP proxy receives owned JSON 504 before its idle timeout and ignores a late writer", async () => {
  let cancelled = false;
  const app = await fixture(async (_req, res) => {
    const signal = getRequestLifecycleSignal();
    signal?.addEventListener("abort", () => {
      cancelled = isIngressRequestDeadline(signal.reason);
    });
    await delay(180);
    res.setHeader("content-type", "application/json");
    res.writeHead(200);
    res.end('{"late":true}');
  });
  try {
    const response = await fetch(app.url + "/v1/messages", { method: "POST", body: "{}" });
    assert.equal(response.status, 504);
    assert.equal((await response.json()).error.code, "request_ingress_deadline_exceeded");
    await delay(150);
    assert.equal(cancelled, true);
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }
});

test("in-budget JSON success and HTTP errors preserve their status, headers, and exact body", async () => {
  for (const status of [200, 401, 429]) {
    const app = await fixture(async (_req, res) => {
      await delay(10);
      res.writeHead(status, { "content-type": "application/json", "x-owner": "original" });
      res.end('{"original":true}');
    });
    try {
      const response = await fetch(app.url + "/v1/messages", { method: "POST", body: "{}" });
      assert.equal(response.status, status);
      assert.equal(response.headers.get("x-owner"), "original");
      assert.equal(await response.text(), '{"original":true}');
    } finally {
      await app.close();
    }
  }
});

test("actual streaming promotion survives the JSON budget without changing its protocol", async () => {
  const app = await fixture(async (_req, res) => {
    promoteRequestToStreaming();
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(": ready\n\n");
    await delay(180);
    throwIfRequestCancelled();
    res.end('data: {"ok":true}\n\n');
  });
  try {
    const response = await fetch(app.url + "/v1/messages", {
      method: "POST",
      body: '{"stream":true}',
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), ': ready\n\ndata: {"ok":true}\n\n');
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }
});

test("pre-route preparation and compact/basePath aliases share the accepted-request budget", async () => {
  let dispatches = 0;
  const app = await fixture(
    async (_req, res) => {
      await delay(160);
      throwIfRequestCancelled();
      dispatches++;
      res.end("unexpected");
    },
    100,
    { ...routing, basePath: "/gateway" }
  );
  try {
    const response = await fetch(app.url + "/gateway/v1/v1/responses/compact", {
      method: "POST",
      body: "{}",
    });
    assert.equal(response.status, 504);
    assert.equal((await response.json()).error.type, "request_deadline");
    await delay(100);
    assert.equal(dispatches, 0);
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }
});

test("supported inference rewrites and Gemini generation paths are bounded, utility routes are not", async () => {
  const app = await fixture(
    async (_req, res) => {
      await delay(80);
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("completed");
    },
    45,
    { ...routing, basePath: "/gateway" }
  );
  try {
    const bounded = [
      "/gateway/chat/completions",
      "/gateway/responses",
      "/gateway/responses/compact",
      "/gateway/RESPONSES/compact",
      "/gateway/codex/session",
      "/gateway/v1beta/models/provider/model:generateContent",
      "/gateway/v1beta/models/provider/model%3AgenerateContent",
      "/gateway/v1beta/models/provider/model:streamGenerateContent",
      "/gateway/v1beta/models/provider/model",
    ];
    for (const path of bounded) {
      const response = await fetch(app.url + path, { method: "POST", body: "{}" });
      assert.equal(response.status, 504, `${path} must receive the inference deadline`);
      await response.arrayBuffer();
    }

    const excluded = [
      "/gateway/healthz",
      "/gateway/v1/models",
      "/gateway/v1/messages/count_tokens",
      "/gateway/v1/responses/input_tokens",
      "/gateway/v1beta/models",
      "/gateway/v1beta/models/",
    ];
    for (const path of excluded) {
      const response = await fetch(app.url + path, { method: "POST", body: "{}" });
      assert.equal(response.status, 200, `${path} must stay outside inference deadline scope`);
      assert.equal(await response.text(), "completed");
    }
    const catalog = await fetch(app.url + "/gateway/v1beta/models", { method: "GET" });
    assert.equal(catalog.status, 200, "the Gemini model catalog GET must stay unbounded");
    assert.equal(await catalog.text(), "completed");
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }

  const caseSensitive = await fixture(
    async (_req, res) => {
      await delay(80);
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("completed");
    },
    45,
    { ...routing, basePath: "/gateway", experimental: { caseSensitiveRoutes: true } }
  );
  try {
    const response = await fetch(caseSensitive.url + "/gateway/RESPONSES/compact", {
      method: "POST",
      body: "{}",
    });
    assert.equal(
      response.status,
      200,
      "case-sensitive route config must keep uppercase alias unmatched"
    );
    assert.equal(await response.text(), "completed");
  } finally {
    await caseSensitive.close();
  }
});

test("fallback cannot restart the total budget or dispatch after it expires", async () => {
  let sends = 0;
  const app = await fixture(async (_req, res) => {
    sends++;
    await delay(150);
    throwIfRequestCancelled();
    sends++;
    res.end("unexpected");
  });
  try {
    const response = await fetch(app.url + "/v1/chat/completions", { method: "POST", body: "{}" });
    assert.equal(response.status, 504);
    await response.text();
    await delay(100);
    assert.equal(sends, 1);
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }
});

test("a real client close preserves cancellation provenance", async () => {
  let cancelled: unknown;
  const app = await fixture(async (_req, _res) => {
    const signal = getRequestLifecycleSignal();
    await new Promise<void>((resolve) =>
      signal?.addEventListener(
        "abort",
        () => {
          cancelled = signal.reason;
          resolve();
        },
        { once: true }
      )
    );
  }, 400);
  try {
    const request = http.request(app.url + "/v1/messages", { method: "POST" });
    request.on("error", () => {});
    request.end("{}");
    await delay(40);
    request.destroy();
    await delay(100);
    assert.ok(cancelled instanceof Error);
    assert.equal(cancelled.name, "AbortError");
    assert.equal(isIngressRequestDeadline(cancelled), false);
    assert.deepEqual(app.errors, []);
  } finally {
    await app.close();
  }
});

test("real upstream slow headers and slow body are cancelled after the owned response", async () => {
  for (const sendHeaders of [false, true]) {
    let upstreamClosed = false;
    const upstream = await serve((_req, res) => {
      res.on("close", () => {
        upstreamClosed = true;
      });
      if (sendHeaders) {
        res.writeHead(200, { "content-type": "application/json" });
        res.flushHeaders();
      }
    });
    const app = await fixture(async (_req, res) => {
      const signal = getRequestLifecycleSignal();
      const response = await fetch(upstream.url, { signal });
      const body = await readNonStreamingResponseBody(response, "application/json", false, 1024, {
        signal,
      });
      throwIfRequestCancelled(signal);
      res.end(body);
    });
    try {
      const response = await fetch(app.url + "/v1/messages", { method: "POST", body: "{}" });
      assert.equal(response.status, 504);
      assert.equal((await response.json()).error.code, "request_ingress_deadline_exceeded");
      await delay(40);
      assert.equal(upstreamClosed, true);
      assert.deepEqual(app.errors, []);
    } finally {
      await app.close();
      await upstream.close();
    }
  }
});

test("a stalled input body receives its owned JSON failure before reader cancellation", async () => {
  let dispatched = false;
  let cancelled = false;
  const app = await fixture(async (req, res) => {
    const signal = getRequestLifecycleSignal();
    const body = Readable.toWeb(req) as ReadableStream<Uint8Array>;
    const reader = body.getReader();
    try {
      while (!(await readRequestChunk(reader, signal)).done) {}
      throwIfRequestCancelled(signal);
      dispatched = true;
      res.end("unexpected");
    } catch (error) {
      cancelled = isIngressRequestDeadline(error);
      throw error;
    } finally {
      reader.releaseLock();
    }
  });
  let request: http.ClientRequest | undefined;
  try {
    const response = await new Promise<{ status: number; body: string }>((resolve, reject) => {
      request = http.request(
        app.url + "/v1/messages",
        {
          method: "POST",
          headers: { "content-type": "application/json", "content-length": "1000" },
        },
        (res) => {
          let body = "";
          res.setEncoding("utf8");
          res.on("data", (part) => {
            body += part;
          });
          res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
          res.on("error", reject);
        }
      );
      request.on("error", reject);
      request.write("{");
    });
    assert.equal(response.status, 504);
    assert.equal(JSON.parse(response.body).error.code, "request_ingress_deadline_exceeded");
    await delay(20);
    assert.equal(cancelled, true);
    assert.equal(dispatched, false);
    assert.deepEqual(app.errors, []);
  } finally {
    request?.destroy();
    await app.close();
  }
});

test("a retained completed ALS context does not retain the native request", async () => {
  v8.setFlagsFromString("--expose-gc");
  const gc = vm.runInNewContext("gc") as () => void;
  let retainedContext: unknown;
  let request: WeakRef<http.IncomingMessage> | undefined;
  const app = await fixture(async (req, res) => {
    const store = (globalThis as unknown as Record<symbol, { getStore(): unknown }>)[
      Symbol.for("omniroute.request-deadline-context.v1")
    ];
    retainedContext = store.getStore();
    request = new WeakRef(req);
    res.writeHead(200, { connection: "close" });
    res.end("ok");
  });
  try {
    const response = await fetch(app.url + "/v1/messages", { method: "POST", body: "{}" });
    assert.equal(await response.text(), "ok");
  } finally {
    await app.close();
  }
  assert.ok(retainedContext);
  for (let i = 0; i < 30; i++) {
    await delay(10);
    gc();
    if (!request?.deref()) break;
  }
  assert.equal(request?.deref(), undefined);
  assert.ok(retainedContext, "retain the store while proving the native request was released");
});
