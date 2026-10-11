import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { EventEmitter, once } from "node:events";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { Readable } from "node:stream";
import test, { afterEach } from "node:test";
import { fileURLToPath } from "node:url";
import {
  bindPersistenceRequestCallback,
  isSqlitePersistenceRequested,
  wrapRequestListenerWithPersistence,
} from "../../scripts/dev/persistence-request-boundary.mjs";
import { maybeHandleWebdav } from "../../scripts/dev/webdav-handler.mjs";

const initialBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const initialBasePath = process.env.OMNIROUTE_BASE_PATH;
const initialHook = globalThis.__omniroutePersistenceRequest;
const initialWebdavReader = globalThis.__omnirouteGetWebdavConfigurationAsync;
afterEach(() => {
  if (initialBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = initialBackend;
  if (initialBasePath === undefined) delete process.env.OMNIROUTE_BASE_PATH;
  else process.env.OMNIROUTE_BASE_PATH = initialBasePath;
  globalThis.__omniroutePersistenceRequest = initialHook;
  globalThis.__omnirouteGetWebdavConfigurationAsync = initialWebdavReader;
});

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

class FakeRequest extends EventEmitter {
  url = "/v1/chat/completions";
  aborted = false;
  pauses = 0;
  resumes = 0;
  pause() {
    this.pauses++;
    return this;
  }
  resume() {
    this.resumes++;
    return this;
  }
}

class FakeResponse extends EventEmitter {
  destroyed = false;
  writableFinished = false;
  writableEnded = false;
  headersSent = false;
  status = 200;
  body = "";
  writeHead(status) {
    this.status = status;
    this.headersSent = true;
  }
  end(body = "") {
    this.body = body;
    this.writableEnded = true;
    this.writableFinished = true;
    this.emit("finish");
  }
  destroy() {
    this.destroyed = true;
    this.emit("close");
  }
}

function installHook(context, gate = Promise.resolve()) {
  globalThis.__omniroutePersistenceRequest = (run, lifetime) =>
    context.run("catalog", async () => {
      await gate;
      await run();
      await lifetime;
    });
}

async function collectReply(requestOptions, bytes) {
  return new Promise((resolve, reject) => {
    const client = http.request(requestOptions, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.once("end", () =>
        resolve({ status: response.statusCode, body: Buffer.concat(chunks).toString("utf8") })
      );
      response.once("error", reject);
    });
    client.once("error", reject);
    client.end(bytes);
  });
}

async function startServer(handler) {
  const server = http.createServer(handler);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return { server, port: address.port };
}

async function closeServer(server) {
  const stopped = new Promise((resolve, reject) =>
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    })
  );
  server.closeAllConnections();
  await stopped;
}

const here = path.dirname(fileURLToPath(import.meta.url));
const devDirectory = path.resolve(here, "../../scripts/dev");

test("SQLite without a hook retains synchronous return, this, and body methods", () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  delete globalThis.__omniroutePersistenceRequest;
  const request = new FakeRequest();
  const response = new FakeResponse();
  const originalOn = request.on;
  const self = {};
  const sentinel = {};
  const handler = wrapRequestListenerWithPersistence(function (req, res) {
    assert.equal(this, self);
    assert.equal(req, request);
    assert.equal(res, response);
    return sentinel;
  });
  assert.equal(handler.call(self, request, response), sentinel);
  assert.equal(request.pauses, 0);
  assert.equal(request.on, originalOn);
  assert.equal(response.listenerCount("finish"), 0);
  assert.equal(isSqlitePersistenceRequested({}), true);
  assert.equal(isSqlitePersistenceRequested({ OMNIROUTE_DATABASE_BACKEND: "postgres" }), false);
  assert.equal(isSqlitePersistenceRequested({ OMNIROUTE_DATABASE_BACKEND: "postgresql" }), false);
});

test("PostgreSQL without admission fails closed and never calls the handler", () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete globalThis.__omniroutePersistenceRequest;
  const request = new FakeRequest();
  const response = new FakeResponse();
  let called = false;
  wrapRequestListenerWithPersistence(() => {
    called = true;
  })(request, response);
  assert.equal(called, false);
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.body).error.code, "persistence_not_ready");
  assert.equal(request.pauses, 0);
});

test("healthz bypasses snapshots, including a normalized configured base path", () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.OMNIROUTE_BASE_PATH = " /team//gateway/ ";
  globalThis.__omniroutePersistenceRequest = () => {
    assert.fail("health requested a snapshot");
  };
  let calls = 0;
  const handler = wrapRequestListenerWithPersistence(() => ++calls);
  for (const url of ["/healthz", "/healthz?x=1", "/team/gateway/healthz?x=1"]) {
    const request = new FakeRequest();
    request.url = url;
    assert.equal(handler(request, new FakeResponse()), calls);
    assert.equal(request.pauses, 0);
  }
  assert.equal(calls, 3);
});

test("HTTP terminal listeners precede admission; finish while waiting skips the handler", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const request = new FakeRequest();
  const response = new FakeResponse();
  const gate = deferred();
  let calls = 0;
  globalThis.__omniroutePersistenceRequest = async (run, lifetime) => {
    for (const event of ["finish", "close", "error"])
      assert.equal(response.listenerCount(event), 1);
    assert.equal(request.listenerCount("aborted"), 1);
    assert.equal(request.listenerCount("error"), 1);
    await gate.promise;
    await run();
    await lifetime;
  };
  const promise = wrapRequestListenerWithPersistence(() => {
    calls++;
  })(request, response);
  response.end();
  gate.resolve();
  await promise;
  assert.equal(calls, 0);
  assert.equal(request.pauses, 1);
  assert.equal(Object.hasOwn(request, "on"), false);
  for (const event of ["finish", "close", "error"]) assert.equal(response.listenerCount(event), 0);
});

test("ownership waits for the handler Promise after HTTP close", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const context = new AsyncLocalStorage();
  installHook(context);
  const request = new FakeRequest();
  const response = new FakeResponse();
  const entered = deferred();
  const handlerGate = deferred();
  let complete = false;
  const promise = wrapRequestListenerWithPersistence(async () => {
    assert.equal(context.getStore(), "catalog");
    entered.resolve();
    await handlerGate.promise;
    assert.equal(context.getStore(), "catalog");
  })(request, response).then(() => {
    complete = true;
  });
  await entered.promise;
  response.destroy();
  await Promise.resolve();
  assert.equal(complete, false);
  handlerGate.resolve();
  await promise;
  assert.equal(complete, true);
});

test("ownership waits for HTTP finish after the handler returns", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const context = new AsyncLocalStorage();
  installHook(context);
  const request = new FakeRequest();
  const response = new FakeResponse();
  const returned = deferred();
  let complete = false;
  const promise = wrapRequestListenerWithPersistence(() => {
    returned.resolve();
  })(request, response).then(() => {
    complete = true;
  });
  await returned.promise;
  await Promise.resolve();
  assert.equal(complete, false);
  response.end();
  await promise;
  assert.equal(complete, true);
});

test("admission rejection restores methods and returns only a fixed safe error", async (t) => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const logs = [];
  t.mock.method(console, "error", (...args) => {
    logs.push(args.join(" "));
  });
  for (const synchronous of [true, false]) {
    const request = new FakeRequest();
    const response = new FakeResponse();
    globalThis.__omniroutePersistenceRequest = () => {
      const error = new Error("password-secret and /private/database/location");
      if (synchronous) throw error;
      return Promise.reject(error);
    };
    await wrapRequestListenerWithPersistence(() => {
      assert.fail("handler ran");
    })(request, response);
    assert.equal(response.status, 503);
    assert.doesNotMatch(response.body, /password-secret|private|at /);
    assert.equal(Object.hasOwn(request, "on"), false);
    assert.equal(Object.hasOwn(request, "addListener"), false);
    assert.equal(response.listenerCount("finish"), 0);
  }
  assert.deepEqual(logs, [
    "[Persistence] Request boundary failed",
    "[Persistence] Request boundary failed",
  ]);
});

test("request abort during admission prevents handler invocation", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const gate = deferred();
  installHook(new AsyncLocalStorage(), gate.promise);
  const request = new FakeRequest();
  const response = new FakeResponse();
  const promise = wrapRequestListenerWithPersistence(() => {
    assert.fail("aborted handler ran");
  })(request, response);
  request.aborted = true;
  request.emit("aborted");
  gate.resolve();
  await promise;
  assert.equal(Object.hasOwn(request, "on"), false);
  assert.equal(response.listenerCount("close"), 0);
});

test("first data consumer runs in its catalog context and remains removable", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const context = new AsyncLocalStorage();
  installHook(context);
  const request = new FakeRequest();
  const response = new FakeResponse();
  const ready = deferred();
  const scopes = [];
  const callback = () => scopes.push(context.getStore());
  const promise = wrapRequestListenerWithPersistence(() => {
    request.on("data", callback);
    ready.resolve();
  })(request, response);
  await ready.promise;
  assert.equal(context.getStore(), undefined);
  request.emit("data", Buffer.from("data"));
  assert.deepEqual(scopes, ["catalog"]);
  request.removeListener("data", callback);
  assert.equal(request.listenerCount("data"), 0);
  assert.equal(Object.hasOwn(request, "on"), false);
  response.end();
  await promise;
});

test("native data once wrappers retain removal and exactly-once behavior", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  installHook(new AsyncLocalStorage());
  for (const removeBeforeEmit of [false, true]) {
    const request = new FakeRequest();
    const response = new FakeResponse();
    const ready = deferred();
    let calls = 0;
    const callback = () => {
      calls++;
    };
    const promise = wrapRequestListenerWithPersistence(() => {
      request.once("data", callback);
      ready.resolve();
    })(request, response);
    await ready.promise;
    assert.equal(request.listeners("data")[0], callback);
    if (removeBeforeEmit) request.removeListener("data", callback);
    request.emit("data", Buffer.from("1"));
    request.emit("data", Buffer.from("2"));
    assert.equal(calls, removeBeforeEmit ? 0 : 1);
    assert.equal(request.listenerCount("data"), 0);
    response.end();
    await promise;
  }
});

test("explicitly bound callback keeps the registration scope on detached emitters", () => {
  const context = new AsyncLocalStorage();
  const emitter = new EventEmitter();
  let observed;
  const callback = context.run("tail-permit", () =>
    bindPersistenceRequestCallback(() => {
      observed = context.getStore();
    })
  );
  emitter.on("done", callback);
  emitter.emit("done");
  assert.equal(observed, "tail-permit");
  emitter.removeListener("done", callback);
  assert.equal(emitter.listenerCount("done"), 0);
});

test(
  "multipart body survives asynchronous admission on a real HTTP IncomingMessage",
  { timeout: 10000 },
  async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    const context = new AsyncLocalStorage();
    const snapshot = deferred();
    const buffered = deferred();
    installHook(context, snapshot.promise);
    const bytes = Buffer.alloc(2 * 1024 * 1024, 0x61);
    bytes[0] = 0x31;
    bytes[bytes.length - 1] = 0x32;
    const digest = createHash("sha256").update(bytes).digest("hex");
    const boundary = "omniroute-persistence-body-fixture";
    const multipart = Buffer.concat([
      Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="body.bin"\r\nContent-Type: application/octet-stream\r\n\r\n`
      ),
      bytes,
      Buffer.from(`\r\n--${boundary}--\r\n`),
    ]);
    let observedBufferedLength = 0;
    let unexpectedEarlyBytes = 0;
    const handlerEntered = deferred();
    let bufferedObserver;
    const handler = wrapRequestListenerWithPersistence(async (req, res) => {
      assert.equal(context.getStore(), "catalog");
      handlerEntered.resolve();
      req.removeListener("readable", bufferedObserver);
      const parsed = new Request("http://127.0.0.1/v1/audio/transcriptions", {
        method: "POST",
        headers: req.headers,
        body: Readable.toWeb(req),
        duplex: "half",
      });
      const form = await parsed.formData();
      const file = form.get("file");
      assert.ok(file && typeof file !== "string");
      const result = Buffer.from(await file.arrayBuffer());
      assert.equal(result.length, bytes.length);
      assert.equal(createHash("sha256").update(result).digest("hex"), digest);
      assert.equal(context.getStore(), "catalog");
      res.end("complete");
    });
    let owned;
    const { server, port } = await startServer((req, res) => {
      // An observer attached before our wrapper creates the real flowing-stream hazard.
      req.on("data", (chunk) => {
        if (!context.getStore()) unexpectedEarlyBytes += chunk.length;
      });
      owned = handler(req, res);
      bufferedObserver = () => {
        observedBufferedLength = req.readableLength;
        buffered.resolve();
      };
      // A once(readable) probe would itself resume existing data consumers when it removes
      // Node's last readable listener. Keep observation passive until handler admission.
      req.on("readable", bufferedObserver);
    });
    try {
      const reply = collectReply(
        {
          host: "127.0.0.1",
          port,
          method: "POST",
          path: "/v1/audio/transcriptions",
          headers: {
            "Content-Type": `multipart/form-data; boundary=${boundary}`,
            "Content-Length": multipart.length,
          },
        },
        multipart
      );
      void reply.catch(() => {});
      await buffered.promise;
      assert.ok(observedBufferedLength > 0);
      assert.ok(
        observedBufferedLength < multipart.length,
        "admission must not buffer the whole upload"
      );
      assert.equal(unexpectedEarlyBytes, 0);
      snapshot.resolve();
      await handlerEntered.promise;
      assert.deepEqual(await reply, { status: 200, body: "complete" });
      await owned;
    } finally {
      snapshot.resolve();
      await closeServer(server);
    }
  }
);

test(
  "the real HTTP fixture falsifies a naive await-before-listener boundary",
  { timeout: 10000 },
  async () => {
    const bytes = Buffer.alloc(256 * 1024, 0x62);
    const snapshot = deferred();
    const bodyEnded = deferred();
    let observerBytes = 0;
    let listenerBytes = -1;
    const { server, port } = await startServer((req, res) => {
      req.on("data", (chunk) => {
        observerBytes += chunk.length;
      });
      req.once("end", () => bodyEnded.resolve());
      void (async () => {
        await snapshot.promise;
        assert.equal(req.readableEnded, true);
        listenerBytes = req.readableLength;
        res.end("prefix-lost");
      })();
    });
    try {
      const reply = collectReply(
        {
          host: "127.0.0.1",
          port,
          method: "POST",
          path: "/upload",
          headers: { "Content-Length": bytes.length },
        },
        bytes
      );
      await bodyEnded.promise;
      snapshot.resolve();
      assert.deepEqual(await reply, { status: 200, body: "prefix-lost" });
      assert.equal(observerBytes, bytes.length);
      assert.equal(listenerBytes, 0);
    } finally {
      snapshot.resolve();
      await closeServer(server);
    }
  }
);

test(
  "an async iterator consumes a paused real HTTP request without a data listener",
  { timeout: 10000 },
  async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    const context = new AsyncLocalStorage();
    installHook(context);
    const bytes = Buffer.alloc(128 * 1024, 0x63);
    const expected = createHash("sha256").update(bytes).digest("hex");
    let owned;
    const handler = wrapRequestListenerWithPersistence(async (req, res) => {
      const chunks = [];
      for await (const chunk of req) {
        assert.equal(context.getStore(), "catalog");
        chunks.push(chunk);
      }
      assert.equal(req.listenerCount("data"), 0);
      const actual = Buffer.concat(chunks);
      assert.equal(actual.length, bytes.length);
      assert.equal(createHash("sha256").update(actual).digest("hex"), expected);
      res.end("iterator-complete");
    });
    const { server, port } = await startServer((req, res) => {
      owned = handler(req, res);
    });
    try {
      const reply = await collectReply(
        {
          host: "127.0.0.1",
          port,
          method: "POST",
          path: "/upload",
          headers: { "Content-Length": bytes.length },
        },
        bytes
      );
      assert.deepEqual(reply, { status: 200, body: "iterator-complete" });
      await owned;
    } finally {
      await closeServer(server);
    }
  }
);

test("WebDAV reads the native configuration within the owned catalog scope", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const context = new AsyncLocalStorage();
  installHook(context);
  let reads = 0;
  globalThis.__omnirouteGetWebdavConfigurationAsync = async () => {
    assert.equal(context.getStore(), "catalog");
    reads++;
    return { enabled: true, username: "fixture", password: "fixture", vaultPath: "/fixture" };
  };
  const request = new FakeRequest();
  request.url = "/api/v1/webdav";
  request.method = "OPTIONS";
  request.headers = { authorization: `Basic ${Buffer.from("fixture:fixture").toString("base64")}` };
  const response = new FakeResponse();
  await wrapRequestListenerWithPersistence((req, res) => maybeHandleWebdav(req, res))(
    request,
    response
  );
  assert.equal(reads, 1);
  assert.equal(response.status, 200);
});

test("WebDAV missing/disabled native configuration returns safe 503 in PostgreSQL", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  for (const reader of [
    undefined,
    async () => ({ enabled: false }),
    async () => {
      throw new Error("credential-and-private-path-should-not-leak");
    },
  ]) {
    globalThis.__omnirouteGetWebdavConfigurationAsync = reader;
    const request = new FakeRequest();
    request.url = "/api/v1/webdav";
    request.method = "OPTIONS";
    request.headers = {};
    const response = new FakeResponse();
    assert.equal(await maybeHandleWebdav(request, response), true);
    assert.equal(response.status, 503);
    assert.doesNotMatch(response.body, /credential-and-private-path|storage\.sqlite|at /);
  }
});

test("transport markers precede Next startup, and the assembly includes the boundary", () => {
  const standalone = fs.readFileSync(path.join(devDirectory, "standalone-server-ws.mjs"), "utf8");
  const dev = fs.readFileSync(path.join(devDirectory, "run-next.mjs"), "utf8");
  const assembly = fs.readFileSync(
    path.resolve(here, "../../scripts/build/assembleStandalone.mjs"),
    "utf8"
  );
  const marker = "globalThis.__omniroutePersistenceTransportInstalled = true";
  assert.ok(standalone.indexOf(marker) < standalone.indexOf('await import("./server.js")'));
  assert.ok(dev.indexOf(marker) < dev.indexOf("await prepareWithHeal()"));
  assert.match(assembly, /dest:\s*\["persistence-request-boundary\.mjs"\]/);
  assert.equal((standalone.match(/wrapRequestListenerWithPersistence\(/g) || []).length, 3);
});
