import test from "node:test";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { createRequire } from "node:module";
import type { IncomingMessage, ServerResponse } from "node:http";
import {
  acquireNativeFileConsumer,
  getNativeFileHttpWork,
  isNativeFileCapacityError,
  NativeFileCapacityError,
  nativeFileDownloadReservation,
  runNativeFileHttpWork,
} from "../../src/lib/db/nativeFileWork.ts";
import {
  withPostgresControlPlane,
  UnsupportedPersistenceDomainError,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { createBodyTooLargeResponse } from "../../src/shared/middleware/bodySizeGuard.ts";
import { setRuntimeAllowedOrigins } from "../../src/server/cors/origins.ts";
import {
  captureNativeFileRouting,
  classifyNativeFileRequest,
} from "../../scripts/dev/persistence-request-boundary.mjs";

type Runtime = NonNullable<typeof globalThis.__omniroutePersistenceRuntime>;
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
async function fixture(body: (identity: object, state: Runtime) => Promise<void> | void) {
  const previous = globalThis.__omniroutePersistenceRuntime,
    identity = {};
  const state: Runtime = {
    backend: "postgres",
    phase: "ready",
    applicationIdentity: identity,
    requests: new Set(),
    background: new Set(),
    tails: new Set(),
    owners: new AsyncLocalStorage(),
  };
  globalThis.__omniroutePersistenceRuntime = state;
  const binding = {
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  try {
    await state.owners.run({ active: true }, () =>
      withPostgresControlPlane(binding, () => body(identity, state))
    );
  } finally {
    globalThis.__omniroutePersistenceRuntime = previous;
  }
}
class Reply extends EventEmitter {
  destroyed = false;
  headersSent = false;
  writableEnded = false;
  writableFinished = false;
  status = 0;
  headers: Record<string, string> = {};
  body = "";
  constructor(readonly lifetime: ReturnType<typeof barrier>) {
    super();
    this.on("finish", lifetime.resolve);
    this.on("close", lifetime.resolve);
  }
  writeHead(status: number, headers: Record<string, string> = {}) {
    this.status = status;
    this.headers = headers;
    this.headersSent = true;
  }
  end(body = "") {
    this.body = body;
    this.writableEnded = true;
    queueMicrotask(() => {
      this.writableFinished = true;
      this.emit("finish");
    });
  }
  destroy() {
    this.destroyed = true;
    this.emit("close");
  }
}
function incoming(headers: Record<string, string> = {}) {
  return Object.assign(new PassThrough(), {
    method: "POST",
    url: "/v1/files",
    headers,
  }) as unknown as IncomingMessage;
}
function transport(
  req: IncomingMessage,
  reply: Reply,
  kind: "upload" | "download" = "upload",
  api = true
) {
  return {
    req,
    res: reply as unknown as ServerResponse,
    kind,
    api,
    onFailure() {
      if (!reply.writableEnded) {
        reply.writeHead(503);
        reply.end("source boundary");
      }
    },
  };
}

test("native file source profile has independent finite count and byte caps with atomic refusal", async () =>
  fixture(async () => {
    const other = (await import(
      new URL("../../src/lib/db/nativeFileWork.ts?separate-bundle", import.meta.url).href
    )) as typeof import("../../src/lib/db/nativeFileWork.ts");
    assert.notEqual(other.NativeFileCapacityError, NativeFileCapacityError);
    assert(isNativeFileCapacityError(new other.NativeFileCapacityError()));
    const work = Array.from({ length: 64 }, (_, index) =>
      index % 2 ? other.acquireNativeFileConsumer() : acquireNativeFileConsumer()
    );
    assert.throws(() => other.acquireNativeFileConsumer(), isNativeFileCapacityError);
    work.forEach((value) => {
      value.release();
      value.release();
    });
    const a = acquireNativeFileConsumer(),
      b = acquireNativeFileConsumer(),
      c = acquireNativeFileConsumer();
    a.reserveDownload(512 * 1024 ** 2);
    b.reserveDownload(512 * 1024 ** 2);
    assert.throws(() => c.reserveDownload(512 * 1024 ** 2), isNativeFileCapacityError);
    a.release();
    b.release();
    c.release();
    assert.equal(nativeFileDownloadReservation(512 * 1024 ** 2), 1536 * 1024 ** 2);
  }));

test("native file consumers reject foreign, ended, unadmitted and invalid authority without changing budget", async () =>
  fixture((identity, state) => {
    const work = acquireNativeFileConsumer();
    const foreign = {
      databaseIdentity: {},
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    withPostgresControlPlane(foreign, () => {
      assert.throws(() => work.reserveDownload(10), UnsupportedPersistenceDomainError);
      assert.throws(() => acquireNativeFileConsumer(), UnsupportedPersistenceDomainError);
    });
    assert.throws(
      () => state.owners.run({ active: false }, () => acquireNativeFileConsumer()),
      UnsupportedPersistenceDomainError
    );
    for (const value of [-1, NaN, 0.1, Number.MAX_SAFE_INTEGER + 1])
      assert.throws(() => work.beforeBuffer(value), TypeError);
    work.release();
    assert.throws(() => work.reserveDownload(10), UnsupportedPersistenceDomainError);
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.bytes, 0);
  }));

test("closing keeps accepted consumers and admitted descendants but denies unadmitted file roots", async () =>
  fixture((identity, state) => {
    const work = acquireNativeFileConsumer();
    state.phase = "closing";
    work.beforeBuffer(1024);
    const descendant = acquireNativeFileConsumer();
    descendant.release();
    assert.throws(
      () => state.owners.run({ active: false }, () => acquireNativeFileConsumer()),
      UnsupportedPersistenceDomainError
    );
    work.release();
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.count, 0);
  }));

test("upload source owner charges8x before forwarding, keeps parser/copy allowance and never recharges replay", async () =>
  fixture(async (identity) => {
    const req = incoming(),
      life = barrier(),
      reply = new Reply(life);
    let forwarded = 0;
    await runNativeFileHttpWork(
      async () => {
        req.on("data", (chunk) => {
          forwarded += chunk.byteLength;
          assert.equal(
            globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.bytes,
            8 * forwarded
          );
        });
        req.emit("data", Buffer.alloc(100));
        const owner = getNativeFileHttpWork();
        owner.beforeForward(100);
        owner.beforeComplete(100);
        const form = new FormData();
        form.append("file", new File([Buffer.alloc(10)], "input"));
        owner.afterMultipart(form);
        owner.beforeFileArrayBuffer(10);
        assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.bytes, 800);
        reply.end();
      },
      life.promise,
      transport(req, reply),
      1000
    );
    assert.equal(forwarded, 100);
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.bytes, 0);
    req.destroy();
  }));

test("accepted file SQL may settle after transport close and the byte credit remains until then", async () =>
  fixture(async (identity) => {
    const req = incoming(),
      life = barrier(),
      sql = barrier(),
      started = barrier(),
      reply = new Reply(life);
    let commits = 0;
    const pending = runNativeFileHttpWork(
      async () => {
        getNativeFileHttpWork().reserveDownload(1024);
        started.resolve();
        await sql.promise;
        commits++;
      },
      life.promise,
      transport(req, reply, "download"),
      500 * 1024 ** 2
    );
    await started.promise;
    reply.destroy();
    await turn();
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.count, 1);
    assert.equal(commits, 0);
    sql.resolve();
    await pending;
    assert.equal(commits, 1);
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.bytes, 0);
    req.destroy();
  }));

test("settled handler retains file credit through transport and rejection completes source boundary before joining it", async () =>
  fixture(async (identity) => {
    const req = incoming(),
      life = barrier(),
      reply = new Reply(life),
      done = barrier();
    const pending = runNativeFileHttpWork(
      async () => {
        getNativeFileHttpWork().reserveDownload(1024);
        done.resolve();
      },
      life.promise,
      transport(req, reply, "download"),
      1024
    );
    await done.promise;
    await turn();
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.count, 1);
    reply.end();
    await pending;
    const rejectedReq = incoming(),
      rejectedLife = barrier(),
      rejectedReply = new Reply(rejectedLife),
      fault = new TypeError("source error witness");
    await assert.rejects(
      runNativeFileHttpWork(
        () => {
          throw fault;
        },
        rejectedLife.promise,
        transport(rejectedReq, rejectedReply),
        1024
      ),
      (error) => error === fault
    );
    assert.equal(rejectedReply.body, "source boundary");
    assert.equal(globalThis.__omnirouteNativeFileWork?.budgets.get(identity)?.count, 0);
    req.destroy();
    rejectedReq.destroy();
  }));

test("early413 precedes capacity and reuses exact source body and token-aware CORS policy", async () =>
  fixture(async () => {
    const previous = [
      process.env.CORS_ALLOW_ALL,
      process.env.CORS_ALLOWED_ORIGINS,
      process.env.CORS_ORIGIN,
    ];
    process.env.CORS_ALLOW_ALL = "false";
    process.env.CORS_ALLOWED_ORIGINS = "";
    process.env.CORS_ORIGIN = "";
    setRuntimeAllowedOrigins(undefined);
    try {
      for (const api of [true, false]) {
        const req = incoming({
            "content-length": "11",
            origin: "https://fixture.invalid",
            authorization: "Bearer public-fixture",
          }),
          life = barrier(),
          reply = new Reply(life);
        let calls = 0;
        await runNativeFileHttpWork(
          () => {
            calls++;
          },
          life.promise,
          transport(req, reply, "upload", api),
          10
        );
        assert.equal(calls, 0);
        assert.equal(reply.status, 413);
        assert.equal(reply.body, await createBodyTooLargeResponse(10).text());
        assert.equal(
          reply.headers["access-control-allow-origin"],
          api ? "https://fixture.invalid" : undefined
        );
        assert.equal(
          reply.headers["access-control-allow-methods"],
          "GET, POST, PUT, DELETE, PATCH, OPTIONS"
        );
      }
    } finally {
      for (const [i, name] of ["CORS_ALLOW_ALL", "CORS_ALLOWED_ORIGINS", "CORS_ORIGIN"].entries()) {
        if (previous[i] === undefined) delete process.env[name];
        else process.env[name] = previous[i];
      }
    }
  }));

test("already closed input cannot leave a capacity refusal waiting on a past close event", async () =>
  fixture(async () => {
    const held = Array.from({ length: 64 }, () => acquireNativeFileConsumer()),
      req = incoming();
    req.destroy();
    await new Promise<void>((resolve) => req.once("close", resolve));
    const life = barrier(),
      reply = new Reply(life);
    await runNativeFileHttpWork(
      () => {
        assert.fail("refused listener must not run");
      },
      life.promise,
      transport(req, reply),
      1024
    );
    assert.equal(reply.status, 503);
    assert.equal(JSON.parse(reply.body).error.code, "service_unavailable");
    assert(!reply.body.includes("at /"));
    held.forEach((work) => work.release());
  }));

test("loaded routing preserves exact tail case, actual aliases/basePath/implicitHEAD and management classification", () => {
  const config = { basePath: "/omni", experimental: { caseSensitiveRoutes: false } },
    routing = captureNativeFileRouting(config);
  config.basePath = "/later";
  assert.deepEqual(
    classifyNativeFileRequest("/OMNI/V1/v1/files/f/content?download=1", "HEAD", routing),
    { kind: "download", api: true }
  );
  assert.deepEqual(classifyNativeFileRequest("/omni/api/files/f/content", "GET", routing), {
    kind: "download",
    api: false,
  });
  assert.equal(classifyNativeFileRequest("/omni/v1/FILES", "POST", routing), undefined);
  assert.equal(classifyNativeFileRequest("/omni/v1/batches/b/content", "GET", routing), undefined);
  assert.throws(() => captureNativeFileRouting(undefined), TypeError);
  const { parseUrl } = createRequire(import.meta.url)(
    "next/dist/shared/lib/router/utils/parse-url.js"
  ) as { parseUrl(url: string): { pathname: string } };
  const root = captureNativeFileRouting({
    basePath: "",
    experimental: { caseSensitiveRoutes: false },
  });
  for (const [raw, normalized] of [
    ["/v1/files#fragment", "/v1/files"],
    ["/v1/a/../files", "/v1/files"],
    ["/api/v1/./files", "/api/v1/files"],
    ["/v1/a/%2e%2e/files", "/v1/files"],
  ]) {
    assert.equal(parseUrl(raw).pathname, normalized);
    assert.deepEqual(classifyNativeFileRequest(raw, "POST", root), {
      kind: "upload",
      api: true,
    });
  }
  for (const raw of ["/v1//files", "/v1\\files"])
    assert.equal(classifyNativeFileRequest(raw, "POST", root), undefined);
  assert.throws(() => parseUrl("invalid-url"), TypeError);
  assert.throws(() => classifyNativeFileRequest("invalid-url", "POST", root), TypeError);
});
