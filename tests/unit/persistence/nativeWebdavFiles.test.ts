import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable, Writable } from "node:stream";
import test from "node:test";
import { fileFixture as io } from "../../helpers/persistence/servingFileFixture.mjs";

const owners = new Set(
  ["webdav-handler", "webdav-native-handler"].map(
    (name) => new URL(`../../../scripts/dev/${name}.mjs`, import.meta.url).href
  )
);
const fixtureUrl = new URL("../../helpers/persistence/servingFileFixture.mjs", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      owners.has(context.parentURL?.split("?")[0] ?? "") &&
      ["node:fs", "node:fs/promises"].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const webdav = await import("../../../scripts/dev/webdav-handler.mjs");
const invoke = webdav.maybeHandleWebdav as unknown as (
  req: RequestStream | EventEmitter,
  res: ResponseStream
) => Promise<boolean>;
const auth = `Basic ${Buffer.from("admin:password").toString("base64")}`;
class RequestStream extends Readable {
  headers: Record<string, string> = { authorization: auth };
  url: string;
  method: string;
  aborted = false;
  sent = false;
  constructor(
    method: string,
    url: string,
    private readonly body?: string,
    private readonly manual = false
  ) {
    super();
    this.method = method;
    this.url = url;
  }
  _read(): void {
    if (this.manual || this.sent) return;
    this.sent = true;
    if (this.body) this.push(Buffer.from(this.body));
    this.push(null);
  }
}
class ResponseStream extends Writable {
  status = 0;
  headers: Record<string, string | number> = {};
  headersSent = false;
  chunks: Buffer[] = [];
  writeHead(status: number, headers: Record<string, string | number>): this {
    this.status = status;
    this.headers = headers;
    this.headersSent = true;
    return this;
  }
  _write(chunk: Buffer, _encoding: string, callback: (error?: Error | null) => void): void {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }
  text(): string {
    return Buffer.concat(this.chunks).toString("utf8");
  }
}
const globals = globalThis as typeof globalThis & {
  __omnirouteGetWebdavConfigurationAsync?: () => Promise<{
    enabled: boolean;
    vaultPath: string;
    username: string;
    password: string;
  }>;
};
const previous = globals.__omnirouteGetWebdavConfigurationAsync,
  backend = process.env.OMNIROUTE_DATABASE_BACKEND;
const turn = () => new Promise<void>((done) => setImmediate(done));
function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((done) => {
    release = done;
  });
  return { promise, release };
}
async function response(
  method: string,
  url: string,
  body?: string,
  headers: Record<string, string> = {}
) {
  const req = new RequestStream(method, url, body);
  Object.assign(req.headers, headers);
  const res = new ResponseStream();
  const finished = new Promise<void>((done) => res.once("finish", done));
  assert.equal(await invoke(req, res), true);
  await finished;
  return res;
}
async function scenario(body: (vault: string, outside: string) => Promise<void>) {
  const directory = await mkdtemp(path.join(tmpdir(), "native-webdav-files-")),
    vault = path.join(directory, "vault"),
    outside = path.join(directory, "outside");
  await mkdir(vault);
  await mkdir(outside);
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  globals.__omnirouteGetWebdavConfigurationAsync = async () => ({
    enabled: true,
    vaultPath: vault,
    username: "admin",
    password: "password",
  });
  io.reset();
  io.allowSync = false;
  try {
    await body(vault, outside);
  } finally {
    io.reset();
    if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    globals.__omnirouteGetWebdavConfigurationAsync = previous;
    await rm(directory, { recursive: true, force: true });
  }
}
test.after(() => hooks.deregister());
test("native WebDAV normal HEAD/GET and auth retain SQLite controls", async () =>
  scenario(async (vault) => {
    await writeFile(path.join(vault, "note.txt"), "hello vault");
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    io.allowSync = true;
    const expected = await response("HEAD", "/api/v1/webdav/note.txt");
    assert.ok(io.syncCalls > 0);
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.reset();
    io.allowSync = false;
    const actual = await response("HEAD", "/api/v1/webdav/note.txt");
    assert.equal(actual.status, expected.status);
    assert.deepEqual(actual.headers, expected.headers);
    assert.equal((await response("GET", "/api/v1/webdav/note.txt")).text(), "hello vault");
    assert.equal(
      (
        await response("GET", "/api/v1/webdav/note.txt", undefined, {
          authorization: "Basic invalid",
        })
      ).status,
      401
    );
    const listing = await response("PROPFIND", "/api/v1/webdav/", undefined, { depth: "1" });
    assert.equal(listing.status, 207);
    assert.ok(listing.text().includes("note.txt"));
    assert.equal(io.syncCalls, 0);
  }));
test("native physical vault guard closes the source-existing child-symlink escape", async () =>
  scenario(async (vault, outside) => {
    await writeFile(path.join(outside, "secret.txt"), "outside secret");
    await symlink(outside, path.join(vault, "escape"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    io.allowSync = true;
    assert.equal(
      (await response("GET", "/api/v1/webdav/escape/secret.txt")).text(),
      "outside secret",
      "counterexample: legacy guard is lexical"
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.reset();
    io.allowSync = false;
    const denied = await response("GET", "/api/v1/webdav/escape/secret.txt");
    assert.equal(denied.status, 403);
    assert.equal(denied.text().includes(outside), false);
    assert.equal((await response("PUT", "/api/v1/webdav/escape/new.txt", "new")).status, 403);
    await writeFile(path.join(vault, "move.txt"), "move");
    assert.equal(
      (
        await response("MOVE", "/api/v1/webdav/move.txt", undefined, {
          destination: "/api/v1/webdav/escape/moved.txt",
        })
      ).status,
      403
    );
    assert.equal(
      (
        await response("MOVE", "/api/v1/webdav/escape/secret.txt", undefined, {
          destination: "/api/v1/webdav/import.txt",
        })
      ).status,
      403
    );
    const listing = await response("PROPFIND", "/api/v1/webdav/");
    assert.equal(listing.text().includes("escape/"), false);
    assert.equal(await readFile(path.join(outside, "secret.txt"), "utf8"), "outside secret");
    assert.equal(io.syncCalls, 0);
  }));
test("native vault allows internal links and keeps missing/broken targets as not found", async () =>
  scenario(async (vault) => {
    await mkdir(path.join(vault, "inside"));
    await writeFile(path.join(vault, "inside/note.txt"), "inside");
    await symlink(path.join(vault, "inside"), path.join(vault, "link"));
    await symlink(path.join(vault, "absent.txt"), path.join(vault, "broken"));
    assert.equal((await response("GET", "/api/v1/webdav/link/note.txt")).text(), "inside");
    assert.equal((await response("GET", "/api/v1/webdav/broken")).status, 404);
    assert.equal((await response("GET", "/api/v1/webdav/missing.txt")).status, 404);
    assert.equal((await response("PUT", "/api/v1/webdav/link/new.txt", "new")).status, 201);
    assert.equal(await readFile(path.join(vault, "inside/new.txt"), "utf8"), "new");
    assert.equal(io.syncCalls, 0);
  }));
test("native GET waits for delayed filesystem callbacks and yields an event-loop turn", async () =>
  scenario(async (vault) => {
    const file = path.join(vault, "note.txt");
    await writeFile(file, "delayed");
    const gate = io.block("stat", file);
    let completed = false;
    const work = response("GET", "/api/v1/webdav/note.txt").then((res) => {
      completed = true;
      return res;
    });
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(completed, false);
    } finally {
      gate.release.release();
    }
    assert.equal((await work).text(), "delayed");
    assert.equal(io.syncCalls, 0);
  }));
test("native PUT awaits actual stream close and rename, and cleans failed temporary files", async () =>
  scenario(async (vault) => {
    const gate = io.block("rename", "*");
    let completed = false;
    const work = response("PUT", "/api/v1/webdav/upload.txt", "uploaded").then((res) => {
      completed = true;
      return res;
    });
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(completed, false);
      assert.ok((await readdir(vault)).some((file) => file.includes(".omniroute-webdav-tmp-")));
    } finally {
      gate.release.release();
    }
    assert.equal((await work).status, 201);
    assert.equal(await readFile(path.join(vault, "upload.txt"), "utf8"), "uploaded");
    io.renameFailure = true;
    const failed = await response("PUT", "/api/v1/webdav/upload.txt", "failed update");
    assert.equal(failed.status, 500);
    assert.equal(await readFile(path.join(vault, "upload.txt"), "utf8"), "uploaded");
    assert.equal(
      (await readdir(vault)).some((file) => file.includes(".omniroute-webdav-tmp-")),
      false
    );
    assert.equal(failed.text().includes(vault), false);
    assert.equal(io.syncCalls, 0);
  }));
test("native MKCOL owns delayed end callback and async mutation; MOVE/COPY status stays authored", async () =>
  scenario(async (vault) => {
    const req = new RequestStream("MKCOL", "/api/v1/webdav/new", undefined, true),
      res = new ResponseStream();
    const attached = barrier();
    req.on("newListener", (name) => {
      if (name === "data") attached.release();
    });
    const gate = io.block("mkdir", path.join(vault, "new"));
    let completed = false;
    const work = invoke(req, res).then(() => {
      completed = true;
    });
    await attached.promise;
    await turn();
    assert.equal(completed, false);
    req.push(null);
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(completed, false);
    } finally {
      gate.release.release();
      await work;
    }
    assert.equal(res.status, 201);
    assert.equal((await response("MKCOL", "/api/v1/webdav/new")).status, 405);
    await writeFile(path.join(vault, "copy-source.txt"), "copy");
    assert.equal(
      (
        await response("COPY", "/api/v1/webdav/copy-source.txt", undefined, {
          destination: "/api/v1/webdav/copied.txt",
        })
      ).status,
      201
    );
    await assert.rejects(
      readFile(path.join(vault, "copy-source.txt")),
      "existing COPY is a MOVE alias"
    );
    assert.equal(await readFile(path.join(vault, "copied.txt"), "utf8"), "copy");
    assert.equal(io.syncCalls, 0);
  }));
test("native PUT byte cap removes its temporary file before completion", async () =>
  scenario(async (vault) => {
    class SizeRequest extends EventEmitter {
      method = "PUT";
      url = "/api/v1/webdav/large";
      headers = { authorization: auth };
      aborted = false;
      destroyed = false;
      destroy() {
        this.destroyed = true;
      }
    }
    const req = new SizeRequest(),
      res = new ResponseStream(),
      attached = barrier();
    req.on("newListener", (name) => {
      if (name === "data") attached.release();
    });
    const work = invoke(req, res);
    await attached.promise;
    req.emit("data", { length: webdav.MAX_PUT_BYTES + 1 });
    await work;
    assert.equal(res.status, 413);
    assert.deepEqual(await readdir(vault), []);
    assert.equal(io.syncCalls, 0);
  }));
