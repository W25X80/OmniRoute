import assert from "node:assert/strict";
import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { threadFileFixture as io } from "../../helpers/persistence/threadFileFixture.ts";

const ownerUrl = new URL("../../../open-sse/services/notionThreadSessions.ts", import.meta.url)
  .href;
const helperUrl = new URL("../../../open-sse/utils/nativeThreadCacheFile.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/threadFileFixture.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (
      (parent === helperUrl && specifier === "node:fs/promises") ||
      (parent === ownerUrl && specifier === "node:fs")
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const notion = await import("../../../open-sse/services/notionThreadSessions.ts");
const original = process.env.DATA_DIR;
const stateSymbol = Symbol.for("omniroute.nativeNotionThreadSessions");
const globals = globalThis as typeof globalThis & { [stateSymbol]?: unknown };
const turn = () => new Promise<void>((done) => setImmediate(done));
async function scenario(body: (directory: string, path: string) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), "native-notion-threads-"));
  process.env.DATA_DIR = directory;
  delete globals[stateSymbol];
  io.reset();
  io.allowSync = false;
  try {
    await body(directory, join(directory, "notion-web-thread-sessions.json"));
  } finally {
    io.readGate?.release.release();
    io.writeGate?.release.release();
    await notion.closeNativeNotionThreadSessions();
    delete globals[stateSymbol];
    io.reset();
    await rm(directory, { recursive: true, force: true });
    if (original === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = original;
  }
}
test.after(() => hooks.deregister());
test("unused native Notion close does not allocate, hydrate or schedule a cache", async () => {
  delete globals[stateSymbol];
  io.reset();
  await notion.closeNativeNotionThreadSessions();
  await notion.flushNativeNotionThreadSessions();
  assert.equal(globals[stateSymbol], undefined);
  assert.equal(io.reads, 0);
  assert.equal(io.writes, 0);
  assert.equal(io.syncCalls, 0);
});
test("native Notion cold read retains prefix priority, namespace and TTL", async () =>
  scenario(async (_directory, path) => {
    const prefix = [
      { role: "user", content: "task" },
      { role: "assistant", content: "reply" },
    ];
    const messages = [...prefix, { role: "user", content: "next" }];
    const now = Date.now();
    const root = notion.notionThreadRootKey("caller:a|space", messages)!;
    const key = notion.hashNotionConversation("caller:a|space", prefix);
    const expiredRoot = notion.notionThreadRootKey("caller:expired|space", messages)!;
    await writeFile(
      path,
      JSON.stringify({
        [root]: { threadId: "root-thread", ts: now, confirmed: true, createAttempted: true },
        [key]: { threadId: "prefix-thread", ts: now, confirmed: true, createAttempted: true },
        [expiredRoot]: { threadId: "expired-thread", ts: now - 8 * 86400000, confirmed: true },
      })
    );
    const found = await notion.resolveNotionThreadBindingAsync("caller:a|space", messages);
    assert.equal(found.threadId, "prefix-thread");
    assert.equal(found.createThread, false);
    const unrelated = await notion.resolveNotionThreadBindingAsync("caller:b|space", messages);
    assert.notEqual(unrelated.threadId, "prefix-thread");
    assert.equal(unrelated.createThread, true);
    const expired = await notion.resolveNotionThreadBindingAsync("caller:expired|space", messages);
    assert.notEqual(expired.threadId, "expired-thread");
    assert.equal(expired.createThread, true);
    assert.equal(io.syncCalls, 0);
  }));
test("native Notion concurrent cold hydration preserves optimistic retry flags", async () =>
  scenario(async (_directory, path) => {
    const gate = io.gate("read", path);
    const messages = [{ role: "user", content: "hi" }];
    const first = notion.resolveNotionThreadBindingAsync("caller|space", messages);
    await gate.entered.promise;
    const second = notion.resolveNotionThreadBindingAsync("caller|space", messages);
    try {
      await turn();
      assert.equal(io.reads, 1);
    } finally {
      gate.release.release();
    }
    const [a, b] = await Promise.all([first, second]);
    assert.equal(a.threadId, b.threadId);
    assert.equal(a.createThread, true);
    assert.equal(b.createThread, true);
    await notion.notionThreadMarkCreateAttemptedAsync(a.rootKey, a.threadId);
    const retry = await notion.resolveNotionThreadBindingAsync("caller|space", messages);
    assert.equal(retry.threadId, a.threadId);
    assert.equal(retry.createThread, false);
    await notion.notionThreadMarkConfirmedAsync(a.rootKey, a.threadId);
    const fresh = await notion.resolveNotionThreadBindingAsync("caller|space", messages);
    assert.notEqual(fresh.threadId, a.threadId);
    assert.equal(fresh.createThread, true);
    assert.equal(io.syncCalls, 0);
  }));
test("native Notion stores remain deferred and close joins the actual file write", async () =>
  scenario(async (_directory, path) => {
    const messages = [{ role: "user", content: "task" }];
    await notion.notionThreadSessionStoreAsync("space", messages, "reply", "thread");
    assert.equal(io.writes, 0);
    await assert.rejects(access(path));
    const gate = io.gate("write", path);
    let closed = false;
    const close = notion.closeNativeNotionThreadSessions().then(() => {
      closed = true;
    });
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(closed, false);
    } finally {
      gate.release.release();
      await close;
    }
    const disk = JSON.parse(await readFile(path, "utf8"));
    assert.equal(disk[notion.notionThreadRootKey("space", messages)!].threadId, "thread");
    assert.equal(io.syncCalls, 0);
  }));
test("native Notion mutation during delayed snapshot flush is written before close", async () =>
  scenario(async (_directory, path) => {
    const messages = [{ role: "user", content: "task" }];
    await notion.notionThreadSessionStoreAsync("a", messages, "a reply", "a-thread");
    const gate = io.gate("write", path);
    const flush = notion.flushNativeNotionThreadSessions();
    await gate.entered.promise;
    await notion.notionThreadSessionStoreAsync("b", messages, "b reply", "b-thread");
    const close = notion.closeNativeNotionThreadSessions();
    gate.release.release();
    await Promise.all([flush, close]);
    const disk = JSON.parse(await readFile(path, "utf8"));
    assert.equal(disk[notion.notionThreadRootKey("a", messages)!].threadId, "a-thread");
    assert.equal(disk[notion.notionThreadRootKey("b", messages)!].threadId, "b-thread");
    assert.equal(io.writes, 2, "revision changed during write, so another snapshot is required");
    assert.equal(io.syncCalls, 0);
  }));
test("native Notion max-entry eviction and failed local writes remain best-effort", async () =>
  scenario(async (directory, path) => {
    const now = Date.now();
    await writeFile(
      path,
      JSON.stringify(
        Object.fromEntries(
          Array.from({ length: 500 }, (_, i) => [
            `old-${i}`,
            { threadId: `thread-${i}`, ts: now - 1000 + i },
          ])
        )
      )
    );
    await notion.resolveNotionThreadBindingAsync("new", [{ role: "user", content: "task" }]);
    await notion.flushNativeNotionThreadSessions();
    const disk = JSON.parse(await readFile(path, "utf8"));
    assert.equal(Object.keys(disk).length, 500);
    assert.equal(disk["old-0"], undefined);
    const regularFile = join(directory, "not-a-directory");
    await writeFile(regularFile, "x");
    process.env.DATA_DIR = regularFile;
    const binding = await notion.resolveNotionThreadBindingAsync("best-effort", [
      { role: "user", content: "task" },
    ]);
    assert.equal(binding.createThread, true);
    await notion.closeNativeNotionThreadSessions();
    assert.equal(io.syncCalls, 0);
  }));
test("delayed native Notion hydration yields to the event loop", async () =>
  scenario(async (_directory, path) => {
    const gate = io.gate("read", path);
    const loading = notion.resolveNotionThreadBindingAsync("responsive", [
      { role: "user", content: "task" },
    ]);
    await gate.entered.promise;
    try {
      let tick = false;
      await turn().then(() => {
        tick = true;
      });
      assert.equal(tick, true);
    } finally {
      gate.release.release();
      await loading;
    }
    assert.equal(io.syncCalls, 0);
  }));
