import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { threadFileFixture as io } from "../../helpers/persistence/threadFileFixture.ts";

const helperUrl = new URL("../../../open-sse/utils/nativeThreadCacheFile.ts", import.meta.url).href;
const owners = new Set([
  new URL("../../../open-sse/executors/hyperagent.ts", import.meta.url).href,
  new URL("../../../open-sse/executors/promptql/threadSticky.ts", import.meta.url).href,
]);
const fixtureUrl = new URL("../../helpers/persistence/threadFileFixture.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (
      (parent === helperUrl && specifier === "node:fs/promises") ||
      (owners.has(parent ?? "") && specifier === "node:fs")
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const hyper = await import("../../../open-sse/executors/hyperagent.ts");
const prompt = await import("../../../open-sse/executors/promptql/threadSticky.ts");
interface Message {
  role: string;
  content: string;
}
interface Provider {
  name: string;
  file: string;
  clear(): void;
  fingerprint(id: string, messages: Message[]): string;
  resolve(id: string, messages: Message[]): { threadId: string; isFollowUp: boolean };
  resolveAsync(id: string, messages: Message[]): Promise<{ threadId: string; isFollowUp: boolean }>;
  store(id: string, messages: Message[], reply: string, thread: string): string | null;
  storeAsync(
    id: string,
    messages: Message[],
    reply: string,
    thread: string
  ): Promise<string | null>;
  binding(id: string, thread: string, updatedAt: number): Record<string, unknown>;
}
const providers: Provider[] = [
  {
    name: "HyperAgent",
    file: "hyperagent-thread-sessions.json",
    clear: hyper.clearHyperAgentThreadBindingsForTests,
    fingerprint: hyper.conversationFingerprint,
    resolve: hyper.resolveHyperAgentThreadBinding,
    resolveAsync: hyper.resolveHyperAgentThreadBindingAsync,
    store: (id, messages, reply, thread) =>
      hyper.storeHyperAgentThreadAfterTurn(id, messages, reply, thread, "session"),
    storeAsync: (id, messages, reply, thread) =>
      hyper.storeHyperAgentThreadAfterTurnAsync(id, messages, reply, thread, "session"),
    binding: (id, thread, updatedAt) => ({
      projectKey: id,
      threadId: thread,
      sessionId: "session",
      updatedAt,
    }),
  },
  {
    name: "PromptQL",
    file: "promptql-thread-sessions.json",
    clear: prompt.clearPromptQlThreadBindingsForTests,
    fingerprint: prompt.conversationFingerprint,
    resolve: prompt.resolvePromptQlThreadBinding,
    resolveAsync: prompt.resolvePromptQlThreadBindingAsync,
    store: prompt.storePromptQlThreadAfterTurn,
    storeAsync: prompt.storePromptQlThreadAfterTurnAsync,
    binding: (id, thread, updatedAt) => ({ projectId: id, threadId: thread, updatedAt }),
  },
];
const originalDataDir = process.env.DATA_DIR;
const turn = () => new Promise<void>((done) => setImmediate(done));
async function scenario(
  provider: Provider,
  body: (directory: string, path: string) => Promise<void>
) {
  const directory = await mkdtemp(join(tmpdir(), "native-provider-threads-"));
  process.env.DATA_DIR = directory;
  provider.clear();
  io.reset();
  try {
    await body(directory, join(directory, provider.file));
  } finally {
    io.reset();
    provider.clear();
    await rm(directory, { recursive: true, force: true });
    if (originalDataDir === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = originalDataDir;
  }
}
test.after(() => hooks.deregister());
for (const provider of providers) {
  test(`${provider.name} native cold prefix load, memory hit and missing/corrupt cache use async files`, async () =>
    scenario(provider, async (_directory, path) => {
      const prefix = [
        { role: "user", content: "cold task" },
        { role: "assistant", content: "cold reply" },
      ];
      const messages = [...prefix, { role: "user", content: "next" }];
      const key = provider.fingerprint("cold", prefix);
      await writeFile(
        path,
        JSON.stringify({ [key]: provider.binding("cold", "cold-thread", Date.now()) })
      );
      io.allowSync = false;
      assert.equal((await provider.resolveAsync("cold", messages)).threadId, "cold-thread");
      const reads = io.reads;
      assert.equal((await provider.resolveAsync("cold", messages)).threadId, "cold-thread");
      assert.equal(io.reads, reads);
      assert.equal((await provider.resolveAsync("unknown", messages)).isFollowUp, false);
      await writeFile(path, "not json");
      assert.equal((await provider.resolveAsync("corrupt", messages)).isFollowUp, false);
      await rm(path);
      assert.equal((await provider.resolveAsync("absent", messages)).isFollowUp, false);
      assert.equal(io.syncCalls, 0);
    }));
  test(`${provider.name} native store keeps sync fingerprints, prefix bindings and max eviction`, async () =>
    scenario(provider, async (directory, path) => {
      const messages = [{ role: "user", content: "parity task" }];
      const clock = Date.now;
      Date.now = () => 123456789;
      try {
        const expectedKey = provider.store("parity", messages, "same reply", "same-thread");
        const expected = JSON.parse(await readFile(path, "utf8"));
        provider.clear();
        await writeFile(path, "{}");
        io.reset();
        io.allowSync = false;
        assert.equal(
          await provider.storeAsync("parity", messages, "same reply", "same-thread"),
          expectedKey
        );
        assert.deepEqual(JSON.parse(await readFile(path, "utf8")), expected);
      } finally {
        Date.now = clock;
      }
      const seed = Object.fromEntries(
        Array.from({ length: 205 }, (_, i) => [
          `old-${i}`,
          provider.binding("old", `thread-${i}`, i),
        ])
      );
      await writeFile(path, JSON.stringify(seed));
      await provider.storeAsync("cap", messages, "capped reply", "new-thread");
      const entries = JSON.parse(await readFile(path, "utf8"));
      assert.equal(Object.keys(entries).length, 200);
      assert.equal(entries["old-0"], undefined);
      assert.equal(
        entries[
          provider.fingerprint("cap", [...messages, { role: "assistant", content: "capped reply" }])
        ].threadId,
        "new-thread"
      );
      assert.equal(io.syncCalls, 0);
      void directory;
    }));
  test(`${provider.name} concurrent native turns serialize complete file RMW`, async () =>
    scenario(provider, async (_directory, path) => {
      await writeFile(path, "{}");
      io.allowSync = false;
      const gate = io.gate("write", path);
      const a = provider.storeAsync(
        "a",
        [{ role: "user", content: "a task" }],
        "a reply",
        "a-thread"
      );
      await gate.entered.promise;
      const b = provider.storeAsync(
        "b",
        [{ role: "user", content: "b task" }],
        "b reply",
        "b-thread"
      );
      try {
        await turn();
        assert.equal(io.reads, 1, "second RMW must not read while first write is pending");
      } finally {
        gate.release.release();
        await Promise.allSettled([a, b]);
      }
      const rows = Object.values(JSON.parse(await readFile(path, "utf8"))) as {
        threadId: string;
      }[];
      assert.ok(rows.some((row) => row.threadId === "a-thread"));
      assert.ok(rows.some((row) => row.threadId === "b-thread"));
      assert.equal(io.syncCalls, 0);
    }));
  test(`${provider.name} delayed native cold I/O leaves the event loop responsive`, async () =>
    scenario(provider, async (_directory, path) => {
      const gate = io.gate("read", path);
      io.allowSync = false;
      const operation = provider.resolveAsync("delay", [
        { role: "user", content: "task" },
        { role: "assistant", content: "reply" },
        { role: "user", content: "next" },
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
        await operation;
      }
      assert.equal(io.syncCalls, 0);
    }));
}

test("HyperAgent executor selects native files and awaits its successful-turn write", async () =>
  scenario(providers[0], async (_directory, path) => {
    const executor = new hyper.HyperAgentExecutor();
    const backend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const fetch = globalThis.fetch;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.allowSync = false;
    const gate = io.gate("write", path);
    globalThis.fetch = async (input, init) => {
      const url = String(input);
      if (init?.method === "PATCH" && url.endsWith("/native-thread"))
        return new Response("{}", { status: 200 });
      if (init?.method === "POST" && url.endsWith("/native-thread/chat"))
        return new Response('data: {"type":"text","content":"native reply"}\n\ndata: [DONE]\n\n', {
          status: 200,
        });
      throw new Error("Unexpected mocked HyperAgent request");
    };
    let completed = false;
    const operation = executor
      .execute({
        model: "fable-latest",
        body: {
          messages: [{ role: "user", content: "native task" }],
          hyperagent_thread_id: "native-thread",
        },
        stream: false,
        credentials: { apiKey: "cookie=native" },
      })
      .then((result) => {
        completed = true;
        return result;
      });
    try {
      await gate.entered.promise;
      await turn();
      assert.equal(completed, false);
      gate.release.release();
      assert.equal((await operation).response.status, 200);
      const entries = Object.values(JSON.parse(await readFile(path, "utf8"))) as {
        threadId: string;
      }[];
      assert.ok(entries.some((entry) => entry.threadId === "native-thread"));
      assert.equal(io.syncCalls, 0);
    } finally {
      gate.release.release();
      await operation;
      globalThis.fetch = fetch;
      if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    }
  }));
