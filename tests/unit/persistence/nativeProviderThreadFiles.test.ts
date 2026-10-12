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
interface ThreadResolution {
  threadId: string;
  sessionId?: string;
  isFollowUp: boolean;
}
interface Provider {
  name: string;
  file: string;
  clear(): void;
  fingerprint(id: string, messages: Message[]): string;
  resolve(id: string, messages: Message[]): ThreadResolution;
  resolveAsync(
    id: string,
    messages: Message[],
    clientThread?: string,
    clientSession?: string
  ): Promise<ThreadResolution>;
  store(
    id: string,
    messages: Message[],
    reply: string,
    thread: string,
    session?: string
  ): string | null;
  storeAsync(
    id: string,
    messages: Message[],
    reply: string,
    thread: string,
    session?: string
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
    store: (id, messages, reply, thread, session = "session") =>
      hyper.storeHyperAgentThreadAfterTurn(id, messages, reply, thread, session),
    storeAsync: (id, messages, reply, thread, session = "session") =>
      hyper.storeHyperAgentThreadAfterTurnAsync(id, messages, reply, thread, session),
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
  test(`${provider.name} native lookup awaits cold stronger keys before a warm assistant fallback`, async () =>
    scenario(provider, async (_directory, path) => {
      const a = [{ role: "user", content: "independent task A" }];
      const b = [{ role: "user", content: "independent task B" }];
      const follow = (messages: Message[]) => [
        ...messages,
        { role: "assistant", content: "Done." },
        { role: "user", content: "Continue" },
      ];
      io.allowSync = false;
      await provider.storeAsync("same-scope", a, "Done.", "thread-alpha", "session-alpha");
      await provider.storeAsync("same-scope", b, "Done.", "thread-beta", "session-beta");
      provider.clear();
      assert.equal((await provider.resolveAsync("same-scope", follow(b))).threadId, "thread-beta");

      const reads = io.reads;
      const gate = io.gate("read", path);
      let settled = false;
      const resolving = provider.resolveAsync("same-scope", follow(a)).then((value) => {
        settled = true;
        return value;
      });
      try {
        assert.equal(
          await Promise.race([
            gate.entered.promise.then(() => "reading"),
            resolving.then(() => "returned"),
          ]),
          "reading",
          "a weaker memory hit must not bypass a missing stronger disk key"
        );
        await turn();
        assert.equal(settled, false);
        gate.release.release();
        const selected = await resolving;
        assert.equal(selected.threadId, "thread-alpha");
        if (provider.name === "HyperAgent") assert.equal(selected.sessionId, "session-alpha");
        assert.equal(io.reads, reads + 1, "one native file read resolves the stronger misses");
      } finally {
        gate.release.release();
        await resolving;
      }

      const hotReads = io.reads;
      assert.equal((await provider.resolveAsync("same-scope", follow(a))).threadId, "thread-alpha");
      const explicit = await provider.resolveAsync(
        "same-scope",
        follow(a),
        "client-thread",
        "client-session"
      );
      assert.equal(explicit.threadId, "client-thread");
      if (provider.name === "HyperAgent") assert.equal(explicit.sessionId, "client-session");
      assert.equal(io.reads, hotReads, "hot exact and explicit IDs retain zero-I/O paths");
      const rewritten = follow([{ role: "user", content: "rewritten task" }]);
      assert.equal((await provider.resolveAsync("same-scope", rewritten)).threadId, "thread-beta");
      assert.equal((await provider.resolveAsync("foreign-scope", follow(a))).isFollowUp, false);
      assert.equal(io.syncCalls, 0);
    }));

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

test("HyperAgent native cold root task outranks a warm assistant fallback", async () =>
  scenario(providers[0], async () => {
    io.allowSync = false;
    await hyper.storeHyperAgentThreadAfterTurnAsync(
      "same-cookie",
      [{ role: "user", content: "task A" }],
      "Done.",
      "thread-alpha",
      "session-alpha"
    );
    await hyper.storeHyperAgentThreadAfterTurnAsync(
      "same-cookie",
      [{ role: "user", content: "task B" }],
      "Done.",
      "thread-beta",
      "session-beta"
    );
    hyper.clearHyperAgentThreadBindingsForTests();
    await hyper.resolveHyperAgentThreadBindingAsync("same-cookie", [
      { role: "user", content: "task B" },
      { role: "assistant", content: "Done." },
      { role: "user", content: "Continue" },
    ]);
    const reads = io.reads;
    const selected = await hyper.resolveHyperAgentThreadBindingAsync("same-cookie", [
      { role: "user", content: "task A" },
      { role: "assistant", content: "Earlier" },
      { role: "user", content: "Part two" },
      { role: "assistant", content: "Done." },
      { role: "user", content: "Continue" },
    ]);
    assert.equal(selected.threadId, "thread-alpha");
    assert.equal(selected.sessionId, "session-alpha");
    assert.equal(io.reads, reads + 1);
    assert.equal(io.syncCalls, 0);
  }));

test("HyperAgent native executor dispatches into the stronger persisted thread and session", async () =>
  scenario(providers[0], async () => {
    const backend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const fetch = globalThis.fetch;
    const cookie = "cookie=thread-attribution";
    const scope = hyper.cookieFingerprint(cookie);
    const a = [{ role: "user", content: "task A" }];
    const b = [{ role: "user", content: "task B" }];
    const follow = (messages: Message[]) => [
      ...messages,
      { role: "assistant", content: "Done." },
      { role: "user", content: "Continue A" },
    ];
    io.allowSync = false;
    await hyper.storeHyperAgentThreadAfterTurnAsync(
      scope,
      a,
      "Done.",
      "thread-alpha",
      "session-alpha"
    );
    await hyper.storeHyperAgentThreadAfterTurnAsync(
      scope,
      b,
      "Done.",
      "thread-beta",
      "session-beta"
    );
    hyper.clearHyperAgentThreadBindingsForTests();
    await hyper.resolveHyperAgentThreadBindingAsync(scope, follow(b));
    const sends: Array<{ url: string; method: string; body: Record<string, unknown> }> = [];
    globalThis.fetch = async (input, init) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      sends.push({ url, method, body: JSON.parse(String(init?.body ?? "{}")) });
      if (method === "PATCH" && /\/thread-(alpha|beta)$/.test(url))
        return new Response("{}", { status: 200 });
      if (method === "POST" && /\/thread-(alpha|beta)\/chat$/.test(url))
        return new Response('data: {"type":"text","content":"native reply"}\n\ndata: [DONE]\n\n', {
          status: 200,
        });
      throw new Error("Unexpected fixture transport request");
    };
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    try {
      const result = await new hyper.HyperAgentExecutor().execute({
        model: "fable-latest",
        body: { messages: follow(a) },
        stream: false,
        credentials: { apiKey: cookie },
      });
      assert.equal(result.response.status, 200);
      assert.ok(
        sends.some((send) => send.method === "PATCH" && send.url.endsWith("/thread-alpha"))
      );
      const chat = sends.find((send) => send.method === "POST" && send.url.endsWith("/chat"));
      assert.ok(chat);
      assert.ok(chat.url.endsWith("/thread-alpha/chat"));
      assert.equal(chat.body.sessionId, "session-alpha");
      assert.equal(chat.body.content, "Continue A");
      assert.equal(io.syncCalls, 0);
    } finally {
      globalThis.fetch = fetch;
      if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    }
  }));

test("PromptQL native executor sends the new turn into the stronger persisted thread", async () =>
  scenario(providers[1], async () => {
    const { PromptQlExecutor } = await import("../../../open-sse/executors/promptql.ts");
    const backend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const fetch = globalThis.fetch;
    const scope = "01a0fe61-baf4-4e31-9311-8cc0bb3eba91";
    const a = [{ role: "user", content: "task A" }];
    const b = [{ role: "user", content: "task B" }];
    const follow = (messages: Message[]) => [
      ...messages,
      { role: "assistant", content: "Done." },
      { role: "user", content: "Continue A" },
    ];
    io.allowSync = false;
    await prompt.storePromptQlThreadAfterTurnAsync(scope, a, "Done.", "thread-alpha");
    await prompt.storePromptQlThreadAfterTurnAsync(scope, b, "Done.", "thread-beta");
    prompt.clearPromptQlThreadBindingsForTests();
    await prompt.resolvePromptQlThreadBindingAsync(scope, follow(b));
    // Claim-extraction fixture only: every outbound call is handled by the stub below.
    const claims = {
      iss: "enrich-token",
      aud: "promptql.hasura.io",
      exp: Math.floor(Date.now() / 1000) + 3600,
      "https://promptql.hasura.io": { "x-hasura-project-id": scope },
    };
    const token = `${Buffer.from('{"alg":"none","typ":"JWT"}').toString("base64url")}.${Buffer.from(JSON.stringify(claims)).toString("base64url")}.fixture`;
    const sends: Array<{ operationName?: string; variables?: Record<string, unknown> }> = [];
    globalThis.fetch = async (_input, init) => {
      const sent = JSON.parse(String(init?.body ?? "{}")) as (typeof sends)[number];
      sends.push(sent);
      if (sent.operationName === "SendThreadMessage")
        return Response.json({ data: { send_thread_message: { thread_event_id: "10" } } });
      if (sent.operationName === "QueryThreadEvents")
        return Response.json({
          data: {
            thread_events: [
              {
                thread_event_id: "11",
                event_data: {
                  AgentMessage: {
                    update: {
                      content: {
                        interaction_update: {
                          main_agent: {
                            actions_parsed: {
                              actions: [{ final_response: { message: "native reply" } }],
                            },
                            action_completed: {
                              result: {
                                agent_loop_action_result_type: "final_response_sent",
                                message: "native reply",
                              },
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            ],
          },
        });
      throw new Error("Unexpected fixture transport request");
    };
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    try {
      const result = await new PromptQlExecutor().execute({
        model: "gemini-3.7-flash",
        body: { messages: follow(a) },
        stream: false,
        credentials: { apiKey: token, providerSpecificData: { projectId: scope } },
      });
      assert.equal(result.response.status, 200);
      const send = sends.find((sent) => sent.operationName === "SendThreadMessage");
      assert.ok(send);
      assert.equal(send.variables?.threadId, "thread-alpha");
      assert.match(String(send.variables?.message), /Continue A/);
      assert.ok(!String(send.variables?.message).includes("task A"));
      assert.equal(io.syncCalls, 0);
    } finally {
      globalThis.fetch = fetch;
      if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    }
  }));

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
