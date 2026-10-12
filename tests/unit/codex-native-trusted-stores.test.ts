import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { parseRequest } from "../../open-sse/vendor/codex-chatgpt-web/responses/parser";
import { MAX_CHATGPT_BROWSER_TABS } from "../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/concurrency";
import { protectedConnectionWrite } from "../../open-sse/vendor/codex-chatgpt-web/storage-state-write";
import { ChatGptThreadEnvironmentStore } from "../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/thread-environment";
import {
  ChatGptLunaCheckpointStore,
  hashChatGptLunaAnswer,
} from "../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/rolling-checkpoint";
import {
  ChatGptTurnSessions,
  ChatGptTraceFeed,
  ChatGptTextFeed,
  type ChatGptTurnRuntime,
} from "../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/turn-execution";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const tick = () => new Promise<void>((resolveTick) => setImmediate(resolveTick));
function runtime(browser: Promise<string> = Promise.resolve("done")): ChatGptTurnRuntime {
  return {
    mode: "read-only",
    browser,
    physicalSettlement: browser.then(() => undefined),
    trace: new ChatGptTraceFeed(),
    text: new ChatGptTextFeed(),
    cancel() {},
  };
}
function child(thread: string, parent: string, home: string, sandbox = "read-only") {
  return parseRequest({
    model: "test",
    input: [
      {
        type: "message",
        role: "user",
        internal_chat_message_metadata_passthrough: { turn_id: "turn" },
        content: [{ type: "input_text", text: "work" }],
      },
    ],
    client_metadata: {
      "x-codex-turn-metadata": JSON.stringify({
        thread_id: thread,
        parent_thread_id: parent,
        turn_id: "turn",
        agent_name: `/root/${thread}`,
        request_kind: "turn",
        subagent_kind: "thread_spawn",
        sandbox,
        workspaces: { [home]: {} },
      }),
    },
  });
}

test("native trusted store boundaries", async (t) => {
  const home = await fs.mkdtemp(join(tmpdir(), "codex-trusted-stores-"));
  t.after(async () => {
    await fs.rm(home, { recursive: true, force: true });
  });
  const at = Date.now();
  const seed = {
    version: 1,
    threads: {
      parent: {
        cwd: home,
        roots: [home],
        writableRoots: [],
        sandboxPolicy: { type: "readOnly", networkAccess: false },
        updatedAt: at,
      },
    },
  };
  const legacy = join(home, "legacy.json"),
    native = join(home, "native.json");
  await fs.writeFile(legacy, JSON.stringify(seed));
  await fs.writeFile(native, JSON.stringify(seed));

  await t.test(
    "default and prepared native authority decisions and file format agree",
    async () => {
      const original = new ChatGptThreadEnvironmentStore(legacy, () => at);
      const selected = new ChatGptThreadEnvironmentStore(native, () => at, "async");
      assert.throws(() => selected.resolve(child("new", "parent", home)), /must be prepared/);
      await selected.prepareAsync();
      const expected = original.resolve(child("new", "parent", home));
      assert.deepEqual(selected.resolve(child("new", "parent", home)), expected);
      await selected.flushAsync();
      assert.deepEqual(
        JSON.parse(await fs.readFile(native, "utf8")),
        JSON.parse(await fs.readFile(legacy, "utf8"))
      );
      assert.throws(() => selected.resolve(child("foreign", "absent", home)));
      assert.throws(
        () => selected.resolve(child("bad", "parent", home, "workspace-write")),
        /conflicts/
      );
    }
  );

  await t.test(
    "metadata file guard prevents B first touch inside A's partial operation",
    async () => {
      const path = join(home, "concurrent.json");
      await fs.writeFile(path, JSON.stringify(seed));
      const first = new ChatGptThreadEnvironmentStore(path, () => at, "async");
      const second = new ChatGptThreadEnvironmentStore(path, () => at, "async");
      const entered = deferred<void>(),
        release = deferred<void>();
      const originalRename = fs.rename;
      let held = false;
      fs.rename = async (...args: Parameters<typeof fs.rename>) => {
        if (String(args[1]) === path && !held) {
          held = true;
          entered.resolve();
          await release.promise;
        }
        return originalRename(...args);
      };
      syncBuiltinESMExports();
      let bEntered = false;
      try {
        const a = protectedConnectionWrite(path, async () => {
          await first.prepareAsync();
          first.resolve(child("child-a", "parent", home));
          await first.flushAsync();
        });
        await entered.promise;
        const b = protectedConnectionWrite(path, async () => {
          bEntered = true;
          await second.prepareAsync();
          const authority = second.resolve(child("child-b", "child-a", home));
          await second.flushAsync();
          return authority;
        });
        await tick();
        assert.equal(bEntered, false);
        release.resolve();
        await a;
        assert.equal((await b).cwd, home);
      } finally {
        release.resolve();
        fs.rename = originalRename;
        syncBuiltinESMExports();
      }
    }
  );

  await t.test("failed required write rejects before tools and retains staged retry", async () => {
    const path = join(home, "failure.json");
    await fs.writeFile(path, JSON.stringify(seed));
    const store = new ChatGptThreadEnvironmentStore(path, () => at, "async");
    await store.prepareAsync();
    store.resolve(child("retry", "parent", home));
    await fs.rm(path);
    await fs.mkdir(path);
    let modelStarted = false;
    await assert.rejects(
      new ChatGptTurnSessions().getOrCreateAfterOwnerRetirement(
        "failure",
        "failure",
        () => runtime(),
        undefined,
        undefined,
        {
          prepareStart: async () => {
            await store.flushAsync();
            return () => {
              modelStarted = true;
              return runtime();
            };
          },
        }
      )
    );
    assert.equal(modelStarted, false);
    await fs.rm(path, { recursive: true });
    await store.flushAsync();
    assert.ok(JSON.parse(await fs.readFile(path, "utf8")).threads.retry);
  });

  await t.test("later staged snapshot is not cleared by an earlier ACK", async () => {
    const path = join(home, "revisions.json");
    await fs.writeFile(path, JSON.stringify(seed));
    const store = new ChatGptThreadEnvironmentStore(path, () => at, "async");
    await store.prepareAsync();
    store.resolve(child("first", "parent", home));
    const entered = deferred<void>(),
      release = deferred<void>();
    const originalRename = fs.rename;
    let held = false;
    fs.rename = async (...args: Parameters<typeof fs.rename>) => {
      if (String(args[1]) === path && !held) {
        held = true;
        entered.resolve();
        await release.promise;
      }
      return originalRename(...args);
    };
    syncBuiltinESMExports();
    try {
      const writing = store.flushAsync();
      await entered.promise;
      store.resolve(child("later", "parent", home));
      const joining = store.flushAsync();
      release.resolve();
      await Promise.all([writing, joining]);
      const state = JSON.parse(await fs.readFile(path, "utf8"));
      assert.ok(state.threads.first);
      assert.ok(state.threads.later);
    } finally {
      release.resolve();
      fs.rename = originalRename;
      syncBuiltinESMExports();
    }
  });

  await t.test(
    "Luna default/native commit bytes agree and answer mismatch still rejects",
    async () => {
      const legacyPath = join(home, "luna-legacy.json"),
        nativePath = join(home, "luna-native.json");
      const input = child("luna", "parent", home);
      const captured = {
        checkpoint: { version: 2 as const, summary: "Preserve the task state." },
        answerHash: hashChatGptLunaAnswer("answer"),
      };
      const original = new ChatGptLunaCheckpointStore(legacyPath, () => at);
      const selected = new ChatGptLunaCheckpointStore(nativePath, () => at, "async");
      await selected.prepareAsync();
      original.commit(input, captured, "answer");
      selected.commit(input, captured, "answer");
      await selected.flushAsync();
      assert.equal(await fs.readFile(nativePath, "utf8"), await fs.readFile(legacyPath, "utf8"));
      assert.throws(() => selected.commit(input, captured, "changed"), /hash/);
      assert.equal(selected.apply(input).applied, false);
      const raw = input._rawBody as {
        input: unknown[];
        client_metadata: Record<string, string>;
      };
      const metadata = JSON.parse(raw.client_metadata["x-codex-turn-metadata"]);
      const follow = parseRequest({
        ...raw,
        input: [
          ...raw.input,
          {
            type: "message",
            role: "assistant",
            internal_chat_message_metadata_passthrough: { turn_id: "turn" },
            content: [{ type: "output_text", text: "answer" }],
          },
          {
            type: "message",
            role: "user",
            internal_chat_message_metadata_passthrough: { turn_id: "turn-next" },
            content: [{ type: "input_text", text: "continue" }],
          },
        ],
        client_metadata: {
          ...raw.client_metadata,
          "x-codex-turn-metadata": JSON.stringify({ ...metadata, turn_id: "turn-next" }),
        },
      });
      follow.modelId = "resolved-model";
      const applied = original.apply(follow);
      assert.equal(applied.applied, true);
      // parseRequest stamps reconstructed messages from its clock on each call.
      const stable = (result: ReturnType<ChatGptLunaCheckpointStore["apply"]>) => ({
        ...result,
        parsed: {
          ...result.parsed,
          context: {
            ...result.parsed.context,
            messages: result.parsed.context.messages.map((message) => ({
              ...message,
              timestamp: 0,
            })),
          },
        },
      });
      assert.deepEqual(stable(selected.apply(follow)), stable(applied));
      assert.equal(applied.parsed.modelId, "resolved-model");
      const reloaded = new ChatGptLunaCheckpointStore(nativePath, () => at, "async");
      await reloaded.prepareAsync();
      assert.deepEqual(stable(reloaded.apply(follow)), stable(applied));

      const badPath = join(home, "malformed.json");
      await fs.writeFile(badPath, '{"version":2}');
      await assert.rejects(
        new ChatGptLunaCheckpointStore(badPath, () => at, "async").prepareAsync(),
        /Invalid/
      );
    }
  );
});

test("native registry initialization boundaries", async (t) => {
  await t.test(
    "same-owner native no-hook starter and duplicate cannot bypass preparation",
    async () => {
      const sessions = new ChatGptTurnSessions();
      const prepare = deferred<void>();
      const browser = deferred<string>();
      let starts = 0;
      const a = sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => {
          throw new Error("unprepared factory");
        },
        "a",
        undefined,
        {
          prepareStart: async () => {
            await prepare.promise;
            return () => {
              starts++;
              return runtime(browser.promise);
            };
          },
        }
      );
      const duplicate = sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => {
          throw new Error("duplicate factory");
        },
        "a2",
        undefined,
        {}
      );
      let duplicatePrepared = false;
      const duplicateWithHook = sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => {
          throw new Error("duplicate factory");
        },
        "a3",
        undefined,
        {
          prepareStart: async () => {
            duplicatePrepared = true;
            return () => runtime();
          },
        }
      );
      const b = sessions.getOrCreateAfterOwnerRetirement(
        "b",
        "owner",
        () => {
          starts++;
          return runtime();
        },
        "b",
        undefined,
        {}
      );
      await tick();
      assert.equal(starts, 0);
      prepare.resolve();
      const created = await a;
      assert.equal(await duplicate, created);
      assert.equal(await duplicateWithHook, created);
      assert.equal(duplicatePrepared, false);
      await tick();
      assert.equal(starts, 1);
      browser.resolve("done");
      await b;
      assert.equal(starts, 2);
    }
  );

  await t.test(
    "source owner retirement precedes initializer and default factory timing stays sync",
    async () => {
      const sessions = new ChatGptTurnSessions();
      const browser = deferred<string>();
      let syncStarted = false,
        prepared = false;
      const first = sessions.getOrCreateAfterOwnerRetirement("first", "owner", () => {
        syncStarted = true;
        return runtime(browser.promise);
      });
      assert.equal(syncStarted, true);
      await first;
      const second = sessions.getOrCreateAfterOwnerRetirement(
        "next",
        "owner",
        () => runtime(),
        undefined,
        undefined,
        {
          prepareStart: async () => {
            prepared = true;
            return () => runtime();
          },
        }
      );
      await tick();
      assert.equal(prepared, false);
      browser.resolve("done");
      await second;
      assert.equal(prepared, true);
    }
  );

  await t.test("same-tick native no-hook cancellation counts one registered session", async () => {
    const sessions = new ChatGptTurnSessions(),
      browser = deferred<string>();
    const pending = sessions.getOrCreateAfterOwnerRetirement(
      "active",
      "owner",
      () => runtime(browser.promise),
      "trace",
      undefined,
      {}
    );
    const cancellation = sessions.cancelTrace("trace");
    browser.resolve("done");
    await pending;
    assert.equal(await cancellation, 1);
  });

  await t.test("failed initializer releases reservation and never starts model", async () => {
    const sessions = new ChatGptTurnSessions();
    let started = false;
    await assert.rejects(
      sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => {
          started = true;
          return runtime();
        },
        undefined,
        undefined,
        {
          prepareStart: async () => {
            throw new Error("required write failed");
          },
        }
      ),
      /required write/
    );
    assert.equal(started, false);
    await sessions.getOrCreateAfterOwnerRetirement(
      "a",
      "owner",
      () => runtime(),
      undefined,
      undefined,
      {}
    );
  });

  await t.test(
    "abort does not release physical initialization reservation or start late model",
    async () => {
      const sessions = new ChatGptTurnSessions(),
        gate = deferred<void>(),
        controller = new AbortController();
      let started = false,
        nextStarted = false;
      const first = sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => runtime(),
        undefined,
        controller.signal,
        {
          prepareStart: async () => {
            await gate.promise;
            return () => {
              started = true;
              return runtime();
            };
          },
        }
      );
      const rejection = assert.rejects(first, /aborted/);
      controller.abort();
      await rejection;
      const next = sessions.getOrCreateAfterOwnerRetirement(
        "b",
        "owner",
        () => {
          nextStarted = true;
          return runtime();
        },
        undefined,
        undefined,
        {}
      );
      await tick();
      assert.equal(nextStarted, false);
      gate.resolve();
      await next;
      assert.equal(started, false);
      assert.equal(nextStarted, true);
    }
  );

  await t.test(
    "clear generation and trace cancel invalidate pending prepared factories",
    async () => {
      const sessions = new ChatGptTurnSessions(),
        gate = deferred<void>();
      let started = false;
      const pending = sessions.getOrCreateAfterOwnerRetirement(
        "a",
        "owner",
        () => runtime(),
        "trace",
        undefined,
        {
          prepareStart: async () => {
            await gate.promise;
            return () => {
              started = true;
              return runtime();
            };
          },
        }
      );
      const rejection = assert.rejects(pending, /aborted/);
      sessions.clear();
      gate.resolve();
      await rejection;
      assert.equal(started, false);
      const gate2 = deferred<void>();
      const pending2 = sessions.getOrCreateAfterOwnerRetirement(
        "b",
        "owner",
        () => runtime(),
        "trace2",
        undefined,
        {
          prepareStart: async () => {
            await gate2.promise;
            return () => {
              started = true;
              return runtime();
            };
          },
        }
      );
      const rejected2 = assert.rejects(pending2, /closed/);
      const cancel = sessions.cancelTrace("trace2");
      gate2.resolve();
      await rejected2;
      assert.equal(await cancel, 1);
      assert.equal(started, false);
    }
  );

  await t.test("pending native initializers reserve the source browser capacity", async () => {
    const sessions = new ChatGptTurnSessions(),
      gate = deferred<void>();
    let prepared = 0;
    const pending = Array.from({ length: MAX_CHATGPT_BROWSER_TABS }, (_, i) =>
      sessions.getOrCreateAfterOwnerRetirement(
        `key-${i}`,
        `owner-${i}`,
        () => runtime(),
        undefined,
        undefined,
        {
          prepareStart: async () => {
            prepared++;
            await gate.promise;
            return () => runtime();
          },
        }
      )
    );
    const duplicate = sessions.getOrCreateAfterOwnerRetirement(
      "key-0",
      "owner-0",
      () => {
        throw new Error("duplicate started");
      },
      undefined,
      undefined,
      {}
    );
    let deniedPrepared = false;
    await assert.rejects(
      sessions.getOrCreateAfterOwnerRetirement(
        "denied",
        "denied",
        () => runtime(),
        undefined,
        undefined,
        {
          prepareStart: async () => {
            deniedPrepared = true;
            return () => runtime();
          },
        }
      ),
      new RegExp(`at most ${MAX_CHATGPT_BROWSER_TABS}`)
    );
    assert.equal(prepared, MAX_CHATGPT_BROWSER_TABS);
    assert.equal(deniedPrepared, false);
    gate.resolve();
    const created = await Promise.all(pending);
    assert.equal(await duplicate, created[0]);
  });

  await t.test(
    "pending entries use the source registry limit and cancel releases after settlement",
    async () => {
      const sessions = new ChatGptTurnSessions(60_000, 1),
        gate = deferred<void>();
      const pending = sessions.getOrCreateAfterOwnerRetirement(
        "pending",
        "pending",
        () => runtime(),
        "trace",
        undefined,
        {
          prepareStart: async () => {
            await gate.promise;
            return () => runtime();
          },
        }
      );
      const rejected = assert.rejects(pending, /closed/);
      const cancel = sessions.cancelTrace("trace");
      let deniedPrepared = false;
      await assert.rejects(
        sessions.getOrCreateAfterOwnerRetirement(
          "denied",
          "denied",
          () => runtime(),
          undefined,
          undefined,
          {
            prepareStart: async () => {
              deniedPrepared = true;
              return () => runtime();
            },
          }
        ),
        /registry is full \(1 entries\)/
      );
      assert.equal(deniedPrepared, false);
      gate.resolve();
      await rejected;
      assert.equal(await cancel, 1);
      await sessions.getOrCreateAfterOwnerRetirement(
        "next",
        "next",
        () => runtime(),
        undefined,
        undefined,
        {}
      );
    }
  );

  await t.test("capacity is rechecked after preparation without launching a model", async () => {
    const sessions = new ChatGptTurnSessions(60_000, 1),
      gate = deferred<void>();
    let started = false;
    const pending = sessions.getOrCreateAfterOwnerRetirement(
      "a",
      "a",
      () => runtime(),
      undefined,
      undefined,
      {
        prepareStart: async () => {
          await gate.promise;
          return () => {
            started = true;
            return runtime();
          };
        },
      }
    );
    sessions.getOrCreate("occupant", () => runtime(), undefined, "other");
    gate.resolve();
    await assert.rejects(pending, /full/);
    assert.equal(started, false);
  });
});
