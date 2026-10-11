import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import { pathToFileURL } from "node:url";
import type { ProviderCredentials } from "../../../open-sse/executors/base.ts";
import { metadataIo, barrier } from "../../helpers/persistence/servingMetadataIoFixture.ts";

const absolute = (path: string) => pathToFileURL(realpathSync(new URL(path, import.meta.url))).href;
const versionBase = absolute("../../../open-sse/utils/cursorAgentCliVersion.ts");
const versionUrl = `${versionBase}?native-serving-metadata`;
const authBase = absolute("../../../src/lib/api/internalServiceAuth.ts");
const authUrl = `${authBase}?native-serving-metadata`;
const cursorUrl = absolute("../../../open-sse/executors/cursor.ts");
const hopUrl = absolute("../../../open-sse/mcp-server/internalFetch.ts");
const managementAuthUrl = absolute("../../../src/lib/api/requireManagementAuth.ts");
const fixtureUrl = new URL("../../helpers/persistence/servingMetadataIoFixture.ts", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (
      [versionBase, authBase].includes(parent || "") &&
      ["node:fs", "node:fs/promises"].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    if (parent === cursorUrl && specifier === "../utils/cursorAgentCliVersion.ts")
      return { url: versionUrl, shortCircuit: true };
    if (parent === cursorUrl && specifier === "./cursor/agentEndpoint.ts")
      return { url: fixtureUrl, shortCircuit: true };
    if (parent === hopUrl && specifier === "../../src/lib/api/internalServiceAuth.ts")
      return { url: authUrl, shortCircuit: true };
    if (parent === managementAuthUrl && specifier === "@/lib/api/internalServiceAuth")
      return { url: authUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const keys = [
  "OMNIROUTE_DATABASE_BACKEND",
  "CURSOR_AGENT_CLI_VERSION",
  "CURSOR_DATA_DIR",
  "NODE_TEST_CONTEXT",
  "OMNIROUTE_INTERNAL_SERVICE_TOKEN",
  "OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE",
  "OMNIROUTE_API_KEY",
  "DATA_DIR",
  "APP_LOG_TO_FILE",
];
const saved = new Map(keys.map((key) => [key, process.env[key]]));
const dataDirectory = mkdtempSync(join(tmpdir(), "native-serving-metadata-"));
process.env.DATA_DIR = dataDirectory;
process.env.APP_LOG_TO_FILE = "false";
const version: typeof import("../../../open-sse/utils/cursorAgentCliVersion.ts") = await import(
  versionUrl
);
const auth: typeof import("../../../src/lib/api/internalServiceAuth.ts") = await import(authUrl);
const { CursorExecutor } = (await import(
  cursorUrl
)) as typeof import("../../../open-sse/executors/cursor.ts");
const { omniRouteFetch } = (await import(
  hopUrl
)) as typeof import("../../../open-sse/mcp-server/internalFetch.ts");
const { withMcpHttpAuthContext } = await import("../../../open-sse/mcp-server/httpAuthContext.ts");
const { AUTHZ_HEADER_PEER_LOCALITY } = await import("../../../src/server/authz/headers.ts");
const { requireManagementAuth } = (await import(
  managementAuthUrl
)) as typeof import("../../../src/lib/api/requireManagementAuth.ts");
const cacheDir = join("/native", "metadata-cache");
const cacheFile = join(cacheDir, "cursor-agent-cli-version.json");
const versionsDir = join("/native", "versions");
const tokenFile = join("/native", "internal-token");
const request = (token: string, locality = "loopback") =>
  new Request("http://localhost/api/cache", {
    headers: { [auth.INTERNAL_SERVICE_AUTH_HEADER]: token, [AUTHZ_HEADER_PEER_LOCALITY]: locality },
  });

async function scenario(body: () => Promise<void>) {
  metadataIo.reset();
  version.resetCursorAgentCliVersionTestHooks();
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.NODE_TEST_CONTEXT = "native-metadata-fixture";
  delete process.env.CURSOR_AGENT_CLI_VERSION;
  process.env.CURSOR_DATA_DIR = versionsDir;
  delete process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN;
  process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE = tokenFile;
  version.configureCursorAgentCliVersionForTests({ cacheDir });
  try {
    await body();
  } finally {
    metadataIo.gate?.resolve();
    version.resetCursorAgentCliVersionTestHooks();
  }
}
test.after(async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  (await import("../../../src/lib/db/core.ts")).resetDbInstance();
  rmSync(dataDirectory, { recursive: true, force: true });
  metadataIo.reset();
  version.resetCursorAgentCliVersionTestHooks();
  hooks.deregister();
  for (const [key, value] of saved) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test(
  "native cache read yields during slow I/O and performs no synchronous filesystem call",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const cached = "2026.09.01-abcd123";
      metadataIo.files.set(cacheFile, JSON.stringify({ version: cached, fetchedAt: Date.now() }));
      const gate = metadataIo.armRead();
      let finished = false;
      const reading = Promise.all(
        Array.from({ length: 6 }, () => version.getCursorAgentCliVersion())
      ).then((values) => {
        finished = true;
        return values;
      });
      try {
        await Promise.race([
          gate.entered,
          reading.then(() => assert.fail("version completed before its asynchronous read")),
        ]);
        await setImmediate();
        assert.equal(finished, false);
        assert.equal(metadataIo.syncCalls, 0);
        gate.release();
        assert.deepEqual(await reading, Array(6).fill(cached));
        assert.equal(await version.getCursorAgentCliVersion(), cached);
        assert.equal(metadataIo.asyncReads, 1);
      } finally {
        gate.release();
        await Promise.allSettled([reading]);
      }
    })
);

test("native version preserves explicit/local/disk priority and the disk date floor", () =>
  scenario(async () => {
    process.env.CURSOR_AGENT_CLI_VERSION = "2026.01.01-abcd123";
    assert.equal(await version.getCursorAgentCliVersion(), "2026.01.01-abcd123");
    assert.equal(metadataIo.asyncReads, 0);
    version.resetCursorAgentCliVersionCache();
    delete process.env.CURSOR_AGENT_CLI_VERSION;
    const home = process.env.HOME || process.env.USERPROFILE || homedir();
    metadataIo.shims.set(
      join(home, ".local", "bin", "agent"),
      "/native/versions/2026.05.01-abcd123/agent"
    );
    assert.equal(await version.getCursorAgentCliVersion(), "2026.05.01-abcd123");
    version.resetCursorAgentCliVersionCache();
    metadataIo.shims.clear();
    metadataIo.files.set(
      cacheFile,
      JSON.stringify({ version: "2026.01.01-abcd123", fetchedAt: Date.now() })
    );
    assert.equal(await version.getCursorAgentCliVersion(), version.CURSOR_AGENT_CLI_VERSION);
    assert.equal(metadataIo.syncCalls, 0);
  }));

test("native directory detection keeps mtime ordering, ties and vanished entries", () =>
  scenario(async () => {
    const dir = join(versionsDir, "versions");
    metadataIo.directories.set(dir, [
      "2026.08.01-abcd123",
      "2026.09.01-abcd123",
      "2026.10.01-abcd123",
      "2026.11.01-abcd123",
    ]);
    metadataIo.stats.set(join(dir, "2026.08.01-abcd123"), { directory: true, mtimeMs: 20 });
    metadataIo.stats.set(join(dir, "2026.09.01-abcd123"), { directory: true, mtimeMs: 20 });
    metadataIo.stats.set(join(dir, "2026.10.01-abcd123"), { directory: true, mtimeMs: 10 });
    assert.equal(await version.getCursorAgentCliVersion(), "2026.09.01-abcd123");
    assert.equal(metadataIo.syncCalls, 0);
  }));

test(
  "a pin-equal override still follows source cache/SWR policy, using only async I/O",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      process.env.CURSOR_AGENT_CLI_VERSION = version.CURSOR_AGENT_CLI_VERSION;
      delete process.env.NODE_TEST_CONTEXT;
      const remote = barrier();
      metadataIo.written = barrier();
      let fetches = 0;
      version.configureCursorAgentCliVersionForTests({
        fetchImpl: (async () => {
          fetches++;
          await remote.promise;
          return new Response("https://downloads.cursor.com/lab/2026.10.01-abcd123/linux/");
        }) as typeof fetch,
      });
      const reading = version.getCursorAgentCliVersion();
      try {
        assert.equal(
          await reading,
          version.CURSOR_AGENT_CLI_VERSION,
          "remote scrape cannot delay the answer"
        );
        assert.equal(fetches, 1);
        assert.equal(metadataIo.asyncWrites, 0);
        remote.resolve();
        await metadataIo.written.promise;
        assert.equal(JSON.parse(metadataIo.files.get(cacheFile)!).version, "2026.10.01-abcd123");
        assert.equal(metadataIo.syncCalls, 0);
      } finally {
        remote.resolve();
      }
    })
);

test("native explicit refresh awaits async cache write and keeps best-effort write failure", () =>
  scenario(async () => {
    version.configureCursorAgentCliVersionForTests({
      fetchImpl: (async () =>
        new Response("https://downloads.cursor.com/lab/2026.10.01-abcd123/linux/")) as typeof fetch,
    });
    const gate = metadataIo.armWrite();
    let done = false;
    const refresh = version.refreshCursorAgentCliVersionFromInstaller().then((value) => {
      done = true;
      return value;
    });
    try {
      await Promise.race([
        gate.entered,
        refresh.then(() => assert.fail("refresh completed before async cache write")),
      ]);
      await setImmediate();
      assert.equal(done, false);
      gate.release();
      assert.equal(await refresh, "2026.10.01-abcd123");
    } finally {
      gate.release();
      await Promise.allSettled([refresh]);
    }
    assert.equal(JSON.parse(metadataIo.files.get(cacheFile)!).version, "2026.10.01-abcd123");
    metadataIo.failWrite = true;
    assert.equal(await version.refreshCursorAgentCliVersionFromInstaller(), "2026.10.01-abcd123");
    assert.equal(metadataIo.syncCalls, 0);
  }));

test(
  "native token-file verification yields, reads per call and rejects old tokens after rotation",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      metadataIo.files.set(tokenFile, "old-token\n");
      const gate = metadataIo.armRead();
      let done = false;
      const reading = auth
        .isTrustedLoopbackInternalServiceRequestAsync(request("old-token"))
        .then((value) => {
          done = true;
          return value;
        });
      try {
        await Promise.race([
          gate.entered,
          reading.then(() => assert.fail("auth completed before asynchronous token read")),
        ]);
        await setImmediate();
        assert.equal(done, false);
        assert.equal(metadataIo.syncCalls, 0);
        gate.release();
        assert.equal(await reading, true);
        metadataIo.files.set(tokenFile, "new-token\n");
        assert.equal(
          await auth.isTrustedLoopbackInternalServiceRequestAsync(request("old-token")),
          false
        );
        assert.equal(
          await auth.isTrustedLoopbackInternalServiceRequestAsync(request("new-token")),
          true
        );
        assert.deepEqual(await auth.getInternalServiceAuthHeadersAsync(), {
          [auth.INTERNAL_SERVICE_AUTH_HEADER]: "new-token",
        });
        assert.equal(metadataIo.asyncReads, 4);
      } finally {
        gate.release();
        await Promise.allSettled([reading]);
      }
    })
);

test("inline token overrides the file and remote locality short-circuits before a read", () =>
  scenario(async () => {
    metadataIo.failRead = true;
    process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN = " inline-token ";
    assert.equal(await auth.isInternalServiceRequestAsync(request("inline-token")), true);
    assert.deepEqual(await auth.getInternalServiceAuthHeadersAsync(), {
      [auth.INTERNAL_SERVICE_AUTH_HEADER]: "inline-token",
    });
    assert.equal(
      await auth.isTrustedLoopbackInternalServiceRequestAsync(request("inline-token", "remote")),
      false
    );
    assert.equal(metadataIo.asyncReads, 0);
    assert.equal(metadataIo.syncCalls, 0);
  }));

test("missing, unreadable and empty native token files fail closed without fallback credentials", () =>
  scenario(async () => {
    for (const state of ["missing", "unreadable", "empty"]) {
      metadataIo.files.delete(tokenFile);
      metadataIo.failRead = state === "unreadable";
      if (state === "empty") metadataIo.files.set(tokenFile, " \n");
      assert.deepEqual(await auth.getInternalServiceAuthHeadersAsync(), {});
      assert.equal(await auth.isInternalServiceRequestAsync(request("caller-token")), false);
    }
    assert.equal(metadataIo.syncCalls, 0);
  }));

test("native MCP hop awaits its file token while preserving HTTP caller and stdio identities", () =>
  scenario(async () => {
    metadataIo.files.set(tokenFile, "hop-token");
    process.env.OMNIROUTE_API_KEY = "server-key";
    const original = globalThis.fetch;
    const seen: Record<string, string>[] = [];
    globalThis.fetch = (async (_url: string | URL | Request, init?: RequestInit) => {
      seen.push(init!.headers as Record<string, string>);
      return new Response("{}");
    }) as typeof fetch;
    try {
      await withMcpHttpAuthContext(
        new Request("http://localhost", { headers: { Authorization: "Bearer caller-key" } }),
        () => omniRouteFetch("/api/settings")
      );
      await omniRouteFetch("/api/settings");
      assert.equal(seen[0].Authorization, "Bearer caller-key");
      assert.equal(seen[1].Authorization, "Bearer server-key");
      for (const headers of seen)
        assert.equal(headers[auth.INTERNAL_SERVICE_AUTH_HEADER], "hop-token");
      assert.equal(metadataIo.syncCalls, 0);
    } finally {
      globalThis.fetch = original;
    }
  }));

test("both actual native Cursor execute branches await version I/O before assembling headers", () =>
  scenario(async () => {
    const id = "2026.09.01-abcd123";
    metadataIo.files.set(cacheFile, JSON.stringify({ version: id, fetchedAt: Date.now() }));
    const credentials: ProviderCredentials = {
      accessToken: "cursor-fixture-token",
      providerSpecificData: {},
    };
    for (const failedUrl of [false, true]) {
      version.resetCursorAgentCliVersionCache();
      metadataIo.agentFailure = failedUrl;
      const executor = new CursorExecutor();
      Object.assign(executor, {
        async buildRequest() {
          return { body: Buffer.alloc(0), blobStore: new Map() };
        },
        async openH2() {
          throw new Error("fixture transport boundary");
        },
      });
      const result = await executor.execute({
        model: "composer-2",
        body: { messages: [] },
        stream: false,
        credentials,
        signal: undefined,
        upstreamExtraHeaders: undefined,
        clientHeaders: undefined,
      });
      assert.equal(result.headers["x-cursor-client-version"], `cli-${id}`);
      assert.equal(metadataIo.syncCalls, 0);
    }
  }));

test("default SQLite facades still select the authored sync APIs and plain Cursor headers", () =>
  scenario(async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    process.env.CURSOR_AGENT_CLI_VERSION = "2026.01.01-abcd123";
    const headers = new CursorExecutor().buildHeaders({
      accessToken: "sqlite-token",
      providerSpecificData: {},
    });
    assert.equal(headers instanceof Promise, false);
    assert.equal(headers["x-cursor-client-version"], "cli-2026.01.01-abcd123");
    assert.equal(await version.getCursorAgentCliVersion(), "2026.01.01-abcd123");
    assert.deepEqual(await auth.getInternalServiceAuthHeadersAsync(), {});
    assert.equal(metadataIo.syncCalls, 1, "SQLite file failure remains caught by its sync owner");
    assert.equal(metadataIo.asyncReads, 0);
  }));

test(
  "cache reset prevents a late native lookup from replacing the new prepared version",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      metadataIo.files.set(
        cacheFile,
        JSON.stringify({ version: "2026.09.01-abcd123", fetchedAt: Date.now() })
      );
      const gate = metadataIo.armRead();
      const old = version.getCursorAgentCliVersion();
      try {
        await Promise.race([
          gate.entered,
          old.then(() => assert.fail("lookup did not reach asynchronous read")),
        ]);
        version.resetCursorAgentCliVersionCache();
        process.env.CURSOR_AGENT_CLI_VERSION = "2026.10.01-abcd123";
        assert.equal(await version.getCursorAgentCliVersion(), "2026.10.01-abcd123");
        gate.release();
        assert.equal(await old, "2026.09.01-abcd123");
        assert.equal(await version.getCursorAgentCliVersion(), "2026.10.01-abcd123");
        assert.equal(metadataIo.syncCalls, 0);
      } finally {
        gate.release();
        await Promise.allSettled([old]);
      }
    })
);

test(
  "native management auth awaits its loopback verdict and never trusts a pending Promise",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      metadataIo.files.set(tokenFile, "management-token");
      const gate = metadataIo.armRead();
      let done = false;
      const pending = requireManagementAuth(request("management-token"), {
        alwaysRequireAuth: true,
      }).then((value) => {
        done = true;
        return value;
      });
      try {
        await Promise.race([
          gate.entered,
          pending.then(() => assert.fail("management verdict returned before its token read")),
        ]);
        await setImmediate();
        assert.equal(done, false);
        gate.release();
        assert.equal(await pending, null);
        const denied = await requireManagementAuth(request("incorrect-token"), {
          alwaysRequireAuth: true,
        });
        assert.ok(denied instanceof Response);
        assert.ok(denied.status === 401 || denied.status === 403);
        assert.equal(metadataIo.syncCalls, 0);
      } finally {
        gate.release();
        await Promise.allSettled([pending]);
      }
    })
);
