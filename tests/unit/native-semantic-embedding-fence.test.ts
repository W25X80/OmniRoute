import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import http from "node:http";
import dns from "node:dns";
import test from "node:test";
import {
  PostgresDatabase,
  type PostgresClient,
  type PostgresResult,
} from "../../src/lib/db/postgres/database.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  CATALOG_TABLES,
  withPostgresCatalogSnapshot,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { createPostgresProviderConnectionRepository } from "../../src/lib/db/postgres/providerRepository.ts";
import { ensureSemanticCacheDbBridge } from "../../src/lib/cache/semanticCacheDbBridge.ts";
import {
  getSemanticCacheManagerAsync,
  resetSemanticCacheManager,
} from "../../open-sse/services/cache/semanticCacheManager.ts";
import { resolveSemanticCacheRuntimeConfigAsync } from "../../open-sse/config/semanticCacheConfig.ts";
import { createDefaultEmbeddingGenerator } from "../../open-sse/services/cache/embeddingClient.ts";
import {
  safeOutboundFetch,
  setSafeOutboundPinnedFetchTestOverride,
} from "../../src/shared/network/safeOutboundFetch.ts";

type Row = Record<string, unknown>;
const nativeFetch = globalThis.fetch;
const originalLookup = dns.promises.lookup;
const originalEnv = { ...process.env };
ensureSemanticCacheDbBridge();

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function listener() {
  const requests: { headers: http.IncomingHttpHeaders; body: string }[] = [];
  const server = http.createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    requests.push({ headers: request.headers, body });
    response.setHeader("Content-Type", "application/json");
    response.end(
      JSON.stringify({
        data: [{ embedding: [1, 0, 0] }],
        usage: { total_tokens: 1 },
      })
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert(address && typeof address !== "string");
  const baseUrl = `http://127.0.0.1:${address.port}`;
  const fetchOptions: RequestInit[] = [];
  globalThis.fetch = async (_input, options) => {
    fetchOptions.push(options ?? {});
    return nativeFetch(`${baseUrl}/v1/embeddings`, options);
  };
  return {
    requests,
    fetchOptions,
    baseUrl,
    async close() {
      globalThis.fetch = nativeFetch;
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      );
    },
  };
}

function fixture(baseUrl: string) {
  const state = {
    row: {
      id: "selected",
      provider: "openai",
      is_active: 1,
      api_key: "stored-key",
      priority: 1,
      provider_specific_data: JSON.stringify({ baseUrl }),
    } as Row | undefined,
    leased: false,
    failLease: false,
    failConnection: false,
    ready: true,
    queries: [] as string[],
    cache: {
      semanticCacheEnabled: true,
      semanticCacheVectorEnabled: true,
      semanticCacheBackend: "memory",
      semanticCacheEmbeddingProvider: "openai",
      semanticCacheEmbeddingModel: "text-embedding-3-small",
      semanticCacheEmbeddingDimension: 3,
      semanticCacheEmbeddingBaseUrl: "",
      semanticCacheEmbeddingApiKey: "",
      semanticCacheRequireZeroTemp: true,
    },
  };
  function values() {
    return Object.entries(state.cache).map(([key, value]) => ({
      namespace: "databaseSettings",
      key: `cache.${key}`,
      value: JSON.stringify(value),
    }));
  }

  class Client extends EventEmitter implements PostgresClient {
    release() {}
    async query<R>({ text }: { text: string; values?: unknown[] }): Promise<PostgresResult<R>> {
      state.queries.push(text);
      let rows: unknown[] = [];
      const command = text.trim().split(/\s/)[0];
      if (text.includes("json_build_object")) {
        rows = [
          {
            values: values(),
            tables: Object.fromEntries(
              CATALOG_TABLES.map((table) => [
                table,
                table === "provider_connections" && state.row ? [state.row] : [],
              ])
            ),
            generation: "1",
            authority: "test-authority",
          },
        ];
      } else if (text.startsWith("SELECT namespace, key, value")) rows = values();
      else if (text.includes("FROM omniroute.provider_connections")) {
        if (state.failConnection) throw new Error("private database connection error");
        rows = state.row ? [{ ...state.row }] : [];
      } else if (text.includes("clock_timestamp()")) rows = [{ now: new Date().toISOString() }];
      else if (text.includes("SELECT * FROM omniroute.exclusive_connection_leases")) {
        if (state.failLease) throw new Error("private database lease error");
        rows = state.leased ? [{ id: 1 }] : [];
      } else if (command === "SELECT") rows = [{}];
      return { rows: rows as R[], rowCount: rows.length, command };
    }
  }
  const pool = new EventEmitter();
  const database = new PostgresDatabase(
    Object.assign(pool, {
      connect: async () => new Client(),
      end: async () => {},
    }),
    { maxConnections: 4, transactionTimeoutMs: 5000 },
    { warn() {} }
  );
  const providers = createPostgresProviderConnectionRepository(database, {
    async assertApiKeyIsNotManagementPassword() {},
    async reconcileCodexUsageHistory() {},
    async deleteConnectionState() {},
    afterConnectionWrite() {},
    afterNodeWrite() {},
  });
  const binding = {
    database,
    databaseIdentity: {},
    providers,
    assertReady() {
      if (!state.ready) throw new Error("Persistence scope has ended");
    },
  } as PostgresControlPlaneBinding;
  return {
    state,
    binding,
    run<T>(body: () => Promise<T>) {
      return withPostgresControlPlane(binding, () => withPostgresCatalogSnapshot(database, body));
    },
  };
}

async function lookup(manager: Awaited<ReturnType<typeof getSemanticCacheManagerAsync>>) {
  return manager.lookup({
    model: "gpt-4.1",
    provider: "openai",
    body: {
      messages: [{ role: "user", content: "embedding fence control" }],
      temperature: 0,
      stream: false,
    },
  });
}

test.beforeEach(() => {
  resetSemanticCacheManager();
  for (const key of Object.keys(process.env))
    if (key.startsWith("OMNIROUTE_SEMANTIC_CACHE_") || key.endsWith("PROXY"))
      delete process.env[key];
  process.env.OMNIROUTE_ALLOW_LOCAL_PROVIDER_URLS = "true";
});
test.afterEach(() => {
  dns.promises.lookup = originalLookup;
  setSafeOutboundPinnedFetchTestOverride(undefined);
  globalThis.fetch = nativeFetch;
});
test.after(() => {
  for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
  Object.assign(process.env, originalEnv);
});

test("native stored-key manager checks live lease on every cached generator use", async () => {
  const server = await listener();
  const f = fixture(server.baseUrl);
  try {
    await f.run(async () => {
      const manager = await getSemanticCacheManagerAsync();
      assert.equal(manager.getConfig().enabled, true);
      assert.equal(manager.getConfig().embeddingApiKey, "stored-key");
      f.state.leased = true;
      assert.deepEqual(await lookup(manager), { hit: false });
      assert.equal(server.requests.length, 0);
      f.state.leased = false;
      await lookup(manager);
      assert.equal(server.requests.length, 1);
      assert.equal(server.requests[0].headers.authorization, "Bearer stored-key");
      assert.deepEqual(JSON.parse(server.requests[0].body), {
        model: "text-embedding-3-small",
        input: "user: embedding fence control",
      });
      assert.equal(await getSemanticCacheManagerAsync(), manager);
      f.state.leased = true;
      await lookup(manager);
      assert.equal(server.requests.length, 1);
      f.state.leased = false;
      await lookup(manager);
      assert.equal(server.requests.length, 2);
      f.state.row = { ...f.state.row!, id: "replacement" };
      const replacement = await getSemanticCacheManagerAsync();
      await lookup(replacement);
      assert.equal(server.requests.length, 3);
      assert(server.fetchOptions.every((options) => !("beforeDispatch" in options)));
      assert(!JSON.stringify(manager.getConfig()).includes("connectionId"));
    });
  } finally {
    await server.close();
  }
});

test("native final fence refuses a lease acquired while controlled public DNS waits", async () => {
  const server = await listener();
  const f = fixture("https://embedding.fixture.invalid/v1");
  const entered = deferred(),
    release = deferred();
  process.env.OMNIROUTE_ALLOW_LOCAL_PROVIDER_URLS = "false";
  dns.promises.lookup = (async () => {
    entered.resolve();
    await release.promise;
    return [{ address: "93.184.216.34", family: 4 }];
  }) as typeof dns.promises.lookup;
  setSafeOutboundPinnedFetchTestOverride(() => globalThis.fetch);
  try {
    await f.run(async () => {
      const manager = await getSemanticCacheManagerAsync();
      const pending = lookup(manager);
      await Promise.race([
        entered.promise,
        pending.then(() => {
          throw new Error("Embedding lookup ended before controlled DNS admission");
        }),
      ]);
      f.state.leased = true;
      release.resolve();
      await pending;
      assert.equal(server.requests.length, 0);
    });
  } finally {
    release.resolve();
    await server.close();
  }
});

test("native selected row and credential operands each fail closed even when no lease exists", async () => {
  const server = await listener();
  const f = fixture(server.baseUrl);
  try {
    await f.run(async () => {
      const manager = await getSemanticCacheManagerAsync();
      const original = { ...f.state.row! };
      for (const changed of [
        undefined,
        { ...original, id: "different" },
        { ...original, provider: "other" },
        { ...original, is_active: 0 },
        { ...original, api_key: "rotated-key" },
        {
          ...original,
          provider_specific_data: JSON.stringify({
            baseUrl: `${server.baseUrl}/different`,
          }),
        },
      ]) {
        f.state.row = changed;
        await lookup(manager);
        assert.equal(server.requests.length, 0);
      }
      f.state.row = original;
      f.state.failConnection = true;
      await lookup(manager);
      assert.equal(server.requests.length, 0);
      f.state.failConnection = false;
      f.state.failLease = true;
      await lookup(manager);
      assert.equal(server.requests.length, 0);
      f.state.failLease = false;
      await lookup(manager);
      assert.equal(server.requests.length, 1);
    });
  } finally {
    await server.close();
  }
});

test("native cached manager refuses ended or foreign authority without reading foreign credentials", async () => {
  const server = await listener();
  const f = fixture(server.baseUrl),
    foreign = fixture(server.baseUrl);
  try {
    const manager = await f.run(() => getSemanticCacheManagerAsync());
    await foreign.run(async () => {
      const before = foreign.state.queries.length;
      await lookup(manager);
      assert.equal(foreign.state.queries.length, before);
      assert.equal(server.requests.length, 0);
    });
    await f.run(async () => {
      f.state.ready = false;
      await lookup(manager);
      assert.equal(server.requests.length, 0);
    });
  } finally {
    f.state.ready = true;
    await server.close();
  }
});

test("explicit DB ENV and override keys retain their own provenance even with equal stored bytes", async () => {
  const server = await listener();
  const f = fixture(server.baseUrl);
  try {
    f.state.cache.semanticCacheEmbeddingApiKey = "stored-key";
    f.state.leased = true;
    await f.run(async () => {
      const resolved = await resolveSemanticCacheRuntimeConfigAsync();
      assert.equal(typeof resolved.storedCredential, "undefined");
      await lookup(await getSemanticCacheManagerAsync());
      assert.equal(server.requests.length, 1);
    });
    f.state.cache.semanticCacheEmbeddingApiKey = "";
    process.env.OMNIROUTE_SEMANTIC_CACHE_EMBEDDING_API_KEY = "stored-key";
    await f.run(async () => {
      assert.equal(
        typeof (await resolveSemanticCacheRuntimeConfigAsync()).storedCredential,
        "undefined"
      );
      await lookup(await getSemanticCacheManagerAsync());
      assert.equal(server.requests.length, 2);
    });
    delete process.env.OMNIROUTE_SEMANTIC_CACHE_EMBEDDING_API_KEY;
    await f.run(async () => {
      assert.equal(
        typeof (
          await resolveSemanticCacheRuntimeConfigAsync({
            embeddingApiKey: "stored-key",
          })
        ).storedCredential,
        "undefined"
      );
    });
  } finally {
    await server.close();
  }
});

test("borrowed key cannot follow an ENV endpoint replacement", async () => {
  const server = await listener();
  const f = fixture(server.baseUrl);
  process.env.OMNIROUTE_SEMANTIC_CACHE_EMBEDDING_BASE_URL = `${server.baseUrl}/other`;
  try {
    await f.run(async () => {
      assert((await resolveSemanticCacheRuntimeConfigAsync()).storedCredential);
      await lookup(await getSemanticCacheManagerAsync());
      assert.equal(server.requests.length, 0);
    });
  } finally {
    await server.close();
  }
});

test("no-hook SQLite generator keeps request bytes and trusted hook is stripped before fetch", async () => {
  const server = await listener();
  try {
    const generator = createDefaultEmbeddingGenerator({
      embeddingProvider: "openai",
      embeddingBaseUrl: server.baseUrl,
      embeddingApiKey: "literal-key",
    });
    assert(await generator("source bytes"));
    assert.equal(server.requests[0].headers.authorization, "Bearer literal-key");
    assert.deepEqual(JSON.parse(server.requests[0].body), {
      model: "text-embedding-3-small",
      input: "source bytes",
    });
    let called = 0;
    await safeOutboundFetch(`${server.baseUrl}/v1/embeddings`, {
      method: "POST",
      body: "exact",
      guard: "block-metadata",
      retry: false,
      beforeDispatch() {
        called++;
      },
    });
    assert.equal(called, 1);
    assert.equal(server.requests[1].body, "exact");
    assert(server.fetchOptions.every((options) => !("beforeDispatch" in options)));
  } finally {
    await server.close();
  }
});

test("abort during final hook prevents HTTP and the guard cannot suppress source abort", async () => {
  const server = await listener();
  const controller = new AbortController();
  try {
    const alreadyAborted = new AbortController();
    alreadyAborted.abort();
    let rejectedHookCalls = 0;
    await assert.rejects(
      safeOutboundFetch(`${server.baseUrl}/v1/embeddings`, {
        method: "POST",
        guard: "block-metadata",
        retry: false,
        signal: alreadyAborted.signal,
        beforeDispatch() {
          rejectedHookCalls++;
        },
      })
    );
    assert.equal(rejectedHookCalls, 0);

    await assert.rejects(
      safeOutboundFetch(`${server.baseUrl}/v1/embeddings`, {
        method: "POST",
        guard: "block-metadata",
        retry: false,
        signal: controller.signal,
        beforeDispatch() {
          controller.abort();
        },
      })
    );
    assert.equal(server.requests.length, 0);
  } finally {
    await server.close();
  }
});
