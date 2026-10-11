import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";
import {
  getOpenRouterCatalog,
  refreshOpenRouterCatalog,
} from "../../src/lib/catalog/openrouterCatalog.ts";
import {
  getOpenRouterProviderStats,
  refreshOpenRouterProviderStats,
} from "../../src/lib/catalog/openrouterProviderStats.ts";
import { getModelCatalogCacheVersion } from "../../src/lib/db/readCache.ts";

async function secondBundle(name: "catalog" | "providers", directory: string) {
  const basename = name === "catalog" ? "openrouterCatalog" : "openrouterProviderStats";
  const sourcePath = fileURLToPath(
    new URL(`../../src/lib/catalog/${basename}.ts`, import.meta.url)
  );
  const source = await fs.promises.readFile(sourcePath, "utf8");
  const compiled = ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    })
    .outputText.replace(/from "([^"]+)"/g, (_match, specifier: string) => {
      if (specifier === "zod") return `from "${import.meta.resolve("zod")}"`;
      if (specifier.startsWith("@/")) {
        const target = new URL(`../../src/${specifier.slice(2)}.ts`, import.meta.url);
        return `from "${target.href}"`;
      }
      return `from "${specifier}"`;
    });
  const modulePath = path.join(directory, `${basename}-second.mjs`);
  await fs.promises.writeFile(modulePath, compiled);
  return import(pathToFileURL(modulePath).href) as Promise<{
    getOpenRouterCatalog: typeof getOpenRouterCatalog;
    refreshOpenRouterCatalog: typeof refreshOpenRouterCatalog;
    getOpenRouterProviderStats: typeof getOpenRouterProviderStats;
    refreshOpenRouterProviderStats: typeof refreshOpenRouterProviderStats;
  }>;
}

const owners = [
  {
    name: "catalog",
    file: "openrouter-catalog.json",
    ttl: "OPENROUTER_CATALOG_TTL_MS",
    get: getOpenRouterCatalog,
    refresh: refreshOpenRouterCatalog,
    cached: [{ id: "cached/model", name: "Cached" }],
    fresh: [{ id: "fresh/model", name: "Fresh" }],
  },
  {
    name: "providers",
    file: "openrouter-provider-stats.json",
    ttl: "OPENROUTER_PROVIDER_STATS_TTL_MS",
    get: getOpenRouterProviderStats,
    refresh: refreshOpenRouterProviderStats,
    cached: [
      {
        slug: "cached",
        displayName: "Cached",
        modelCount: 1,
        totalTokens: 3,
        totalRequests: 1,
        popularityRank: 1,
      },
    ],
    fresh: [
      {
        slug: "fresh",
        displayName: "Fresh",
        modelCount: 1,
        totalTokens: 15,
        totalRequests: 1,
        popularityRank: 1,
      },
    ],
  },
] as const;

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function guardSync(context: TestContext): string[] {
  const calls: string[] = [];
  for (const method of ["existsSync", "mkdirSync", "readFileSync", "writeFileSync"] as const)
    context.mock.method(fs, method, () => {
      calls.push(method);
      throw new Error(`Unexpected synchronous cache I/O: ${method}`);
    });
  return calls;
}
async function ticker() {
  const advanced = gate();
  let ticks = 0;
  const step = () => {
    if (++ticks === 3) advanced.resolve();
    else setImmediate(step);
  };
  setImmediate(step);
  await advanced.promise;
  return ticks;
}
function freshFetch(input: Parameters<typeof fetch>[0]): Promise<Response> {
  const url = String(input);
  let data: unknown;
  if (url.includes("frontend/v1/all-providers")) data = [{ slug: "fresh", displayName: "Fresh" }];
  else if (url.includes("frontend/v1/catalog/models"))
    data = [{ permaslug: "model", endpoint: { variant: "standard", provider_slug: "fresh" } }];
  else if (url.includes("frontend/v1/rankings/models"))
    data = [
      {
        model_permaslug: "model",
        variant: "standard",
        total_prompt_tokens: 10,
        total_completion_tokens: 5,
        count: 1,
      },
    ];
  else if (url === "https://openrouter.ai/api/v1/models")
    data = [{ id: "fresh/model", name: "Fresh" }];
  else throw new Error("Unexpected fetch target");
  return Promise.resolve(
    new Response(JSON.stringify({ data }), { headers: { "Content-Type": "application/json" } })
  );
}
function normalize(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value));
}
async function fixture(
  owner: (typeof owners)[number],
  body: (directory: string, file: string) => Promise<void>
) {
  const originalBackend = process.env.OMNIROUTE_DATABASE_BACKEND,
    originalDirectory = process.env.DATA_DIR,
    originalTTL = process.env[owner.ttl];
  const directory = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "omniroute-native-openrouter-")
  );
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = directory;
  process.env[owner.ttl] = "3600000";
  try {
    await body(directory, path.join(directory, "cache", owner.file));
  } finally {
    await fs.promises.rm(directory, { recursive: true, force: true });
    if (originalBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = originalBackend;
    if (originalDirectory === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = originalDirectory;
    if (originalTTL === undefined) delete process.env[owner.ttl];
    else process.env[owner.ttl] = originalTTL;
  }
}

for (const owner of owners) {
  test(`${owner.name}: delayed native cache read leaves the event loop responsive`, async (context) =>
    fixture(owner, async (_directory, file) => {
      const fetchedAt = new Date().toISOString();
      await fs.promises.mkdir(path.dirname(file), { recursive: true });
      await fs.promises.writeFile(file, JSON.stringify({ fetchedAt, data: owner.cached }));
      const reading = gate(),
        release = gate(),
        originalRead = fs.promises.readFile;
      context.mock.method(
        fs.promises,
        "readFile",
        async (...args: Parameters<typeof originalRead>) => {
          if (String(args[0]) === file) {
            reading.resolve();
            await release.promise;
          }
          return originalRead(...args);
        }
      );
      context.mock.method(globalThis, "fetch", async () => {
        throw new Error("TTL hit must not fetch");
      });
      const sync = guardSync(context);
      const operation = owner.get();
      try {
        await Promise.race([
          reading.promise,
          operation.then(() => {
            throw new Error("Read was not delayed");
          }),
        ]);
        assert.equal(await ticker(), 3);
        release.resolve();
        assert.deepEqual(await operation, {
          data: owner.cached,
          stale: false,
          cachedAt: fetchedAt,
          fromCache: true,
        });
        assert.deepEqual(sync, []);
      } finally {
        release.resolve();
        await operation.catch(() => {});
      }
    }));

  test(`${owner.name}: valid, stale, corrupt and missing real cache preserve fallback shapes`, async (context) =>
    fixture(owner, async (_directory, file) => {
      context.mock.method(console, "warn", () => {});
      const sync = guardSync(context);
      context.mock.method(globalThis, "fetch", async () => {
        throw new Error("fetch unavailable");
      });
      assert.deepEqual(await owner.get(), {
        data: [],
        stale: true,
        cachedAt: null,
        fromCache: false,
      });
      await fs.promises.writeFile(file, "{");
      assert.deepEqual(await owner.get(), {
        data: [],
        stale: true,
        cachedAt: null,
        fromCache: false,
      });
      const fetchedAt = "2000-01-01T00:00:00.000Z";
      await fs.promises.writeFile(file, JSON.stringify({ fetchedAt, data: owner.cached }));
      assert.deepEqual(await owner.get(), {
        data: owner.cached,
        stale: true,
        cachedAt: fetchedAt,
        fromCache: true,
      });
      context.mock.method(globalThis, "fetch", freshFetch);
      const fresh = await owner.get();
      assert.equal(fresh.fromCache, false);
      assert.equal(fresh.stale, false);
      assert.equal(fresh.cachedAt, null);
      assert.deepEqual(normalize(fresh.data), owner.fresh);
      const saved = JSON.parse(await fs.promises.readFile(file, "utf8")) as {
        fetchedAt: string;
        data: unknown;
      };
      assert.equal(Number.isFinite(Date.parse(saved.fetchedAt)), true);
      assert.deepEqual(saved.data, owner.fresh);
      assert.deepEqual(sync, []);
    }));

  test(`${owner.name}: delayed write blocks its concurrent native reader and completes before refresh returns`, async (context) =>
    fixture(owner, async (_directory, file) => {
      const fetchedAt = new Date().toISOString();
      await fs.promises.mkdir(path.dirname(file), { recursive: true });
      await fs.promises.writeFile(file, JSON.stringify({ fetchedAt, data: owner.cached }));
      const writing = gate(),
        release = gate(),
        originalWrite = fs.promises.writeFile,
        originalRead = fs.promises.readFile;
      let reads = 0,
        refreshComplete = false;
      context.mock.method(
        fs.promises,
        "writeFile",
        async (...args: Parameters<typeof originalWrite>) => {
          if (String(args[0]) === file) {
            writing.resolve();
            await release.promise;
          }
          return originalWrite(...args);
        }
      );
      context.mock.method(
        fs.promises,
        "readFile",
        async (...args: Parameters<typeof originalRead>) => {
          if (String(args[0]) === file) reads++;
          return originalRead(...args);
        }
      );
      context.mock.method(globalThis, "fetch", freshFetch);
      const sync = guardSync(context);
      const before = getModelCatalogCacheVersion();
      const refresh = owner.refresh().then((result) => {
        refreshComplete = true;
        return result;
      });
      let getter: ReturnType<typeof owner.get> | undefined;
      try {
        await Promise.race([
          writing.promise,
          refresh.then(() => {
            throw new Error("Write was not delayed");
          }),
        ]);
        getter = owner.get();
        assert.equal(await ticker(), 3);
        assert.equal(reads, 0);
        assert.equal(refreshComplete, false);
        release.resolve();
        assert.equal((await refresh).ok, true);
        assert.deepEqual(normalize((await getter).data), owner.fresh);
        assert.equal(getModelCatalogCacheVersion() - before, owner.name === "catalog" ? 1 : 0);
        assert.deepEqual(sync, []);
      } finally {
        release.resolve();
        await refresh.catch(() => {});
        await getter?.catch(() => {});
      }
    }));

  test(`${owner.name}: duplicate bundles share native file sections and serialize concurrent writers`, async (context) =>
    fixture(owner, async (directory, file) => {
      const duplicate = await secondBundle(owner.name, directory);
      const foreignGet =
        owner.name === "catalog"
          ? duplicate.getOpenRouterCatalog
          : duplicate.getOpenRouterProviderStats;
      const foreignRefresh =
        owner.name === "catalog"
          ? duplicate.refreshOpenRouterCatalog
          : duplicate.refreshOpenRouterProviderStats;
      await fs.promises.mkdir(path.dirname(file), { recursive: true });
      await fs.promises.writeFile(
        file,
        JSON.stringify({ fetchedAt: new Date().toISOString(), data: owner.cached })
      );
      const writing = gate(),
        release = gate(),
        originalWrite = fs.promises.writeFile,
        originalRead = fs.promises.readFile;
      let writes = 0,
        active = 0,
        peak = 0,
        reads = 0;
      context.mock.method(
        fs.promises,
        "writeFile",
        async (...args: Parameters<typeof originalWrite>) => {
          if (String(args[0]) !== file) return originalWrite(...args);
          const write = ++writes;
          active++;
          peak = Math.max(peak, active);
          try {
            if (write === 1) {
              writing.resolve();
              await release.promise;
            }
            await originalWrite(...args);
          } finally {
            active--;
          }
        }
      );
      context.mock.method(
        fs.promises,
        "readFile",
        async (...args: Parameters<typeof originalRead>) => {
          if (String(args[0]) === file) reads++;
          return originalRead(...args);
        }
      );
      context.mock.method(globalThis, "fetch", freshFetch);
      const sync = guardSync(context);
      const first = owner.refresh();
      let second: ReturnType<typeof foreignRefresh> | undefined;
      let read: ReturnType<typeof foreignGet> | undefined;
      try {
        await Promise.race([
          writing.promise,
          first.then(() => {
            throw new Error("First write was not delayed");
          }),
        ]);
        second = foreignRefresh();
        read = foreignGet();
        assert.equal(await ticker(), 3);
        assert.equal(writes, 1);
        assert.equal(reads, 0);
        release.resolve();
        assert.equal((await first).ok, true);
        assert.equal((await second).ok, true);
        await read;
        assert.equal(peak, 1);
        assert.equal(writes, 2);
        assert.deepEqual(sync, []);
        const saved = JSON.parse(await originalRead(file, "utf8")) as { data: unknown };
        assert.deepEqual(saved.data, owner.fresh);
      } finally {
        release.resolve();
        await first.catch(() => {});
        await second?.catch(() => {});
        await read?.catch(() => {});
      }
    }));

  test(`${owner.name}: a native public operation captures its startup profile and cache target`, async (context) =>
    fixture(owner, async (_directory, file) => {
      const otherDirectory = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "omniroute-openrouter-other-")
      );
      const fetching = gate(),
        release = gate();
      context.mock.method(globalThis, "fetch", async (input: Parameters<typeof fetch>[0]) => {
        fetching.resolve();
        await release.promise;
        return freshFetch(input);
      });
      const sync = guardSync(context);
      const operation = owner.get();
      try {
        await Promise.race([
          fetching.promise,
          operation.then(() => {
            throw new Error("Fetch was not delayed");
          }),
        ]);
        process.env.DATA_DIR = otherDirectory;
        process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
        release.resolve();
        assert.equal((await operation).fromCache, false);
        const saved = JSON.parse(await fs.promises.readFile(file, "utf8")) as { data: unknown };
        assert.deepEqual(saved.data, owner.fresh);
        await assert.rejects(fs.promises.access(path.join(otherDirectory, "cache", owner.file)));
        assert.deepEqual(sync, []);
      } finally {
        release.resolve();
        await operation.catch(() => {});
        await fs.promises.rm(otherDirectory, { recursive: true, force: true });
      }
    }));

  test(`${owner.name}: async write failure stays best-effort and the next file operation recovers`, async (context) =>
    fixture(owner, async (_directory, file) => {
      const fetchedAt = new Date().toISOString();
      await fs.promises.mkdir(path.dirname(file), { recursive: true });
      await fs.promises.writeFile(file, JSON.stringify({ fetchedAt, data: owner.cached }));
      context.mock.method(globalThis, "fetch", freshFetch);
      context.mock.method(console, "warn", () => {});
      const originalWrite = fs.promises.writeFile;
      let fail = true;
      context.mock.method(
        fs.promises,
        "writeFile",
        async (...args: Parameters<typeof originalWrite>) => {
          if (String(args[0]) === file && fail) throw new Error("cache write failed");
          return originalWrite(...args);
        }
      );
      const sync = guardSync(context);
      const before = getModelCatalogCacheVersion();
      assert.equal((await owner.refresh()).ok, true);
      assert.equal(getModelCatalogCacheVersion() - before, owner.name === "catalog" ? 1 : 0);
      assert.deepEqual((await owner.get()).data, owner.cached);
      fail = false;
      assert.equal((await owner.refresh()).ok, true);
      assert.deepEqual(normalize((await owner.get()).data), owner.fresh);
      assert.deepEqual(sync, []);
    }));

  test(`${owner.name}: directory failures retain their boundary and do not poison later work`, async (context) =>
    fixture(owner, async (_directory, file) => {
      context.mock.method(globalThis, "fetch", freshFetch);
      context.mock.method(console, "warn", () => {});
      const originalMkdir = fs.promises.mkdir;
      let fail = true;
      context.mock.method(
        fs.promises,
        "mkdir",
        async (...args: Parameters<typeof originalMkdir>) => {
          if (String(args[0]) === path.dirname(file) && fail)
            throw new Error("directory unavailable");
          return originalMkdir(...args);
        }
      );
      const sync = guardSync(context);
      await assert.rejects(owner.get(), /directory unavailable/);
      const before = getModelCatalogCacheVersion();
      const failed = await owner.refresh();
      assert.equal(failed.ok, false);
      assert.equal(failed.error, "directory unavailable");
      assert.equal(getModelCatalogCacheVersion(), before);
      fail = false;
      assert.equal((await owner.get()).stale, false);
      assert.deepEqual(sync, []);
    }));

  test(`${owner.name}: an existing regular file at cache-directory path preserves write-failure behavior`, async (context) =>
    fixture(owner, async (directory, _file) => {
      await fs.promises.writeFile(path.join(directory, "cache"), "existing file");
      context.mock.method(globalThis, "fetch", freshFetch);
      context.mock.method(console, "warn", () => {});
      const sync = guardSync(context);
      const result = await owner.get();
      assert.equal(result.stale, false);
      assert.equal(result.fromCache, false);
      assert.deepEqual(normalize(result.data), owner.fresh);
      assert.equal((await owner.refresh()).ok, true);
      assert.deepEqual(sync, []);
    }));
}
