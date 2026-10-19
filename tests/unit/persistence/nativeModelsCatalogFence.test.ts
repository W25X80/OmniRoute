import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Socket } from "node:net";
import { MockAgent, getGlobalDispatcher, setGlobalDispatcher } from "undici";

const directory = await mkdtemp(join(tmpdir(), "native-models-fence-"));
const saved = new Map(
  ["DATA_DIR", "OMNIROUTE_DATABASE_BACKEND", "APP_LOG_TO_FILE", "JWT_SECRET"].map((name) => [
    name,
    process.env[name],
  ])
);
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.APP_LOG_TO_FILE = "false";
process.env.JWT_SECRET = "native-models-fence-test-secret";
const fixtureUrl = new URL(
  "../../helpers/persistence/nativeModelsCatalogFenceFixture.ts",
  import.meta.url
).href;
const dispatcher = getGlobalDispatcher();
const network = new MockAgent();
network.disableNetConnect();
setGlobalDispatcher(network);
const connect = Socket.prototype.connect;
const networkAttempts: string[] = [];
Socket.prototype.connect = function () {
  networkAttempts.push("socket");
  throw new Error("Catalog fixture forbids sockets");
};
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/models/catalog.ts") &&
      [
        "@/lib/catalog/openrouterCatalog",
        "@omniroute/open-sse/services/aihordeImageCatalog",
        "@/lib/db/ccDiscoveryMetrics",
      ].includes(specifier)
    ) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return next(specifier, context);
  },
});
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => {
  networkAttempts.push("fetch");
  throw new Error("Network is outside this catalog fixture");
};
const { withPostgresControlPlane } = await import("../../../src/lib/db/controlPlaneContext");
const snapshot = await import("../../../src/lib/db/postgres/catalogSnapshot");
const { setModelIsHiddenAsync } = await import("../../../src/lib/db/models");
const { upsertSyncedCapabilitiesAsync, getSyncedCapability } =
  await import("../../../src/lib/modelsDevSync");
const { openRouterCapabilityEntry } =
  await import("../../../src/app/api/v1/models/catalogOpenrouter");
const { GET } = await import("../../../src/app/api/v1/models/route");
const { createManagementSessionToken } = await import("../../helpers/managementSession");
const { barrier, catalogFixture, createCatalogFixture } = (await import(
  fixtureUrl
)) as typeof import("../../helpers/persistence/nativeModelsCatalogFenceFixture");

test.after(async () => {
  hooks.deregister();
  globalThis.fetch = originalFetch;
  Socket.prototype.connect = connect;
  setGlobalDispatcher(dispatcher);
  await network.close();
  for (const [name, value] of saved) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  await rm(directory, { recursive: true, force: true });
  assert.deepEqual(networkAttempts, [], "fixture must not initiate unrelated network activity");
});

const model = {
  id: "fence-fixture/nonempty",
  name: "Fence fixture",
  architecture: { input_modalities: ["text"], output_modalities: ["text"] },
  context_length: 8192,
};
function prepare(empty = false) {
  catalogFixture.fetches = 0;
  catalogFixture.discoveryHits = 0;
  catalogFixture.aiHordeRefreshes = 0;
  catalogFixture.gate = undefined;
  catalogFixture.catalog = { data: empty ? [] : [model] } as typeof catalogFixture.catalog;
  return createCatalogFixture();
}
function request(token?: string) {
  return new Request("http://localhost/api/v1/models?prefix=canonical", {
    headers: {
      "user-agent": "claude-cli/fence-test",
      ...(token ? { cookie: "auth_token=" + token } : {}),
    },
  });
}
async function run(f: ReturnType<typeof createCatalogFixture>, authorized = true) {
  const token = authorized
    ? await withPostgresControlPlane(f.binding, () => createManagementSessionToken())
    : undefined;
  return withPostgresControlPlane(f.binding, () =>
    snapshot.withPostgresCatalogSnapshot(f.database, () => GET(request(token)))
  );
}
async function ids(response: Response) {
  assert.equal(response.status, 200, await response.clone().text());
  const body = (await response.json()) as { data: Array<{ id: string }> };
  return body.data.map((entry) => entry.id);
}

test("native GET rebuilds old derived rows after acknowledged nonempty capabilities and concurrent hide", async () => {
  const f = prepare();
  const entered = barrier(),
    release = barrier();
  catalogFixture.gate = { entered, release };
  const pending = run(f);
  await Promise.race([
    entered.promise,
    pending.then(() => {
      throw new Error("Catalog did not reach provider gate");
    }),
  ]);
  try {
    await withPostgresControlPlane(f.binding, () =>
      snapshot.withPostgresCatalogSnapshot(f.database, () =>
        setModelIsHiddenAsync("openai", "gpt-4o", true)
      )
    );
    assert.equal(f.generation(), "2");
  } finally {
    release.resolve();
  }
  const result = await ids(await pending);
  assert.equal(
    f.capabilityWrites(),
    1,
    "one acknowledged capability write, never replayed by rebuild"
  );
  assert.ok(
    !result.includes("openai/gpt-4o"),
    "published hide must survive own capability publication"
  );
  assert.ok(result.includes("openrouter/" + model.id));
  assert.equal(catalogFixture.discoveryHits, 1);
  assert.equal(catalogFixture.fetches, 1);
  assert.equal(catalogFixture.aiHordeRefreshes, 1);
});

test("native GET retries after acknowledged publication without repeating owned effects", async () => {
  const f = prepare();
  const entered = barrier(),
    release = barrier();
  f.gateProviderListRawCall(2, entered, release);
  const pending = run(f);
  await entered.promise;
  try {
    await withPostgresControlPlane(f.binding, () =>
      snapshot.withPostgresCatalogSnapshot(f.database, () =>
        setModelIsHiddenAsync("openai", "gpt-4o", true)
      )
    );
  } finally {
    release.resolve();
  }
  const result = await ids(await pending);
  assert.equal(f.capabilityWrites(), 1, "acknowledged capability publication is not replayed");
  assert.ok(!result.includes("openai/gpt-4o"), "retry uses the newer policy snapshot");
  assert.ok(result.includes("openrouter/" + model.id));
  assert.equal(catalogFixture.fetches, 1, "retry reuses the provider catalog");
  assert.equal(catalogFixture.discoveryHits, 1, "retry does not repeat the request metric");
  assert.equal(catalogFixture.aiHordeRefreshes, 1, "retry does not repeat AI Horde refresh");
});

test("native GET retries one same-authority policy change from a fresh snapshot", async () => {
  const f = prepare(true);
  const entered = barrier(),
    release = barrier();
  catalogFixture.gate = { entered, release };
  const pending = run(f);
  await entered.promise;
  try {
    await withPostgresControlPlane(f.binding, () =>
      snapshot.withPostgresCatalogSnapshot(f.database, () =>
        setModelIsHiddenAsync("openai", "gpt-4o", true)
      )
    );
  } finally {
    release.resolve();
  }
  const result = await ids(await pending);
  assert.equal(f.capabilityWrites(), 0);
  assert.ok(!result.includes("openai/gpt-4o"), "retry must use the newly hidden model policy");
  assert.equal(catalogFixture.fetches, 1, "retry reuses the already fetched provider catalog");
  assert.equal(catalogFixture.discoveryHits, 1, "retry does not duplicate the discovery metric");
});

test("native GET with its own acknowledged capability publication still succeeds without policy drift", async () => {
  const f = prepare();
  const result = await ids(await run(f));
  assert.ok(result.includes("openai/gpt-4o"));
  assert.ok(result.includes("openrouter/" + model.id));
  assert.equal(f.capabilityWrites(), 1);
});

test("native GET rejects authentication before provider fetch or capability publication", async () => {
  const f = prepare();
  const response = await run(f, false);
  assert.equal(response.status, 401);
  assert.equal(catalogFixture.fetches, 0);
  assert.equal(f.capabilityWrites(), 0);
  assert.equal(f.generation(), "1");
  assert.equal(catalogFixture.discoveryHits, 0);
  assert.equal(catalogFixture.aiHordeRefreshes, 0);
});

test("native GET never retries an uncertain capability COMMIT", async () => {
  const f = prepare();
  f.unknownNextCapabilityCommit();
  await assert.rejects(run(f), /Catalog policy changed/);
  assert.equal(f.capabilityWrites(), 1);
  assert.equal(catalogFixture.fetches, 1);
});

test("native GET rechecks dashboard authentication after a generation change", async () => {
  const f = prepare(true);
  const entered = barrier(),
    release = barrier();
  catalogFixture.gate = { entered, release };
  const pending = run(f);
  await entered.promise;
  try {
    f.rotateManagementAuthGeneration();
  } finally {
    release.resolve();
  }
  const response = await pending;
  assert.equal(response.status, 401);
  assert.equal(f.capabilityWrites(), 0);
  assert.equal(catalogFixture.fetches, 1, "denied retry does not refetch the provider catalog");
  assert.equal(catalogFixture.discoveryHits, 1, "denied retry does not repeat the metric");
});

test("native GET returns a sanitized retryable response after repeated generation churn", async () => {
  const f = prepare(true);
  f.driftAfterNextCatalogSnapshotLoad(2);
  const response = await run(f);
  assert.equal(response.status, 503);
  assert.equal(response.headers.get("retry-after"), "1");
  const body = (await response.json()) as {
    error?: { message?: string };
    data?: unknown;
  };
  assert.equal(body.error?.message, "Model catalog changed while preparing the response");
  assert.equal(body.data, undefined, "no catalog from either rejected generation is returned");
  assert.equal(catalogFixture.fetches, 1, "the retry reuses the first provider catalog");
  assert.equal(catalogFixture.discoveryHits, 1, "the retry does not duplicate the metric");
  assert.equal(f.capabilityWrites(), 0);
});

test("native GET does not retry a generation drift against a replacement authority", async () => {
  const singleBuild = prepare(true);
  await run(singleBuild);
  const singleBuildProviderListCalls = singleBuild.providerListCalls();

  const f = prepare(true);
  const entered = barrier(),
    release = barrier();
  f.driftAfterNextCatalogSnapshotLoad();
  f.gateCatalogSnapshotLoad(2, entered, release);
  const pending = run(f);
  await entered.promise;
  try {
    f.replaceCatalogAuthority();
  } finally {
    release.resolve();
  }
  const outcome = await pending.then(
    (response) => ({ status: response.status, error: undefined as unknown }),
    (error: unknown) => ({ status: undefined, error })
  );
  const violations: string[] = [];
  if (outcome.error === undefined) {
    violations.push(`unexpected fulfilled catalog response HTTP ${outcome.status}`);
  } else if (!String(outcome.error).includes("Catalog authority changed")) {
    violations.push(`unexpected rejection: ${String(outcome.error)}`);
  }
  if (f.providerListCalls() !== singleBuildProviderListCalls) {
    violations.push(
      `provider-list calls ${f.providerListCalls()} exceed single-build baseline ${singleBuildProviderListCalls}`
    );
  }
  assert.deepEqual(violations, [], violations.join("; "));
  assert.equal(f.capabilityWrites(), 0);
  assert.equal(catalogFixture.fetches, 1, "authority change is rejected before a second build");
  assert.equal(catalogFixture.discoveryHits, 1);
});

test("native GET does not retry a failed capability publication after independent drift", async () => {
  const f = prepare();
  const entered = barrier(),
    release = barrier();
  catalogFixture.gate = { entered, release };
  f.rejectNextCapabilityCommit();
  const pending = run(f);
  await entered.promise;
  try {
    await withPostgresControlPlane(f.binding, () =>
      snapshot.withPostgresCatalogSnapshot(f.database, () =>
        setModelIsHiddenAsync("openai", "gpt-4o", true)
      )
    );
  } finally {
    release.resolve();
  }
  await assert.rejects(pending, /Catalog policy changed/);
  assert.equal(f.capabilityWrites(), 1);
  assert.equal(catalogFixture.fetches, 1, "failed publication blocks retry and refetch");
  assert.equal(catalogFixture.discoveryHits, 1);
});

test("ordinary capability consumers retain acknowledged read-own-writes", async () => {
  const f = prepare();
  await withPostgresControlPlane(f.binding, () =>
    snapshot.withPostgresCatalogSnapshot(f.database, async () => {
      const before = snapshot.catalogGeneration();
      const originalVersion = snapshot.capturePostgresCatalogVersion();
      const cap = openRouterCapabilityEntry(model, ["text"], ["text"], {});
      assert.ok(cap);
      await upsertSyncedCapabilitiesAsync("openrouter", { [model.id]: cap });
      assert.notEqual(snapshot.catalogGeneration(), before);
      assert.equal(getSyncedCapability("openrouter", model.id)?.limit_context, 8192);
      await snapshot.assertPostgresCatalogSnapshotCurrent();
      await assert.rejects(
        snapshot.assertPostgresCatalogSnapshotCurrent(originalVersion),
        /Catalog policy changed/
      );
    })
  );
});

test("a rejected optional capability commit does not rebuild or replay", async () => {
  const f = prepare();
  f.rejectNextCapabilityCommit();
  const result = await ids(await run(f));
  assert.ok(result.includes("openai/gpt-4o"));
  assert.equal(f.capabilityWrites(), 1);
  assert.equal(catalogFixture.fetches, 1);
  assert.equal(catalogFixture.discoveryHits, 1);
  assert.equal(f.generation(), "1");
});

test("acknowledged capability writes do not rebind derived output to a replacement authority", async () => {
  const f = prepare();
  f.replaceAuthorityAfterCapabilityCommit();
  await assert.rejects(run(f), /Catalog policy changed|Catalog authority changed/);
  assert.equal(f.capabilityWrites(), 1);
  assert.equal(catalogFixture.fetches, 1);
});
