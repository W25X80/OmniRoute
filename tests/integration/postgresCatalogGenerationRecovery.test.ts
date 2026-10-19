import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import type {
  PostgresDatabase,
  PostgresResult,
  PostgresTransaction,
  PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";

const GUARD_SQL =
  "SELECT authority_id,generation FROM omniroute.api_key_policy_epoch WHERE singleton";
const CAPABILITY_INSERT = "INSERT INTO omniroute.model_capabilities";

type CatalogProbe = {
  guardReads: number;
  capabilityWriteQueries: number;
  capabilityWriteCommits: number;
  validations: number;
  metadataReads: number;
  targetPolicyDecisions: boolean[];
};

type CatalogHooks = {
  beforeGuard?: (ordinal: number) => void | Promise<void>;
  afterGuard?: (ordinal: number, result: PostgresResult) => void | Promise<void>;
};

type NativeControlPlane = Awaited<
  ReturnType<
    typeof import("../../src/lib/db/postgres/controlPlane.ts").initializePostgresControlPlane
  >
>;

function emptyProbe(): CatalogProbe {
  return {
    guardReads: 0,
    capabilityWriteQueries: 0,
    capabilityWriteCommits: 0,
    validations: 0,
    metadataReads: 0,
    targetPolicyDecisions: [],
  };
}

/** Observe native SQL and repository calls while returning every real result unchanged. */
function observeBinding(
  binding: PostgresControlPlaneBinding,
  probe: CatalogProbe,
  targetModelId: string,
  hooks: CatalogHooks = {}
): PostgresControlPlaneBinding {
  const database = new Proxy(binding.database!, {
    get(target, property, receiver) {
      if (property !== "transaction") return Reflect.get(target, property, receiver);
      const transaction = Reflect.get(target, property, target) as PostgresDatabase["transaction"];
      return async <T>(
        body: (transaction: PostgresTransaction) => Promise<T>,
        options?: PostgresTransactionOptions
      ): Promise<T> => {
        let wroteCapabilities = false;
        const value = await transaction(
          async (nativeTransaction) =>
            body(
              new Proxy(nativeTransaction, {
                get(transactionTarget, transactionProperty, transactionReceiver) {
                  if (transactionProperty !== "query")
                    return Reflect.get(transactionTarget, transactionProperty, transactionReceiver);
                  const query = Reflect.get(
                    transactionTarget,
                    transactionProperty,
                    transactionTarget
                  ) as PostgresTransaction["query"];
                  return async <Row>(
                    sql: string,
                    values?: unknown[]
                  ): Promise<PostgresResult<Row>> => {
                    if (sql.includes(CAPABILITY_INSERT)) {
                      probe.capabilityWriteQueries++;
                      wroteCapabilities = true;
                    }
                    const isGuard = sql.trim() === GUARD_SQL;
                    const ordinal = isGuard ? ++probe.guardReads : 0;
                    if (isGuard) await hooks.beforeGuard?.(ordinal);
                    const result = (await Reflect.apply(query, transactionTarget, [
                      sql,
                      values,
                    ])) as PostgresResult<Row>;
                    if (isGuard) await hooks.afterGuard?.(ordinal, result as PostgresResult);
                    return result;
                  };
                },
              })
            ),
          options
        );
        if (wroteCapabilities) probe.capabilityWriteCommits++;
        return value;
      };
    },
  }) as PostgresDatabase;

  const apiKeys = new Proxy(binding.apiKeys, {
    get(target, property, receiver) {
      const method = Reflect.get(target, property, receiver) as unknown;
      if (typeof method !== "function") return method;
      if (property === "validateApiKey")
        return (...args: unknown[]) => {
          probe.validations++;
          return Reflect.apply(method, target, args);
        };
      if (property === "getApiKeyMetadata")
        return (...args: unknown[]) => {
          probe.metadataReads++;
          return Reflect.apply(method, target, args);
        };
      if (property === "isModelAllowedForKey")
        return async (...args: unknown[]) => {
          const allowed = await Reflect.apply(method, target, args);
          if (args[1] === targetModelId) probe.targetPolicyDecisions.push(Boolean(allowed));
          return allowed;
        };
      return method;
    },
  });

  return { ...binding, database, apiKeys };
}

function openRouterTransport(models: Array<Record<string, unknown>>, calls: string[]) {
  const original = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = input instanceof Request ? input.url : String(input);
    calls.push(url);
    assert.equal(url, "https://openrouter.ai/api/v1/models", "unexpected outbound request");
    return Response.json({ data: models });
  };
  return () => {
    globalThis.fetch = original;
  };
}

type CleanupAction = {
  label: string;
  run: () => unknown;
};

async function runCleanupActions(actions: CleanupAction[]): Promise<Error[]> {
  const failures: Error[] = [];
  // Actions are registered in acquisition order; run LIFO and keep going after failures.
  for (const action of [...actions].reverse()) {
    try {
      await action.run();
    } catch (cause) {
      failures.push(new Error(`Cleanup failed: ${action.label}`, { cause }));
    }
  }
  return failures;
}

function throwWithCleanupFailures(
  hasPrimaryFailure: boolean,
  primaryFailure: unknown,
  cleanupFailures: Error[]
): void {
  if (hasPrimaryFailure) {
    if (cleanupFailures.length === 0) throw primaryFailure;
    throw new AggregateError(
      [primaryFailure, ...cleanupFailures],
      "Scenario failed and one or more cleanup actions also failed",
      { cause: primaryFailure }
    );
  }
  if (cleanupFailures.length > 0)
    throw new AggregateError(cleanupFailures, "One or more cleanup actions failed");
}

async function withCleanup<T>(body: () => Promise<T>, actions: CleanupAction[]): Promise<T> {
  let value: T | undefined;
  let primaryFailure: unknown;
  let hasPrimaryFailure = false;
  try {
    value = await body();
  } catch (error) {
    primaryFailure = error;
    hasPrimaryFailure = true;
  }
  const cleanupFailures = await runCleanupActions(actions);
  throwWithCleanupFailures(hasPrimaryFailure, primaryFailure, cleanupFailures);
  return value as T;
}

test("fixture cleanup attempts every finalizer and preserves the scenario failure", async () => {
  const attempts: string[] = [];
  const primaryFailure = new Error("scenario assertion failed");
  const sqlCleanupFailure = new Error("fixture SQL cleanup failed");
  await assert.rejects(
    withCleanup(async () => {
      throw primaryFailure;
    }, [
      { label: "restore environment", run: () => attempts.push("environment") },
      { label: "remove temporary directory", run: () => attempts.push("temporary directory") },
      { label: "close TLS proxy", run: () => attempts.push("proxy") },
      { label: "close database pool", run: () => attempts.push("pool") },
      { label: "restore native settings", run: () => attempts.push("settings") },
      { label: "restore fetch", run: () => attempts.push("fetch") },
      {
        label: "delete fixture SQL rows",
        run: () => {
          attempts.push("SQL cleanup");
          throw sqlCleanupFailure;
        },
      },
    ]),
    (error: unknown) => {
      if (!(error instanceof AggregateError)) return false;
      assert.equal(error.cause, primaryFailure);
      assert.equal(error.errors[0], primaryFailure);
      assert.equal(error.errors.length, 2);
      const cleanupFailure = error.errors[1];
      assert.ok(cleanupFailure instanceof Error);
      assert.equal(cleanupFailure.cause, sqlCleanupFailure);
      assert.deepEqual(attempts, [
        "SQL cleanup",
        "fetch",
        "settings",
        "pool",
        "proxy",
        "temporary directory",
        "environment",
      ]);
      return true;
    }
  );
});

function apiRequest(key: string): Request {
  return new Request("http://localhost/v1/models", {
    headers: { authorization: "Bearer " + key },
  });
}

async function readCatalog(response: Response): Promise<{ data: Array<{ id: string }> }> {
  return (await response.json()) as { data: Array<{ id: string }> };
}

async function createScenarioRows(plane: NativeControlPlane, label: string, modelId: string) {
  return plane.run(async (binding) => {
    const connection = await binding.providers.create({
      provider: "openrouter",
      authType: "apikey",
      apiKey: "native-catalog-fixture-credential",
      isActive: true,
      name: label,
      providerSpecificData: { primaryTransport: "openai" },
    });
    assert.ok(connection);
    const key = await binding.apiKeys.createApiKey(label, "native-catalog-generation", [], {
      modelAccessMode: "restricted",
      allowedModels: [modelId],
    });
    return { connectionId: String(connection.id), keyId: key.id, key: key.key };
  });
}

test(
  "native model catalog retries only generation drift and fences authority changes",
  { skip: !postgresTestsEnabled, timeout: 120_000 },
  async (t) => {
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "STORAGE_ENCRYPTION_KEY",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
      "OPENROUTER_CATALOG_TTL_MS",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    let directory: string | undefined;

    let plane: NativeControlPlane | undefined;
    let policyWriterPlane: NativeControlPlane | undefined;
    let writer: PostgresDatabase | undefined;
    const created: Array<{ keyId: string; connectionId: string }> = [];
    let previousRequireLogin: unknown;
    let previousRequireAuthForModels: unknown;
    const cleanupActions: CleanupAction[] = [];
    for (const [name, value] of previous) {
      cleanupActions.push({
        label: `restore environment variable ${name}`,
        run: () => {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        },
      });
    }
    let primaryFailure: unknown;
    let hasPrimaryFailure = false;
    try {
      directory = await mkdtemp(join(tmpdir(), "omni-catalog-generation-"));
      const tempDirectory = directory;
      cleanupActions.push({
        label: "remove generation fixture temporary directory",
        run: () => rm(tempDirectory, { recursive: true, force: true }),
      });
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.DATA_DIR = directory;
      process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
      process.env.INITIAL_PASSWORD = "native-catalog-auth-fixture";
      process.env.OPENROUTER_CATALOG_TTL_MS = "0";
      const [
        { initializePostgresControlPlane },
        { withPostgresControlPlane },
        { withPostgresCatalogSnapshot },
      ] = await Promise.all([
        import("../../src/lib/db/postgres/controlPlane.ts"),
        import("../../src/lib/db/controlPlaneContext.ts"),
        import("../../src/lib/db/postgres/catalogSnapshot.ts"),
      ]);
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
        logger: { warn() {} },
      });
      cleanupActions.push({
        label: "close primary native control plane",
        run: () => plane!.close(),
      });
      process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
      writer = await createTestPostgresDatabase();
      cleanupActions.push({
        label: "close independent epoch writer",
        run: () => writer!.close(),
      });
      policyWriterPlane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
        logger: { warn() {} },
      });
      cleanupActions.push({
        label: "close independent policy writer control plane",
        run: () => policyWriterPlane!.close(),
      });
      const settings = await plane.run((binding) => binding.settings.get());
      previousRequireLogin = settings.requireLogin;
      previousRequireAuthForModels = settings.requireAuthForModels;
      cleanupActions.push({
        label: "restore native model authentication settings",
        run: async () => {
          const restore: Record<string, unknown> = {};
          if (typeof previousRequireLogin === "boolean")
            restore.requireLogin = previousRequireLogin;
          if (previousRequireAuthForModels === false) restore.requireAuthForModels = false;
          if (Object.keys(restore).length)
            await plane!.run((binding) => binding.settings.update(restore));
        },
      });
      await plane.run((binding) =>
        binding.settings.update({
          requireLogin: true,
          ...(settings.requireAuthForModels === false ? { requireAuthForModels: true } : {}),
        })
      );
      const { getUnifiedModelsResponse } = await import("../../src/app/api/v1/models/catalog.ts");

      await t.test(
        "a committed key-policy write retries with live auth and policy, one fetch and publication",
        async () => {
          const rawModelId = "fixture/retry-" + randomUUID();
          const targetModelId = "openrouter/" + rawModelId;
          const rows = await createScenarioRows(
            plane!,
            "catalog-retry-" + randomUUID(),
            targetModelId
          );
          created.push({ keyId: rows.keyId, connectionId: rows.connectionId });
          const probe = emptyProbe();
          const fetches: string[] = [];
          let policyUpdated = false;
          const hooks: CatalogHooks = {
            async beforeGuard(ordinal) {
              if (ordinal !== 1) return;
              const updated = await policyWriterPlane!.run((binding) =>
                binding.apiKeys.updateApiKeyPermissions(rows.keyId, {
                  modelAccessMode: "restricted",
                  allowedModels: ["openrouter/not-the-fixture-model"],
                })
              );
              assert.equal(updated, true);
              policyUpdated = true;
            },
          };
          const restoreFetch = openRouterTransport(
            [
              {
                id: rawModelId,
                name: rawModelId,
                architecture: { input_modalities: ["text"], output_modalities: ["text"] },
                top_provider: { max_completion_tokens: 1024 },
              },
            ],
            fetches
          );
          const response = await withCleanup(
            () =>
              plane!.run((binding) => {
                const observed = observeBinding(binding, probe, targetModelId, hooks);
                return withPostgresControlPlane(observed, () =>
                  withPostgresCatalogSnapshot(observed.database!, () =>
                    getUnifiedModelsResponse(apiRequest(rows.key))
                  )
                );
              }),
            [{ label: "restore OpenRouter test transport", run: restoreFetch }]
          );
          assert.equal(policyUpdated, true);
          assert.equal(response.status, 200, await response.clone().text());
          assert.equal(
            (await readCatalog(response)).data.some((model) => model.id === targetModelId),
            false
          );
          assert.equal(probe.guardReads, 2, "initial attempt drifts; the fresh retry is stable");
          assert.equal(
            probe.validations,
            3,
            "initial build, post-publication rebuild, and retry authenticate"
          );
          assert.ok(probe.metadataReads >= 3, "each catalog build reads live key metadata");
          assert.ok(
            probe.targetPolicyDecisions.includes(true),
            "the original policy permits the model"
          );
          assert.ok(
            probe.targetPolicyDecisions.includes(false),
            "the retry applies the committed restriction"
          );
          assert.deepEqual(fetches, ["https://openrouter.ai/api/v1/models"]);
          assert.equal(probe.capabilityWriteQueries, 1);
          assert.equal(probe.capabilityWriteCommits, 1);
        }
      );

      await t.test("a second committed generation drift returns retryable 503", async () => {
        const rawModelId = "fixture/drift-" + randomUUID();
        const targetModelId = "openrouter/" + rawModelId;
        const rows = await createScenarioRows(
          plane!,
          "catalog-drift-" + randomUUID(),
          targetModelId
        );
        created.push({ keyId: rows.keyId, connectionId: rows.connectionId });
        const probe = emptyProbe();
        const fetches: string[] = [];
        const hooks: CatalogHooks = {
          async beforeGuard(ordinal) {
            if (ordinal > 2) return;
            await writer!.transaction((tx) =>
              tx.query(
                "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
              )
            );
          },
        };
        const restoreFetch = openRouterTransport(
          [
            {
              id: rawModelId,
              name: rawModelId,
              architecture: { input_modalities: ["text"], output_modalities: ["text"] },
            },
          ],
          fetches
        );
        const response = await withCleanup(
          () =>
            plane!.run((binding) => {
              const observed = observeBinding(binding, probe, targetModelId, hooks);
              return withPostgresControlPlane(observed, () =>
                withPostgresCatalogSnapshot(observed.database!, () =>
                  getUnifiedModelsResponse(apiRequest(rows.key))
                )
              );
            }),
          [{ label: "restore OpenRouter test transport", run: restoreFetch }]
        );
        assert.equal(response.status, 503);
        assert.equal(response.headers.get("Retry-After"), "1");
        assert.equal(probe.guardReads, 2);
        assert.deepEqual(fetches, ["https://openrouter.ai/api/v1/models"]);
        assert.equal(probe.capabilityWriteQueries, 1);
        assert.equal(probe.capabilityWriteCommits, 1);
      });

      await t.test(
        "authority switch after the drift witness rejects before retry builder",
        async () => {
          const rawModelId = "fixture/authority-" + randomUUID();
          const targetModelId = "openrouter/" + rawModelId;
          const rows = await createScenarioRows(
            plane!,
            "catalog-authority-" + randomUUID(),
            targetModelId
          );
          created.push({ keyId: rows.keyId, connectionId: rows.connectionId });
          const probe = emptyProbe();
          const fetches: string[] = [];
          const authority = await writer!.transaction(async (tx) => {
            const result = await tx.query<{ authority_id: string }>(
              "SELECT authority_id FROM omniroute.api_key_policy_epoch WHERE singleton"
            );
            return result.rows[0].authority_id;
          });
          const hooks: CatalogHooks = {
            async beforeGuard(ordinal) {
              if (ordinal === 1)
                await writer!.transaction((tx) =>
                  tx.query(
                    "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
                  )
                );
            },
            async afterGuard(ordinal) {
              if (ordinal === 1)
                await writer!.transaction((tx) =>
                  tx.query(
                    "UPDATE omniroute.api_key_policy_epoch SET authority_id=gen_random_uuid(),generation=generation+1 WHERE singleton"
                  )
                );
            },
          };
          const restoreFetch = openRouterTransport(
            [
              {
                id: rawModelId,
                name: rawModelId,
                architecture: { input_modalities: ["text"], output_modalities: ["text"] },
              },
            ],
            fetches
          );
          await withCleanup(async () => {
            await assert.rejects(
              plane!.run((binding) => {
                const observed = observeBinding(binding, probe, targetModelId, hooks);
                return withPostgresControlPlane(observed, () =>
                  withPostgresCatalogSnapshot(observed.database!, () =>
                    getUnifiedModelsResponse(apiRequest(rows.key))
                  )
                );
              }),
              /Catalog authority changed during request preparation/
            );
          }, [
            { label: "restore OpenRouter test transport", run: restoreFetch },
            {
              label: "restore prior catalog authority UUID",
              run: () =>
                writer!.transaction((tx) =>
                  tx.query(
                    "UPDATE omniroute.api_key_policy_epoch SET authority_id=$1,generation=generation+1 WHERE singleton",
                    [authority]
                  )
                ),
            },
          ]);
          assert.equal(probe.guardReads, 1, "retry authority check fails before its builder guard");
          assert.equal(
            probe.validations,
            2,
            "only the original and acknowledged-publication builds authenticate"
          );
          assert.deepEqual(fetches, ["https://openrouter.ai/api/v1/models"]);
          assert.equal(probe.capabilityWriteQueries, 1);
          assert.equal(probe.capabilityWriteCommits, 1);
        }
      );
    } catch (error) {
      primaryFailure = error;
      hasPrimaryFailure = true;
    }
    if (plane) {
      for (const { keyId, connectionId } of created) {
        cleanupActions.push({
          label: `delete provider connection ${connectionId}`,
          run: () => plane!.run((binding) => binding.providers.deleteById(connectionId)),
        });
        cleanupActions.push({
          label: `delete API key ${keyId}`,
          run: () => plane!.run((binding) => binding.apiKeys.deleteApiKey(keyId)),
        });
      }
    }
    const cleanupFailures = await runCleanupActions(cleanupActions);
    throwWithCleanupFailures(hasPrimaryFailure, primaryFailure, cleanupFailures);
  }
);

test(
  "native model catalog never replays after an uncertain capability COMMIT",
  {
    skip: !postgresTestsEnabled || !process.env.OMNIROUTE_TEST_POSTGRES_KEY,
    timeout: 90_000,
  },
  async () => {
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "STORAGE_ENCRYPTION_KEY",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
      "OPENROUTER_CATALOG_TTL_MS",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    let directory: string | undefined;

    let plane: NativeControlPlane | undefined;
    let verification: PostgresDatabase | undefined;
    let proxy: Awaited<ReturnType<typeof createPostgresReplyLossProxy>> | undefined;
    let keyId = "";
    let apiKey = "";
    let connectionId = "";
    let previousRequireLogin: unknown;
    let previousRequireAuthForModels: unknown;
    const rawModelId = "fixture/commit-unknown-" + randomUUID();
    const targetModelId = "openrouter/" + rawModelId;
    const fetches: string[] = [];
    const cleanupActions: CleanupAction[] = [];
    for (const [name, value] of previous) {
      cleanupActions.push({
        label: `restore environment variable ${name}`,
        run: () => {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        },
      });
    }
    let primaryFailure: unknown;
    let hasPrimaryFailure = false;
    try {
      directory = await mkdtemp(join(tmpdir(), "omni-catalog-commit-unknown-"));
      const tempDirectory = directory;
      cleanupActions.push({
        label: "remove COMMIT-loss fixture temporary directory",
        run: () => rm(tempDirectory, { recursive: true, force: true }),
      });
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.DATA_DIR = directory;
      process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
      process.env.INITIAL_PASSWORD = "native-catalog-auth-fixture";
      process.env.OPENROUTER_CATALOG_TTL_MS = "0";
      const [
        { initializePostgresControlPlane },
        { withPostgresControlPlane },
        { withPostgresCatalogSnapshot },
      ] = await Promise.all([
        import("../../src/lib/db/postgres/controlPlane.ts"),
        import("../../src/lib/db/controlPlaneContext.ts"),
        import("../../src/lib/db/postgres/catalogSnapshot.ts"),
      ]);
      const options = await testPostgresOptions();
      proxy = await createPostgresReplyLossProxy({
        upstreamPort: options.port!,
        certificate: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_CA!, "utf8"),
        key: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_KEY!, "utf8"),
        dropCommitAfterQuery: CAPABILITY_INSERT,
      });
      cleanupActions.push({
        label: "close PostgreSQL COMMIT-reply proxy",
        run: () => proxy!.close(),
      });
      plane = await initializePostgresControlPlane({
        connection: {
          ...options,
          port: proxy.port,
          queryTimeoutMs: 1_000,
          statementTimeoutMs: 800,
          lockTimeoutMs: 200,
        },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
        logger: { warn() {} },
      });
      cleanupActions.push({
        label: "close proxied native control plane",
        run: () => plane!.close(),
      });
      process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
      verification = await createTestPostgresDatabase();
      cleanupActions.push({
        label: "close direct TLS verification pool",
        run: () => verification!.close(),
      });
      await plane.run(async (binding) => {
        const settings = await binding.settings.get();
        previousRequireLogin = settings.requireLogin;
        previousRequireAuthForModels = settings.requireAuthForModels;
        cleanupActions.push({
          label: "restore COMMIT-loss fixture authentication settings",
          run: async () => {
            const restore: Record<string, unknown> = {};
            if (typeof previousRequireLogin === "boolean")
              restore.requireLogin = previousRequireLogin;
            if (previousRequireAuthForModels === false) restore.requireAuthForModels = false;
            if (Object.keys(restore).length)
              await plane!.run((currentBinding) => currentBinding.settings.update(restore));
          },
        });
        await binding.settings.update({
          requireLogin: true,
          ...(settings.requireAuthForModels === false ? { requireAuthForModels: true } : {}),
        });
        const connection = await binding.providers.create({
          provider: "openrouter",
          authType: "apikey",
          apiKey: "native-catalog-unknown-fixture",
          isActive: true,
          name: "catalog-unknown-" + randomUUID(),
          providerSpecificData: { primaryTransport: "openai" },
        });
        assert.ok(connection);
        connectionId = String(connection.id);
        const key = await binding.apiKeys.createApiKey(
          "catalog-unknown-" + randomUUID(),
          "native-catalog-generation",
          [],
          { modelAccessMode: "restricted", allowedModels: [targetModelId] }
        );
        keyId = key.id;
        apiKey = key.key;
      });
      const { getUnifiedModelsResponse } = await import("../../src/app/api/v1/models/catalog.ts");
      const probe = emptyProbe();
      const restoreFetch = openRouterTransport(
        [
          {
            id: rawModelId,
            name: rawModelId,
            architecture: { input_modalities: ["text"], output_modalities: ["text"] },
          },
        ],
        fetches
      );
      await withCleanup(async () => {
        await assert.rejects(
          plane.run((binding) => {
            const observed = observeBinding(binding, probe, targetModelId);
            return withPostgresControlPlane(observed, () =>
              withPostgresCatalogSnapshot(observed.database!, () =>
                getUnifiedModelsResponse(apiRequest(apiKey))
              )
            );
          })
        );
      }, [{ label: "restore OpenRouter test transport", run: restoreFetch }]);
      assert.equal(proxy.dropped(), true, "the isolated proxy dropped the actual COMMIT reply");
      assert.deepEqual(fetches, ["https://openrouter.ai/api/v1/models"]);
      assert.equal(
        probe.guardReads,
        1,
        "unknown publication is fenced without retrying the catalog build"
      );
      assert.equal(probe.validations, 1, "no authentication replay occurs after unknown COMMIT");
      assert.equal(probe.capabilityWriteQueries, 1);
      assert.equal(
        probe.capabilityWriteCommits,
        0,
        "the client received no publication commit receipt"
      );
      const stored = await verification.transaction((tx) =>
        tx.query<{ model_id: string }>(
          "SELECT model_id FROM omniroute.model_capabilities WHERE provider='openrouter' AND model_id=$1",
          [rawModelId]
        )
      );
      assert.deepEqual(
        stored.rows.map((row) => row.model_id),
        [rawModelId]
      );
    } catch (error) {
      primaryFailure = error;
      hasPrimaryFailure = true;
    }
    if (plane) {
      if (connectionId) {
        cleanupActions.push({
          label: `delete provider connection ${connectionId}`,
          run: () => plane!.run((binding) => binding.providers.deleteById(connectionId)),
        });
      }
      if (keyId) {
        cleanupActions.push({
          label: `delete API key ${keyId}`,
          run: () => plane!.run((binding) => binding.apiKeys.deleteApiKey(keyId)),
        });
      }
    }
    const cleanupFailures = await runCleanupActions(cleanupActions);
    throwWithCleanupFailures(hasPrimaryFailure, primaryFailure, cleanupFailures);
  }
);
