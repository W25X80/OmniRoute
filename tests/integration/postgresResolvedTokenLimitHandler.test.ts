import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  getSelectedControlPlane,
  getSelectedPostgresDatabase,
  withPostgresControlPlane,
} from "../../src/lib/db/controlPlaneContext.ts";
import { PostgresError, type PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native resolved-model token limits block the real handler before upstream dispatch",
  {
    skip: !postgresTestsEnabled,
    timeout: 30_000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const directory = mkdtempSync(join(tmpdir(), "pg-resolved-token-handler-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    const keys: string[] = [];
    let connectionId: string | undefined;
    let upstreamCalls = 0;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
      process.env.JWT_SECRET = "isolated-resolved-limit-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-resolved-limit-api-secret";
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      const providers = await import("../../src/lib/db/providers.ts");
      const apiKeys = await import("../../src/lib/db/apiKeys.ts");
      const limits = await import("../../src/lib/db/tokenLimits.ts");
      const counters = await import("../../open-sse/services/tokenLimitCounter.ts");
      const pending = await import("../../src/lib/usage/usageHistory.ts");
      const breakers = await import("../../src/shared/utils/circuitBreaker.ts");
      const predicates = await import("../../open-sse/services/combo/comboPredicates.ts");
      const { handleChatCore } = await import("../../open-sse/handlers/chatCore.ts");
      // The source's unannotated default-null parameter narrows its inferred API to null.
      // Production dispatch supplies key metadata; describe that input at this test seam.
      type HandlerInput = Omit<Parameters<typeof handleChatCore>[0], "apiKeyInfo"> & {
        apiKeyInfo: { id: string; name?: string; noLog: boolean };
      };
      const invokeHandler = handleChatCore as unknown as (
        options: HandlerInput
      ) => ReturnType<typeof handleChatCore>;
      t.mock.method(globalThis, "fetch", async () => {
        upstreamCalls++;
        return new Response(
          JSON.stringify({
            error: { message: "isolated upstream rejection", type: "invalid_request_error" },
          }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      });
      await runtime!.runPersistenceRequest(async () => {
        const connection = await providers.createProviderConnection({
          provider: "openai",
          authType: "apikey",
          isActive: true,
          apiKey: "fixture-only-credential",
          name: `resolved-limit-${randomUUID()}`,
        });
        assert.ok(connection);
        connectionId = String(connection.id);
      });
      for (const scopeType of ["provider", "model"] as const) {
        await t.test(
          `${scopeType} limit absent from requested-alias check is enforced after resolution`,
          async () => {
            const resolvedModel = `resolved-limit-${randomUUID().replaceAll("-", "")}`;
            const requestedAlias = `client-alias-${randomUUID().replaceAll("-", "")}`;
            let callbacks = 0;
            await runtime!.runPersistenceRequest(async () => {
              const key = await apiKeys.createApiKey(
                `resolved-${scopeType}`,
                "isolated-limit-fixture"
              );
              const keyId = String(key.id);
              keys.push(keyId);
              assert.ok(await apiKeys.getApiKeyById(keyId));
              const global = await limits.upsertTokenLimitAsync({
                apiKeyId: keyId,
                scopeType: "global",
                tokenLimit: 1000,
              });
              const scoped = await limits.upsertTokenLimitAsync({
                apiKeyId: keyId,
                scopeType,
                scopeValue: scopeType === "provider" ? "openai" : resolvedModel,
                tokenLimit: 10,
              });
              await counters.recordTokenUsageAsync(keyId, "openai", resolvedModel, 12);
              // These are actual native counters, not an in-memory accelerator or a fabricated verdict.
              assert.equal(await limits.getWindowUsageAsync(global, Date.now()), 12);
              assert.equal(await limits.getWindowUsageAsync(scoped, Date.now()), 12);
              // Tier 1 knows only the client alias and the unbreached global limit.
              assert.equal(
                await counters.checkTokenLimitsAsync(keyId, undefined, requestedAlias),
                null
              );
              const before = await providers.getProviderConnectionById(connectionId!);
              const breakerBefore = breakers.getCircuitBreaker("openai").getStatus();
              const body = {
                model: requestedAlias,
                messages: [{ role: "user", content: "hi" }],
                max_tokens: 8,
                stream: false,
              };
              const result = await invokeHandler({
                body,
                modelInfo: { provider: "openai", model: resolvedModel, extendedContext: false },
                credentials: {
                  apiKey: "fixture-only-credential",
                  connectionId,
                  providerSpecificData: {},
                },
                log: { debug() {}, info() {}, warn() {}, error() {} },
                clientRawRequest: {
                  endpoint: "/v1/chat/completions",
                  body,
                  headers: new Headers(),
                },
                apiKeyInfo: { id: keyId, name: key.name, noLog: true },
                connectionId,
                userAgent: "native-resolved-limit-test",
                comboName: undefined,
                onCredentialsRefreshed: () => {
                  callbacks++;
                },
                onRequestSuccess: () => {
                  callbacks++;
                },
                onStreamFailure: () => {
                  callbacks++;
                },
                onDisconnect: () => {
                  callbacks++;
                },
              });
              assert.equal(result.success, false);
              assert.equal(result.status, 429);
              assert.equal(result.errorCode, "TOKEN_LIMIT_EXCEEDED");
              const errorBody: unknown = await result.response.json();
              assert.equal(predicates.isTokenLimitBreachErrorBody(errorBody), true);
              assert.equal(predicates.isLocalKeyPolicyBreachErrorBody(errorBody), true);
              assert.equal(upstreamCalls, 0);
              assert.equal(callbacks, 0);
              assert.equal(pending.getPendingById().size, 0);
              assert.equal(
                pending.getPendingRequests().byModel[`${resolvedModel} (openai)`] ?? 0,
                0
              );
              assert.equal(
                pending.getPendingRequests().byAccount[connectionId!]?.[
                  `${resolvedModel} (openai)`
                ] ?? 0,
                0
              );
              assert.deepEqual(await providers.getProviderConnectionById(connectionId!), before);
              assert.deepEqual(breakers.getCircuitBreaker("openai").getStatus(), breakerBefore);
            });
          }
        );
      }
      await t.test(
        "a transient resolved counter read failure retains the source fail-open response policy",
        async () => {
          await runtime!.runPersistenceRequest(async () => {
            const key = await apiKeys.createApiKey("resolved-read-fault", "isolated-limit-fixture");
            const keyId = String(key.id);
            keys.push(keyId);
            await limits.upsertTokenLimitAsync({
              apiKeyId: keyId,
              scopeType: "provider",
              scopeValue: "openai",
              tokenLimit: 10,
            });
            const binding = getSelectedControlPlane()!;
            const database = getSelectedPostgresDatabase()!;
            let injected = 0;
            let warning = 0;
            // The live native transaction and all other statements use actual PostgreSQL.
            // Only this counter-read rejection is injected; this does not prove transport failover.
            const intercepted = new Proxy(database, {
              get(target, property, receiver) {
                if (property !== "transaction") return Reflect.get(target, property, receiver);
                return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
                  target.transaction((tx) =>
                    body({
                      query: async <Row = Record<string, unknown>>(
                        sql: string,
                        values?: unknown[]
                      ) => {
                        if (
                          sql.includes(
                            "SELECT * FROM omniroute.api_key_token_limits WHERE api_key_id=$1"
                          ) &&
                          values?.[0] === keyId
                        ) {
                          injected++;
                          throw new PostgresError("query_failed", "08006");
                        }
                        return tx.query<Row>(sql, values);
                      },
                    })
                  );
              },
            });
            const body = {
              model: "isolated-client-read-fault-alias",
              messages: [{ role: "user", content: "hi" }],
              max_tokens: 8,
              stream: false,
            };
            const result = await withPostgresControlPlane(
              { ...binding, database: intercepted },
              () =>
                invokeHandler({
                  body,
                  modelInfo: {
                    provider: "openai",
                    model: "isolated-resolved-read-fault",
                    extendedContext: false,
                  },
                  credentials: {
                    apiKey: "fixture-only-credential",
                    connectionId,
                    providerSpecificData: {},
                  },
                  log: {
                    debug() {},
                    info() {},
                    error() {},
                    warn(tag: string, message: string) {
                      if (
                        tag === "TOKEN_LIMIT" &&
                        message === "Tier 2 token-limit check failed; allowing request"
                      )
                        warning++;
                    },
                  },
                  clientRawRequest: {
                    endpoint: "/v1/chat/completions",
                    body,
                    headers: new Headers(),
                  },
                  apiKeyInfo: { id: keyId, noLog: true },
                  connectionId,
                  userAgent: "native-resolved-limit-fault-test",
                  comboName: undefined,
                  onCredentialsRefreshed: undefined,
                  onRequestSuccess: undefined,
                  onStreamFailure: undefined,
                  onDisconnect: undefined,
                })
            );
            assert.equal(injected, 1);
            assert.equal(warning, 1);
            assert.equal(upstreamCalls, 1);
            assert.equal(result.success, false);
            assert.equal(result.status, 400);
            assert.equal(pending.getPendingById().size, 0);
          });
        }
      );
      await runtime!.runPersistenceRequest(async () => {
        for (const id of keys.splice(0)) await apiKeys.deleteApiKey(id);
        if (connectionId) await providers.deleteProviderConnection(connectionId);
        connectionId = undefined;
      });
      await runtime!.closePersistenceRuntime();
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      if (runtime?.getPersistenceRuntimeStatus().phase === "ready")
        await runtime!
          .runPersistenceRequest(async () => {
            const { deleteApiKey } = await import("../../src/lib/db/apiKeys.ts");
            const { deleteProviderConnection } = await import("../../src/lib/db/providers.ts");
            for (const id of keys.splice(0)) await deleteApiKey(id);
            if (connectionId) await deleteProviderConnection(connectionId);
          })
          .catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      t.mock.restoreAll();
      hooks.deregister();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
