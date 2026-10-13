import { randomBytes } from "node:crypto";
import type { Logger } from "pino";
import { generateApiKeyWithSecret } from "@/shared/utils/apiKeyGeneration";
import type { PostgresControlPlaneBinding } from "../controlPlaneContext";
import { withPostgresControlPlane } from "../controlPlaneContext";
import { requireExplicitStorageEncryptionKey } from "../encryption";
import { assertApiKeyIsNotManagementPassword } from "../providers/credentialGuard";
import {
  invalidateCachedLKGP,
  invalidateConnectionUpdate,
  invalidateDbCache,
  invalidateModelCatalogCache,
} from "../readCache";
import { bumpProxyRegistryGeneration } from "../proxies/registryGeneration";
import { setNoLog } from "../../compliance/noLog";
import { createPostgresDatabase } from "./connect";
import type { PostgresOptions } from "./config";
import {
  createPostgresSettingsRepository,
  type PostgresSettingsEffects,
} from "./settingsRepository";
import {
  createPostgresProviderConnectionRepository,
  type PostgresProviderEffects,
} from "./providerRepository";
import { createPostgresProviderNodeRepository } from "./providerNodesRepository";
import { createPostgresApiKeyRepository } from "./apiKeyRepository";
import { createPostgresApiKeyGroupsRepository } from "./apiKeyGroupsRepository";
import {
  advanceApiKeyPolicyEpoch,
  lockApiKeyPolicy,
  type PostgresApiKeyEffects,
} from "./apiKeyPolicyState";
import { createPostgresComboRepository } from "./comboRepository";
import { createPostgresModelComboMappingRepository } from "./modelComboMappingRepository";
import { createPostgresModelPolicy } from "./modelPolicy";
import { createPostgresSecretRepository } from "./secretRepository";
import {
  cleanupPostgresConnectionStateAfterCommit,
  deleteApiKeyDependents,
  deleteConnectionState,
  reconcileCodexUsageHistory,
  recordPostgresApiKeyAuditEvent,
} from "./controlPlaneEffects";
import { migratePostgresSchema } from "./migrations";
import { initializePostgresCompressionSeed } from "./compressionSeedUpgrade";
import { hasPostgresCatalogSnapshot, refreshPostgresCatalogSnapshot } from "./catalogSnapshot";

type ControlPlaneState = "ready" | "closing" | "closed";

/** Guard direct repository APIs as well as facades, including extracted method references. */
function scopeRepository<T extends object>(repository: T, assertScope: () => void): T {
  return new Proxy(repository, {
    get(target, property, receiver) {
      const member: unknown = Reflect.get(target, property, receiver);
      if (typeof member !== "function") return member;
      return (...args: unknown[]) => {
        assertScope();
        return Reflect.apply(member, target, args);
      };
    },
  });
}

export interface PostgresControlPlane {
  readonly state: ControlPlaneState;
  /** Values for a future application startup boundary; never installed into global env here. */
  readonly bootstrapSecrets: Readonly<{ jwtSecret: string; apiKeySecret: string }>;
  run<T>(body: (repositories: PostgresControlPlaneBinding) => T | Promise<T>): Promise<T>;
  close(): Promise<void>;
}

export interface PostgresControlPlaneOptions {
  connection: PostgresOptions;
  logger: Pick<Logger, "warn">;
  /** Optional test substitutions; the default uses the existing application runtime owners. */
  runtime?: Pick<
    PostgresSettingsEffects,
    "applyRuntimeSettings" | "scheduleCatalogRevalidationAfterInit"
  >;
}

/**
 * Opt-in async initialization for the covered control-plane owners. There is deliberately
 * no global app selector: unported owners are rejected by core.ts within every run scope.
 */
export async function initializePostgresControlPlane(
  options: PostgresControlPlaneOptions
): Promise<PostgresControlPlane> {
  requireExplicitStorageEncryptionKey();
  const explicitJwt = process.env.JWT_SECRET;
  const explicitApiKey = process.env.API_KEY_SECRET;
  const database = await createPostgresDatabase(options.connection, options.logger);
  try {
    await migratePostgresSchema(database);
    await initializePostgresCompressionSeed(database);
    const secrets = createPostgresSecretRepository(database);
    const jwtSecret = explicitJwt?.trim()
      ? explicitJwt
      : await secrets.getOrCreate("jwtSecret", () => randomBytes(48).toString("base64"));
    const apiKeySecret = explicitApiKey?.trim()
      ? explicitApiKey
      : await secrets.getOrCreate("apiKeySecret", () => randomBytes(32).toString("hex"));

    async function refreshPreparedCatalog(): Promise<void> {
      if (hasPostgresCatalogSnapshot(database)) {
        await refreshPostgresCatalogSnapshot(database);
      }
    }

    const settings = createPostgresSettingsRepository(
      database,
      {
        async invalidateSettingsCache() {
          invalidateDbCache("settings");
          await refreshPreparedCatalog();
        },
        bumpProxyConfigGeneration: async () =>
          (await import("../settings")).bumpProxyConfigGeneration(),
        applyRuntimeSettings:
          options.runtime?.applyRuntimeSettings ??
          (async (next, source) => {
            await (await import("../../config/runtimeSettings")).applyRuntimeSettings(next, source);
          }),
        scheduleCatalogRevalidationAfterInit:
          options.runtime?.scheduleCatalogRevalidationAfterInit ??
          (async () => {
            (
              await import("@/shared/services/codexCatalogRevalidation")
            ).scheduleCodexCatalogRevalidationAfterInit();
          }),
      },
      {
        markPolicyChanged: async (transaction) => {
          await lockApiKeyPolicy(transaction);
          await advanceApiKeyPolicyEpoch(transaction);
        },
      }
    );

    const invalidateReasoning = async () => {
      (await import("../reasoningRoutingRules")).invalidateReasoningRoutingRuleCache();
    };
    const combos = createPostgresComboRepository(database, {
      bumpProxyRegistryGeneration,
      invalidateCachedLKGP,
    });
    const modelComboMappings = createPostgresModelComboMappingRepository(database);
    const providerEffects: PostgresProviderEffects = {
      assertApiKeyIsNotManagementPassword: (value) =>
        assertApiKeyIsNotManagementPassword(value, settings.get),
      reconcileCodexUsageHistory,
      deleteConnectionState,
      async afterConnectionWrite(change) {
        const failures: string[] = [];
        async function attempt(name: string, effect: () => void | Promise<void>) {
          try {
            await effect();
          } catch {
            failures.push(name);
          }
        }
        if (change.deletion) {
          const cleanup = await cleanupPostgresConnectionStateAfterCommit(
            database,
            change.deletion,
            {
              combos: {
                list: combos.list,
                async update(id, patch) {
                  const result = await combos.update(id, patch);
                  if (result) invalidateDbCache("combos");
                  return result;
                },
              },
              invalidateCachedLKGP,
              reportFailure: (operation) =>
                options.logger.warn({ operation }, "Provider cleanup failed"),
            }
          );
          if (cleanup.combosTouched) invalidateDbCache("combos");
          const ids = change.deletion.requestedIds;
          await attempt("health", async () => {
            const rotator = await import("@omniroute/open-sse/services/apiKeyRotator.ts");
            for (const id of ids) {
              rotator.removeConnectionHealth(id);
              rotator.removeConnectionIndex(id);
              void import("@omniroute/open-sse/services/combo/nativeCodexTurnPin.ts")
                .then((module) => module.revokeNativeCodexTurnPinsForConnection(id))
                .catch(() => {});
            }
          });
          if (change.deletion.mode === "provider") {
            try {
              await database.transaction((tx) =>
                tx.query(
                  "DELETE FROM omniroute.key_value WHERE namespace='syncedAvailableModels' AND starts_with(key,$1)",
                  [`${change.deletion!.provider}:`]
                )
              );
            } catch {
              /* Source provider catalog purge is best effort. */
            }
          }
        }
        await attempt("connections", () => {
          if (change.kind === "update")
            invalidateConnectionUpdate(change.id!, change.patch ?? {}, change.options);
          else if (change.kind === "merge" || change.kind === "reset") {
            invalidateDbCache("connections", change.id, { skipModelCatalog: true });
          } else invalidateDbCache("connections");
        });
        if (change.kind === "delete") await attempt("reasoning", invalidateReasoning);
        if (change.kind !== "merge" && change.kind !== "reset" && change.deletion?.mode !== "ids") {
          await attempt("proxy", async () =>
            (await import("../settings")).bumpProxyConfigGeneration()
          );
        }
        await attempt("catalog_snapshot", refreshPreparedCatalog);
        if (failures.length) throw new Error("Provider application effects failed");
      },
      async afterNodeWrite() {
        invalidateDbCache("nodes");
        await refreshPreparedCatalog();
      },
    };
    const nodes = createPostgresProviderNodeRepository(database, providerEffects, {
      markPolicyChanged: async (transaction) => {
        await lockApiKeyPolicy(transaction);
        await advanceApiKeyPolicyEpoch(transaction);
      },
    });
    const providers = createPostgresProviderConnectionRepository(database, providerEffects);
    const keyEffects: PostgresApiKeyEffects = {
      setNoLog,
      invalidateAuthCaches: async () => (await import("../apiKeys")).clearApiKeyCaches(),
      async invalidateModelCatalogCache() {
        invalidateModelCatalogCache();
        await refreshPreparedCatalog();
      },
      invalidateReasoningRoutingRuleCache: invalidateReasoning,
      deleteRedisAuthCacheEntries: async (...hashes) =>
        (await import("../apiKeys")).deleteRedisAuthCacheEntries(...hashes),
      logAuditEvent: (event) => recordPostgresApiKeyAuditEvent(database, event),
    };
    const apiKeys = createPostgresApiKeyRepository(
      database,
      {
        effects: keyEffects,
        modelPolicy: createPostgresModelPolicy(database, nodes, settings),
        deleteDependents: deleteApiKeyDependents,
      },
      {
        generateApiKeyWithMachine: (machineId) => generateApiKeyWithSecret(machineId, apiKeySecret),
      }
    );
    const keyGroups = createPostgresApiKeyGroupsRepository(database, keyEffects);
    // Read critical startup state before exposing readiness. Any failure closes the owned pool.
    await settings.get();

    let state: ControlPlaneState = "ready";
    const databaseIdentity = Object.freeze({});
    let admitted = 0;
    let drain: (() => void) | undefined;
    let closing: Promise<void> | undefined;
    function assertReady() {
      if (state !== "ready") throw new Error("PostgreSQL control plane is not ready");
      requireExplicitStorageEncryptionKey();
    }
    return {
      get state() {
        return state;
      },
      bootstrapSecrets: Object.freeze({ jwtSecret, apiKeySecret }),
      async run(body) {
        assertReady();
        admitted++;
        let alive = true;
        const assertScope = () => {
          if (!alive || state === "closed")
            throw new Error("PostgreSQL control-plane scope has ended");
          requireExplicitStorageEncryptionKey();
        };
        const binding: PostgresControlPlaneBinding = {
          settings: scopeRepository(settings, assertScope),
          providers: scopeRepository(providers, assertScope),
          nodes: scopeRepository(nodes, assertScope),
          apiKeys: scopeRepository(apiKeys, assertScope),
          keyGroups: scopeRepository(keyGroups, assertScope),
          secrets: scopeRepository(secrets, assertScope),
          database: scopeRepository(database, assertScope),
          databaseIdentity,
          combos: scopeRepository(combos, assertScope),
          modelComboMappings: scopeRepository(modelComboMappings, assertScope),
          assertReady: assertScope,
        };
        try {
          return await withPostgresControlPlane(binding, () => body(binding));
        } finally {
          alive = false;
          admitted--;
          if (admitted === 0) drain?.();
        }
      },
      close() {
        if (!closing) {
          state = "closing";
          closing = (async () => {
            if (admitted > 0)
              await new Promise<void>((resolve) => {
                drain = resolve;
              });
            try {
              await database.close();
            } finally {
              state = "closed";
            }
          })();
        }
        return closing;
      },
    };
  } catch (error) {
    await database.close();
    throw error;
  }
}
