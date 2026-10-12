import { getProviderConnections, getProviderConnectionById } from "@/lib/db/providers";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { isConnectionUnavailableToAuxiliaryActivity } from "@/lib/exclusiveLeaseIsolation";
import { getUserDatabaseSettings, getUserDatabaseSettingsAsync } from "@/lib/db/databaseSettings";
import { getDbInstance } from "@/lib/db/core";
import { decryptConnectionFields } from "@/lib/db/encryption";
import {
  registerSemanticCacheConfigResolver,
  registerSemanticCacheAsyncConfigResolver,
} from "@omniroute/open-sse/config/semanticCacheConfig.ts";
import {
  embeddingEndpoint,
  resolveEmbeddingCredentials,
} from "@omniroute/open-sse/services/cache/embeddingEndpoint";

let registered = false;

export function resolveProviderConnectionDetails(provider: string): {
  baseUrl?: string;
  apiKey?: string;
} {
  if (!provider) return {};
  try {
    const db = getDbInstance();
    const row = db
      .prepare(
        "SELECT * FROM provider_connections WHERE provider = ? AND is_active != 0 ORDER BY priority ASC, id ASC LIMIT 1"
      )
      .get(provider) as Record<string, unknown> | undefined;

    if (!row) return {};
    const decrypted = decryptConnectionFields(row);
    let baseUrl: string | undefined;
    if (decrypted.provider_specific_data) {
      try {
        const parsed =
          typeof decrypted.provider_specific_data === "string"
            ? JSON.parse(decrypted.provider_specific_data)
            : decrypted.provider_specific_data;
        if (typeof parsed?.baseUrl === "string" && parsed.baseUrl.trim()) {
          baseUrl = parsed.baseUrl.trim();
        }
      } catch {
        // Ignore parse error
      }
    }
    const rawKey = decrypted.api_key || decrypted.apiKey;
    const apiKey = typeof rawKey === "string" && rawKey.trim() ? rawKey.trim() : undefined;
    return { baseUrl, apiKey };
  } catch {
    return {};
  }
}

export function ensureSemanticCacheDbBridge(): void {
  if (registered) return;
  registered = true;
  registerSemanticCacheConfigResolver(() => {
    try {
      // Configuration reads must not run full DB statistics or integrity checks.
      const s = getUserDatabaseSettings().cache;
      if (!s) return null;

      const conn = s.semanticCacheEmbeddingProvider
        ? resolveProviderConnectionDetails(s.semanticCacheEmbeddingProvider)
        : {};

      const { baseUrl: embeddingBaseUrl, apiKey: embeddingApiKey } = resolveEmbeddingCredentials(
        {
          baseUrl: s.semanticCacheEmbeddingBaseUrl,
          apiKey: s.semanticCacheEmbeddingApiKey,
        },
        conn
      );

      return {
        // The vector layer is opt-in (#14159): it only runs when the operator turned
        // on BOTH the master semantic-cache toggle and the vector-layer toggle. With
        // it off, chatCore behaves exactly like the legacy SQLite exact-match cache.
        enabled: s.semanticCacheEnabled !== false && s.semanticCacheVectorEnabled === true,
        backend: s.semanticCacheBackend,
        similarityThreshold: s.semanticCacheThreshold,
        ttlMs: s.semanticCacheTTL,
        maxEntries: s.semanticCacheMaxSize,
        embeddingProvider: s.semanticCacheEmbeddingProvider,
        embeddingModel: s.semanticCacheEmbeddingModel,
        embeddingDimension: s.semanticCacheEmbeddingDimension,
        embeddingBaseUrl,
        embeddingApiKey,
        redisUrl: s.semanticCacheRedisUrl,
        redisPrefix: s.semanticCacheRedisPrefix,
        requireZeroTemperature: s.semanticCacheRequireZeroTemp,
      };
    } catch {
      return null;
    }
  });
  registerSemanticCacheAsyncConfigResolver(async (captureStoredCredential) => {
    try {
      // Configuration reads must not run full DB statistics or integrity checks.
      const s = (await getUserDatabaseSettingsAsync()).cache;
      if (!s) return null;

      const conn = s.semanticCacheEmbeddingProvider
        ? await resolveProviderConnectionSelectionAsync(s.semanticCacheEmbeddingProvider)
        : {};

      const { baseUrl: embeddingBaseUrl, apiKey: embeddingApiKey } = resolveEmbeddingCredentials(
        {
          baseUrl: s.semanticCacheEmbeddingBaseUrl,
          apiKey: s.semanticCacheEmbeddingApiKey,
        },
        conn
      );

      const identity = captureStoredCredential ? getSelectedPersistenceIdentity() : undefined;
      if (
        identity &&
        !s.semanticCacheEmbeddingApiKey &&
        "connectionId" in conn &&
        typeof conn.connectionId === "string" &&
        typeof embeddingApiKey === "string" &&
        embeddingApiKey === conn.apiKey
      ) {
        const connectionId = conn.connectionId;
        const provider = s.semanticCacheEmbeddingProvider;
        const endpoint = conn.baseUrl ? embeddingEndpoint(conn.baseUrl) : undefined;
        captureStoredCredential!(
          Object.freeze({
            connectionId,
            apiKey: embeddingApiKey,
            endpoint,
            async assertFresh() {
              const unavailable = () => new Error("Stored embedding credentials are unavailable");
              if (getSelectedPersistenceIdentity() !== identity) throw unavailable();
              const current = await getProviderConnectionById(connectionId);
              const data = current?.providerSpecificData as Record<string, unknown> | undefined;
              const currentEndpoint =
                typeof data?.baseUrl === "string" && data.baseUrl.trim()
                  ? embeddingEndpoint(data.baseUrl.trim())
                  : undefined;
              if (
                !current ||
                current.id !== connectionId ||
                current.provider !== provider ||
                current.isActive !== true ||
                typeof current.apiKey !== "string" ||
                current.apiKey.trim() !== embeddingApiKey ||
                currentEndpoint !== endpoint
              )
                throw unavailable();
              // true means leased/unavailable; false is necessary, never sufficient by itself.
              if (await isConnectionUnavailableToAuxiliaryActivity(connectionId))
                throw unavailable();
            },
          })
        );
      }

      return {
        // The vector layer is opt-in (#14159): it only runs when the operator turned
        // on BOTH the master semantic-cache toggle and the vector-layer toggle. With
        // it off, chatCore behaves exactly like the legacy SQLite exact-match cache.
        enabled: s.semanticCacheEnabled !== false && s.semanticCacheVectorEnabled === true,
        backend: s.semanticCacheBackend,
        similarityThreshold: s.semanticCacheThreshold,
        ttlMs: s.semanticCacheTTL,
        maxEntries: s.semanticCacheMaxSize,
        embeddingProvider: s.semanticCacheEmbeddingProvider,
        embeddingModel: s.semanticCacheEmbeddingModel,
        embeddingDimension: s.semanticCacheEmbeddingDimension,
        embeddingBaseUrl,
        embeddingApiKey,
        redisUrl: s.semanticCacheRedisUrl,
        redisPrefix: s.semanticCacheRedisPrefix,
        requireZeroTemperature: s.semanticCacheRequireZeroTemp,
      };
    } catch {
      return null;
    }
  });
}

export async function resolveProviderConnectionDetailsAsync(
  provider: string
): Promise<{ baseUrl?: string; apiKey?: string }> {
  const selected = await resolveProviderConnectionSelectionAsync(provider);
  if (!("connectionId" in selected)) return {};
  return { baseUrl: selected.baseUrl, apiKey: selected.apiKey };
}

async function resolveProviderConnectionSelectionAsync(
  provider: string
): Promise<{ connectionId?: string; baseUrl?: string; apiKey?: string }> {
  if (!provider) return {};
  try {
    const connections = await getProviderConnections({
      provider,
      isActive: true,
    });
    connections.sort((a, b) => {
      const priority =
        a.priority == null
          ? b.priority == null
            ? 0
            : -1
          : b.priority == null
            ? 1
            : Number(a.priority) - Number(b.priority);
      const left = String(a.id),
        right = String(b.id);
      return priority || (left < right ? -1 : left > right ? 1 : 0);
    });
    const connection = connections[0];
    if (!connection) return {};
    const data = connection.providerSpecificData as Record<string, unknown> | undefined;
    return {
      connectionId: String(connection.id),
      baseUrl:
        typeof data?.baseUrl === "string" && data.baseUrl.trim() ? data.baseUrl.trim() : undefined,
      apiKey:
        typeof connection.apiKey === "string" && connection.apiKey.trim()
          ? connection.apiKey.trim()
          : undefined,
    };
  } catch {
    return {};
  }
}
