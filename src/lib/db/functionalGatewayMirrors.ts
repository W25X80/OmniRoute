import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogEnvironmentValue,
  catalogNamespaceRows,
  catalogValue,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogValue,
} from "./postgres/catalogSnapshot";
/**
 * db/functionalGatewayMirrors.ts — functional-gateway mirror gate.
 *
 * Storage: `key_value` table, namespace `functionalGatewayMirrors`, following the
 * ccDiscoveryAliases pattern. Global flag `EXPOSE_FUNCTIONAL_GATEWAY_MIRRORS`
 * (env "1"|"true" > DB flag > default false). Provider/model overrides stored as
 * `provider:<id>` and `model:<provider>/<model>` keys with value "on"/"off".
 */
import { getFeatureFlagOverride } from "./featureFlags";
import { getDbInstance } from "./core";

const NAMESPACE = "functionalGatewayMirrors";
const FLAG_KEY = "EXPOSE_FUNCTIONAL_GATEWAY_MIRRORS";

export type FunctionalGatewaySetting = "on" | "off" | null;

function parseSetting(value: string | undefined): FunctionalGatewaySetting {
  if (value === "on" || value === "off") return value;
  return null;
}

function providerKey(providerId: string): string {
  return `provider:${providerId}`;
}

function modelKey(modelId: string): string {
  return `model:${modelId}`;
}

export function getFunctionalGatewayProviderSetting(providerId: string): FunctionalGatewaySetting {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = (
    getSelectedPostgresDatabase()
      ? catalogValue(NAMESPACE, providerKey(providerId))
      : db!
          .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
          .get(NAMESPACE, providerKey(providerId))
  ) as { value: string } | undefined;
  return parseSetting(row?.value);
}

export async function setFunctionalGatewayProviderSettingAsync(
  providerId: string,
  v: FunctionalGatewaySetting
): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const key = providerKey(providerId);
    if (v === null) {
      getSelectedPostgresDatabase()
        ? await deleteCatalogValue(NAMESPACE, key)
        : db!.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, key);
      return;
    }
    getSelectedPostgresDatabase()
      ? await putCatalogValue(NAMESPACE, key, v)
      : db!
          .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)")
          .run(NAMESPACE, key, v);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function getFunctionalGatewayModelSetting(modelId: string): FunctionalGatewaySetting {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = (
    getSelectedPostgresDatabase()
      ? catalogValue(NAMESPACE, modelKey(modelId))
      : db!
          .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
          .get(NAMESPACE, modelKey(modelId))
  ) as { value: string } | undefined;
  return parseSetting(row?.value);
}

export async function setFunctionalGatewayModelSettingAsync(
  modelId: string,
  v: FunctionalGatewaySetting
): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const key = modelKey(modelId);
    if (v === null) {
      getSelectedPostgresDatabase()
        ? await deleteCatalogValue(NAMESPACE, key)
        : db!.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, key);
      return;
    }
    getSelectedPostgresDatabase()
      ? await putCatalogValue(NAMESPACE, key, v)
      : db!
          .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)")
          .run(NAMESPACE, key, v);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function getFunctionalGatewaySettingsBulk(): {
  providers: Map<string, "on" | "off">;
  models: Map<string, "on" | "off">;
} {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const rows = (
    getSelectedPostgresDatabase()
      ? catalogNamespaceRows(NAMESPACE)
      : db!.prepare("SELECT key, value FROM key_value WHERE namespace = ?").all(NAMESPACE)
  ) as Array<{ key: string; value: string }>;

  const providers = new Map<string, "on" | "off">();
  const models = new Map<string, "on" | "off">();

  for (const row of rows) {
    const setting = parseSetting(row.value);
    if (setting === null) continue;
    if (row.key.startsWith("provider:")) {
      providers.set(row.key.slice("provider:".length), setting);
    } else if (row.key.startsWith("model:")) {
      models.set(row.key.slice("model:".length), setting);
    }
  }

  return { providers, models };
}

function envForcesGlobalOn(): boolean {
  const raw = catalogEnvironmentValue(FLAG_KEY);
  return raw === "1" || raw === "true";
}

export function getFunctionalGatewayGlobalState(): {
  enabled: boolean;
  source: "env" | "db" | "default";
} {
  if (envForcesGlobalOn()) {
    return { enabled: true, source: "env" };
  }

  const dbOverride = getFeatureFlagOverride(FLAG_KEY);
  if (dbOverride !== undefined) {
    return { enabled: dbOverride === "true" || dbOverride === "1", source: "db" };
  }

  return { enabled: false, source: "default" };
}

export function isFunctionalGatewayGlobalEnabled(): boolean {
  return getFunctionalGatewayGlobalState().enabled;
}

export function setFunctionalGatewayProviderSetting(
  providerId: string,
  v: FunctionalGatewaySetting
): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const key = providerKey(providerId);
  if (v === null) {
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, key);
    return;
  }
  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    NAMESPACE,
    key,
    v
  );
}

export function setFunctionalGatewayModelSetting(
  modelId: string,
  v: FunctionalGatewaySetting
): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const key = modelKey(modelId);
  if (v === null) {
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, key);
    return;
  }
  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    NAMESPACE,
    key,
    v
  );
}
