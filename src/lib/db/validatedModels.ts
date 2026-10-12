import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  catalogNamespaceRows,
  catalogValue,
  compareCatalogText,
  withPostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
} from "./postgres/catalogSnapshot";
import { createHash } from "node:crypto";
import { getDbInstance } from "./core";
import { isExclusiveConnectionActivelyLeased } from "./exclusiveConnectionLeases";
import { finishModelCatalogWriteWithBackup } from "./models/modelCatalogWriteSignals";
import { ModelValidationError, type ValidationInput } from "../modelValidation/http";

interface ConnectionRow {
  id: string;
  provider: string;
  is_active: number;
  rate_limited_until?: string;
  test_status?: string;
  expires_at?: string;
  provider_specific_data?: string;
  [field: string]: unknown;
}
interface ValueRow {
  value: string;
}
export interface ValidationSnapshot {
  readonly provider: string;
  readonly modelId: string;
  readonly connectionId: string;
  readonly startedAt: number;
  /** Internal only. Never serialize this credential/configuration fingerprint. */
  readonly fingerprint: string;
}

const VOLATILE_FIELDS = new Set([
  "updated_at",
  "last_used_at",
  "last_selected_at",
  "sticky_use_count",
  "synced_models_at",
]);

function unavailable(): never {
  throw new ModelValidationError(
    409,
    "VALIDATION_CONNECTION_UNAVAILABLE",
    "Selected connection is unavailable or its configuration changed"
  );
}

function assertConnectionAvailable(
  connection: ConnectionRow | undefined,
  input: Pick<ValidationInput, "provider" | "connectionId">
): asserts connection is ConnectionRow {
  if (!connection || connection.provider !== input.provider || connection.is_active !== 1)
    unavailable();
  if (connection.rate_limited_until && Date.parse(connection.rate_limited_until) > Date.now())
    unavailable();
  if (["banned", "expired", "credits_exhausted", "invalid"].includes(connection.test_status ?? ""))
    unavailable();
  if (
    getSelectedPostgresDatabase()
      ? catalogTableRows<{ connection_id: string; state: string; expires_at: string }>(
          "exclusive_connection_leases"
        ).some(
          (lease) =>
            lease.connection_id === input.connectionId &&
            lease.state === "ACTIVE" &&
            compareCatalogText(lease.expires_at, new Date().toISOString()) > 0
        )
      : isExclusiveConnectionActivelyLeased(input.connectionId)
  )
    unavailable();
  const data = connection.provider_specific_data
    ? JSON.parse(connection.provider_specific_data)
    : {};
  if (data.cliproxyapiMode === "claude-native" || data.darioMode === "claude-native") unavailable();
}

function readState(input: Pick<ValidationInput, "provider" | "connectionId" | "modelId">) {
  if (getSelectedPostgresDatabase()) {
    const connection = catalogTableRows<ConnectionRow>("provider_connections").find(
      (row) => row.id === input.connectionId
    );
    assertConnectionAvailable(connection, input);
    const proxy = catalogTableRows("upstream_proxy_config").find(
      (row) => row.provider_id === input.provider
    );
    if (proxy?.enabled && proxy.mode !== "native") unavailable();
    const stableConnection = Object.fromEntries(
      Object.entries(connection).filter(([key]) => !VOLATILE_FIELDS.has(key))
    );
    const nodes = catalogTableRows("provider_nodes").sort((a, b) => compareCatalogText(a.id, b.id));
    const plugins = catalogTableRows("plugins").sort((a, b) => compareCatalogText(a.id, b.id));
    const settings = [
      ...catalogNamespaceRows("settings"),
      ...catalogNamespaceRows("proxyConfig"),
    ].sort(
      (a, b) => compareCatalogText(a.namespace, b.namespace) || compareCatalogText(a.key, b.key)
    );
    const row = catalogValue("customModels", input.provider);
    const models: Array<Record<string, unknown>> = row ? JSON.parse(row.value) : [];
    if (!Array.isArray(models)) unavailable();
    if (models.some((model) => model.id === input.modelId))
      throw new ModelValidationError(
        409,
        "VALIDATION_MODEL_EXISTS",
        "Custom model already exists; validation does not overwrite models"
      );
    return {
      models,
      fingerprint: createHash("sha256")
        .update(JSON.stringify({ stableConnection, nodes, settings, proxy, plugins }))
        .digest("hex"),
    };
  }
  const db = getDbInstance();
  const connection = db
    .prepare("SELECT * FROM provider_connections WHERE id = ?")
    .get(input.connectionId) as ConnectionRow | undefined;
  assertConnectionAvailable(connection, input);
  const proxy = db
    .prepare("SELECT * FROM upstream_proxy_config WHERE provider_id = ?")
    .get(input.provider) as Record<string, unknown> | undefined;
  if (proxy?.enabled && proxy.mode !== "native") unavailable();
  const stableConnection = Object.fromEntries(
    Object.entries(connection).filter(([key]) => !VOLATILE_FIELDS.has(key))
  );
  // Legacy compatible connections can reference a node type, not its concrete id.
  const nodes = db.prepare("SELECT * FROM provider_nodes ORDER BY id").all();
  const plugins = db.prepare("SELECT * FROM plugins ORDER BY id").all();
  const settings = db
    .prepare(
      "SELECT namespace, key, value FROM key_value WHERE namespace IN ('settings', 'proxyConfig') ORDER BY namespace, key"
    )
    .all();
  const modelRow = db
    .prepare("SELECT value FROM key_value WHERE namespace = 'customModels' AND key = ?")
    .get(input.provider) as ValueRow | undefined;
  const models: Array<Record<string, unknown>> = modelRow ? JSON.parse(modelRow.value) : [];
  if (!Array.isArray(models)) unavailable();
  if (models.some((model) => model.id === input.modelId)) {
    throw new ModelValidationError(
      409,
      "VALIDATION_MODEL_EXISTS",
      "Custom model already exists; validation does not overwrite models"
    );
  }
  const fingerprint = createHash("sha256")
    .update(JSON.stringify({ stableConnection, nodes, settings, proxy, plugins }))
    .digest("hex");
  return { fingerprint, models };
}

export async function createValidationSnapshotAsync(
  input: ValidationInput
): Promise<ValidationSnapshot> {
  const database = getSelectedPostgresDatabase();
  if (database)
    return withPostgresCatalogSnapshot(database, () => ({
      provider: input.provider,
      modelId: input.modelId,
      connectionId: input.connectionId,
      startedAt: Date.now(),
      fingerprint: readState(input).fingerprint,
    }));
  return {
    provider: input.provider,
    modelId: input.modelId,
    connectionId: input.connectionId,
    startedAt: Date.now(),
    fingerprint: readState(input).fingerprint,
  };
}

export async function assertValidationSnapshotAsync(snapshot: ValidationSnapshot): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (database && !isPostgresCatalogMutation(database))
    return withPostgresCatalogSnapshot(database, () => assertValidationSnapshotFresh(snapshot));
  assertValidationSnapshotFresh(snapshot);
}

function assertValidationSnapshotFresh(snapshot: ValidationSnapshot): void {
  if (
    Date.now() - snapshot.startedAt > 90_000 ||
    readState(snapshot).fingerprint !== snapshot.fingerprint
  )
    unavailable();
}

/** Compare configuration, insert, and read back under one SQLite write transaction. */
export async function commitValidatedModelAsync(
  snapshot: ValidationSnapshot,
  input: ValidationInput,
  validation: Record<string, unknown>
) {
  if (
    input.provider !== snapshot.provider ||
    input.modelId !== snapshot.modelId ||
    input.connectionId !== snapshot.connectionId
  )
    unavailable();
  const database = getSelectedPostgresDatabase();
  const db = database ? undefined : getDbInstance();
  const model = {
    id: input.modelId,
    name: input.modelName ?? input.modelId,
    source: "manual",
    apiFormat: input.apiFormat,
    supportedEndpoints: ["chat"],
    ...(input.max_input_tokens === undefined ? {} : { inputTokenLimit: input.max_input_tokens }),
    ...(input.max_output_tokens === undefined ? {} : { outputTokenLimit: input.max_output_tokens }),
    validation,
  };
  if (database)
    return mutatePostgresCatalog(
      database,
      async () => {
        assertValidationSnapshotFresh(snapshot);
        const { models } = readState(input);
        await putCatalogValue("customModels", input.provider, JSON.stringify([...models, model]));
        const row = catalogValue("customModels", input.provider);
        const saved = row
          ? JSON.parse(row.value).find((item: Record<string, unknown>) => item.id === input.modelId)
          : undefined;
        if (JSON.stringify(saved) !== JSON.stringify(model))
          throw new Error("Validated model readback failed");
        finishModelCatalogWriteWithBackup();
        return model;
      },
      { validation: true }
    );
  db!.transaction(() => {
    assertValidationSnapshotFresh(snapshot);
    const { models } = readState(input);
    db!
      .prepare(
        "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('customModels', ?, ?)"
      )
      .run(input.provider, JSON.stringify([...models, model]));
    const row = db!
      .prepare("SELECT value FROM key_value WHERE namespace = 'customModels' AND key = ?")
      .get(input.provider) as ValueRow;
    const saved = JSON.parse(row.value).find(
      (item: Record<string, unknown>) => item.id === input.modelId
    );
    if (JSON.stringify(saved) !== JSON.stringify(model))
      throw new Error("Validated model readback failed");
  })();
  finishModelCatalogWriteWithBackup();
  return model;
}

export function createValidationSnapshot(input: ValidationInput): ValidationSnapshot {
  assertSqliteBackendAllowed();
  return {
    provider: input.provider,
    modelId: input.modelId,
    connectionId: input.connectionId,
    startedAt: Date.now(),
    fingerprint: readState(input).fingerprint,
  };
}

export function assertValidationSnapshot(snapshot: ValidationSnapshot): void {
  assertSqliteBackendAllowed();
  if (
    Date.now() - snapshot.startedAt > 90_000 ||
    readState(snapshot).fingerprint !== snapshot.fingerprint
  )
    unavailable();
}

export function commitValidatedModel(
  snapshot: ValidationSnapshot,
  input: ValidationInput,
  validation: Record<string, unknown>
) {
  assertSqliteBackendAllowed();
  if (
    input.provider !== snapshot.provider ||
    input.modelId !== snapshot.modelId ||
    input.connectionId !== snapshot.connectionId
  )
    unavailable();
  const db = getDbInstance();
  const model = {
    id: input.modelId,
    name: input.modelName ?? input.modelId,
    source: "manual",
    apiFormat: input.apiFormat,
    supportedEndpoints: ["chat"],
    ...(input.max_input_tokens === undefined ? {} : { inputTokenLimit: input.max_input_tokens }),
    ...(input.max_output_tokens === undefined ? {} : { outputTokenLimit: input.max_output_tokens }),
    validation,
  };
  db.transaction(() => {
    assertValidationSnapshot(snapshot);
    const { models } = readState(input);
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('customModels', ?, ?)"
    ).run(input.provider, JSON.stringify([...models, model]));
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = 'customModels' AND key = ?")
      .get(input.provider) as ValueRow;
    const saved = JSON.parse(row.value).find(
      (item: Record<string, unknown>) => item.id === input.modelId
    );
    if (JSON.stringify(saved) !== JSON.stringify(model))
      throw new Error("Validated model readback failed");
  })();
  finishModelCatalogWriteWithBackup();
  return model;
}
