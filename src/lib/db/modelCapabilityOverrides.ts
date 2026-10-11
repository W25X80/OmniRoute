import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  compareCatalogText,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  writePostgresCatalog,
  afterPostgresCatalogCommit,
} from "./postgres/catalogSnapshot";
import {
  parseReasoningEffortsOverride,
  REASONING_EFFORT_OVERRIDE_VALUES,
  type ReasoningEffortOverrideValue,
} from "@/shared/reasoning/reasoningEffortsOverride";
import { getDbInstance } from "./core";
import { invalidateDbCache } from "./readCache";

export type NumericModelCapabilityOverrideKey =
  "max_input_tokens" | "max_output_tokens" | "max_token";
export type ModelCapabilityOverrideKey = NumericModelCapabilityOverrideKey | "reasoning_efforts";

interface ModelCapabilityOverrideBase {
  provider: string;
  modelId: string;
  target: string;
  refreshedAt: string;
}

export type ModelCapabilityOverride =
  | (ModelCapabilityOverrideBase & {
      key: NumericModelCapabilityOverrideKey;
      value: number;
    })
  | (ModelCapabilityOverrideBase & {
      key: "reasoning_efforts";
      value: ReasoningEffortOverrideValue[];
    });

interface OverrideRow {
  provider: string;
  model_id: string;
  override_key: string;
  override_value: string;
  refreshed_at: string;
}

function isNumericKey(value: unknown): value is NumericModelCapabilityOverrideKey {
  return value === "max_input_tokens" || value === "max_output_tokens" || value === "max_token";
}

function isSupportedKey(value: unknown): value is ModelCapabilityOverrideKey {
  return isNumericKey(value) || value === "reasoning_efforts";
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

function isReasoningEfforts(value: unknown): value is ReasoningEffortOverrideValue[] {
  if (!Array.isArray(value) || value.length === 0) return false;
  const allowed = new Set<string>(REASONING_EFFORT_OVERRIDE_VALUES);
  return (
    value.every((entry) => typeof entry === "string" && allowed.has(entry)) &&
    new Set(value).size === value.length
  );
}

export function parseModelOverrideTarget(
  target: unknown
): { provider: string; modelId: string } | null {
  const raw = typeof target === "string" ? target.trim() : "";
  const slashIndex = raw.indexOf("/");
  if (slashIndex <= 0 || slashIndex === raw.length - 1) return null;

  const provider = raw.slice(0, slashIndex).trim();
  const modelId = raw.slice(slashIndex + 1).trim();
  if (!provider || !modelId) return null;
  return { provider, modelId };
}

function toOverride(row: OverrideRow): ModelCapabilityOverride | null {
  if (!isSupportedKey(row.override_key)) return null;

  let parsedValue: unknown;
  try {
    parsedValue = JSON.parse(row.override_value);
  } catch {
    return null;
  }

  const base: ModelCapabilityOverrideBase = {
    provider: row.provider,
    modelId: row.model_id,
    target: `${row.provider}/${row.model_id}`,
    refreshedAt: row.refreshed_at,
  };
  if (row.override_key === "reasoning_efforts") {
    return isReasoningEfforts(parsedValue)
      ? { ...base, key: row.override_key, value: parsedValue }
      : null;
  }
  return isPositiveInteger(parsedValue)
    ? { ...base, key: row.override_key, value: parsedValue }
    : null;
}

/** Nested provider → model → numeric override map used by build-local snapshots. */
export type NestedMaxTokenOverrideMap = ReadonlyMap<string, ReadonlyMap<string, number>>;

export function getModelCapabilityOverride(
  provider: string | null | undefined,
  modelId: string | null | undefined,
  key: NumericModelCapabilityOverrideKey,
  bulkMaxTokenOverrides?: NestedMaxTokenOverrideMap | null
): number | null {
  if (!bulkMaxTokenOverrides) requirePostgresCatalogSnapshot();

  const target = parseModelOverrideTarget(`${provider || ""}/${modelId || ""}`);
  if (!target || !isNumericKey(key)) return null;

  if (bulkMaxTokenOverrides) {
    // The caller pairs the bulk map with the key it was built for
    // (max_output_tokens or max_input_tokens in the #9199 snapshot).
    return bulkMaxTokenOverrides.get(target.provider)?.get(target.modelId) ?? null;
  }

  try {
    const row = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("model_capability_overrides").filter(
            (row) =>
              row.provider === target.provider &&
              row.model_id === target.modelId &&
              row.override_key === key
          )[0]
        : getDbInstance()
            .prepare(
              "SELECT provider, model_id, override_key, override_value, refreshed_at " +
                "FROM model_capability_overrides WHERE provider = ? AND model_id = ? AND override_key = ?"
            )
            .get(target.provider, target.modelId, key)
    ) as OverrideRow | undefined;
    const override = row ? toOverride(row) : null;
    return override && override.key !== "reasoning_efforts" ? override.value : null;
  } catch {
    return null;
  }
}

export function getReasoningEffortsOverride(
  provider: string | null | undefined,
  modelId: string | null | undefined,
  bulk?: ReadonlyMap<string, ReadonlyMap<string, readonly ReasoningEffortOverrideValue[]>> | null
): readonly ReasoningEffortOverrideValue[] | null {
  if (!bulk) requirePostgresCatalogSnapshot();

  const target = parseModelOverrideTarget(`${provider || ""}/${modelId || ""}`);
  if (!target) return null;
  if (bulk) return bulk.get(target.provider)?.get(target.modelId) ?? null;

  try {
    const row = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("model_capability_overrides").filter(
            (row) =>
              row.provider === target.provider &&
              row.model_id === target.modelId &&
              row.override_key === "reasoning_efforts"
          )[0]
        : getDbInstance()
            .prepare(
              "SELECT provider, model_id, override_key, override_value, refreshed_at " +
                "FROM model_capability_overrides WHERE provider = ? AND model_id = ? AND override_key = 'reasoning_efforts'"
            )
            .get(target.provider, target.modelId)
    ) as OverrideRow | undefined;
    const override = row ? toOverride(row) : null;
    return override?.key === "reasoning_efforts" ? override.value : null;
  } catch {
    return null;
  }
}

export async function setModelCapabilityOverrideAsync(
  target: string,
  key: ModelCapabilityOverrideKey,
  value: number | string | readonly string[]
): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const parsedTarget = parseModelOverrideTarget(target);
    if (!parsedTarget || !isSupportedKey(key)) return false;

    let normalizedValue: number | ReasoningEffortOverrideValue[];
    if (key === "reasoning_efforts") {
      const parsed = Array.isArray(value)
        ? parseReasoningEffortsOverride(value.join(","))
        : parseReasoningEffortsOverride(value);
      if (!parsed.ok) return false;
      normalizedValue = parsed.efforts;
    } else {
      if (!isPositiveInteger(value)) return false;
      normalizedValue = value;
    }

    getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "INSERT INTO omniroute.model_capability_overrides (provider, model_id, override_key, override_value, refreshed_at) VALUES ($1, $2, $3, $4, to_char(timezone('UTC',clock_timestamp()),'YYYY-MM-DD HH24:MI:SS')) ON CONFLICT(provider,model_id,override_key) DO UPDATE SET override_value=EXCLUDED.override_value,refreshed_at=EXCLUDED.refreshed_at",
                [parsedTarget.provider, parsedTarget.modelId, key, JSON.stringify(normalizedValue)]
              )
            ).rowCount ?? 0,
        }
      : getDbInstance()
          .prepare(
            "INSERT OR REPLACE INTO model_capability_overrides " +
              "(provider, model_id, override_key, override_value, refreshed_at) " +
              "VALUES (?, ?, ?, ?, datetime('now'))"
          )
          .run(parsedTarget.provider, parsedTarget.modelId, key, JSON.stringify(normalizedValue));
    if (getSelectedPostgresDatabase())
      afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
    else invalidateDbCache("model-capabilities");
    return true;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export async function removeModelCapabilityOverrideAsync(
  target: string,
  key: ModelCapabilityOverrideKey
): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const parsedTarget = parseModelOverrideTarget(target);
    if (!parsedTarget || !isSupportedKey(key)) return false;

    const info = getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "DELETE FROM omniroute.model_capability_overrides WHERE provider = $1 AND model_id = $2 AND override_key = $3",
                [parsedTarget.provider, parsedTarget.modelId, key]
              )
            ).rowCount ?? 0,
        }
      : getDbInstance()
          .prepare(
            "DELETE FROM model_capability_overrides " +
              "WHERE provider = ? AND model_id = ? AND override_key = ?"
          )
          .run(parsedTarget.provider, parsedTarget.modelId, key);
    if (info.changes > 0) {
      if (getSelectedPostgresDatabase())
        afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
      else invalidateDbCache("model-capabilities");
    }
    return info.changes > 0;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function listModelCapabilityOverrides(): ModelCapabilityOverride[] {
  requirePostgresCatalogSnapshot();

  try {
    const rows = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("model_capability_overrides").sort(
            (a, b) =>
              compareCatalogText(b.refreshed_at, a.refreshed_at) ||
              compareCatalogText(a.provider, b.provider) ||
              compareCatalogText(a.model_id, b.model_id) ||
              compareCatalogText(a.override_key, b.override_key)
          )
        : getDbInstance()
            .prepare(
              "SELECT provider, model_id, override_key, override_value, refreshed_at " +
                "FROM model_capability_overrides ORDER BY refreshed_at DESC"
            )
            .all()
    ) as OverrideRow[];
    return rows.map(toOverride).filter((entry): entry is ModelCapabilityOverride => entry !== null);
  } catch {
    return [];
  }
}

export function setModelCapabilityOverride(
  target: string,
  key: ModelCapabilityOverrideKey,
  value: number | string | readonly string[]
): boolean {
  assertSqliteBackendAllowed();
  const parsedTarget = parseModelOverrideTarget(target);
  if (!parsedTarget || !isSupportedKey(key)) return false;

  let normalizedValue: number | ReasoningEffortOverrideValue[];
  if (key === "reasoning_efforts") {
    const parsed = Array.isArray(value)
      ? parseReasoningEffortsOverride(value.join(","))
      : parseReasoningEffortsOverride(value);
    if (!parsed.ok) return false;
    normalizedValue = parsed.efforts;
  } else {
    if (!isPositiveInteger(value)) return false;
    normalizedValue = value;
  }

  getDbInstance()
    .prepare(
      "INSERT OR REPLACE INTO model_capability_overrides " +
        "(provider, model_id, override_key, override_value, refreshed_at) " +
        "VALUES (?, ?, ?, ?, datetime('now'))"
    )
    .run(parsedTarget.provider, parsedTarget.modelId, key, JSON.stringify(normalizedValue));
  invalidateDbCache("model-capabilities");
  return true;
}

export function removeModelCapabilityOverride(
  target: string,
  key: ModelCapabilityOverrideKey
): boolean {
  assertSqliteBackendAllowed();
  const parsedTarget = parseModelOverrideTarget(target);
  if (!parsedTarget || !isSupportedKey(key)) return false;

  const info = getDbInstance()
    .prepare(
      "DELETE FROM model_capability_overrides " +
        "WHERE provider = ? AND model_id = ? AND override_key = ?"
    )
    .run(parsedTarget.provider, parsedTarget.modelId, key);
  if (info.changes > 0) invalidateDbCache("model-capabilities");
  return info.changes > 0;
}
