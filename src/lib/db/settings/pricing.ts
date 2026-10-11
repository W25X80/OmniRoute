/**
 * db/settings/pricing.ts — Pricing data CRUD (user overrides, LiteLLM sync, models.dev sync).
 */

import { getDbInstance } from "../core";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import { listPostgresKeyValues } from "../postgres/keyValueRepository";
import {
  hasPostgresCatalogSnapshot,
  readPostgresCatalogNamespace,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogValue,
  deleteCatalogMatchingKeys,
  afterPostgresCatalogCommit,
} from "../postgres/catalogSnapshot";
import { backupDbFile } from "../backup";
import { getCachedPricing, invalidateDbCache } from "../readCache";
import { PROVIDER_ID_TO_ALIAS } from "@omniroute/open-sse/config/providerModels.ts";
import { invalidateUserPricingMemo } from "@/lib/catalogUserPricing";
import { type JsonRecord, toRecord } from "./shared";

type PricingModels = Record<string, JsonRecord>;
type PricingByProvider = Record<string, PricingModels>;
export type PricingSource = "default" | "litellm" | "modelsDev" | "user";
export type PricingSourceMap = Record<string, Record<string, PricingSource>>;

async function touchPricing(): Promise<void> {
  invalidateDbCache("pricing");
  invalidateUserPricingMemo();
  try {
    const { clearTierCache } = await import("@omniroute/open-sse/services/tierResolver");
    clearTierCache();
  } catch {
    // fail-open: a missed tier invalidation must never break a price write
  }
}

function readPricingNamespace(
  db: ReturnType<typeof getDbInstance>,
  namespace: string
): PricingByProvider {
  const rows = db.prepare("SELECT key, value FROM key_value WHERE namespace = ?").all(namespace);
  return parsePricingRows(rows, true);
}

function parsePricingRows(rows: readonly unknown[], tolerateCorrupt: boolean): PricingByProvider {
  const pricing: PricingByProvider = {};

  for (const row of rows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;

    try {
      pricing[key] = toRecord(JSON.parse(rawValue)) as PricingModels;
    } catch (error) {
      if (!tolerateCorrupt) throw error;
      // Corrupted data — skip silently, fallback to lower layers
    }
  }

  return pricing;
}

function mergePricingLayers(layers: PricingByProvider[]): PricingByProvider {
  const mergedPricing: PricingByProvider = {};

  for (const layer of layers) {
    for (const [provider, models] of Object.entries(layer)) {
      if (!mergedPricing[provider]) {
        mergedPricing[provider] = { ...models };
        continue;
      }

      for (const [model, pricing] of Object.entries(models)) {
        mergedPricing[provider][model] = mergedPricing[provider][model]
          ? { ...(mergedPricing[provider][model] || {}), ...toRecord(pricing) }
          : pricing;
      }
    }
  }

  return mergedPricing;
}

function buildPricingSourceMap(layers: {
  defaults: PricingByProvider;
  litellm: PricingByProvider;
  modelsDev: PricingByProvider;
  user: PricingByProvider;
}): PricingSourceMap {
  const sourceMap: PricingSourceMap = {};
  const mergedPricing = mergePricingLayers([
    layers.defaults,
    layers.litellm,
    layers.modelsDev,
    layers.user,
  ]);

  for (const [provider, models] of Object.entries(mergedPricing)) {
    sourceMap[provider] = {};

    for (const model of Object.keys(models)) {
      if (layers.user[provider]?.[model]) {
        sourceMap[provider][model] = "user";
      } else if (layers.modelsDev[provider]?.[model]) {
        sourceMap[provider][model] = "modelsDev";
      } else if (layers.litellm[provider]?.[model]) {
        sourceMap[provider][model] = "litellm";
      } else {
        sourceMap[provider][model] = "default";
      }
    }
  }

  return sourceMap;
}

async function getPricingLayers() {
  // Layer 1: Hardcoded defaults (lowest priority)
  const { getDefaultPricing } = await import("@/shared/constants/pricing");
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const namespaces = ["pricing_synced", "models_dev_pricing", "pricing"] as const;
    const rowsByNamespace = hasPostgresCatalogSnapshot(postgres)
      ? namespaces.map((namespace) => readPostgresCatalogNamespace(namespace) ?? [])
      : await postgres.transaction(async (transaction) => {
          const rows: Awaited<ReturnType<typeof listPostgresKeyValues>>[] = [];
          for (const namespace of namespaces)
            rows.push(await listPostgresKeyValues(transaction, namespace));
          return rows;
        });
    return {
      defaults: getDefaultPricing(),
      litellm: parsePricingRows(rowsByNamespace[0], true),
      modelsDev: parsePricingRows(rowsByNamespace[1], true),
      user: parsePricingRows(rowsByNamespace[2], true),
    };
  }
  const db = getDbInstance();
  return {
    defaults: getDefaultPricing(),
    litellm: readPricingNamespace(db, "pricing_synced"),
    modelsDev: readPricingNamespace(db, "models_dev_pricing"),
    user: readPricingNamespace(db, "pricing"),
  };
}

export async function getPricing() {
  const layers = await getPricingLayers();
  // Merge: defaults → LiteLLM → models.dev → user (each layer overrides the previous)
  return mergePricingLayers([layers.defaults, layers.litellm, layers.modelsDev, layers.user]);
}

export async function getPricingWithSources(): Promise<{
  pricing: PricingByProvider;
  sourceMap: PricingSourceMap;
}> {
  const layers = await getPricingLayers();
  return {
    pricing: mergePricingLayers([layers.defaults, layers.litellm, layers.modelsDev, layers.user]),
    sourceMap: buildPricingSourceMap(layers),
  };
}

export async function getPricingForModel(provider: string, model: string) {
  const pricing = (await getCachedPricing()) as PricingByProvider;

  const findKeyInsensitive = <T>(
    obj: Record<string, T> | undefined | null,
    key: string
  ): T | undefined => {
    if (!obj || !key) return undefined;
    const lowerKey = key.toLowerCase();
    for (const [k, v] of Object.entries(obj)) {
      if (k.toLowerCase() === lowerKey) return v;
    }
    return undefined;
  };

  const pLower = (provider || "").toLowerCase();
  let providerPricing = findKeyInsensitive<PricingModels>(pricing, pLower);

  if (!providerPricing) {
    const alias = findKeyInsensitive<string>(PROVIDER_ID_TO_ALIAS, pLower);
    if (alias) providerPricing = findKeyInsensitive(pricing, alias);
  }

  if (!providerPricing) {
    for (const [id, mappedAlias] of Object.entries(PROVIDER_ID_TO_ALIAS)) {
      if (typeof mappedAlias === "string" && mappedAlias.toLowerCase() === pLower) {
        providerPricing = findKeyInsensitive(pricing, id);
        if (providerPricing) break;
      }
    }
  }

  if (!providerPricing) {
    const np = pLower.replace(/-cn$/, "");
    if (np && np !== pLower) {
      providerPricing = findKeyInsensitive(pricing, np);
    }
  }

  if (!providerPricing) return null;

  const mLower = (model || "").toLowerCase();
  let modelPricing = findKeyInsensitive<JsonRecord>(providerPricing, mLower);

  if (!modelPricing) {
    const hyphenModel = mLower.replace(/\./g, "-");
    modelPricing = findKeyInsensitive(providerPricing, hyphenModel);
  }

  return modelPricing || null;
}

export async function updatePricing(pricingData: PricingByProvider) {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    return mutatePostgresCatalog(postgres, async () => {
      const existing = parsePricingRows(readPostgresCatalogNamespace("pricing") ?? [], false);
      for (const [provider, models] of Object.entries(pricingData)) {
        await putCatalogValue(
          "pricing",
          provider,
          JSON.stringify({ ...(existing[provider] || {}), ...models })
        );
      }
      afterPostgresCatalogCommit(touchPricing);
      return parsePricingRows(readPostgresCatalogNamespace("pricing") ?? [], false);
    });
  }
  const db = getDbInstance();
  const insert = db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('pricing', ?, ?)"
  );

  const rows = db.prepare("SELECT key, value FROM key_value WHERE namespace = 'pricing'").all();
  const existing: PricingByProvider = {};
  for (const row of rows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;
    existing[key] = toRecord(JSON.parse(rawValue)) as PricingModels;
  }

  const tx = db.transaction(() => {
    for (const [provider, models] of Object.entries(pricingData)) {
      insert.run(provider, JSON.stringify({ ...(existing[provider] || {}), ...models }));
    }
  });
  tx();
  backupDbFile("pre-write");
  await touchPricing();
  const updated: PricingByProvider = {};
  const allRows = db.prepare("SELECT key, value FROM key_value WHERE namespace = 'pricing'").all();
  for (const row of allRows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;
    updated[key] = toRecord(JSON.parse(rawValue)) as PricingModels;
  }
  return updated;
}

export async function resetPricing(provider: string, model?: string) {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    return mutatePostgresCatalog(postgres, async () => {
      if (model) {
        const row = (readPostgresCatalogNamespace("pricing") ?? []).find(
          (entry) => entry.key === provider
        );
        if (row) {
          const models = toRecord(JSON.parse(row.value));
          delete models[model];
          if (Object.keys(models).length === 0) await deleteCatalogValue("pricing", provider);
          else await putCatalogValue("pricing", provider, JSON.stringify(models));
        }
      } else {
        await deleteCatalogValue("pricing", provider);
      }
      afterPostgresCatalogCommit(touchPricing);
      return Object.fromEntries(
        (readPostgresCatalogNamespace("pricing") ?? []).map((row) => [
          row.key,
          JSON.parse(row.value),
        ])
      );
    });
  }
  const db = getDbInstance();

  if (model) {
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = 'pricing' AND key = ?")
      .get(provider);
    if (row) {
      const rowRecord = toRecord(row);
      const value = typeof rowRecord.value === "string" ? rowRecord.value : "{}";
      const models = toRecord(JSON.parse(value));
      delete models[model];
      if (Object.keys(models).length === 0) {
        db.prepare("DELETE FROM key_value WHERE namespace = 'pricing' AND key = ?").run(provider);
      } else {
        db.prepare("UPDATE key_value SET value = ? WHERE namespace = 'pricing' AND key = ?").run(
          JSON.stringify(models),
          provider
        );
      }
    }
  } else {
    db.prepare("DELETE FROM key_value WHERE namespace = 'pricing' AND key = ?").run(provider);
  }

  backupDbFile("pre-write");
  await touchPricing();
  const allRows = db.prepare("SELECT key, value FROM key_value WHERE namespace = 'pricing'").all();
  const result: Record<string, unknown> = {};
  for (const row of allRows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;
    result[key] = JSON.parse(rawValue);
  }
  return result;
}

export async function resetAllPricing() {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    await mutatePostgresCatalog(postgres, async () => {
      await deleteCatalogMatchingKeys("pricing", () => true);
      afterPostgresCatalogCommit(touchPricing);
    });
    return {};
  }
  const db = getDbInstance();
  db.prepare("DELETE FROM key_value WHERE namespace = 'pricing'").run();
  backupDbFile("pre-write");
  await touchPricing();
  return {};
}
