import { getDefaultPricing } from "@/shared/constants/pricing";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import { catalogSnapshotReadIdentity, readPostgresCatalogNamespace } from "./catalogSnapshot";

type PreparedUserPricing = Record<string, Record<string, Record<string, unknown>>>;

interface DecodedRow {
  key: string;
  value: unknown;
}
export interface PreparedTierPrice {
  input: number;
  output: number;
}
interface PreparedPricing {
  namespaces: Map<string, readonly DecodedRow[]>;
  user?: PreparedUserPricing;
  tier?: ReadonlyMap<string, PreparedTierPrice>;
}

const snapshots = new WeakMap<object, PreparedPricing>();

function selectedPricing(): PreparedPricing | undefined {
  if (!getSelectedPostgresDatabase()) return undefined;
  const identity = catalogSnapshotReadIdentity();
  let pricing = snapshots.get(identity);
  if (!pricing) {
    pricing = { namespaces: new Map() };
    snapshots.set(identity, pricing);
  }
  return pricing;
}

function decode(pricing: PreparedPricing, namespace: string): readonly DecodedRow[] {
  const cached = pricing.namespaces.get(namespace);
  if (cached) return cached;
  const rows: DecodedRow[] = [];
  for (const row of readPostgresCatalogNamespace(namespace) ?? []) {
    try {
      rows.push({ key: row.key, value: JSON.parse(row.value) });
    } catch {
      // Source pricing readers skip corrupt rows and retain the lower layers.
    }
  }
  pricing.namespaces.set(namespace, rows);
  return rows;
}

/** Same bulk user namespace and object guard as the SQLite catalog reader. */
export function readPreparedPostgresUserPricing(): PreparedUserPricing | undefined {
  const pricing = selectedPricing();
  if (!pricing) return undefined;
  if (!pricing.user) {
    const user: PreparedUserPricing = {};
    for (const row of decode(pricing, "pricing")) {
      if (row.value && typeof row.value === "object") {
        user[row.key] = row.value as PreparedUserPricing[string];
      }
    }
    pricing.user = user;
  }
  return pricing.user;
}

/** Preserve the sync economic-tier snapshot's source layer and numeric coercion rules. */
export function readPreparedPostgresTierPricing():
  ReadonlyMap<string, PreparedTierPrice> | undefined {
  const pricing = selectedPricing();
  if (!pricing) return undefined;
  if (!pricing.tier) {
    const merged: Record<string, Record<string, unknown>> = {
      ...(getDefaultPricing() as Record<string, Record<string, unknown>>),
    };
    for (const namespace of ["pricing_synced", "models_dev_pricing", "pricing"]) {
      for (const row of decode(pricing, namespace)) {
        merged[row.key] = {
          ...(merged[row.key] || {}),
          ...(row.value as Record<string, unknown>),
        };
      }
    }
    const tier = new Map<string, PreparedTierPrice>();
    for (const [provider, models] of Object.entries(merged)) {
      for (const [model, raw] of Object.entries(models)) {
        const price = raw as { input?: unknown; output?: unknown };
        const input = Number(price?.input);
        const output = Number(price?.output);
        if (Number.isFinite(input) && input >= 0) {
          tier.set(`${provider.toLowerCase()}::${model.toLowerCase()}`, {
            input,
            output: Number.isFinite(output) && output >= 0 ? output : input,
          });
        }
      }
    }
    pricing.tier = tier;
  }
  return pricing.tier;
}
