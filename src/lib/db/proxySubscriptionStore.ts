import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresProxySubscriptionRepository } from "./postgres/proxySubscriptionRepository";
/** Internal domain selection; callers never receive a driver or SQL executor. */
export function getSelectedProxySubscriptionStore() {
  const postgres = getSelectedPostgresDatabase();
  return postgres ? createPostgresProxySubscriptionRepository(postgres) : undefined;
}

export async function persistSubscriptionRow(
  db: { prepare: (sql: string) => { run: (...args: unknown[]) => unknown } } | null,
  args: {
    id: string;
    name: string;
    url: string;
    enabled: number;
    mode: string;
    ruleProviders: string[] | null;
    localCoreEndpoint: string | null;
    updateIntervalMinutes: number;
    controlUrl: string | null;
    coreConfigPath: string | null;
    controlSecretEnc: string | null | undefined;
    gapSeconds: number;
    now: string;
  }
): Promise<void> {
  const store = getSelectedProxySubscriptionStore();
  if (store) return store.update(args);
  if (!db) throw new Error("SQLite subscription persistence requires a database handle");
  if (args.controlSecretEnc === undefined) {
    db.prepare(
      `UPDATE proxy_subscriptions
         SET name = ?, url = ?, enabled = ?, mode = ?, rule_providers = ?, local_core_endpoint = ?, update_interval_minutes = ?, control_url = ?, core_config_path = ?, selector_min_gap_seconds = ?, updated_at = ?
       WHERE id = ?`
    ).run(
      args.name,
      args.url,
      args.enabled,
      args.mode,
      args.ruleProviders ? JSON.stringify(args.ruleProviders) : null,
      args.localCoreEndpoint || null,
      args.updateIntervalMinutes,
      args.controlUrl,
      args.coreConfigPath,
      args.gapSeconds,
      args.now,
      args.id
    );
  } else {
    db.prepare(
      `UPDATE proxy_subscriptions
         SET name = ?, url = ?, enabled = ?, mode = ?, rule_providers = ?, local_core_endpoint = ?, update_interval_minutes = ?, control_url = ?, core_config_path = ?, control_secret_enc = ?, selector_min_gap_seconds = ?, updated_at = ?
       WHERE id = ?`
    ).run(
      args.name,
      args.url,
      args.enabled,
      args.mode,
      args.ruleProviders ? JSON.stringify(args.ruleProviders) : null,
      args.localCoreEndpoint || null,
      args.updateIntervalMinutes,
      args.controlUrl,
      args.coreConfigPath,
      args.controlSecretEnc,
      args.gapSeconds,
      args.now,
      args.id
    );
  }
}

/** Read the standing switch-failure warning for a subscription, if any.
 * Returns a `SELECTOR_SWITCH_FAILED` error code while the last recorded
 * switch result is a failure, null otherwise (no record, or last was "ok").
 * Never throws (missing columns on old DBs → null). */
export function readStaleSwitchWarning(subscriptionId: string): string | null {
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT selector_last_switch_result FROM proxy_subscriptions WHERE id = ?")
      .get(subscriptionId) as { selector_last_switch_result?: unknown } | undefined;
    const result =
      typeof row?.selector_last_switch_result === "string" ? row.selector_last_switch_result : null;
    if (!result || result === "ok" || result === "throttled") return null;
    return JSON.stringify({ code: "SELECTOR_SWITCH_FAILED", detail: result });
  } catch {
    return null;
  }
}

export async function readStaleSwitchWarningAsync(subscriptionId: string): Promise<string | null> {
  const store = getSelectedProxySubscriptionStore();
  if (!store) return readStaleSwitchWarning(subscriptionId);
  const row = await store.get(subscriptionId);
  const result =
    typeof row?.selector_last_switch_result === "string" ? row.selector_last_switch_result : null;
  return !result || result === "ok" || result === "throttled"
    ? null
    : JSON.stringify({ code: "SELECTOR_SWITCH_FAILED", detail: result });
}
