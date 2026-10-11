import type { PostgresDatabase } from "./database";
import { lockProxyState } from "./proxyRepository";
import { bumpProxyRegistryGeneration } from "../proxies/registryGeneration";
export interface ProxySubscriptionWrite {
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
  controlSecretEnc?: string | null;
  gapSeconds: number;
  now: string;
}
export function createPostgresProxySubscriptionRepository(database: PostgresDatabase) {
  return {
    list: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "SELECT * FROM omniroute.proxy_subscriptions ORDER BY updated_at DESC,name"
            )
          ).rows
      ),
    get: (id: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("SELECT * FROM omniroute.proxy_subscriptions WHERE id=$1", [id]))
            .rows[0] ?? null
      ),
    insert: (args: ProxySubscriptionWrite) =>
      database.transaction(async (tx) => {
        await lockProxyState(tx);
        await tx.query(
          "INSERT INTO omniroute.proxy_subscriptions(id,name,url,enabled,mode,rule_providers,local_core_endpoint,update_interval_minutes,control_url,core_config_path,control_secret_enc,selector_min_gap_seconds,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'empty',$13,$13)",
          [
            args.id,
            args.name,
            args.url,
            args.enabled,
            args.mode,
            args.ruleProviders ? JSON.stringify(args.ruleProviders) : null,
            args.localCoreEndpoint,
            args.updateIntervalMinutes,
            args.controlUrl,
            args.coreConfigPath,
            args.controlSecretEnc ?? null,
            args.gapSeconds,
            args.now,
          ]
        );
      }),
    update: (args: ProxySubscriptionWrite) =>
      database.transaction(async (tx) => {
        await lockProxyState(tx);
        const values: unknown[] = [
          args.id,
          args.name,
          args.url,
          args.enabled,
          args.mode,
          args.ruleProviders ? JSON.stringify(args.ruleProviders) : null,
          args.localCoreEndpoint,
          args.updateIntervalMinutes,
          args.controlUrl,
          args.coreConfigPath,
          args.gapSeconds,
          args.now,
        ];
        let secret = "";
        if (args.controlSecretEnc !== undefined) {
          values.push(args.controlSecretEnc);
          secret = ",control_secret_enc=$13";
        }
        await tx.query(
          `UPDATE omniroute.proxy_subscriptions SET name=$2,url=$3,enabled=$4,mode=$5,rule_providers=$6,local_core_endpoint=$7,update_interval_minutes=$8,control_url=$9,core_config_path=$10,selector_min_gap_seconds=$11,updated_at=$12${secret} WHERE id=$1`,
          values
        );
      }),
    delete: (id: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("DELETE FROM omniroute.proxy_subscriptions WHERE id=$1", [id]))
            .rowCount ?? 0
      ),
    proxyIds: (id: string, active = false, kept?: string[]) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<{ id: string }>(
              `SELECT id FROM omniroute.proxy_registry WHERE subscription_id=$1 ${active ? "AND status <> 'error'" : ""} ${kept?.length ? "AND NOT(id=ANY($2::text[]))" : ""}`,
              kept?.length ? [id, kept] : [id]
            )
          ).rows
      ),
    detach: (id: string) =>
      database
        .transaction(async (tx) => {
          await lockProxyState(tx);
          return (
            (
              await tx.query(
                "DELETE FROM omniroute.proxy_assignments WHERE proxy_id IN(SELECT id FROM omniroute.proxy_registry WHERE subscription_id=$1)",
                [id]
              )
            ).rowCount ?? 0
          );
        })
        .then((count) => {
          if (count) bumpProxyRegistryGeneration();
          return count;
        }),
    setStatus: (
      id: string,
      status: string,
      error: string | null,
      lastNodes: unknown[] | null,
      lastErrorAt: string | null,
      failures: number
    ) =>
      database.transaction(async (tx) => {
        const now = new Date().toISOString();
        await tx.query(
          "UPDATE omniroute.proxy_subscriptions SET status=$2,error=$3,last_nodes=$4,last_fetched_at=$5,last_error_at=$6,consecutive_failures=$7,updated_at=$5 WHERE id=$1",
          [
            id,
            status,
            error,
            lastNodes ? JSON.stringify(lastNodes) : null,
            now,
            lastErrorAt,
            failures,
          ]
        );
      }),
    recordSwitch: (
      args: {
        subscriptionId: string;
        result: string;
        member?: string | null;
        kind?: string | null;
      },
      error: string
    ) =>
      database.transaction(async (tx) => {
        const now = new Date().toISOString();
        await tx.query(
          "UPDATE omniroute.proxy_subscriptions SET selector_last_switch_at=$2,selector_last_switch_result=$3,selector_last_switch_member=$4,selector_last_switch_kind=$5,updated_at=$2 WHERE id=$1",
          [args.subscriptionId, now, args.result, args.member ?? null, args.kind ?? null]
        );
        if (args.result !== "ok" && args.result !== "throttled")
          await tx.query(
            "UPDATE omniroute.proxy_subscriptions SET error=$2,updated_at=$3 WHERE id=$1 AND status='ok'",
            [args.subscriptionId, error, now]
          );
      }),
    nonSubscriptionGlobal: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "SELECT 1 FROM omniroute.proxy_assignments a JOIN omniroute.proxy_registry p ON p.id=a.proxy_id WHERE a.scope='global' AND (p.subscription_id IS NULL OR p.subscription_id='') LIMIT 1"
            )
          ).rows.length > 0
      ),
    boundEnabled: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "SELECT 1 FROM omniroute.proxy_subscriptions s JOIN omniroute.proxy_registry p ON p.subscription_id=s.id JOIN omniroute.proxy_assignments a ON a.proxy_id=p.id WHERE s.enabled=1 LIMIT 1"
            )
          ).rows.length > 0
      ),
    setEnabledFlag: (value: boolean) =>
      database.transaction(async (tx) => {
        await lockProxyState(tx);
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings','proxyEnabled',$1) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
          [JSON.stringify(value)]
        );
      }),
    controlUrlConfigured: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "SELECT 1 FROM omniroute.proxy_subscriptions WHERE control_url IS NOT NULL LIMIT 1"
            )
          ).rows.length > 0
      ),
    selectorRows: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "SELECT p.id AS rid,p.type,p.host,p.port,p.username,p.subscription_id,p.updated_at,s.control_url,s.control_secret_enc,s.selector_min_gap_seconds,s.local_core_endpoint FROM omniroute.proxy_registry p JOIN omniroute.proxy_subscriptions s ON s.id=p.subscription_id WHERE p.subscription_id IS NOT NULL AND s.control_url IS NOT NULL ORDER BY p.updated_at DESC,p.id"
            )
          ).rows
      ),
    allowlist: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<{ value: string }>(
              "SELECT value FROM omniroute.key_value WHERE namespace='feature_flags' AND key='SELECTOR_CONTROL_ALLOWLIST'"
            )
          ).rows[0]?.value ?? null
      ),
  };
}
