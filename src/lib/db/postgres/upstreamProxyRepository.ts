import type { PostgresDatabase } from "./database";
import type { FallbackBackend } from "../upstreamProxy";
interface Input {
  providerId: string;
  mode?: string;
  cliproxyapiModelMapping?: Record<string, unknown> | null;
  nativePriority?: number;
  cliproxyapiPriority?: number;
  enabled?: boolean;
  family?: string;
  fallbackBackend?: FallbackBackend;
}
function fallback(value: unknown): FallbackBackend {
  return value === "dario" ? "dario" : "cliproxyapi";
}
export function mapUpstreamProxyRow(record: Record<string, unknown>) {
  let mapping: Record<string, unknown> | null = null;
  if (typeof record.cliproxyapi_model_mapping === "string" && record.cliproxyapi_model_mapping) {
    try {
      mapping = JSON.parse(record.cliproxyapi_model_mapping);
    } catch {}
  }
  return {
    id: Number(record.id),
    providerId: String(record.provider_id),
    mode: String(record.mode),
    cliproxyapiModelMapping: mapping,
    nativePriority: Number(record.native_priority),
    cliproxyapiPriority: Number(record.cliproxyapi_priority),
    enabled: record.enabled === 1 || record.enabled === true,
    family: typeof record.family === "string" ? record.family : "auto",
    fallbackBackend: fallback(record.fallback_backend),
    createdAt: String(record.created_at),
    updatedAt: String(record.updated_at),
  };
}
export function createPostgresUpstreamProxyRepository(database: PostgresDatabase) {
  return {
    get: (id: string) =>
      database.transaction(async (tx) => {
        const row = (
          await tx.query("SELECT * FROM omniroute.upstream_proxy_config WHERE provider_id=$1", [id])
        ).rows[0];
        return row ? mapUpstreamProxyRow(row) : null;
      }),
    upsert: (data: Input) =>
      database.transaction(async (tx) => {
        const now = new Date().toISOString().slice(0, 19).replace("T", " ");
        const row = (
          await tx.query(
            "INSERT INTO omniroute.upstream_proxy_config(provider_id,mode,cliproxyapi_model_mapping,native_priority,cliproxyapi_priority,enabled,family,fallback_backend,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9) ON CONFLICT(provider_id) DO UPDATE SET mode=excluded.mode,cliproxyapi_model_mapping=excluded.cliproxyapi_model_mapping,native_priority=excluded.native_priority,cliproxyapi_priority=excluded.cliproxyapi_priority,enabled=excluded.enabled,family=excluded.family,fallback_backend=excluded.fallback_backend,updated_at=excluded.updated_at RETURNING *",
            [
              data.providerId,
              data.mode ?? "native",
              data.cliproxyapiModelMapping !== undefined
                ? JSON.stringify(data.cliproxyapiModelMapping)
                : null,
              data.nativePriority ?? 1,
              data.cliproxyapiPriority ?? 2,
              data.enabled !== false ? 1 : 0,
              data.family ?? "auto",
              fallback(data.fallbackBackend),
              now,
            ]
          )
        ).rows[0];
        return mapUpstreamProxyRow(row);
      }),
    update: (id: string, updates: Record<string, unknown>) =>
      database.transaction(async (tx) => {
        const current = (
          await tx.query(
            "SELECT * FROM omniroute.upstream_proxy_config WHERE provider_id=$1 FOR UPDATE",
            [id]
          )
        ).rows[0];
        if (!current) throw new Error(`Provider ${id} not found`);
        const columns: Record<string, string> = {
          mode: "mode",
          cliproxyapiModelMapping: "cliproxyapi_model_mapping",
          nativePriority: "native_priority",
          cliproxyapiPriority: "cliproxyapi_priority",
          enabled: "enabled",
          family: "family",
          fallbackBackend: "fallback_backend",
        };
        const values: unknown[] = [id, new Date().toISOString().slice(0, 19).replace("T", " ")];
        const sets = ["updated_at=$2"];
        for (const [key, column] of Object.entries(columns)) {
          if (updates[key] === undefined) continue;
          const value =
            key === "cliproxyapiModelMapping"
              ? updates[key] === null
                ? null
                : JSON.stringify(updates[key])
              : key === "enabled"
                ? updates[key] === true
                  ? 1
                  : 0
                : key === "fallbackBackend"
                  ? fallback(updates[key])
                  : updates[key];
          values.push(value);
          sets.push(`${column}=$${values.length}`);
        }
        return mapUpstreamProxyRow(
          (
            await tx.query(
              `UPDATE omniroute.upstream_proxy_config SET ${sets.join(",")} WHERE provider_id=$1 RETURNING *`,
              values
            )
          ).rows[0]
        );
      }),
    delete: (id: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("DELETE FROM omniroute.upstream_proxy_config WHERE provider_id=$1", [id]))
            .rowCount! > 0
      ),
    byMode: (mode: string) =>
      database.transaction(async (tx) =>
        (
          await tx.query(
            'SELECT * FROM omniroute.upstream_proxy_config WHERE mode=$1 AND enabled=1 ORDER BY provider_id COLLATE "C"',
            [mode]
          )
        ).rows.map(mapUpstreamProxyRow)
      ),
    oneproxy: (options?: {
      protocol?: string;
      countryCode?: string;
      minQuality?: number;
      limit?: number;
    }) =>
      database.transaction(async (tx) => {
        const values: unknown[] = [];
        let sql =
          "SELECT * FROM omniroute.proxy_registry WHERE source='oneproxy' AND status='active'";
        for (const [column, value] of [
          ["type", options?.protocol],
          ["country_code", options?.countryCode],
          ["quality_score", options?.minQuality],
        ] as const) {
          if (value === undefined || value === null || value === "") continue;
          values.push(value);
          sql += ` AND ${column} ${column === "quality_score" ? ">=" : "="} $${values.length}`;
        }
        sql += " ORDER BY quality_score DESC NULLS LAST,last_validated DESC NULLS LAST";
        if (options?.limit) {
          values.push(options.limit);
          sql += ` LIMIT CASE WHEN $${values.length}::bigint < 0 THEN NULL ELSE $${values.length}::bigint END`;
        }
        return (await tx.query(sql, values)).rows;
      }),
  };
}
