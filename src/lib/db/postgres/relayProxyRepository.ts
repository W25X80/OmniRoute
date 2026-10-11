import { randomBytes, createHash } from "node:crypto";
import type { PostgresDatabase } from "./database";
import { rowToCamel } from "../caseMapping";
import type {
  CreateRelayTokenInput,
  RelayToken,
  RelayTokenRow,
  RelayTokenWithSecret,
  RelayLogRow,
} from "../relayProxies";
type StoredTokenRow = Omit<
  RelayTokenRow,
  "created_at" | "updated_at" | "expires_at" | "last_used_at"
> & {
  created_at: string | number;
  updated_at: string | number;
  expires_at: string | number | null;
  last_used_at: string | number | null;
};
type StoredLogRow = Omit<RelayLogRow, "id" | "created_at"> & {
  id: string | number;
  created_at: string | number;
};
function normalizeTokenRow(row: StoredTokenRow): RelayTokenRow {
  const { insertion_order: _order, ...value } = row as StoredTokenRow & {
    insertion_order?: unknown;
  };
  return {
    ...value,
    created_at: Number(row.created_at),
    updated_at: Number(row.updated_at),
    expires_at: row.expires_at === null ? null : Number(row.expires_at),
    last_used_at: row.last_used_at === null ? null : Number(row.last_used_at),
  };
}
function token(row: StoredTokenRow): RelayToken {
  return {
    ...rowToCamel(normalizeTokenRow(row)),
    enabled: row.enabled === 1,
  } as unknown as RelayToken;
}
type Usage = {
  requestId?: string;
  model?: string;
  promptTokens?: number;
  completionTokens?: number;
  cost?: number;
  status?: string;
  statusCode?: number;
  latencyMs?: number;
  clientIp?: string;
  userAgent?: string;
};
/** Check-only admission mirrors SQLite; usage is recorded by the completion path. */
export function createPostgresRelayProxyRepository(database: PostgresDatabase) {
  return {
    create: (input: CreateRelayTokenInput) =>
      database.transaction(async (tx) => {
        const id = "rl_" + randomBytes(16).toString("hex"),
          rawToken = "relay_" + randomBytes(24).toString("hex"),
          hash = createHash("sha256").update(rawToken).digest("hex"),
          now = Math.floor(Date.now() / 1000);
        const row = (
          await tx.query<StoredTokenRow>(
            "INSERT INTO omniroute.relay_tokens(id,name,token_hash,token_prefix,description,combo_id,allowed_models,max_tokens_per_request,max_requests_per_minute,max_requests_per_day,max_cost_per_day,enabled,created_at,updated_at,expires_at,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,$12,$12,$13,$14) RETURNING *",
            [
              id,
              input.name,
              hash,
              "rl_" + rawToken.slice(6, 14),
              input.description || "",
              input.comboId || null,
              JSON.stringify(input.allowedModels || ["*"]),
              input.maxTokensPerRequest || 128000,
              input.maxRequestsPerMinute || 60,
              input.maxRequestsPerDay || 10000,
              input.maxCostPerDay || 0,
              now,
              input.expiresAt || null,
              JSON.stringify(input.metadata || {}),
            ]
          )
        ).rows[0];
        return {
          ...(rowToCamel(normalizeTokenRow(row)) as unknown as RelayToken),
          rawToken,
        } satisfies RelayTokenWithSecret;
      }),
    list: () =>
      database.transaction(async (tx) =>
        (
          await tx.query<StoredTokenRow>(
            "SELECT * FROM omniroute.relay_tokens ORDER BY created_at DESC,insertion_order"
          )
        ).rows.map(token)
      ),
    get: (id: string) =>
      database.transaction(async (tx) => {
        const row = (
          await tx.query<StoredTokenRow>("SELECT * FROM omniroute.relay_tokens WHERE id=$1", [id])
        ).rows[0];
        return row ? token(row) : null;
      }),
    byHash: (hash: string) =>
      database.transaction(async (tx) => {
        const row = (
          await tx.query<StoredTokenRow>(
            "SELECT * FROM omniroute.relay_tokens WHERE token_hash=$1 AND enabled=1",
            [hash]
          )
        ).rows[0];
        return row ? token(row) : null;
      }),
    update: (id: string, updates: Partial<CreateRelayTokenInput>) =>
      database.transaction(async (tx) => {
        const columns: Record<string, string> = {
          name: "name",
          description: "description",
          comboId: "combo_id",
          allowedModels: "allowed_models",
          maxTokensPerRequest: "max_tokens_per_request",
          maxRequestsPerMinute: "max_requests_per_minute",
          maxRequestsPerDay: "max_requests_per_day",
          maxCostPerDay: "max_cost_per_day",
        };
        const values: unknown[] = [id, Math.floor(Date.now() / 1000)];
        const sets = ["updated_at=$2"];
        for (const [key, column] of Object.entries(columns)) {
          const value = updates[key as keyof CreateRelayTokenInput];
          if (value === undefined) continue;
          values.push(key === "allowedModels" ? JSON.stringify(value) : value);
          sets.push(`${column}=$${values.length}`);
        }
        const row = (
          await tx.query<StoredTokenRow>(
            `UPDATE omniroute.relay_tokens SET ${sets.join(",")} WHERE id=$1 RETURNING *`,
            values
          )
        ).rows[0];
        return row ? token(row) : null;
      }),
    delete: (id: string) =>
      database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.relay_logs WHERE token_id=$1", [id]);
        await tx.query("DELETE FROM omniroute.relay_rate_limits WHERE token_id=$1", [id]);
        await tx.query("DELETE FROM omniroute.relay_tokens WHERE id=$1", [id]);
      }),
    toggle: (id: string, enabled: boolean) =>
      database.transaction(async (tx) => {
        const row = (
          await tx.query<StoredTokenRow>(
            "UPDATE omniroute.relay_tokens SET enabled=$2,updated_at=$3 WHERE id=$1 RETURNING *",
            [id, enabled ? 1 : 0, Math.floor(Date.now() / 1000)]
          )
        ).rows[0];
        return row ? token(row) : null;
      }),
    check: (id: string, existing?: RelayToken) =>
      database.transaction(async (tx) => {
        let current = existing;
        if (!current) {
          const row = (
            await tx.query<StoredTokenRow>("SELECT * FROM omniroute.relay_tokens WHERE id=$1", [id])
          ).rows[0];
          if (!row) return { allowed: false, remaining: 0, resetIn: 0 };
          current = token(row);
        }
        const now = Math.floor(Date.now() / 1000),
          minute = Math.floor(now / 60) * 60,
          day = Math.floor(now / 86400) * 86400;
        const minuteCount = Number(
          (
            await tx.query<{ request_count: number }>(
              "SELECT request_count FROM omniroute.relay_rate_limits WHERE token_id=$1 AND window_start=$2",
              [id, minute]
            )
          ).rows[0]?.request_count || 0
        );
        if (minuteCount >= current.maxRequestsPerMinute)
          return { allowed: false, remaining: 0, resetIn: 60 - (now % 60) };
        const dayCount = Number(
          (
            await tx.query<{ total: string | null }>(
              "SELECT sum(request_count) AS total FROM omniroute.relay_rate_limits WHERE token_id=$1 AND window_start >= $2",
              [id, day]
            )
          ).rows[0]?.total || 0
        );
        if (dayCount >= current.maxRequestsPerDay)
          return {
            allowed: false,
            remaining: 0,
            resetIn: 86400 - (now % 86400),
          };
        return {
          allowed: true,
          remaining: Math.min(
            current.maxRequestsPerMinute - minuteCount,
            current.maxRequestsPerDay - dayCount
          ),
          resetIn: 60 - (now % 60),
        };
      }),
    record: (id: string, params: Usage) =>
      database.transaction(async (tx) => {
        const now = Math.floor(Date.now() / 1000),
          minute = Math.floor(now / 60) * 60;
        await tx.query(
          "INSERT INTO omniroute.relay_rate_limits(token_id,window_start,request_count,cost) VALUES($1,$2,1,$3) ON CONFLICT(token_id,window_start) DO UPDATE SET request_count=omniroute.relay_rate_limits.request_count+1,cost=omniroute.relay_rate_limits.cost+excluded.cost",
          [id, minute, params.cost || 0]
        );
        await tx.query("UPDATE omniroute.relay_tokens SET last_used_at=$2 WHERE id=$1", [id, now]);
        await tx.query(
          "INSERT INTO omniroute.relay_logs(token_id,request_id,model,prompt_tokens,completion_tokens,cost,status,status_code,latency_ms,client_ip,user_agent,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
          [
            id,
            params.requestId || null,
            params.model || null,
            params.promptTokens || 0,
            params.completionTokens || 0,
            params.cost || 0,
            params.status || "success",
            params.statusCode || 200,
            params.latencyMs || 0,
            params.clientIp || null,
            params.userAgent || null,
            now,
          ]
        );
      }),
    usage: (id: string, since: number) =>
      database.transaction(async (tx) => {
        const row = (
          await tx.query<{ request_count: string; total_cost: number }>(
            "SELECT count(*) AS request_count,COALESCE(sum(cost),0) AS total_cost FROM omniroute.relay_logs WHERE token_id=$1 AND created_at >= $2",
            [id, since]
          )
        ).rows[0];
        return {
          requestCount: Number(row.request_count),
          totalCost: Number(row.total_cost),
        };
      }),
    logs: (id?: string, limit = 50) =>
      database.transaction(async (tx) =>
        (
          await tx.query<StoredLogRow>(
            `SELECT * FROM omniroute.relay_logs ${id ? "WHERE token_id=$1" : ""} ORDER BY created_at DESC,id DESC LIMIT CASE WHEN $${id ? 2 : 1}::bigint < 0 THEN NULL ELSE $${id ? 2 : 1}::bigint END`,
            id ? [id, limit] : [limit]
          )
        ).rows.map((row) => ({ ...row, id: Number(row.id), created_at: Number(row.created_at) }))
      ),
  };
}
