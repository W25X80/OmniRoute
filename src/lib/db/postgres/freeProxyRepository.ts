import { randomUUID } from "node:crypto";
import type { FreeProxyItem, FreeProxySourceId } from "@/lib/freeProxyProviders/types";
import type { PostgresDatabase } from "./database";
import { lockProxyState } from "./proxyRepository";
import { asciiFoldSql } from "./routingSemantics";
import { bumpProxyRegistryGeneration } from "../proxies/registryGeneration";
import type { FreeProxyRecord, FreeProxyStats, FreeProxySyncErrors } from "../freeProxies";
type Row = Record<string, unknown>;
export function mapFreeProxyRow(r: Row): FreeProxyRecord {
  return {
    id: String(r.id ?? ""),
    source: String(r.source ?? "1proxy") as FreeProxySourceId,
    host: String(r.host ?? ""),
    port: Number(r.port) || 0,
    type: String(r.type ?? "http"),
    countryCode: r.country_code != null ? String(r.country_code) : null,
    qualityScore: r.quality_score != null ? Number(r.quality_score) : null,
    latencyMs: r.latency_ms != null ? Number(r.latency_ms) : null,
    anonymity: r.anonymity != null ? String(r.anonymity) : null,
    lastValidated: r.last_validated != null ? String(r.last_validated) : null,
    inPool: r.in_pool === 1 || r.in_pool === true,
    poolProxyId: r.pool_proxy_id != null ? String(r.pool_proxy_id) : null,
    createdAt: String(r.created_at ?? ""),
    updatedAt: String(r.updated_at ?? ""),
  };
}
interface Filters {
  sources?: FreeProxySourceId[];
  protocol?: string;
  country?: string;
  minQuality?: number;
  onlyInPool?: boolean;
  onlyNotInPool?: boolean;
  search?: string;
  sortBy?: "quality" | "latency" | "recent";
  limit?: number;
  offset?: number;
}
function where(options: Filters = {}) {
  const clauses: string[] = [];
  const values: unknown[] = [];
  const add = (column: string, value: unknown) => {
    values.push(value);
    clauses.push(`${column}=$${values.length}`);
  };
  if (options.sources?.length) {
    values.push(options.sources);
    clauses.push(`source=ANY($${values.length}::text[])`);
  }
  if (options.protocol) add("type", options.protocol);
  if (options.country) add("country_code", options.country.toUpperCase());
  if (options.minQuality != null) {
    values.push(options.minQuality);
    clauses.push(`quality_score >= $${values.length}`);
  }
  if (options.onlyInPool) clauses.push("in_pool=1");
  if (options.onlyNotInPool) clauses.push("in_pool=0");
  if (options.search) {
    values.push(`%${options.search}%`);
    clauses.push(
      `${asciiFoldSql("host")} LIKE ${asciiFoldSql(`$${values.length}::text`)} ESCAPE ''`
    );
  }
  return {
    sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    values,
  };
}
export function createPostgresFreeProxyRepository(database: PostgresDatabase) {
  return {
    upsert: (item: FreeProxyItem) =>
      database.transaction(async (tx) => {
        await lockProxyState(tx);
        const now = new Date().toISOString();
        const existing = (
          await tx.query<{ id: string }>(
            "SELECT id FROM omniroute.free_proxies WHERE source=$1 AND host=$2 AND port=$3",
            [item.source, item.host, item.port]
          )
        ).rows[0];
        const values = [
          item.type,
          item.countryCode ?? null,
          item.qualityScore ?? null,
          item.latencyMs ?? null,
          item.anonymity ?? null,
          item.lastValidated ?? now,
          now,
        ];
        if (existing) {
          await tx.query(
            "UPDATE omniroute.free_proxies SET type=$2,country_code=$3,quality_score=$4,latency_ms=$5,anonymity=$6,last_validated=$7,updated_at=$8 WHERE id=$1",
            [existing.id, ...values]
          );
          return { id: existing.id, action: "updated" as const };
        }
        const id = randomUUID();
        await tx.query(
          "INSERT INTO omniroute.free_proxies(id,source,host,port,type,country_code,quality_score,latency_ms,anonymity,last_validated,in_pool,pool_proxy_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,NULL,$11,$11)",
          [id, item.source, item.host, item.port, ...values]
        );
        return { id, action: "created" as const };
      }),
    list: (options?: Filters) =>
      database.transaction(async (tx) => {
        const filter = where(options);
        let sql = `SELECT * FROM omniroute.free_proxies ${filter.sql} `;
        sql +=
          options?.sortBy === "latency"
            ? "ORDER BY latency_ms ASC NULLS LAST"
            : options?.sortBy === "recent"
              ? "ORDER BY last_validated DESC NULLS LAST"
              : "ORDER BY quality_score DESC NULLS LAST,last_validated DESC NULLS LAST";
        if (options?.limit) {
          filter.values.push(options.limit);
          sql += ` LIMIT CASE WHEN $${filter.values.length}::bigint < 0 THEN NULL ELSE $${filter.values.length}::bigint END`;
          if (options.offset) {
            filter.values.push(options.offset);
            sql += ` OFFSET GREATEST($${filter.values.length}::bigint,0)`;
          }
        }
        return (await tx.query(sql, filter.values)).rows.map(mapFreeProxyRow);
      }),
    count: (options?: Filters) =>
      database.transaction(async (tx) => {
        const f = where(options);
        return Number(
          (
            await tx.query<{ count: string }>(
              `SELECT count(*) AS count FROM omniroute.free_proxies ${f.sql}`,
              f.values
            )
          ).rows[0].count
        );
      }),
    get: (id: string) =>
      database.transaction(async (tx) => {
        const row = (await tx.query("SELECT * FROM omniroute.free_proxies WHERE id=$1", [id]))
          .rows[0];
        return row ? mapFreeProxyRow(row) : null;
      }),
    markInPool: (id: string, poolProxyId: string) =>
      database.transaction(async (tx) => {
        await tx.query(
          "UPDATE omniroute.free_proxies SET in_pool=1,pool_proxy_id=$2,updated_at=$3 WHERE id=$1",
          [id, poolProxyId, new Date().toISOString()]
        );
      }),
    promote: (
      id: string,
      payload: {
        name: string;
        type: string;
        host: string;
        port: number;
        source: string;
      }
    ) =>
      database
        .transaction(async (tx) => {
          await lockProxyState(tx);
          if (
            !(await tx.query("SELECT id FROM omniroute.free_proxies WHERE id=$1 FOR UPDATE", [id]))
              .rows[0]
          )
            return null;
          const proxyId = randomUUID(),
            now = new Date().toISOString();
          await tx.query(
            "INSERT INTO omniroute.proxy_registry(id,name,type,host,port,username,password,status,source,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'','','active',$6,$7,$7)",
            [
              proxyId,
              payload.name,
              payload.type,
              payload.host,
              Number(payload.port),
              payload.source,
              now,
            ]
          );
          await tx.query(
            "UPDATE omniroute.free_proxies SET in_pool=1,pool_proxy_id=$2,updated_at=$3 WHERE id=$1",
            [id, proxyId, now]
          );
          return proxyId;
        })
        .then((result) => {
          if (result) bumpProxyRegistryGeneration();
          return result;
        }),
    delete: (id: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("DELETE FROM omniroute.free_proxies WHERE id=$1", [id])).rowCount! > 0
      ),
    clearSource: (source: FreeProxySourceId) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query("DELETE FROM omniroute.free_proxies WHERE source=$1 AND in_pool=0", [
              source,
            ])
          ).rowCount ?? 0
      ),
    prune: (source: FreeProxySourceId, active: ReadonlySet<string>) =>
      database.transaction(async (tx) => {
        await lockProxyState(tx);
        const rows = (
          await tx.query<{ id: string; host: string; port: number }>(
            "SELECT id,host,port FROM omniroute.free_proxies WHERE source=$1 AND in_pool=0",
            [source]
          )
        ).rows;
        const stale = rows.filter((r) => !active.has(`${r.host}:${r.port}`)).map((r) => r.id);
        if (!stale.length) return 0;
        return (
          (
            await tx.query(
              "DELETE FROM omniroute.free_proxies WHERE id=ANY($1::text[]) AND in_pool=0",
              [stale]
            )
          ).rowCount ?? 0
        );
      }),
    recordSync: (at?: string) =>
      database.transaction(async (tx) => {
        const value = at ?? new Date().toISOString();
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('free_proxies','last_sync_at',$1) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
          [value]
        );
        return value;
      }),
    stats: () =>
      database.transaction(async (tx) => {
        const totals = (
          await tx.query<Row>(
            "SELECT count(*) AS total,sum(CASE WHEN in_pool=1 THEN 1 ELSE 0 END) AS in_pool_count,avg(quality_score) AS avg_quality,max(last_validated) AS last_sync_at FROM omniroute.free_proxies"
          )
        ).rows[0];
        const source = (
          await tx.query<{ source: string; count: string }>(
            "SELECT source,count(*) AS count FROM omniroute.free_proxies GROUP BY source ORDER BY count DESC"
          )
        ).rows;
        const sync = (
          await tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='free_proxies' AND key='last_sync_at'"
          )
        ).rows[0];
        return {
          total: Number(totals.total) || 0,
          inPool: Number(totals.in_pool_count) || 0,
          avgQuality: totals.avg_quality != null ? Math.round(Number(totals.avg_quality)) : null,
          bySource: source.map((r) => ({
            source: r.source,
            count: Number(r.count),
          })),
          lastSyncAt:
            sync?.value ?? (totals.last_sync_at != null ? String(totals.last_sync_at) : null),
        } satisfies FreeProxyStats;
      }),
    recordErrors: (source: FreeProxySourceId, errors: string[]) =>
      database.transaction(async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.free_proxy_sync_errors(source,errors,updated_at) VALUES($1,$2,$3) ON CONFLICT(source) DO UPDATE SET errors=excluded.errors,updated_at=excluded.updated_at",
          [source, JSON.stringify(errors), new Date().toISOString()]
        );
      }),
    clearErrors: (source: FreeProxySourceId) =>
      database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.free_proxy_sync_errors WHERE source=$1", [source]);
      }),
    errors: () =>
      database.transaction(async (tx) => {
        const out: FreeProxySyncErrors = {};
        for (const row of (
          await tx.query<{ source: string; errors: string }>(
            "SELECT source,errors FROM omniroute.free_proxy_sync_errors"
          )
        ).rows) {
          if (!row.source) continue;
          try {
            const parsed: unknown = JSON.parse(row.errors);
            out[row.source] = Array.isArray(parsed) ? parsed.map(String) : [row.errors];
          } catch {
            out[row.source] = [row.errors];
          }
        }
        return out;
      }),
  };
}
