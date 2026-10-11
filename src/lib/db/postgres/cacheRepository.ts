import { randomUUID } from "node:crypto";
import type { ReasoningCacheEntry, ReasoningCacheStats } from "../reasoningCache";
import type { SemanticCacheListOptions, SemanticCacheListResult } from "../semanticCache";
import type { PostgresDatabase, PostgresTransaction } from "./database";

const CACHE_TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"';
const CACHE_CLOCK = "date_trunc('milliseconds', statement_timestamp())";
const CACHE_CREATED_AT = `to_char(${CACHE_CLOCK} AT TIME ZONE 'UTC', '${CACHE_TIMESTAMP_FORMAT}')`;

export const POSTGRES_CACHE_SCHEMA_STATEMENTS = [
  `CREATE TABLE omniroute.semantic_cache (id text PRIMARY KEY, signature text NOT NULL UNIQUE,
    model text NOT NULL, prompt_hash text, response text NOT NULL, tokens_saved bigint NOT NULL DEFAULT 0,
    hit_count bigint NOT NULL DEFAULT 0, created_at text NOT NULL, expires_at text NOT NULL)`,
  `CREATE INDEX semantic_cache_expiry ON omniroute.semantic_cache(expires_at)`,
  `CREATE TABLE omniroute.cache_metrics (key text PRIMARY KEY, value bigint NOT NULL DEFAULT 0, updated_at text NOT NULL)`,
  `INSERT INTO omniroute.cache_metrics(key,value,updated_at) VALUES('hits',0,''),('misses',0,''),('tokens_saved',0,'')`,
  `CREATE TABLE omniroute.reasoning_cache (tool_call_id text PRIMARY KEY, provider text NOT NULL,
    model text NOT NULL, reasoning text NOT NULL, char_count integer NOT NULL DEFAULT 0,
    created_at text NOT NULL, expires_at bigint NOT NULL)`,
  `CREATE INDEX reasoning_cache_expiry ON omniroute.reasoning_cache(expires_at)`,
] as const;

async function metric(tx: PostgresTransaction, key: string, amount: number): Promise<void> {
  await tx.query(
    "UPDATE omniroute.cache_metrics SET value = value + $1, updated_at = $2 WHERE key = $3",
    [amount, new Date().toISOString(), key]
  );
}

export function createPostgresSemanticCacheRepository(database: PostgresDatabase) {
  return {
    get: (signature: string) =>
      database.transaction(async (tx) => {
        const { rows } = await tx.query<{ response: string; tokens_saved: string }>(
          `SELECT response,tokens_saved FROM omniroute.semantic_cache
         WHERE signature = $1 AND expires_at::timestamptz > statement_timestamp() FOR UPDATE`,
          [signature]
        );
        if (!rows[0]) {
          await metric(tx, "misses", 1);
          return null;
        }
        let response: unknown;
        try {
          response = JSON.parse(rows[0].response);
        } catch {
          await metric(tx, "misses", 1);
          return null;
        }
        await tx.query(
          "UPDATE omniroute.semantic_cache SET hit_count=hit_count+1 WHERE signature=$1",
          [signature]
        );
        await metric(tx, "hits", 1);
        await metric(tx, "tokens_saved", Number(rows[0].tokens_saved));
        return response;
      }),
    set: (
      signature: string,
      model: string,
      response: unknown,
      tokensSaved: number,
      ttlMs: number
    ) =>
      database.transaction(async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.semantic_cache
          (id,signature,model,prompt_hash,response,tokens_saved,hit_count,created_at,expires_at)
          VALUES($1,$2,$3,$4,$5,$6,0,${CACHE_CREATED_AT},
            to_char((${CACHE_CLOCK} + $7::double precision * interval '1 millisecond')
              AT TIME ZONE 'UTC', '${CACHE_TIMESTAMP_FORMAT}'))
          ON CONFLICT(signature) DO UPDATE SET
          id=EXCLUDED.id, model=EXCLUDED.model, prompt_hash=EXCLUDED.prompt_hash,
          response=EXCLUDED.response, tokens_saved=EXCLUDED.tokens_saved, hit_count=0,
          created_at=EXCLUDED.created_at, expires_at=EXCLUDED.expires_at`,
          [
            randomUUID(),
            signature,
            model,
            signature.slice(0, 16),
            JSON.stringify(response),
            tokensSaved,
            ttlMs,
          ]
        );
      }),
    hit: (signature: string, tokensSaved: number) =>
      database.transaction(async (tx) => {
        if (signature)
          await tx.query(
            `UPDATE omniroute.semantic_cache SET hit_count = hit_count + 1
        WHERE signature = $1 OR prompt_hash = $2`,
            [signature, signature.slice(0, 16)]
          );
        await metric(tx, "hits", 1);
        if (tokensSaved > 0) await metric(tx, "tokens_saved", tokensSaved);
      }),
    deleteBySignature: (signature: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("DELETE FROM omniroute.semantic_cache WHERE signature = $1", [signature]))
            .rowCount ?? 0
      ),
    deleteByModel: (model: string) =>
      database.transaction(
        async (tx) =>
          (await tx.query("DELETE FROM omniroute.semantic_cache WHERE model = $1", [model]))
            .rowCount ?? 0
      ),
    deleteOlderThan: (cutoff: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              "DELETE FROM omniroute.semantic_cache WHERE created_at::timestamptz < $1::timestamptz",
              [cutoff]
            )
          ).rowCount ?? 0
      ),
    clear: () =>
      database.transaction(async (tx) => {
        const count = (await tx.query("DELETE FROM omniroute.semantic_cache")).rowCount ?? 0;
        await tx.query("UPDATE omniroute.cache_metrics SET value = 0");
        return count;
      }),
    stats: () =>
      database.transaction(async (tx) => {
        const count = await tx.query<{ count: string }>(
          "SELECT count(*) FROM omniroute.semantic_cache WHERE expires_at::timestamptz > statement_timestamp()"
        );
        const metrics = await tx.query<{ key: string; value: string }>(
          "SELECT key,value FROM omniroute.cache_metrics"
        );
        const values = Object.fromEntries(metrics.rows.map((r) => [r.key, Number(r.value)]));
        const hits = values.hits ?? 0,
          misses = values.misses ?? 0;
        return {
          memoryEntries: 0,
          dbEntries: Number(count.rows[0].count),
          hits,
          misses,
          hitRate: hits + misses > 0 ? ((hits / (hits + misses)) * 100).toFixed(1) : "0.0",
          tokensSaved: values.tokens_saved ?? 0,
        };
      }),
    list: (options: SemanticCacheListOptions): Promise<SemanticCacheListResult> =>
      database.transaction(async (tx) => {
        const values: unknown[] = [];
        const clauses: string[] = [];
        if (options.search) {
          values.push(`%${options.search}%`);
          clauses.push(`(signature ILIKE $${values.length} OR model ILIKE $${values.length})`);
        }
        if (options.model) {
          values.push(options.model);
          clauses.push(`model = $${values.length}`);
        }
        const where = clauses.length ? ` WHERE ${clauses.join(" AND ")}` : "";
        const allowed = ["created_at", "expires_at", "hit_count", "tokens_saved", "model"];
        const column = allowed.includes(options.sortBy) ? options.sortBy : "created_at";
        const direction = options.sortOrder === "asc" ? "ASC" : "DESC";
        // Stable tie-breakers preserve pagination when sort values are equal.
        values.push(options.limit, (options.page - 1) * options.limit);
        const result = await tx.query<{
          total: string;
          entries: SemanticCacheListResult["entries"];
        }>(
          `
        WITH filtered AS (SELECT id,signature,model,hit_count,tokens_saved,created_at,expires_at
          FROM omniroute.semantic_cache${where}),
        page AS (SELECT * FROM filtered ORDER BY ${column} ${direction},id ASC
          LIMIT $${values.length - 1} OFFSET $${values.length})
        SELECT (SELECT count(*) FROM filtered) AS total,
          coalesce((SELECT json_agg(page ORDER BY ${column} ${direction},id ASC) FROM page),'[]'::json) AS entries`,
          values
        );
        return { total: Number(result.rows[0].total), entries: result.rows[0].entries };
      }),
  };
}

export function createPostgresReasoningCacheRepository(database: PostgresDatabase) {
  const active = "expires_at > floor(extract(epoch FROM statement_timestamp()))::bigint";
  return {
    set: (
      toolCallId: string,
      provider: string,
      model: string,
      reasoning: string,
      ttlMs = 7200000
    ) =>
      database.transaction(async (tx) => {
        if (!reasoning || reasoning.trim() === "(prior reasoning summary unavailable)") return;
        const content = reasoning.slice(0, 10000);
        await tx.query(
          `INSERT INTO omniroute.reasoning_cache(tool_call_id,provider,model,reasoning,char_count,created_at,expires_at)
        VALUES($1,$2,$3,$4,$5,${CACHE_CREATED_AT},
          floor(extract(epoch FROM ${CACHE_CLOCK}) + $6::numeric / 1000)::bigint)
        ON CONFLICT(tool_call_id) DO UPDATE SET provider=EXCLUDED.provider,
        model=EXCLUDED.model,reasoning=EXCLUDED.reasoning,char_count=EXCLUDED.char_count,
        created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at`,
          [toolCallId, provider, model, content, content.length, ttlMs]
        );
      }),
    get: (toolCallId: string) =>
      database.transaction(async (tx) => {
        const { rows } = await tx.query<{
          reasoning: string;
          provider: string;
          model: string;
          expires_at: string;
        }>(
          `SELECT reasoning,provider,model,expires_at FROM omniroute.reasoning_cache WHERE tool_call_id=$1 AND ${active}`,
          [toolCallId]
        );
        const row = rows[0];
        return row && row.reasoning.trim() !== "(prior reasoning summary unavailable)"
          ? {
              reasoning: row.reasoning,
              provider: row.provider,
              model: row.model,
              expiresAt: new Date(Number(row.expires_at) * 1000).toISOString(),
            }
          : null;
      }),
    getMany: (keys: string[]) =>
      database.transaction(async (tx) => {
        const { rows } = await tx.query<{
          tool_call_id: string;
          reasoning: string;
          provider: string;
          model: string;
          expires_at: string;
        }>(
          `SELECT tool_call_id,reasoning,provider,model,expires_at FROM omniroute.reasoning_cache
         WHERE tool_call_id=ANY($1::text[]) AND ${active}`,
          [keys]
        );
        return rows
          .filter((r) => r.reasoning.trim() !== "(prior reasoning summary unavailable)")
          .map((r) => ({
            key: r.tool_call_id,
            reasoning: r.reasoning,
            provider: r.provider,
            model: r.model,
            expiresAt: new Date(Number(r.expires_at) * 1000).toISOString(),
          }));
      }),
    delete: (toolCallId: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query("DELETE FROM omniroute.reasoning_cache WHERE tool_call_id=$1", [
              toolCallId,
            ])
          ).rowCount ?? 0
      ),
    cleanup: () =>
      database.transaction(
        async (tx) =>
          (await tx.query(`DELETE FROM omniroute.reasoning_cache WHERE NOT(${active})`)).rowCount ??
          0
      ),
    clear: (provider?: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query(
              `DELETE FROM omniroute.reasoning_cache${provider ? " WHERE provider=$1" : ""}`,
              provider ? [provider] : []
            )
          ).rowCount ?? 0
      ),
    stats: (): Promise<ReasoningCacheStats> =>
      database.transaction(async (tx) => {
        const totals = await tx.query<{
          total_entries: string;
          total_chars: string;
          oldest: string | null;
          newest: string | null;
        }>(
          `SELECT count(*) AS total_entries,coalesce(sum(char_count),0) AS total_chars,
         min(created_at) AS oldest,max(created_at) AS newest FROM omniroute.reasoning_cache WHERE ${active}`
        );
        const provider = await tx.query<{
          name: string;
          entries: string;
          chars: string;
        }>(`SELECT provider AS name,count(*) AS entries,sum(char_count) AS chars
        FROM omniroute.reasoning_cache WHERE ${active} GROUP BY provider`);
        const model = await tx.query<{
          name: string;
          entries: string;
          chars: string;
        }>(`SELECT model AS name,count(*) AS entries,sum(char_count) AS chars
        FROM omniroute.reasoning_cache WHERE ${active} GROUP BY model`);
        const map = (rows: { name: string; entries: string; chars: string }[]) =>
          Object.fromEntries(
            rows.map((r) => [r.name, { entries: Number(r.entries), chars: Number(r.chars) }])
          );
        const row = totals.rows[0];
        return {
          totalEntries: Number(row.total_entries),
          totalChars: Number(row.total_chars),
          byProvider: map(provider.rows),
          byModel: map(model.rows),
          oldestEntry: row.oldest,
          newestEntry: row.newest,
        };
      }),
    list: (
      options: { limit?: number; offset?: number; provider?: string; model?: string } = {}
    ): Promise<ReasoningCacheEntry[]> =>
      database.transaction(async (tx) => {
        const clauses = [active];
        const values: unknown[] = [];
        for (const key of ["provider", "model"] as const)
          if (options[key]) {
            values.push(options[key]);
            clauses.push(`${key}=$${values.length}`);
          }
        values.push(Math.min(options.limit ?? 50, 200), options.offset ?? 0);
        const { rows } = await tx.query<{
          tool_call_id: string;
          provider: string;
          model: string;
          reasoning: string;
          char_count: number;
          created_at: string;
          expires_at: string;
        }>(
          `SELECT * FROM omniroute.reasoning_cache WHERE ${clauses.join(" AND ")}
         ORDER BY created_at DESC, tool_call_id ASC LIMIT $${values.length - 1} OFFSET $${values.length}`,
          values
        );
        return rows.map((r) => ({
          toolCallId: r.tool_call_id,
          provider: r.provider,
          model: r.model,
          reasoning: r.reasoning,
          charCount: r.char_count,
          createdAt: r.created_at,
          expiresAt: new Date(Number(r.expires_at) * 1000).toISOString(),
        }));
      }),
  };
}
