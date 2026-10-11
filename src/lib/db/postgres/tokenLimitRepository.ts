import { randomUUID } from "node:crypto";
import {
  resetWindowIfElapsed,
  TokenLimitConflictError,
  TokenLimitNotFoundError,
} from "../tokenLimits/persistenceSemantics";
import type { TokenLimit, UpsertTokenLimitInput } from "../tokenLimits";
import type { PostgresDatabase, PostgresTransaction } from "./database";

function tokenCount(value: unknown): number {
  const count = Number(value);
  if (!Number.isSafeInteger(count) || count < 0)
    throw new RangeError("Token count exceeds the supported integer range");
  return count;
}

function fromRow(row: Record<string, unknown>): TokenLimit {
  return {
    id: String(row.id),
    apiKeyId: String(row.api_key_id),
    scopeType: row.scope_type as TokenLimit["scopeType"],
    scopeValue: String(row.scope_value),
    tokenLimit: tokenCount(row.token_limit),
    resetInterval: row.reset_interval as TokenLimit["resetInterval"],
    resetTime: String(row.reset_time || "00:00"),
    enabled: Number(row.enabled) !== 0,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

export class PostgresTokenLimitRepository {
  constructor(private readonly database: PostgresDatabase) {}

  private async locked<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    return this.database.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(18765,9)");
      return body(tx);
    });
  }

  async upsert(input: UpsertTokenLimitInput): Promise<TokenLimit> {
    const scope = ["model", "provider", "global"].includes(input.scopeType)
      ? input.scopeType
      : "global";
    const value = scope === "global" ? "" : (input.scopeValue || "").trim();
    const interval = ["daily", "weekly", "monthly"].includes(input.resetInterval || "")
      ? input.resetInterval!
      : "monthly";
    const now = new Date().toISOString();
    return this.locked(async (tx) => {
      const id = input.id?.trim();
      if (id) {
        const clash = (
          await tx.query(
            `SELECT id FROM omniroute.api_key_token_limits
          WHERE api_key_id=$1 AND scope_type=$2 AND scope_value=$3 AND reset_interval=$4 AND id<>$5`,
            [input.apiKeyId, scope, value, interval, id]
          )
        ).rows[0];
        if (clash) throw new TokenLimitConflictError(scope, interval);
        const written = (
          await tx.query(
            `UPDATE omniroute.api_key_token_limits
          SET scope_type=$2,scope_value=$3,token_limit=$4,reset_interval=$5,reset_time=$6,enabled=$7,updated_at=$8
          WHERE id=$1 AND api_key_id=$9 RETURNING *`,
            [
              id,
              scope,
              value,
              Math.floor(Number(input.tokenLimit) || 0),
              interval,
              input.resetTime || "00:00",
              input.enabled === false ? 0 : 1,
              now,
              input.apiKeyId,
            ]
          )
        ).rows[0];
        if (!written) throw new TokenLimitNotFoundError();
        return fromRow(written);
      }
      const written = (
        await tx.query(
          `INSERT INTO omniroute.api_key_token_limits
        (id,api_key_id,scope_type,scope_value,token_limit,reset_interval,reset_time,enabled,created_at,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
        ON CONFLICT(api_key_id,scope_type,scope_value,reset_interval) DO UPDATE SET
          token_limit=excluded.token_limit,reset_time=excluded.reset_time,enabled=excluded.enabled,
          updated_at=excluded.updated_at RETURNING *`,
          [
            randomUUID(),
            input.apiKeyId,
            scope,
            value,
            Math.floor(Number(input.tokenLimit) || 0),
            interval,
            input.resetTime || "00:00",
            input.enabled === false ? 0 : 1,
            now,
          ]
        )
      ).rows[0];
      return fromRow(written);
    });
  }

  async list(apiKeyId: string): Promise<TokenLimit[]> {
    return this.database.transaction(async (tx) =>
      (
        await tx.query(
          `SELECT * FROM omniroute.api_key_token_limits
      WHERE api_key_id=$1 ORDER BY CASE scope_type WHEN 'model' THEN 0 WHEN 'provider' THEN 1 ELSE 2 END,
      scope_value,id`,
          [apiKeyId]
        )
      ).rows.map(fromRow)
    );
  }

  async forRequest(apiKeyId: string, provider: string, model: string): Promise<TokenLimit[]> {
    return this.database.transaction((tx) => this.matching(tx, apiKeyId, provider, model));
  }

  private async matching(tx: PostgresTransaction, id: string, provider: string, model: string) {
    return (
      await tx.query(
        `SELECT * FROM omniroute.api_key_token_limits WHERE api_key_id=$1 AND enabled=1
      AND (scope_type='global' OR (scope_type='model' AND scope_value=$2)
      OR (scope_type='provider' AND scope_value=$3)) ORDER BY id`,
        [id, model || "", provider || ""]
      )
    ).rows.map(fromRow);
  }

  async delete(id: string): Promise<boolean> {
    return this.locked(
      async (tx) =>
        (await tx.query("DELETE FROM omniroute.api_key_token_limits WHERE id=$1", [id])).rowCount! >
        0
    );
  }

  async readUsage(limit: TokenLimit, now = Date.now()): Promise<number> {
    const { windowStart } = resetWindowIfElapsed(limit, now);
    return this.database.transaction(async (tx) =>
      tokenCount(
        (
          await tx.query(
            "SELECT tokens_used FROM omniroute.api_key_token_counters WHERE limit_id=$1 AND window_start=$2",
            [limit.id, windowStart]
          )
        ).rows[0]?.tokens_used || 0
      )
    );
  }

  async increment(limitId: string, windowStart: string, tokens: number): Promise<number> {
    const delta = Number.isFinite(tokens) ? tokenCount(Math.max(0, Math.floor(tokens))) : 0;
    return this.locked((tx) => this.add(tx, limitId, windowStart, delta));
  }

  private async add(
    tx: PostgresTransaction,
    id: string,
    window: string,
    delta: number
  ): Promise<number> {
    const row = (
      await tx.query(
        `INSERT INTO omniroute.api_key_token_counters
      (limit_id,window_start,tokens_used,updated_at) VALUES($1,$2,$3,$4)
      ON CONFLICT(limit_id,window_start) DO UPDATE SET
        tokens_used=api_key_token_counters.tokens_used+excluded.tokens_used,
        updated_at=excluded.updated_at RETURNING tokens_used`,
        [id, window, delta, new Date().toISOString()]
      )
    ).rows[0];
    return tokenCount(row.tokens_used);
  }

  async logReset(id: string, prev: number, window: string) {
    await this.locked((tx) => this.appendReset(tx, id, prev, window));
  }

  private async appendReset(tx: PostgresTransaction, id: string, prev: number, window: string) {
    await tx.query(
      `INSERT INTO omniroute.api_key_token_limit_reset_logs
      (limit_id,reset_at,prev_tokens,window_start) VALUES($1,$2,$3,$4)`,
      [id, new Date().toISOString(), Math.max(0, Math.floor(prev)), window]
    );
  }

  async seedFromHistory(limit: TokenLimit, now = Date.now()): Promise<number> {
    return this.database.transaction((tx) => this.historyTotal(tx, limit, now));
  }

  private async historyTotal(
    tx: PostgresTransaction,
    limit: TokenLimit,
    now: number
  ): Promise<number> {
    const params: unknown[] = [
      limit.apiKeyId,
      new Date(resetWindowIfElapsed(limit, now).periodStartAt).toISOString(),
    ];
    const scope =
      limit.scopeType === "model"
        ? " AND model=$3"
        : limit.scopeType === "provider"
          ? " AND provider=$3"
          : "";
    if (scope) params.push(limit.scopeValue);
    const row = (
      await tx.query(
        `SELECT COALESCE(SUM(COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
      +COALESCE(tokens_reasoning,0)),0) AS total FROM omniroute.usage_history
      WHERE api_key_id=$1 AND timestamp >= $2${scope}`,
        params
      )
    ).rows[0];
    return tokenCount(row.total);
  }

  /** Row absence, not zero usage, decides seeding. Lock 9 makes cold seed/reset single-writer. */
  private async seedIfAbsent(
    tx: PostgresTransaction,
    limit: TokenLimit,
    now: number,
    excludeHistoryId?: number
  ): Promise<number> {
    const { windowStart, periodStartAt, nextResetAt } = resetWindowIfElapsed(limit, now);
    const row = (
      await tx.query(
        "SELECT tokens_used FROM omniroute.api_key_token_counters WHERE limit_id=$1 AND window_start=$2",
        [limit.id, windowStart]
      )
    ).rows[0];
    if (row) return tokenCount(row.tokens_used);
    const prior = (
      await tx.query(
        `SELECT tokens_used FROM omniroute.api_key_token_counters
      WHERE limit_id=$1 AND window_start < $2 ORDER BY window_start DESC LIMIT 1`,
        [limit.id, windowStart]
      )
    ).rows[0];
    if (Number(prior?.tokens_used) > 0)
      await this.appendReset(tx, limit.id, Number(prior.tokens_used), windowStart);
    const params: unknown[] = [
      limit.id,
      windowStart,
      new Date(periodStartAt).toISOString(),
      limit.apiKeyId,
      new Date().toISOString(),
      new Date(nextResetAt).toISOString(),
    ];
    const scope =
      limit.scopeType === "model"
        ? " AND model=$7"
        : limit.scopeType === "provider"
          ? " AND provider=$7"
          : "";
    if (scope) params.push(limit.scopeValue);
    let exclusion = "";
    if (excludeHistoryId !== undefined) {
      params.push(excludeHistoryId);
      exclusion = ` AND id<>$${params.length}`;
    }
    // Both SUM and receipt marks consume this one statement snapshot. Another
    // history writer cannot be marked without also contributing to the seed.
    const seeded = (
      await tx.query(
        `WITH history AS MATERIALIZED (
        SELECT id,COALESCE(tokens_input,0)+COALESCE(tokens_output,0)+COALESCE(tokens_reasoning,0) AS tokens
        FROM omniroute.usage_history WHERE api_key_id=$4 AND timestamp >= $3 AND timestamp < $6${scope}${exclusion}
      ), seed AS (
        INSERT INTO omniroute.api_key_token_counters(limit_id,window_start,tokens_used,updated_at)
        SELECT $1,$2,GREATEST(0,COALESCE(SUM(tokens),0)),$5 FROM history RETURNING tokens_used
      ), receipts AS (
        INSERT INTO omniroute.api_key_token_usage_receipts(limit_id,usage_history_id,window_start)
        SELECT $1,id,$2 FROM history ON CONFLICT(limit_id,usage_history_id) DO NOTHING
      ) SELECT tokens_used FROM seed`,
        params
      )
    ).rows[0];
    return tokenCount(seeded.tokens_used);
  }

  async currentUsage(limit: TokenLimit, now = Date.now()): Promise<number> {
    return this.locked(async (tx) => {
      // A deleted limit must not be resurrected by a stale cached caller.
      const active = (
        await tx.query("SELECT * FROM omniroute.api_key_token_limits WHERE id=$1", [limit.id])
      ).rows[0];
      if (!active) throw new TokenLimitNotFoundError();
      return this.seedIfAbsent(tx, fromRow(active), now);
    });
  }

  async readRequestUsage(
    id: string,
    provider: string,
    model: string,
    now = Date.now()
  ): Promise<Array<{ limit: TokenLimit; tokensUsed: number }>> {
    return this.locked(async (tx) => {
      const result: Array<{ limit: TokenLimit; tokensUsed: number }> = [];
      for (const limit of await this.matching(tx, id, provider, model))
        result.push({ limit, tokensUsed: await this.seedIfAbsent(tx, limit, now) });
      return result;
    });
  }

  /** All matching counters/reset logs commit together; no operation is replayed after uncertain COMMIT. */
  async recordUsage(
    id: string,
    provider: string,
    model: string,
    tokens: number,
    now = Date.now(),
    usageHistoryId?: number
  ) {
    const delta = Number.isFinite(tokens) ? tokenCount(Math.max(0, Math.floor(tokens))) : 0;
    if (!id || !delta) return;
    await this.locked(async (tx) => {
      const limits = await this.matching(tx, id, provider, model);
      for (const limit of limits) {
        if (usageHistoryId !== undefined) {
          if (!Number.isSafeInteger(usageHistoryId) || usageHistoryId <= 0)
            throw new TypeError("Invalid usage history receipt");
          const already = (
            await tx.query(
              "SELECT 1 FROM omniroute.api_key_token_usage_receipts WHERE limit_id=$1 AND usage_history_id=$2",
              [limit.id, usageHistoryId]
            )
          ).rows[0];
          if (already) continue;
          const history = (
            await tx.query(
              "SELECT timestamp FROM omniroute.usage_history WHERE id=$1 AND api_key_id=$2",
              [usageHistoryId, id]
            )
          ).rows[0];
          const occurredAt = history ? Date.parse(String(history.timestamp)) : Number.NaN;
          if (!Number.isFinite(occurredAt))
            throw new Error("Usage history receipt no longer available");
          // Late accounting belongs to the persisted event's window, not the
          // clock when the callback eventually runs.
          now = occurredAt;
        }
        await this.seedIfAbsent(tx, limit, now, usageHistoryId);
        const { windowStart } = resetWindowIfElapsed(limit, now);
        await this.add(tx, limit.id, windowStart, delta);
        if (usageHistoryId !== undefined)
          await tx.query(
            `INSERT INTO omniroute.api_key_token_usage_receipts
          (limit_id,usage_history_id,window_start) VALUES($1,$2,$3)`,
            [limit.id, usageHistoryId, windowStart]
          );
      }
    });
  }
}
