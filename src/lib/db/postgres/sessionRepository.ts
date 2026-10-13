import { createHash, randomBytes, randomUUID } from "node:crypto";
import type { HandoffPayload } from "../contextHandoffs";
import type { CcrBlockRow } from "../ccrBlocks";
import type { ConnectionRuntimeState } from "../connectionRuntimeState";
import type {
  AccessTokenRecord,
  RevokeAccessTokenResult,
  VerifiedAccessToken,
} from "../accessTokens";
import { normalizeScope, type AccessScope } from "../../accessTokens/scopes";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { databaseTimestamp } from "./leaseRepository";
import { acquirePostgresLock } from "./locks";

type Row = Record<string, unknown>;
export interface SessionAffinityRecord {
  connectionId: string;
  createdAt: string;
  lastUsedAt: string;
  expiresAt: string;
}
function key(session: string, provider: string): string {
  return `${provider}:${createHash("sha256").update(`${provider}:${session}`).digest("hex")}`;
}
function parseAffinity(value: unknown): SessionAffinityRecord | null {
  if (typeof value !== "string") return null;
  try {
    const parsed = JSON.parse(value) as Partial<SessionAffinityRecord>;
    if (
      typeof parsed.connectionId !== "string" ||
      !parsed.connectionId.trim() ||
      typeof parsed.expiresAt !== "string" ||
      !Number.isFinite(Date.parse(parsed.expiresAt))
    )
      return null;
    return {
      connectionId: parsed.connectionId,
      expiresAt: parsed.expiresAt,
      createdAt:
        typeof parsed.createdAt === "string" && Number.isFinite(Date.parse(parsed.createdAt))
          ? parsed.createdAt
          : parsed.expiresAt,
      lastUsedAt:
        typeof parsed.lastUsedAt === "string" && Number.isFinite(Date.parse(parsed.lastUsedAt))
          ? parsed.lastUsedAt
          : parsed.expiresAt,
    };
  } catch {
    return null;
  }
}
function positiveTtl(ttl: number | undefined): number {
  return Number.isFinite(ttl) && Number(ttl) > 0 ? Number(ttl) : 0;
}
async function affinityRow(transaction: PostgresTransaction, value: string) {
  const result = await transaction.query<{ value: string }>(
    `SELECT value FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=$1 FOR UPDATE`,
    [value]
  );
  return parseAffinity(result.rows[0]?.value);
}
function tokenRecord(row: Row): AccessTokenRecord {
  return {
    id: String(row.id),
    name: String(row.name),
    scope: normalizeScope(row.scope),
    tokenPrefix: String(row.token_prefix),
    createdAt: String(row.created_at),
    lastUsedAt: row.last_used_at == null ? null : String(row.last_used_at),
    expiresAt: row.expires_at == null ? null : String(row.expires_at),
    revokedAt: row.revoked_at == null ? null : String(row.revoked_at),
  };
}
function array(value: unknown): string[] {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    return Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
  } catch {
    return [];
  }
}
function handoff(row: Row | undefined): HandoffPayload | null {
  if (!row) return null;
  return {
    id: String(row.id),
    sessionId: String(row.session_id),
    comboName: String(row.combo_name),
    fromAccount: String(row.from_account),
    summary: String(row.summary),
    keyDecisions: array(row.key_decisions),
    taskProgress: String(row.task_progress),
    activeEntities: array(row.active_entities),
    messageCount: Number(row.message_count),
    model: String(row.model),
    lastModel: row.last_model == null ? undefined : String(row.last_model),
    warningThresholdPct: Number(row.warning_threshold_pct),
    generatedAt: String(row.generated_at),
    expiresAt: String(row.expires_at),
    createdAt: String(row.created_at),
  };
}
export function createPostgresSessionRepository(database: PostgresDatabase) {
  // Fences absent KV rows and compare/delete with concurrent replacement of the same pin.
  function affinityTransaction<T>(body: (transaction: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "sessions");
      return body(transaction);
    });
  }
  async function nowMs(transaction: PostgresTransaction, override?: number) {
    return override ?? Date.parse(await databaseTimestamp(transaction));
  }
  async function writeAffinity(
    transaction: PostgresTransaction,
    affinityKey: string,
    connectionId: string,
    ttl: number,
    now: number,
    old: SessionAffinityRecord | null
  ) {
    const timestamp = new Date(now).toISOString();
    const record: SessionAffinityRecord = {
      connectionId,
      createdAt: old && Date.parse(old.expiresAt) > now ? old.createdAt : timestamp,
      lastUsedAt: timestamp,
      expiresAt: new Date(now + ttl).toISOString(),
    };
    await transaction.query(
      `INSERT INTO omniroute.key_value(namespace,key,value) VALUES('session_account_affinity',$1,$2) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value`,
      [affinityKey, JSON.stringify(record)]
    );
  }
  return {
    getSessionAccountAffinity(session: string, provider: string, ttl = 0, now?: number) {
      if (!session || !provider || !positiveTtl(ttl)) return Promise.resolve(null);
      return affinityTransaction(async (transaction) => {
        const affinityKey = key(session, provider);
        const record = await affinityRow(transaction, affinityKey);
        if (!record) return null;
        if (Date.parse(record.expiresAt) <= (await nowMs(transaction, now))) {
          await transaction.query(
            `DELETE FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=$1`,
            [affinityKey]
          );
          return null;
        }
        return record;
      });
    },
    upsertSessionAccountAffinity(
      session: string,
      provider: string,
      connectionId: string,
      now?: number,
      ttl = 0
    ) {
      if (!session || !provider || !connectionId || !positiveTtl(ttl)) return Promise.resolve();
      return affinityTransaction(async (transaction) => {
        const affinityKey = key(session, provider);
        const old = await affinityRow(transaction, affinityKey);
        await writeAffinity(
          transaction,
          affinityKey,
          connectionId,
          ttl,
          await nowMs(transaction, now),
          old
        );
      });
    },
    touchSessionAccountAffinity(session: string, provider: string, now?: number, ttl = 0) {
      if (!positiveTtl(ttl) || !session || !provider) return Promise.resolve();
      return affinityTransaction(async (transaction) => {
        const affinityKey = key(session, provider);
        const old = await affinityRow(transaction, affinityKey);
        const timestamp = await nowMs(transaction, now);
        if (old && Date.parse(old.expiresAt) > timestamp)
          await writeAffinity(transaction, affinityKey, old.connectionId, ttl, timestamp, old);
      });
    },
    deleteSessionAccountAffinity(session: string, provider: string) {
      if (!session || !provider) return Promise.resolve();
      return affinityTransaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=$1`,
          [key(session, provider)]
        );
      });
    },
    evictSessionAccountAffinityForConnection(
      session: string,
      provider: string,
      connectionId: string
    ) {
      if (!session || !provider || !connectionId) return Promise.resolve(false);
      return affinityTransaction(async (transaction) => {
        const affinityKey = key(session, provider);
        const old = await affinityRow(transaction, affinityKey);
        if (old?.connectionId !== connectionId) return false;
        await transaction.query(
          `DELETE FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=$1`,
          [affinityKey]
        );
        return true;
      });
    },
    cleanupStaleSessionAccountAffinities(_ttl?: number, now?: number) {
      return affinityTransaction(async (transaction) => {
        const timestamp = await nowMs(transaction, now);
        const rows = await transaction.query<{ key: string; value: string }>(
          `SELECT key,value FROM omniroute.key_value WHERE namespace='session_account_affinity'`
        );
        let deleted = 0;
        for (const row of rows.rows) {
          const record = parseAffinity(row.value);
          if (!record || Date.parse(record.expiresAt) <= timestamp)
            deleted +=
              (
                await transaction.query(
                  `DELETE FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=$1`,
                  [row.key]
                )
              ).rowCount ?? 0;
        }
        return deleted;
      });
    },
    upsertHandoff(payload: HandoffPayload) {
      return database.transaction(async (transaction) => {
        const now = await databaseTimestamp(transaction);
        await transaction.query(
          `INSERT INTO omniroute.context_handoffs(session_id,combo_name,from_account,summary,key_decisions,task_progress,active_entities,message_count,model,last_model,warning_threshold_pct,generated_at,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(session_id,combo_name) DO UPDATE SET from_account=excluded.from_account,summary=excluded.summary,key_decisions=excluded.key_decisions,task_progress=excluded.task_progress,active_entities=excluded.active_entities,message_count=excluded.message_count,model=excluded.model,last_model=excluded.last_model,warning_threshold_pct=excluded.warning_threshold_pct,generated_at=excluded.generated_at,expires_at=excluded.expires_at,created_at=excluded.created_at`,
          [
            payload.sessionId,
            payload.comboName,
            payload.fromAccount,
            payload.summary,
            JSON.stringify(payload.keyDecisions || []),
            payload.taskProgress,
            JSON.stringify(payload.activeEntities || []),
            payload.messageCount,
            payload.model,
            payload.lastModel || null,
            payload.warningThresholdPct,
            payload.generatedAt,
            payload.expiresAt,
            now,
          ]
        );
      });
    },
    getHandoff(session: string, combo: string) {
      return database.transaction(async (transaction) =>
        handoff(
          (
            await transaction.query(
              `SELECT * FROM omniroute.context_handoffs WHERE session_id=$1 AND combo_name=$2 AND expires_at>$3 ORDER BY created_at DESC LIMIT 1`,
              [session, combo, await databaseTimestamp(transaction)]
            )
          ).rows[0]
        )
      );
    },
    deleteHandoff(session: string, combo: string) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.context_handoffs WHERE session_id=$1 AND combo_name=$2`,
          [session, combo]
        );
      });
    },
    cleanupExpiredHandoffs() {
      return database.transaction(
        async (transaction) =>
          (
            await transaction.query(`DELETE FROM omniroute.context_handoffs WHERE expires_at<=$1`, [
              await databaseTimestamp(transaction),
            ])
          ).rowCount ?? 0
      );
    },
    hasActiveHandoff(session: string, combo: string) {
      return database.transaction(async (transaction) =>
        Boolean(
          (
            await transaction.query(
              `SELECT 1 FROM omniroute.context_handoffs WHERE session_id=$1 AND combo_name=$2 AND expires_at>$3 LIMIT 1`,
              [session, combo, await databaseTimestamp(transaction)]
            )
          ).rows[0]
        )
      );
    },
    recordSessionModelUsage(
      session: string,
      combo: string,
      model: string,
      provider: string,
      connection?: string
    ) {
      return database.transaction(async (transaction) => {
        await acquirePostgresLock(transaction, "sessions");
        await transaction.query(
          `INSERT INTO omniroute.session_model_history(session_id,combo_name,model_str,provider,connection_id,used_at) VALUES($1,$2,$3,$4,$5,$6)`,
          [
            session,
            combo,
            model,
            provider,
            connection || null,
            await databaseTimestamp(transaction),
          ]
        );
      });
    },
    getLastSessionModel(session: string, combo: string) {
      return database.transaction(
        async (transaction) =>
          (
            await transaction.query<{ model_str: string }>(
              `SELECT model_str FROM omniroute.session_model_history WHERE session_id=$1 AND combo_name=$2 ORDER BY used_at DESC,id DESC LIMIT 1`,
              [session, combo]
            )
          ).rows[0]?.model_str ?? null
      );
    },
    clearSessionModelHistoryForCombo(combo: string) {
      return database.transaction(
        async (transaction) =>
          (
            await transaction.query(
              `DELETE FROM omniroute.session_model_history WHERE combo_name=$1`,
              [combo]
            )
          ).rowCount ?? 0
      );
    },
    deleteSessionModelHistory(session: string, combo: string) {
      return database.transaction(
        async (transaction) =>
          (
            await transaction.query(
              `DELETE FROM omniroute.session_model_history WHERE session_id=$1 AND combo_name=$2`,
              [session, combo]
            )
          ).rowCount ?? 0
      );
    },
    getConnectionRuntimeState(connection: string): Promise<ConnectionRuntimeState | null> {
      return database.transaction(async (transaction) => {
        const row = (
          await transaction.query(
            `SELECT * FROM omniroute.connection_runtime_state WHERE connection_id=$1`,
            [connection]
          )
        ).rows[0];
        if (!row) return null;
        const nullable = (key: string) => (row[key] == null ? null : String(row[key]));
        return {
          connectionId: String(row.connection_id),
          refreshCircuitStreak: Number(row.refresh_circuit_streak ?? 0),
          refreshCircuitUntil: nullable("refresh_circuit_until"),
          refreshLastFailAt: nullable("refresh_last_fail_at"),
          warmupCircuitStreak: Number(row.warmup_circuit_streak ?? 0),
          warmupCircuitUntil: nullable("warmup_circuit_until"),
          warmupLastFailAt: nullable("warmup_last_fail_at"),
          lastWarmupAt: nullable("last_warmup_at"),
          lastWarmupResult: nullable("last_warmup_result"),
          warmupTokensUsed: Number(row.warmup_tokens_used ?? 0),
          updatedAt: String(row.updated_at),
        };
      });
    },
    upsertWarmupState(
      connection: string,
      state: { lastWarmupAt: string; lastResult: string; tokensUsed: number }
    ) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `INSERT INTO omniroute.connection_runtime_state(connection_id,last_warmup_at,last_warmup_result,warmup_tokens_used,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(connection_id) DO UPDATE SET last_warmup_at=excluded.last_warmup_at,last_warmup_result=excluded.last_warmup_result,warmup_tokens_used=excluded.warmup_tokens_used,updated_at=excluded.updated_at`,
          [
            connection,
            state.lastWarmupAt,
            state.lastResult,
            state.tokensUsed,
            await databaseTimestamp(transaction),
          ]
        );
      });
    },
    upsertWarmupCircuit(
      connection: string,
      circuit: { streak: number; until: string; lastFailAt: string }
    ) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `INSERT INTO omniroute.connection_runtime_state(connection_id,warmup_circuit_streak,warmup_circuit_until,warmup_last_fail_at,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(connection_id) DO UPDATE SET warmup_circuit_streak=excluded.warmup_circuit_streak,warmup_circuit_until=excluded.warmup_circuit_until,warmup_last_fail_at=excluded.warmup_last_fail_at,updated_at=excluded.updated_at`,
          [
            connection,
            circuit.streak,
            circuit.until,
            circuit.lastFailAt,
            await databaseTimestamp(transaction),
          ]
        );
      });
    },
    clearWarmupCircuit(connection: string) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `UPDATE omniroute.connection_runtime_state SET warmup_circuit_streak=0,warmup_circuit_until=NULL,warmup_last_fail_at=NULL,updated_at=$2 WHERE connection_id=$1`,
          [connection, await databaseTimestamp(transaction)]
        );
      });
    },
    markForbidden(connection: string, at: string) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `INSERT INTO omniroute.connection_runtime_state(connection_id,last_warmup_result,last_warmup_at,updated_at) VALUES($1,'forbidden',$2,$3) ON CONFLICT(connection_id) DO UPDATE SET last_warmup_result='forbidden',last_warmup_at=excluded.last_warmup_at,updated_at=excluded.updated_at`,
          [connection, at, await databaseTimestamp(transaction)]
        );
      });
    },
    persistCcrBlock(row: CcrBlockRow) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `INSERT INTO omniroute.ccr_blocks(principal_id,hash,content,bytes,chars,lines,content_type,source,created_at,last_accessed_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(principal_id,hash) DO UPDATE SET content=excluded.content,bytes=excluded.bytes,chars=excluded.chars,lines=excluded.lines,content_type=excluded.content_type,source=excluded.source,created_at=excluded.created_at,last_accessed_at=excluded.last_accessed_at,expires_at=excluded.expires_at`,
          [
            row.principalId,
            row.hash,
            row.content,
            row.bytes,
            row.chars,
            row.lines,
            row.contentType,
            row.source,
            row.createdAt,
            row.lastAccessedAt,
            row.expiresAt,
          ]
        );
      });
    },
    loadCcrBlock(principal: string, hash: string, now: number): Promise<CcrBlockRow | null> {
      return database.transaction(async (transaction) => {
        // A conditional delete cannot delete a concurrently renewed row.
        await transaction.query(
          `DELETE FROM omniroute.ccr_blocks WHERE principal_id=$1 AND hash=$2 AND expires_at<=$3`,
          [principal, hash, now]
        );
        const row = (
          await transaction.query(
            `SELECT * FROM omniroute.ccr_blocks WHERE principal_id=$1 AND hash=$2 AND expires_at>$3`,
            [principal, hash, now]
          )
        ).rows[0];
        if (!row) return null;
        return {
          principalId: principal,
          hash,
          content: String(row.content),
          bytes: Number(row.bytes),
          chars: Number(row.chars),
          lines: Number(row.lines),
          contentType: String(row.content_type),
          source: String(row.source),
          createdAt: Number(row.created_at),
          lastAccessedAt: Number(row.last_accessed_at),
          expiresAt: Number(row.expires_at),
        };
      });
    },
    touchCcrBlock(principal: string, hash: string, at: number) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `UPDATE omniroute.ccr_blocks SET last_accessed_at=$3 WHERE principal_id=$1 AND hash=$2`,
          [principal, hash, at]
        );
      });
    },
    deleteCcrBlockRow(principal: string, hash: string) {
      return database.transaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.ccr_blocks WHERE principal_id=$1 AND hash=$2`,
          [principal, hash]
        );
      });
    },
    deleteAllCcrBlocks() {
      return database.transaction(async (transaction) => {
        await transaction.query(`DELETE FROM omniroute.ccr_blocks`);
      });
    },
    pruneExpiredCcrBlocks(now: number) {
      return database.transaction(
        async (transaction) =>
          (await transaction.query(`DELETE FROM omniroute.ccr_blocks WHERE expires_at<=$1`, [now]))
            .rowCount ?? 0
      );
    },
    countCcrBlocks(principal?: string) {
      return database.transaction(async (transaction) =>
        Number(
          (
            await transaction.query<{ n: string }>(
              principal === undefined
                ? `SELECT count(*) AS n FROM omniroute.ccr_blocks`
                : `SELECT count(*) AS n FROM omniroute.ccr_blocks WHERE principal_id=$1`,
              principal === undefined ? [] : [principal]
            )
          ).rows[0].n
        )
      );
    },
    createAccessToken(input: {
      name: string;
      scope?: AccessScope | string;
      expiresAt?: string | null;
    }): Promise<{ record: AccessTokenRecord; secret: string }> {
      const name = (input.name ?? "").trim();
      if (!name) return Promise.reject(new Error("Access token name is required"));
      const secret = `oma_live_${randomBytes(32).toString("base64url")}`;
      return database.transaction(async (transaction) => {
        await acquirePostgresLock(transaction, "accessTokens");
        const record: AccessTokenRecord = {
          id: `tok_${randomUUID()}`,
          name,
          scope: normalizeScope(input.scope, "read"),
          tokenPrefix: secret.slice(0, 15),
          createdAt: await databaseTimestamp(transaction),
          lastUsedAt: null,
          expiresAt: input.expiresAt ?? null,
          revokedAt: null,
        };
        await transaction.query(
          `INSERT INTO omniroute.cli_access_tokens(id,token_hash,token_prefix,name,scope,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7)`,
          [
            record.id,
            createHash("sha256").update(secret).digest("hex"),
            record.tokenPrefix,
            record.name,
            record.scope,
            record.createdAt,
            record.expiresAt,
          ]
        );
        return { record, secret };
      });
    },
    async verifyAccessToken(
      secret: string | null | undefined
    ): Promise<VerifiedAccessToken | null> {
      if (typeof secret !== "string" || !secret) return null;
      const result = await database.transaction(async (transaction) => {
        const row = (
          await transaction.query(`SELECT * FROM omniroute.cli_access_tokens WHERE token_hash=$1`, [
            createHash("sha256").update(secret).digest("hex"),
          ])
        ).rows[0];
        if (!row || row.revoked_at) return null;
        const now = await databaseTimestamp(transaction);
        const expiration = row.expires_at == null ? Number.NaN : Date.parse(String(row.expires_at));
        if (Number.isFinite(expiration) && expiration <= Date.parse(now)) return null;
        const lastUsed =
          row.last_used_at == null ? Number.NaN : Date.parse(String(row.last_used_at));
        return {
          verified: {
            id: String(row.id),
            name: String(row.name),
            scope: normalizeScope(row.scope),
          },
          now,
          touch: !Number.isFinite(lastUsed) || Date.parse(now) - lastUsed >= 60_000,
        };
      });
      if (!result) return null;
      if (result.touch) {
        try {
          await database.transaction(async (transaction) => {
            await transaction.query(
              `UPDATE omniroute.cli_access_tokens SET last_used_at=$2 WHERE id=$1 AND revoked_at IS NULL AND (last_used_at IS NULL OR last_used_at<$2)`,
              [result.verified.id, result.now]
            );
          });
        } catch {
          // A bookkeeping failure does not invalidate a token already verified.
        }
      }
      return result.verified;
    },
    listAccessTokens() {
      return database.transaction(async (transaction) =>
        (
          await transaction.query(
            `SELECT * FROM omniroute.cli_access_tokens ORDER BY created_at DESC`
          )
        ).rows.map(tokenRecord)
      );
    },
    getAccessToken(id: string) {
      return database.transaction(async (transaction) => {
        const row = (
          await transaction.query(`SELECT * FROM omniroute.cli_access_tokens WHERE id=$1`, [id])
        ).rows[0];
        return row ? tokenRecord(row) : null;
      });
    },
    revokeAccessToken(idOrPrefix: string): Promise<RevokeAccessTokenResult> {
      const none = { revoked: false, alreadyRevoked: false, ambiguous: false };
      if (!idOrPrefix) return Promise.resolve(none);
      return database.transaction(async (transaction) => {
        await acquirePostgresLock(transaction, "accessTokens");
        const matches = (
          await transaction.query<{ id: string; revoked_at: string | null }>(
            `SELECT id,revoked_at FROM omniroute.cli_access_tokens WHERE id=$1 OR token_prefix=$1 FOR UPDATE`,
            [idOrPrefix]
          )
        ).rows;
        if (!matches.length) return none;
        const live = matches.filter((row) => row.revoked_at == null);
        const exact = matches.find((row) => row.id === idOrPrefix);
        if (!exact && live.length > 1) return { ...none, ambiguous: true };
        const target = exact ?? live[0] ?? (matches.length === 1 ? matches[0] : undefined);
        if (!target) return none;
        if (target.revoked_at) return { revoked: true, alreadyRevoked: true, ambiguous: false };
        await transaction.query(
          `UPDATE omniroute.cli_access_tokens SET revoked_at=$2 WHERE id=$1 AND revoked_at IS NULL`,
          [target.id, await databaseTimestamp(transaction)]
        );
        return { revoked: true, alreadyRevoked: false, ambiguous: false };
      });
    },
  };
}
