import type * as Leases from "../exclusiveConnectionLeases";
import { hashLeaseOwnerId } from "../exclusiveConnectionLeases";
import type { PostgresDatabase, PostgresTransaction } from "./database";

type Lease = Leases.ExclusiveConnectionLease;
type Input<Name extends keyof typeof Leases> = Parameters<
  Extract<(typeof Leases)[Name], (...args: never[]) => unknown>
>[0];
type Result<Name extends keyof typeof Leases> = ReturnType<
  Extract<(typeof Leases)[Name], (...args: never[]) => unknown>
>;
type Row = Record<string, unknown>;

function safeInteger(value: unknown): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error("Lease integer exceeds the supported range");
  return number;
}
function mapLease(row: Row): Lease {
  return {
    id: safeInteger(row.id),
    leaseOwnerHash: String(row.lease_owner_hash),
    apiKeyId: String(row.api_key_id),
    provider: String(row.provider),
    connectionId: String(row.connection_id),
    generation: safeInteger(row.generation),
    state: row.state as Lease["state"],
    acquiredAt: String(row.acquired_at),
    renewedAt: String(row.renewed_at),
    expiresAt: String(row.expires_at),
    endedAt: row.ended_at == null ? null : String(row.ended_at),
    endReason: row.end_reason == null ? null : String(row.end_reason),
  };
}
function expiry(now: string, ttlMs?: number): string {
  const ttl = Math.min(1_800_000, Math.max(1_000, ttlMs ?? 120_000));
  return new Date(Date.parse(now) + ttl).toISOString();
}
export async function databaseTimestamp(
  transaction: PostgresTransaction,
  override?: string
): Promise<string> {
  if (override !== undefined) {
    const parsed = Date.parse(override);
    if (!Number.isFinite(parsed)) throw new Error("now must be a valid ISO timestamp");
    return new Date(parsed).toISOString();
  }
  const { rows } = await transaction.query<{ now: string }>(
    `SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS now`
  );
  return rows[0].now;
}
async function expire(transaction: PostgresTransaction, now: string): Promise<number> {
  const result = await transaction.query(
    `UPDATE omniroute.exclusive_connection_leases SET state='EXPIRED',ended_at=$1,end_reason='TTL_EXPIRED'
     WHERE state='ACTIVE' AND expires_at <= $1`,
    [now]
  );
  return result.rowCount ?? 0;
}
async function active(
  transaction: PostgresTransaction,
  column: "lease_owner_hash" | "connection_id",
  value: string
): Promise<Row | undefined> {
  return (
    await transaction.query(
      `SELECT * FROM omniroute.exclusive_connection_leases WHERE state='ACTIVE' AND ${column}=$1`,
      [value]
    )
  ).rows[0];
}
async function insert(
  transaction: PostgresTransaction,
  input: {
    ownerHash: string;
    apiKeyId: string;
    provider: string;
    connectionId: string;
    generation: number;
    now: string;
    ttlMs?: number;
  }
): Promise<Lease> {
  const { rows } = await transaction.query(
    `INSERT INTO omniroute.exclusive_connection_leases
     (lease_owner_hash,api_key_id,provider,connection_id,generation,state,acquired_at,renewed_at,expires_at)
     VALUES($1,$2,$3,$4,$5,'ACTIVE',$6,$6,$7) RETURNING *`,
    [
      input.ownerHash,
      input.apiKeyId,
      input.provider,
      input.connectionId,
      input.generation,
      input.now,
      expiry(input.now, input.ttlMs),
    ]
  );
  return mapLease(rows[0]);
}
/** The lock fences missing owner/connection rows as well as existing rows. */
export function createPostgresLeaseRepository(database: PostgresDatabase) {
  function run<T>(
    now: string | undefined,
    body: (transaction: PostgresTransaction, now: string) => Promise<T>
  ) {
    return database.transaction(async (transaction) => {
      await transaction.query("SELECT pg_advisory_xact_lock(18765,4)");
      await transaction.query("SELECT pg_advisory_xact_lock(18765,7)");
      const timestamp = await databaseTimestamp(transaction, now);
      await expire(transaction, timestamp);
      return body(transaction, timestamp);
    });
  }
  async function update<T extends "INVALIDATED" | "RENEWED" | "RELEASED">(
    input: {
      leaseOwnerId: string;
      generation: number;
      apiKeyId: string;
      now?: string;
    },
    kind: T,
    reason?: string,
    ttlMs?: number
  ): Promise<{ kind: T; lease: Lease } | { kind: "STALE" }> {
    const ownerHash = hashLeaseOwnerId(input.leaseOwnerId);
    return run(input.now, async (transaction, now) => {
      const values = [ownerHash, input.generation, input.apiKeyId, now];
      const sql =
        kind === "RENEWED"
          ? `UPDATE omniroute.exclusive_connection_leases SET renewed_at=$4,expires_at=$5 WHERE lease_owner_hash=$1 AND generation=$2 AND api_key_id=$3 AND state='ACTIVE' AND expires_at>$4 RETURNING *`
          : `UPDATE omniroute.exclusive_connection_leases SET state='${kind}',ended_at=$4,end_reason=$5 WHERE lease_owner_hash=$1 AND generation=$2 AND api_key_id=$3 AND state='ACTIVE' AND expires_at>$4 RETURNING *`;
      const result = await transaction.query(sql, [
        ...values,
        kind === "RENEWED" ? expiry(now, ttlMs) : reason,
      ]);
      if (result.rows[0]) return { kind, lease: mapLease(result.rows[0]) };
      if (kind === "RELEASED") {
        const { rows } = await transaction.query(
          `SELECT * FROM omniroute.exclusive_connection_leases WHERE lease_owner_hash=$1 AND generation=$2 ORDER BY id DESC LIMIT 1`,
          [ownerHash, input.generation]
        );
        if (rows[0]?.state === "RELEASED" && rows[0].api_key_id === input.apiKeyId)
          return { kind, lease: mapLease(rows[0]) };
      }
      return { kind: "STALE" };
    });
  }
  return {
    reconcileExpiredExclusiveConnectionLeases: (now?: string) =>
      database.transaction(async (transaction) => {
        await transaction.query("SELECT pg_advisory_xact_lock(18765,4)");
        await transaction.query("SELECT pg_advisory_xact_lock(18765,7)");
        return expire(transaction, await databaseTimestamp(transaction, now));
      }),
    acquireExclusiveConnectionLease(
      input: Input<"acquireExclusiveConnectionLease">
    ): Promise<Result<"acquireExclusiveConnectionLease">> {
      const ownerHash = hashLeaseOwnerId(input.leaseOwnerId);
      return run(input.now, async (transaction, now) => {
        const owner = await active(transaction, "lease_owner_hash", ownerHash);
        if (owner) {
          if (owner.api_key_id !== input.apiKeyId || owner.connection_id !== input.connectionId)
            return { kind: "OWNER_ALREADY_ACTIVE", lease: mapLease(owner) };
          const result = await transaction.query(
            `UPDATE omniroute.exclusive_connection_leases SET renewed_at=$1,expires_at=$2,provider=$3 WHERE id=$4 AND state='ACTIVE' AND api_key_id=$5 RETURNING *`,
            [now, expiry(now, input.ttlMs), input.provider, owner.id, input.apiKeyId]
          );
          return { kind: "REUSED", lease: mapLease(result.rows[0]) };
        }
        const occupied = await active(transaction, "connection_id", input.connectionId);
        if (occupied) return { kind: "CONNECTION_BUSY", retryAfter: String(occupied.expires_at) };
        const result = await transaction.query<{ generation: string }>(
          `SELECT COALESCE(MAX(generation),0) AS generation FROM omniroute.exclusive_connection_leases WHERE lease_owner_hash=$1`,
          [ownerHash]
        );
        const generation = safeInteger(result.rows[0].generation) + 1;
        if (!Number.isSafeInteger(generation) || generation <= 0)
          throw new Error("lease generation exhausted");
        return {
          kind: "ACQUIRED",
          lease: await insert(transaction, { ...input, ownerHash, generation, now }),
        };
      });
    },
    transitionExclusiveConnectionLease(
      input: Input<"transitionExclusiveConnectionLease">
    ): Promise<Result<"transitionExclusiveConnectionLease">> {
      const ownerHash = hashLeaseOwnerId(input.leaseOwnerId);
      return run(input.now, async (transaction, now) => {
        const owner = await active(transaction, "lease_owner_hash", ownerHash);
        if (
          !owner ||
          safeInteger(owner.generation) !== input.generation ||
          owner.api_key_id !== input.apiKeyId
        )
          return { kind: "STALE" };
        if (owner.connection_id === input.connectionId)
          return { kind: "REUSED", lease: mapLease(owner) };
        const occupied = await active(transaction, "connection_id", input.connectionId);
        if (occupied) return { kind: "CONNECTION_BUSY", retryAfter: String(occupied.expires_at) };
        await transaction.query(
          `UPDATE omniroute.exclusive_connection_leases SET state='INVALIDATED',ended_at=$1,end_reason=$2 WHERE id=$3 AND state='ACTIVE'`,
          [now, input.reason, owner.id]
        );
        return {
          kind: "TRANSITIONED",
          lease: await insert(transaction, { ...input, ownerHash, now }),
        };
      });
    },
    invalidateExclusiveConnectionLease: (input: Input<"invalidateExclusiveConnectionLease">) =>
      update(input, "INVALIDATED", input.reason),
    renewExclusiveConnectionLease: (input: Input<"renewExclusiveConnectionLease">) =>
      update(input, "RENEWED", undefined, input.ttlMs),
    releaseExclusiveConnectionLease: (input: Input<"releaseExclusiveConnectionLease">) =>
      update(input, "RELEASED", input.reason ?? "OWNER_EXIT"),
    assertExclusiveConnectionLeaseFence(
      input: Input<"assertExclusiveConnectionLeaseFence">
    ): Promise<Result<"assertExclusiveConnectionLeaseFence">> {
      const ownerHash = hashLeaseOwnerId(input.leaseOwnerId);
      return run(input.now, async (transaction) => {
        const row = await active(transaction, "lease_owner_hash", ownerHash);
        if (!row) return { kind: "REQUIRED" };
        if (safeInteger(row.generation) !== input.generation) return { kind: "STALE" };
        const lease = mapLease(row);
        if (row.api_key_id !== input.apiKeyId) return { kind: "AUTHORIZATION_MISMATCH", lease };
        return {
          kind: row.connection_id === input.connectionId ? "VALID" : "CONNECTION_MISMATCH",
          lease,
        };
      });
    },
    getActiveExclusiveConnectionLease(leaseOwnerId: string, now?: string) {
      const ownerHash = hashLeaseOwnerId(leaseOwnerId);
      return run(now, async (transaction) => {
        const row = await active(transaction, "lease_owner_hash", ownerHash);
        return row ? mapLease(row) : null;
      });
    },
    getExclusiveLeaseOccupancy(connectionIds: readonly string[], now?: string) {
      return run(now, async (transaction) => {
        const result = await transaction.query<{
          connection_id: string;
          lease_owner_hash: string;
          expires_at: string;
        }>(
          `SELECT connection_id,lease_owner_hash,expires_at FROM omniroute.exclusive_connection_leases WHERE state='ACTIVE' AND connection_id=ANY($1::text[])`,
          [[...connectionIds]]
        );
        return new Map(
          result.rows.map((row) => [
            row.connection_id,
            { leaseOwnerHash: row.lease_owner_hash, expiresAt: row.expires_at },
          ])
        );
      });
    },
    isExclusiveConnectionActivelyLeased(connectionId: string, now?: string) {
      if (!connectionId) return Promise.resolve(false);
      return run(now, async (transaction) =>
        Boolean(await active(transaction, "connection_id", connectionId))
      );
    },
    getExclusiveConnectionLeaseStatus(
      input: Input<"getExclusiveConnectionLeaseStatus">
    ): Promise<Result<"getExclusiveConnectionLeaseStatus">> {
      const ownerHash = hashLeaseOwnerId(input.leaseOwnerId);
      return run(input.now, async (transaction, now) => {
        const { rows } = await transaction.query(
          `SELECT leases.*,connections.provider AS connection_provider,connections.auth_type AS connection_auth_type,connections.name AS connection_name,connections.email AS connection_email,connections.display_name AS connection_display_name FROM omniroute.exclusive_connection_leases leases INNER JOIN omniroute.provider_connections connections ON connections.id=leases.connection_id WHERE leases.lease_owner_hash=$1 AND leases.api_key_id=$2 AND leases.generation=$3 AND leases.state='ACTIVE' AND leases.expires_at>$4 LIMIT 1`,
          [ownerHash, input.apiKeyId, input.generation, now]
        );
        const row = rows[0];
        if (!row) return null;
        let name = typeof row.connection_name === "string" ? row.connection_name.trim() : null;
        if (!name || name.includes("@")) name = null;
        if (
          name &&
          (row.connection_auth_type === "oauth" || row.connection_auth_type === "access_token") &&
          [row.connection_email, row.connection_display_name].some(
            (value) =>
              typeof value === "string" && value.trim().toLowerCase() === name!.toLowerCase()
          )
        )
          name = null;
        return {
          lease: mapLease(row),
          provider: String(row.connection_provider),
          connectionName: name,
        };
      });
    },
  };
}
