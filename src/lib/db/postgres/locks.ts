import type { PostgresTransaction } from "./database";

/**
 * Transaction-scoped semantic fences. IDs are a persisted coordination protocol.
 * Existing owners retain their IDs; 21-26 partition legacy agent/memory contention.
 * Deploy this protocol only after draining the sole old writer (no rolling overlap).
 *
 * Compound acquisition order (before row locks):
 * schema -> applicationImport; routing -> catalogKeyValue -> keyPolicy -> sessions;
 * routing -> providers -> validation -> catalog -> catalogKeyValue -> keyPolicy;
 * settings/providers -> keyPolicy -> accounting/usage; providers -> validation;
 * keyPolicy -> quotaGroups; validation -> accounting.
 * These are dependency edges, not numeric ordering. Do not reverse an edge.
 * Accounting intentionally still covers receipts, artifacts, budgets, breakers and
 * quota-reset history until their cross-table invariants admit a narrower fence.
 */
export const POSTGRES_LOCK_OWNERS = {
  schema: 1,
  routing: 2,
  settings: 3,
  providers: 4,
  keyPolicy: 5,
  secrets: 6,
  validation: 7,
  accounting: 8,
  quota: 9,
  proxies: 10,
  quotaGroups: 11,
  compression: 12,
  sessions: 13,
  accessTokens: 14,
  thoughtSignatures: 15,
  knowledge: 16,
  gamification: 17,
  catalog: 18,
  usage: 19,
  catalogKeyValue: 20,
  conversations: 21,
  skills: 22,
  a2a: 23,
  cloudAgents: 24,
  agentMetadata: 25,
  memory: 26,
  applicationImport: 31,
} as const;

export type PostgresLockOwner = keyof typeof POSTGRES_LOCK_OWNERS;
export type PostgresLockMode = "shared" | "exclusive";

export async function acquirePostgresLock(
  transaction: PostgresTransaction,
  owner: PostgresLockOwner,
  mode: PostgresLockMode = "exclusive"
): Promise<void> {
  transaction.noteAdvisoryLock?.(owner, mode);
  await transaction.query(
    mode === "shared"
      ? "SELECT pg_advisory_xact_lock_shared(18765,$1)"
      : "SELECT pg_advisory_xact_lock(18765,$1)",
    [POSTGRES_LOCK_OWNERS[owner]]
  );
}

/** Hash collisions conservatively serialize independent tuples; never mix rows. */
export async function acquirePostgresEntityLock(
  transaction: PostgresTransaction,
  owner: "quotaCounter" | "providerQuota",
  identity: readonly string[]
): Promise<void> {
  transaction.noteAdvisoryLock?.(owner, "exclusive");
  await transaction.query("SELECT pg_advisory_xact_lock($1,hashtext($2))", [
    owner === "quotaCounter" ? 18766 : 18767,
    JSON.stringify(identity),
  ]);
}

export type PostgresServiceLockOwner =
  | "files-batches"
  | "registered-keys"
  | "radar"
  | "presets"
  | "log-export"
  | "eval-suites"
  | "version-manager"
  | "webhooks";

/** The bigint key space is distinct from the two-int key spaces above. */
export async function acquirePostgresServiceLock(
  transaction: PostgresTransaction,
  owner: PostgresServiceLockOwner
): Promise<void> {
  transaction.noteAdvisoryLock?.(owner, "exclusive");
  await transaction.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
    `omniroute.services:${owner}`,
  ]);
}

/** Absent conversation roots coordinate with orphan cleanup in the separate bigint key space. */
export async function acquireConversationCreationLock(
  transaction: PostgresTransaction,
  id: string,
  skip = false
): Promise<boolean> {
  transaction.noteAdvisoryLock?.("conversation-root", "exclusive");
  const identity = `omniroute.conversation.root:${id}`;
  if (skip)
    return (
      await transaction.query<{ locked: boolean }>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS locked",
        [identity]
      )
    ).rows[0].locked;
  await transaction.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [identity]);
  return true;
}
