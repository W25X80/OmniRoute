import type { ComboRecord, ComboRepository } from "@/domain/persistence/comboRepositories";
import {
  getCodexUsageHistoryReconciliation,
  type CodexUsageHistoryReconciliationInput,
} from "../providers/usageIdentityReconciliation";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import type { ApiKeyAuditEvent } from "./apiKeyPolicyState";
import type { ProviderDeletionInput } from "./providerRepository";
import { lockRouting } from "./routingSemantics";
import { lockPostgresKeyValueMutation } from "./keyValueRepository";

/** Runs in the provider update transaction, preserving the SQLite identity predicate. */
export async function reconcileCodexUsageHistory(
  transaction: PostgresTransaction,
  input: CodexUsageHistoryReconciliationInput
): Promise<void> {
  const change = getCodexUsageHistoryReconciliation(input);
  if (!change) return;
  await transaction.query("SELECT pg_advisory_xact_lock(18765,19)");
  await transaction.query(
    `UPDATE omniroute.usage_history SET account_key=$1,
      account_label=CASE WHEN $3::integer > COALESCE(account_label_priority, 0)
        THEN $2 ELSE account_label END,
      account_label_priority=GREATEST(COALESCE(account_label_priority, 0), $3::integer)
      WHERE connection_id=$4 AND account_key=$5`,
    [
      change.newAccountKey,
      change.newLabel,
      change.newLabelPriority,
      change.connectionId,
      change.oldAccountKey,
    ]
  );
}

/** Atomic collateral only; providerRepository owns proxy assignments and the provider DELETE. */
export async function deleteConnectionState(
  transaction: PostgresTransaction,
  input: ProviderDeletionInput
): Promise<void> {
  if (input.requestedIds.length === 0) return;
  await transaction.query(
    "DELETE FROM omniroute.quota_snapshots WHERE connection_id=ANY($1::text[])",
    [input.requestedIds]
  );
  // reasoning_routing_rules is removed by the provider FK cascade, as in migration 126.
  // Limits, combo references and LKGP are source-defined best-effort post-commit cleanup.
}

/** Key DELETE and its source-owned budget/cost cleanup share the caller's transaction. */
export async function deleteApiKeyDependents(
  transaction: PostgresTransaction,
  id: string
): Promise<void> {
  await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
  await transaction.query("DELETE FROM omniroute.domain_budgets WHERE api_key_id=$1", [id]);
  await transaction.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id=$1", [id]);
  // The key FK cascade already deletes reasoning rules. Do not purge usage or historical logs.
}

// Same key normalization/redaction as compliance/index.ts. Importing that owner
// would import SQLite core, so this narrow key-management writer keeps a local leaf.
const SENSITIVE_AUDIT_KEYS = new Set([
  "apikey",
  "extraapikeys",
  "accesstoken",
  "refreshtoken",
  "idtoken",
  "authtoken",
  "jwttoken",
  "token",
  "secret",
  "password",
  "authorization",
  "cookie",
  "setcookie",
  "consoleapikey",
  "clientsecret",
]);

function sanitizeAuditValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sanitizeAuditValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => {
        const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
        const sensitive =
          SENSITIVE_AUDIT_KEYS.has(normalized) ||
          normalized.endsWith("apikey") ||
          normalized.endsWith("token") ||
          normalized.endsWith("secret") ||
          normalized.endsWith("password");
        return [key, sensitive ? "[redacted]" : sanitizeAuditValue(nested)];
      })
    );
  }
  return value;
}

function serializeAuditValue(value: unknown): string | null {
  if (value === undefined || value === null || value === "") return null;
  const sanitized = sanitizeAuditValue(value);
  if (typeof sanitized === "string") return sanitized;
  try {
    return JSON.stringify(sanitized);
  } catch {
    return String(sanitized);
  }
}

/**
 * Narrow API-key management audit writer, preserving logAuditEvent's silent best effort.
 * Source key events omit actor/status/requestId: store system/null/null, not speculative context.
 */
export async function recordPostgresApiKeyAuditEvent(
  database: PostgresDatabase,
  event: ApiKeyAuditEvent
): Promise<void> {
  try {
    const details = serializeAuditValue(event.details);
    const metadata = serializeAuditValue(
      event.details && typeof event.details === "object" ? event.details : null
    );
    await database.transaction((transaction) =>
      transaction.query(
        `INSERT INTO omniroute.audit_log
        (timestamp, action, actor, target, details, ip_address, resource_type, status, request_id, metadata)
       VALUES ($1, $2, 'system', $3, $4, NULL, NULL, NULL, NULL, $5)`,
        [new Date().toISOString(), event.action, event.target || null, details, metadata]
      )
    );
  } catch {
    // Administrative audit failure must not break the already committed key mutation.
  }
}

type CleanupOperation = "combos" | "lkgp" | "lkgp_cache" | "provider_limits_cache";

export interface PostgresConnectionCleanupDependencies {
  /** Must be the repository for the selected backend, including its real write invalidations. */
  combos: Pick<ComboRepository, "list" | "update">;
  invalidateCachedLKGP(key: string): void | Promise<void>;
  /** Receives only an operation label; driver errors and credentials are never exposed. */
  reportFailure(operation: CleanupOperation): void;
}

export interface PostgresConnectionCleanupReceipt {
  combosTouched: number;
  lkgpKeys: string[];
  providerLimitsPurged: number;
  failedOperations: CleanupOperation[];
}

function withoutConnectionReferences(
  combo: ComboRecord,
  deletedConnectionIds: Set<string>
): ComboRecord[] | null {
  if (!Array.isArray(combo.models)) return null;
  let changed = false;
  const models = (combo.models as ComboRecord[]).map((step) => {
    let out = step;
    if (typeof out.connectionId === "string" && deletedConnectionIds.has(out.connectionId)) {
      const { connectionId: _connectionId, ...rest } = out;
      out = rest;
      changed = true;
    }
    if (Array.isArray(out.allowedConnectionIds)) {
      const filtered = out.allowedConnectionIds.filter(
        (id) => typeof id !== "string" || !deletedConnectionIds.has(id)
      );
      if (filtered.length !== out.allowedConnectionIds.length) {
        out = { ...out, allowedConnectionIds: filtered };
        changed = true;
      }
    }
    return out;
  });
  return changed ? models : null;
}

/**
 * Invoke only after an acknowledged provider COMMIT. Preserve deletion.ts's best-effort
 * collateral contract: independent cleanup failures never turn a completed delete into a 500.
 * No external callback executes inside a database transaction.
 */
export async function cleanupPostgresConnectionStateAfterCommit(
  database: PostgresDatabase,
  deletion: ProviderDeletionInput,
  dependencies: PostgresConnectionCleanupDependencies
): Promise<PostgresConnectionCleanupReceipt> {
  const receipt: PostgresConnectionCleanupReceipt = {
    combosTouched: 0,
    lkgpKeys: [],
    providerLimitsPurged: 0,
    failedOperations: [],
  };
  const fail = (operation: CleanupOperation) => {
    if (!receipt.failedOperations.includes(operation)) receipt.failedOperations.push(operation);
    try {
      dependencies.reportFailure(operation);
    } catch {
      // Logging itself cannot change the source-defined best-effort delete result.
    }
  };
  // Bulk deletion cleans combo/LKGP refs for existing IDs only, but purges limits for all IDs.
  const referenceIds =
    deletion.mode === "ids"
      ? deletion.existingRows.map((row) => String(row.id))
      : deletion.requestedIds;
  const deletedConnectionIds = new Set(referenceIds.filter(Boolean));

  await Promise.all([
    (async () => {
      if (deletedConnectionIds.size === 0) return;
      try {
        const combos = await dependencies.combos.list();
        for (const combo of combos) {
          const models = withoutConnectionReferences(combo, deletedConnectionIds);
          if (!models || typeof combo.id !== "string") continue;
          try {
            const { id: _id, ...rest } = combo;
            await dependencies.combos.update(combo.id, { ...rest, models });
            receipt.combosTouched++;
          } catch {
            fail("combos");
          }
        }
      } catch {
        fail("combos");
      }
    })(),
    (async () => {
      if (deletedConnectionIds.size === 0) return;
      try {
        receipt.lkgpKeys = await database.transaction(async (transaction) => {
          // Same lock order as combo writes: routing lock precedes key_value row locks.
          await lockRouting(transaction);
          await lockPostgresKeyValueMutation(transaction);
          const { rows } = await transaction.query<{ key: string; value: string }>(
            `SELECT key, value FROM omniroute.key_value WHERE namespace='lkgp'
             ORDER BY key COLLATE "C" FOR UPDATE`
          );
          const keys: string[] = [];
          for (const row of rows) {
            if (!row.key || !row.value) continue;
            let parsed: unknown;
            try {
              parsed = JSON.parse(row.value);
            } catch {
              continue;
            }
            if (typeof parsed !== "object" || parsed === null) continue;
            const connectionId = (parsed as { connectionId?: unknown }).connectionId;
            if (typeof connectionId !== "string" || !deletedConnectionIds.has(connectionId))
              continue;
            await transaction.query(
              "DELETE FROM omniroute.key_value WHERE namespace='lkgp' AND key=$1",
              [row.key]
            );
            keys.push(row.key);
          }
          return keys;
        });
      } catch {
        fail("lkgp");
        return;
      }
      for (const key of receipt.lkgpKeys) {
        try {
          await dependencies.invalidateCachedLKGP(key);
        } catch {
          fail("lkgp_cache");
        }
      }
    })(),
  ]);

  for (const id of deletion.requestedIds) {
    try {
      const result = await database.transaction((transaction) =>
        transaction.query(
          "DELETE FROM omniroute.key_value WHERE namespace='providerLimitsCache' AND key=$1",
          [id]
        )
      );
      receipt.providerLimitsPurged += result.rowCount ?? 0;
    } catch {
      fail("provider_limits_cache");
    }
  }
  return receipt;
}
