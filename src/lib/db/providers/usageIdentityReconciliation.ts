import { resolveUsageAccountIdentity } from "@/lib/usage/accountIdentity";
import { parseProviderSpecificData } from "../webSessionDedup";
import { toStringOrNull } from "./columns";

type JsonRecord = Record<string, unknown>;

interface StatementLike {
  run: (...params: unknown[]) => { changes?: number };
}

interface DbLike {
  prepare: <_TRow = unknown>(sql: string) => StatementLike;
}

export interface CodexUsageHistoryReconciliationInput {
  connectionId: string;
  existing: JsonRecord;
  merged: JsonRecord;
  matchedExistingCodexByWorkspace?: boolean;
}

export interface CodexUsageHistoryReconciliation {
  connectionId: string;
  oldAccountKey: string;
  newAccountKey: string;
  newLabel: string;
  newLabelPriority: number;
}

function nonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed || null;
}

function isSafeCodexIdentityEnrichment(existing: JsonRecord, merged: JsonRecord): boolean {
  const oldProviderData = parseProviderSpecificData(existing.provider_specific_data) || {};
  const newProviderData = parseProviderSpecificData(merged.providerSpecificData) || {};
  const oldUserId = nonEmptyString(oldProviderData.chatgptUserId);
  const newUserId = nonEmptyString(newProviderData.chatgptUserId);
  const oldWorkspaceId = nonEmptyString(oldProviderData.workspaceId);
  const newWorkspaceId = nonEmptyString(newProviderData.workspaceId);
  const oldEmail = nonEmptyString(existing.email);
  const newEmail = nonEmptyString(merged.email);

  return (
    toStringOrNull(existing.provider) === "codex" &&
    toStringOrNull(existing.auth_type) === "oauth" &&
    toStringOrNull(merged.provider) === "codex" &&
    toStringOrNull(merged.authType) === "oauth" &&
    oldUserId === null &&
    newUserId !== null &&
    (oldWorkspaceId === null || oldWorkspaceId === newWorkspaceId) &&
    (oldEmail === null || oldEmail === newEmail)
  );
}

/**
 * Derive the admitted usage-history identity change without accessing storage.
 * Both backends apply these parameters in the provider row update transaction.
 */
export function getCodexUsageHistoryReconciliation(
  input: CodexUsageHistoryReconciliationInput
): CodexUsageHistoryReconciliation | null {
  const oldIdentity = resolveUsageAccountIdentity(input.existing);
  const newIdentity = resolveUsageAccountIdentity(input.merged);
  const identityChanged = oldIdentity.accountKey !== newIdentity.accountKey;
  const permitted =
    input.matchedExistingCodexByWorkspace === true ||
    isSafeCodexIdentityEnrichment(input.existing, input.merged);

  if (!identityChanged || !permitted) return null;

  return {
    connectionId: input.connectionId,
    oldAccountKey: oldIdentity.accountKey,
    newAccountKey: newIdentity.accountKey,
    newLabel: newIdentity.accountLabel,
    newLabelPriority: newIdentity.accountLabelPriority,
  };
}

/** SQLite execution of the shared identity-change predicate. */
export function reconcileCodexUsageHistory(
  db: DbLike,
  input: CodexUsageHistoryReconciliationInput
): void {
  const reconciliation = getCodexUsageHistoryReconciliation(input);
  if (!reconciliation) return;

  db.prepare(
    `UPDATE usage_history
     SET account_key = @newAccountKey,
         account_label = CASE
           WHEN @newLabelPriority > COALESCE(account_label_priority, 0)
           THEN @newLabel
           ELSE account_label
         END,
         account_label_priority = MAX(
           COALESCE(account_label_priority, 0),
           @newLabelPriority
         )
     WHERE connection_id = @connectionId
       AND account_key = @oldAccountKey`
  ).run(reconciliation);
}
