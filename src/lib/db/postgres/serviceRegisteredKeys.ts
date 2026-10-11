import { createHash, randomBytes } from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { rowToCamel } from "../caseMapping";
import { RegisteredKeyQuotaError } from "../registeredKeys/errors";
import type {
  RegisteredKey,
  RegisteredKeyWithSecret,
  IssueKeyParams,
  QuotaCheckResult,
  ProviderKeyLimit,
  AccountKeyLimit,
} from "../registeredKeys";
import type { PostgresTransaction } from "./database";
import { first, rows, affected, safeInteger, lockServices } from "./serviceQuery";

type Row = Record<string, unknown>;
const day = () => new Date().toISOString().slice(0, 10);
const hour = () => new Date().toISOString().slice(0, 13);
function map(row: Row): RegisteredKey {
  return { ...rowToCamel(row), isActive: row.is_active === 1 } as unknown as RegisteredKey;
}
async function resetWindow(
  transaction: PostgresTransaction,
  table: "provider_key_limits" | "account_key_limits",
  idField: "provider" | "account_id",
  id: string
) {
  await transaction.query(
    `UPDATE omniroute.${table} SET daily_issued = CASE WHEN last_reset_day <> $1 THEN 0 ELSE daily_issued END,
    hourly_issued = CASE WHEN last_reset_hour <> $2 THEN 0 ELSE hourly_issued END, last_reset_day = $1, last_reset_hour = $2 WHERE ${idField} = $3`,
    [day(), hour(), id]
  );
}
export async function checkQuota(
  transaction: PostgresTransaction,
  provider = "",
  accountId = ""
): Promise<QuotaCheckResult> {
  await lockServices(transaction, "registered-keys");
  for (const kind of ["provider", "account"] as const) {
    const id = kind === "provider" ? provider : accountId;
    if (!id) continue;
    const table = kind === "provider" ? "provider_key_limits" : "account_key_limits";
    const field = kind === "provider" ? "provider" : "account_id";
    await resetWindow(transaction, table, field, id);
    const limit = await first(transaction, `SELECT * FROM omniroute.${table} WHERE ${field} = $1`, [
      id,
    ]);
    if (!limit) continue;
    const scope = kind === "provider" ? { provider } : { accountId };
    const quotaCode = kind === "provider" ? "PROVIDER_QUOTA_EXCEEDED" : "ACCOUNT_QUOTA_EXCEEDED";
    for (const window of ["hourly", "daily"] as const) {
      const maximum = limit[`${window}_issue_limit`];
      if (maximum !== null && safeInteger(limit[`${window}_issued`]) >= safeInteger(maximum)) {
        return {
          allowed: false,
          errorCode: quotaCode,
          errorMessage: `${window === "hourly" ? "Hourly" : "Daily"} issue limit (${maximum}) reached for ${kind} '${id}'`,
          ...scope,
        };
      }
    }
    if (limit.max_active_keys !== null) {
      const count = safeInteger(
        (
          await first<{ count: string }>(
            transaction,
            `SELECT count(*) AS count FROM omniroute.registered_keys WHERE ${field} = $1 AND is_active = 1`,
            [id]
          )
        )?.count ?? 0
      );
      if (count >= safeInteger(limit.max_active_keys))
        return {
          allowed: false,
          errorCode: "MAX_ACTIVE_KEYS_EXCEEDED",
          errorMessage: `Max active keys (${limit.max_active_keys}) reached for ${kind} '${id}'`,
          ...scope,
          ...(kind === "provider" ? { providerActiveKeys: count } : { accountActiveKeys: count }),
        };
    }
  }
  return { allowed: true };
}
export async function issueRegisteredKey(
  transaction: PostgresTransaction,
  params: IssueKeyParams
): Promise<RegisteredKeyWithSecret | { idempotencyConflict: true; existing: RegisteredKey }> {
  await lockServices(transaction, "registered-keys");
  const { name, provider = "", accountId = "", idempotencyKey } = params;
  if (idempotencyKey) {
    const existing = await first(
      transaction,
      "SELECT * FROM omniroute.registered_keys WHERE idempotency_key = $1",
      [idempotencyKey]
    );
    if (existing) return { idempotencyConflict: true, existing: map(existing) };
  }
  const quota = await checkQuota(transaction, provider, accountId);
  if (!quota.allowed) throw new RegisteredKeyQuotaError(quota);
  const rawKey = "ork_" + randomBytes(24).toString("base64url");
  const id = uuidv4();
  await transaction.query(
    `INSERT INTO omniroute.registered_keys
    (id, key, key_prefix, name, provider, account_id, idempotency_key, expires_at, daily_budget, hourly_budget, last_reset_day, last_reset_hour)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      id,
      createHash("sha256").update(rawKey).digest("hex"),
      rawKey.slice(0, 12),
      name,
      provider,
      accountId,
      idempotencyKey ?? null,
      params.expiresAt ?? null,
      params.dailyBudget ?? null,
      params.hourlyBudget ?? null,
      day(),
      hour(),
    ]
  );
  for (const kind of ["provider", "account"] as const) {
    const value = kind === "provider" ? provider : accountId;
    if (!value) continue;
    const table = kind === "provider" ? "provider_key_limits" : "account_key_limits";
    const column = kind === "provider" ? "provider" : "account_id";
    await resetWindow(transaction, table, column, value);
    await transaction.query(
      `INSERT INTO omniroute.${table} (${column}, daily_issued, hourly_issued, last_reset_day, last_reset_hour)
      VALUES ($1, 1, 1, $2, $3) ON CONFLICT (${column}) DO UPDATE SET
      daily_issued = omniroute.${table}.daily_issued + 1, hourly_issued = omniroute.${table}.hourly_issued + 1, updated_at = $4`,
      [value, day(), hour(), new Date().toISOString()]
    );
  }
  return { ...(await getRegisteredKey(transaction, id))!, rawKey };
}
export async function getRegisteredKey(
  transaction: PostgresTransaction,
  id: string
): Promise<RegisteredKey | null> {
  const row = await first(transaction, "SELECT * FROM omniroute.registered_keys WHERE id = $1", [
    id,
  ]);
  return row ? map(row) : null;
}
export async function listRegisteredKeys(
  transaction: PostgresTransaction,
  opts: { provider?: string; accountId?: string } = {}
): Promise<RegisteredKey[]> {
  const conditions: string[] = [];
  const values: unknown[] = [];
  if (opts.provider) {
    values.push(opts.provider);
    conditions.push(`provider = $${values.length}`);
  }
  if (opts.accountId) {
    values.push(opts.accountId);
    conditions.push(`account_id = $${values.length}`);
  }
  return (
    await rows(
      transaction,
      `SELECT * FROM omniroute.registered_keys ${conditions.length ? `WHERE ${conditions.join(" AND ")}` : ""} ORDER BY created_at COLLATE "C" DESC, id COLLATE "C" DESC LIMIT 500`,
      values
    )
  ).map(map);
}
export async function revokeRegisteredKey(
  transaction: PostgresTransaction,
  id: string
): Promise<boolean> {
  return (
    (await affected(
      transaction,
      "UPDATE omniroute.registered_keys SET is_active = 0, revoked_at = $1, updated_at = $1 WHERE id = $2 AND is_active = 1",
      [new Date().toISOString(), id]
    )) > 0
  );
}
export async function validateRegisteredKey(
  transaction: PostgresTransaction,
  rawKey: string
): Promise<RegisteredKey | null> {
  if (!rawKey || typeof rawKey !== "string") return null;
  const row = await first(
    transaction,
    "SELECT * FROM omniroute.registered_keys WHERE key = $1 AND is_active = 1 AND (expires_at IS NULL OR expires_at > $2) FOR UPDATE",
    [createHash("sha256").update(rawKey).digest("hex"), new Date().toISOString()]
  );
  if (!row) return null;
  const today = day();
  const currentHour = hour();
  const daily = row.last_reset_day !== today ? 0 : safeInteger(row.daily_used);
  const hourly = row.last_reset_hour !== currentHour ? 0 : safeInteger(row.hourly_used);
  if (row.last_reset_day !== today || row.last_reset_hour !== currentHour)
    await transaction.query(
      "UPDATE omniroute.registered_keys SET daily_used = $1, hourly_used = $2, last_reset_day = $3, last_reset_hour = $4 WHERE id = $5",
      [daily, hourly, today, currentHour, row.id]
    );
  if (
    (row.daily_budget !== null && daily >= safeInteger(row.daily_budget)) ||
    (row.hourly_budget !== null && hourly >= safeInteger(row.hourly_budget))
  )
    return null;
  return map({
    ...row,
    daily_used: daily,
    hourly_used: hourly,
    last_reset_day: today,
    last_reset_hour: currentHour,
  });
}
export async function incrementRegisteredKeyUsage(
  transaction: PostgresTransaction,
  id: string
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.registered_keys SET daily_used = daily_used + 1, hourly_used = hourly_used + 1, updated_at = $1 WHERE id = $2",
    [new Date().toISOString(), id]
  );
}
export async function setProviderKeyLimit(
  transaction: PostgresTransaction,
  provider: string,
  limits: Partial<Omit<ProviderKeyLimit, "provider" | "dailyIssued" | "hourlyIssued" | "updatedAt">>
): Promise<void> {
  await setLimit(transaction, "provider_key_limits", "provider", provider, limits);
}
export async function setAccountKeyLimit(
  transaction: PostgresTransaction,
  accountId: string,
  limits: Partial<Omit<AccountKeyLimit, "accountId" | "dailyIssued" | "hourlyIssued" | "updatedAt">>
): Promise<void> {
  await setLimit(transaction, "account_key_limits", "account_id", accountId, limits);
}
async function setLimit(
  transaction: PostgresTransaction,
  table: "provider_key_limits" | "account_key_limits",
  column: "provider" | "account_id",
  id: string,
  limits: {
    maxActiveKeys?: number | null;
    dailyIssueLimit?: number | null;
    hourlyIssueLimit?: number | null;
  }
) {
  await lockServices(transaction, "registered-keys");
  await transaction.query(
    `INSERT INTO omniroute.${table} (${column}, max_active_keys, daily_issue_limit, hourly_issue_limit, last_reset_day, last_reset_hour)
    VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (${column}) DO UPDATE SET max_active_keys = excluded.max_active_keys,
    daily_issue_limit = excluded.daily_issue_limit, hourly_issue_limit = excluded.hourly_issue_limit, updated_at = $7`,
    [
      id,
      limits.maxActiveKeys ?? null,
      limits.dailyIssueLimit ?? null,
      limits.hourlyIssueLimit ?? null,
      day(),
      hour(),
      new Date().toISOString(),
    ]
  );
}
export async function getProviderKeyLimit(
  transaction: PostgresTransaction,
  provider: string
): Promise<ProviderKeyLimit | null> {
  const row = await first(
    transaction,
    "SELECT * FROM omniroute.provider_key_limits WHERE provider = $1",
    [provider]
  );
  return row ? (rowToCamel(row) as unknown as ProviderKeyLimit) : null;
}
export async function getAccountKeyLimit(
  transaction: PostgresTransaction,
  accountId: string
): Promise<AccountKeyLimit | null> {
  const row = await first(
    transaction,
    "SELECT * FROM omniroute.account_key_limits WHERE account_id = $1",
    [accountId]
  );
  return row ? (rowToCamel(row) as unknown as AccountKeyLimit) : null;
}
