import type { ComboRecord } from "@/domain/persistence/comboRepositories";
import { normalizeComboRecord } from "@/lib/combos/steps";
import type { PostgresTransaction } from "./database";

export interface ComboRow extends Record<string, unknown> {
  id: string;
  name?: string;
  data: string;
  sort_order: number;
  context_cache_protection?: number;
}

// SQLite NOCASE and default LIKE fold ASCII only. Unicode lower() would broaden lookup.
export function asciiFoldSql(expression: string): string {
  return `translate(${expression}, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')`;
}

export const COMBO_ORDER_SQL = `sort_order ASC NULLS FIRST, ${asciiFoldSql("name")} COLLATE "C" ASC, insertion_order ASC`;

/**
 * Routing namespace (18765, 2): all combo/mapping mutations hold the exclusive lock.
 * Multi-query reads hold its shared counterpart so normalization and page totals observe
 * one routing state. Acquire this lock before row locks; transaction end releases it.
 */
export async function lockRouting(transaction: PostgresTransaction, shared = false): Promise<void> {
  await transaction.query(
    shared
      ? "SELECT pg_advisory_xact_lock_shared(18765, 2)"
      : "SELECT pg_advisory_xact_lock(18765, 2)"
  );
}

/** Preserve the SQLite JSON parser: empty payload is absent; invalid JSON throws. */
export function parseComboRow(row: ComboRow | undefined): ComboRecord | null {
  if (!row || typeof row.data !== "string" || !row.data) return null;
  const combo = JSON.parse(row.data) as ComboRecord;
  if (typeof row.sort_order === "number") combo.sortOrder = row.sort_order;
  if (typeof row.id === "string" && row.id.trim().length > 0) combo.id = row.id;
  // A zero column preserves legacy JSON values; only explicit 1 overrides the blob.
  if (row.context_cache_protection === 1) combo.context_cache_protection = true;
  return combo;
}

export async function normalizeStoredCombo(
  combo: ComboRecord,
  transaction: PostgresTransaction,
  extraNames: string[] = []
): Promise<ComboRecord> {
  const { rows } = await transaction.query<{ name: string }>("SELECT name FROM omniroute.combos");
  const names = new Set<string>();
  for (const name of [...rows.map((row) => row.name), ...extraNames]) {
    if (typeof name === "string" && name.trim().length > 0) names.add(name.trim());
  }
  return normalizeComboRecord(combo, { allCombos: names });
}

export async function listCombos(
  transaction: PostgresTransaction,
  limit?: number,
  offset?: number
): Promise<ComboRecord[]> {
  let sql = `SELECT id, data, sort_order, context_cache_protection FROM omniroute.combos ORDER BY ${COMBO_ORDER_SQL}`;
  const parameters: unknown[] = [];
  if (limit !== undefined) {
    // SQLite LIMIT -1 means unlimited; the domain does not use OFFSET without LIMIT.
    sql +=
      " LIMIT CASE WHEN $1::bigint < 0 THEN NULL ELSE $1::bigint END OFFSET GREATEST($2::bigint, 0)";
    parameters.push(limit, offset ?? 0);
  }
  const { rows } = await transaction.query<ComboRow>(sql, parameters);
  const combos = rows.map(parseComboRow).filter((combo): combo is ComboRecord => combo !== null);
  // List normalization intentionally uses only the returned page, matching SQLite.
  const names = combos
    .map((combo) => (typeof combo.name === "string" ? combo.name.trim() : ""))
    .filter((name) => name.length > 0);
  return combos.map((combo) => normalizeComboRecord(combo, { allCombos: names }));
}
