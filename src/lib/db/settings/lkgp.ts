/**
 * db/settings/lkgp.ts — Last Known Good Provider (LKGP) persistence.
 */

import { randomUUID } from "node:crypto";
import { getDbInstance } from "../core";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import {
  getPostgresKeyValue,
  writePostgresKeyValue,
  deletePostgresKeyValue,
  deletePostgresKeyValueNamespace,
  withPostgresKeyValueMutation,
  listPostgresKeyValues,
} from "../postgres/keyValueRepository";

/**
 * Escape SQLite `LIKE` wildcards so a combo name containing `%` or `_` cannot
 * widen the prefix match into unrelated combos' pins.
 */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}

export function deleteLKGPRowsByComboName(comboName: string): string[] {
  if (!comboName) return [];

  const db = getDbInstance();
  const prefix = `${comboName}:`;
  const rows = db
    .prepare("SELECT key FROM key_value WHERE namespace = 'lkgp' AND key LIKE ? ESCAPE '\\'")
    .all(`${escapeLikePattern(prefix)}%`) as Array<{ key?: string }>;

  const staleKeys = rows.map((row) => row?.key).filter((key): key is string => Boolean(key));
  if (staleKeys.length === 0) return [];

  const deleteStatement = db.prepare("DELETE FROM key_value WHERE namespace = 'lkgp' AND key = ?");
  for (const key of staleKeys) {
    deleteStatement.run(key);
  }

  return staleKeys;
}

export interface LKGPRecord {
  provider: string;
  connectionId?: string;
}

export type LKGPObservation = ReadonlyMap<string, string>;

function decodePin(value: string): LKGPRecord {
  try {
    const parsed = JSON.parse(value);
    if (typeof parsed === "object" && parsed !== null && "provider" in parsed) {
      const { revision: _revision, ...record } = parsed;
      return record as LKGPRecord;
    }
    return { provider: String(parsed) };
  } catch {
    return { provider: value };
  }
}

export async function getLKGP(comboName: string, modelId: string): Promise<LKGPRecord | null> {
  const postgres = getSelectedPostgresDatabase();
  const key = `${comboName}:${modelId}`;
  const row = postgres
    ? { value: await getPostgresKeyValue(postgres, "lkgp", key) }
    : (getDbInstance()
        .prepare("SELECT value FROM key_value WHERE namespace = 'lkgp' AND key = ?")
        .get(key) as { value?: string } | undefined);
  if (!row?.value) return null;
  return decodePin(row.value);
}

export async function setLKGP(
  comboName: string,
  modelId: string,
  providerId: string,
  connectionId?: string
) {
  await setLKGPPins(comboName, [modelId], providerId, connectionId);
}

/** Related routing hints commit together; each success has a distinct ABA token. */
export async function setLKGPPins(
  comboName: string,
  modelIds: readonly string[],
  providerId: string,
  connectionId?: string
): Promise<void> {
  const keys = [...new Set(modelIds.map((modelId) => `${comboName}:${modelId}`))];
  const value = JSON.stringify({
    provider: providerId,
    ...(connectionId ? { connectionId } : {}),
    revision: randomUUID(),
  });
  const postgres = getSelectedPostgresDatabase();
  try {
    if (postgres) {
      await withPostgresKeyValueMutation(postgres, async (transaction) => {
        for (const key of keys) await writePostgresKeyValue(transaction, "lkgp", key, value);
      });
    } else {
      const db = getDbInstance();
      const insert = db.prepare(
        "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('lkgp', ?, ?)"
      );
      db.transaction(() => {
        for (const key of keys) insert.run(key, value);
      })();
    }
  } finally {
    // An unknown COMMIT also invalidates the cache; it never authorizes a write replay.
    await invalidatePostgresPins(keys);
  }
}

/** Capture before target dispatch; background effects retain data, never a scoped DB handle. */
export async function observeLKGPPins(comboName: string): Promise<LKGPObservation> {
  const prefix = `${comboName}:`;
  const postgres = getSelectedPostgresDatabase();
  const rows = postgres
    ? await postgres.transaction(
        async (tx) =>
          (
            await tx.query<{ key: string; value: string }>(
              "SELECT key,value FROM omniroute.key_value WHERE namespace='lkgp' AND starts_with(key,$1)",
              [prefix]
            )
          ).rows
      )
    : (getDbInstance()
        .prepare(
          "SELECT key,value FROM key_value WHERE namespace='lkgp' AND key LIKE ? ESCAPE '\\'"
        )
        .all(`${escapeLikePattern(prefix)}%`) as Array<{ key: string; value: string }>);
  return new Map(rows.map((row) => [row.key, row.value]));
}

/** A delayed failure can invalidate only the exact hint observed before that attempt. */
export async function clearObservedLKGPPins(
  comboName: string,
  executionKey: string | null | undefined,
  comboId: string | null | undefined,
  observed: LKGPObservation,
  failed?: { provider?: string | null; connectionId?: string | null } | null
): Promise<void> {
  const comboKey = `${comboName}:${comboId || comboName}`;
  const keys = new Set(executionKey ? [`${comboName}:${executionKey}`] : []);
  const comboValue = observed.get(comboKey);
  if (comboValue !== undefined) {
    const pin = decodePin(comboValue);
    if (
      !failed?.provider ||
      (pin.provider === failed.provider &&
        (!pin.connectionId || !failed.connectionId || pin.connectionId === failed.connectionId))
    )
      keys.add(comboKey);
  }
  const expected = [...keys].flatMap((key) => {
    const value = observed.get(key);
    return value === undefined ? [] : [{ key, value }];
  });
  if (!expected.length) return;
  const postgres = getSelectedPostgresDatabase();
  const removed: string[] = [];
  try {
    if (postgres) {
      await withPostgresKeyValueMutation(postgres, async (tx) => {
        for (const { key, value } of expected) {
          const result = await tx.query(
            "DELETE FROM omniroute.key_value WHERE namespace='lkgp' AND key=$1 AND value=$2",
            [key, value]
          );
          if (result.rowCount) removed.push(key);
        }
      });
    } else {
      const db = getDbInstance();
      const erase = db.prepare(
        "DELETE FROM key_value WHERE namespace='lkgp' AND key=? AND value=?"
      );
      db.transaction(() => {
        for (const { key, value } of expected) if (erase.run(key, value).changes) removed.push(key);
      })();
    }
  } catch (error) {
    // An unknown COMMIT must not leave a known-stale local hint or replay the mutation.
    await invalidatePostgresPins(expected.map(({ key }) => key));
    throw error;
  }
  await invalidatePostgresPins(removed);
}

export function clearAllLKGP(): void {
  const db = getDbInstance();
  db.prepare("DELETE FROM key_value WHERE namespace = 'lkgp'").run();
}

/**
 * Delete one persisted LKGP pin after its target fails. `setLKGP` is only ever
 * called on success — nothing previously invalidated a pin once its provider
 * started failing, so a *separate* subsequent request kept re-selecting the
 * same just-failed provider via `applyStrategyOrdering.ts`'s LKGP reordering
 * (live incident: 3 consecutive requests all picked the same timed-out
 * opencode-zen/big-pickle target instead of failing over to another combo
 * model). Circuit breaker / model lockout deliberately don't react to this
 * failure class (request-scoped timeouts, see comboPredicates.ts), so nothing
 * else clears the stale pin.
 */
export async function clearLKGP(comboName: string, modelId: string): Promise<void> {
  const key = `${comboName}:${modelId}`;
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    await withPostgresKeyValueMutation(postgres, (transaction) =>
      deletePostgresKeyValue(transaction, "lkgp", key)
    );
  } else {
    getDbInstance().prepare("DELETE FROM key_value WHERE namespace = 'lkgp' AND key = ?").run(key);
  }
  const { invalidateCachedLKGP } = await import("../readCache");
  invalidateCachedLKGP(key);
}

/**
 * Delete every persisted LKGP pin belonging to a combo. Pins are keyed by
 * `${comboName}:${modelId}`, so deleting a combo leaves its pins addressable by
 * a name that no longer resolves — `clearAllLKGP()` is too broad and
 * `clearLKGP()` needs a modelId the caller no longer knows. Combo delete paths
 * call this so the pins die with their combo instead of accumulating as
 * unreachable rows.
 */
export async function deleteLKGPByComboName(comboName: string): Promise<number> {
  const staleKeys = await deleteLKGPRowsByComboNameAsync(comboName);

  if (staleKeys.length === 0) return 0;

  const { invalidateCachedLKGP } = await import("../readCache");
  for (const key of staleKeys) {
    invalidateCachedLKGP(key);
  }

  return staleKeys.length;
}

/**
 * Delete persisted LKGP pins whose connectionId references a removed provider
 * connection (#8887). A pin persisted by `setLKGP()` carries the connection it
 * was learned from, so deleting that connection leaves the pin pointing at a
 * row that no longer exists; provider-connection delete paths call this so
 * the pin dies with its connection instead of becoming unbounded stale state.
 * Provider-level pins and legacy/unparseable values are preserved.
 */
export async function deleteLKGPByConnectionIds(connectionIds: string[]): Promise<number> {
  if (connectionIds.length === 0) return 0;

  const deletedConnectionIds = new Set(connectionIds.filter(Boolean));
  if (deletedConnectionIds.size === 0) return 0;

  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const keys = await withPostgresKeyValueMutation(postgres, async (transaction) => {
      const rows = await listPostgresKeyValues(transaction, "lkgp");
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
        const connectionId = (parsed as LKGPRecord).connectionId;
        if (typeof connectionId === "string" && deletedConnectionIds.has(connectionId))
          keys.push(row.key);
      }
      for (const key of keys) await deletePostgresKeyValue(transaction, "lkgp", key);
      return keys;
    });
    await invalidatePostgresPins(keys);
    return keys.length;
  }
  const db = getDbInstance();
  const rows = db
    .prepare("SELECT key, value FROM key_value WHERE namespace = 'lkgp'")
    .all() as Array<{ key?: string; value?: string }>;

  const staleKeys: string[] = [];

  for (const row of rows) {
    if (!row?.key || !row.value) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(row.value);
    } catch {
      continue;
    }

    if (typeof parsed !== "object" || parsed === null) continue;

    const connectionId = (parsed as LKGPRecord).connectionId;
    if (typeof connectionId === "string" && deletedConnectionIds.has(connectionId)) {
      staleKeys.push(row.key);
    }
  }

  if (staleKeys.length === 0) return 0;

  const deleteStatement = db.prepare("DELETE FROM key_value WHERE namespace = 'lkgp' AND key = ?");
  for (const key of staleKeys) {
    deleteStatement.run(key);
  }

  const { invalidateCachedLKGP } = await import("../readCache");
  for (const key of staleKeys) {
    invalidateCachedLKGP(key);
  }

  return staleKeys.length;
}

async function invalidatePostgresPins(keys: readonly string[]): Promise<void> {
  if (keys.length === 0) return;
  const { invalidateCachedLKGP } = await import("../readCache");
  for (const key of keys) invalidateCachedLKGP(key);
}

export async function deleteLKGPRowsByComboNameAsync(comboName: string): Promise<string[]> {
  const postgres = getSelectedPostgresDatabase();
  if (!postgres) return deleteLKGPRowsByComboName(comboName);
  if (!comboName) return [];
  return withPostgresKeyValueMutation(postgres, async (transaction) => {
    const { rows } = await transaction.query<{ key: string }>(
      `DELETE FROM omniroute.key_value WHERE namespace = 'lkgp' AND
       starts_with(translate(key, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),
       translate($1::text, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')) RETURNING key`,
      [`${comboName}:`]
    );
    return rows.map((row) => row.key).filter(Boolean);
  });
}

export async function clearAllLKGPAsync(): Promise<void> {
  const postgres = getSelectedPostgresDatabase();
  if (!postgres) return clearAllLKGP();
  await withPostgresKeyValueMutation(postgres, (transaction) =>
    deletePostgresKeyValueNamespace(transaction, "lkgp")
  );
}
