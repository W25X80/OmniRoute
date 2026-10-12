import { v4 as uuidv4 } from "uuid";
import { advanceApiKeyPolicyEpoch, lockApiKeyPolicy } from "./apiKeyPolicyState";
import { lockPostgresKeyValueMutation } from "./keyValueRepository";
import type { ComboRecord, ComboRepository } from "@/domain/persistence/comboRepositories";
import { validateComboInvariant } from "@/lib/combos/invariants";
import { normalizeComboRecord } from "@/lib/combos/steps";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import {
  asciiFoldSql,
  COMBO_ORDER_SQL,
  listCombos,
  lockRouting,
  normalizeStoredCombo,
  parseComboRow,
  type ComboRow,
} from "./routingSemantics";

/** Required application callbacks; isolated tests supply explicit inert callbacks. */
export interface PostgresComboEffects {
  bumpProxyRegistryGeneration(): void | Promise<void>;
  invalidateCachedLKGP(key: string): void | Promise<void>;
}

export class PostgresComboEffectsError extends Error {
  readonly committed = true;

  constructor(readonly failedEffects: readonly string[]) {
    super("Combo deletion committed but cache invalidation failed");
    this.name = "PostgresComboEffectsError";
  }
}

async function lockComboMutation(transaction: PostgresTransaction): Promise<void> {
  await lockRouting(transaction);
  await lockPostgresKeyValueMutation(transaction);
  await lockApiKeyPolicy(transaction);
}

async function nextSortOrder(transaction: PostgresTransaction): Promise<number> {
  const { rows } = await transaction.query<{ sort_order: number }>(
    "SELECT COALESCE(MAX(sort_order), 0) AS sort_order FROM omniroute.combos"
  );
  return rows[0].sort_order + 1;
}

export function createPostgresComboRepository(
  database: PostgresDatabase,
  effects: PostgresComboEffects
): ComboRepository {
  async function find(
    predicate: string,
    value: string,
    extraName: (combo: ComboRecord) => string[]
  ): Promise<ComboRecord | null> {
    return database.transaction(async (transaction) => {
      await lockRouting(transaction, true);
      const { rows } = await transaction.query<ComboRow>(
        `SELECT id, data, sort_order, context_cache_protection FROM omniroute.combos
         WHERE ${predicate} ORDER BY insertion_order ASC LIMIT 1`,
        [value]
      );
      const combo = parseComboRow(rows[0]);
      return combo ? normalizeStoredCombo(combo, transaction, extraName(combo)) : null;
    });
  }

  return {
    list: (limit, offset) =>
      database.transaction((transaction) => listCombos(transaction, limit, offset)),

    count: () =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query<{ count: string }>(
          "SELECT count(*) AS count FROM omniroute.combos"
        );
        return Number(rows[0].count);
      }),

    findById: (id) =>
      find("id = $1", id, (combo) => (typeof combo.name === "string" ? [combo.name] : [])),

    findByName: (name) => find('name COLLATE "C" = $1 COLLATE "C"', name, () => [name]),

    findByNameInsensitive: (name) =>
      find(
        `${asciiFoldSql("name")} COLLATE "C" = ${asciiFoldSql("$1::text")} COLLATE "C"`,
        name,
        (combo) => [typeof combo.name === "string" ? combo.name : name]
      ),

    create: (data) =>
      database.transaction(async (transaction) => {
        await lockComboMutation(transaction);
        const now = new Date().toISOString();
        const sortOrder =
          typeof data.sortOrder === "number" ? data.sortOrder : await nextSortOrder(transaction);
        const comboId =
          typeof data.id === "string" && data.id.trim().length > 0 ? data.id : uuidv4();
        const combo = await normalizeStoredCombo(
          {
            ...data,
            id: comboId,
            name: data.name,
            models: data.models || [],
            strategy: data.strategy || "priority",
            config: data.config || {},
            isHidden: Boolean(data.isHidden),
            sortOrder,
            createdAt: now,
            updatedAt: now,
          },
          transaction,
          typeof data.name === "string" ? [data.name] : []
        );
        validateComboInvariant(combo);
        await transaction.query(
          `INSERT INTO omniroute.combos
           (id, name, data, sort_order, created_at, updated_at, context_cache_protection)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            combo.id,
            combo.name,
            JSON.stringify(combo),
            Number.isNaN(sortOrder) ? null : sortOrder,
            now,
            now,
            data.context_cache_protection ? 1 : 0,
          ]
        );
        await advanceApiKeyPolicyEpoch(transaction);
        return combo;
      }),

    update: (id, data) =>
      database.transaction(async (transaction) => {
        await lockComboMutation(transaction);
        if (data.models !== undefined) {
          // Lock order: routing -> key-value -> policy -> session history. Session
          // writers/fences take only the last lock, before touching history rows.
          await transaction.query("SELECT pg_advisory_xact_lock(18765,13)");
        }
        const { rows } = await transaction.query<ComboRow>(
          `SELECT id, data, sort_order, context_cache_protection FROM omniroute.combos
           WHERE id = $1 FOR UPDATE`,
          [id]
        );
        const current = parseComboRow(rows[0]);
        if (!current) return null;
        const sortOrder =
          typeof data.sortOrder === "number"
            ? data.sortOrder
            : typeof current.sortOrder === "number"
              ? current.sortOrder
              : await nextSortOrder(transaction);
        const merged: ComboRecord = {
          ...current,
          ...data,
          sortOrder,
          updatedAt: new Date().toISOString(),
        };
        for (const key of Object.keys(data)) {
          if (data[key] === null) delete merged[key];
        }
        const currentName = typeof current.name === "string" ? current.name : "";
        const nextName =
          typeof merged.name === "string" && merged.name.trim().length > 0
            ? merged.name
            : currentName;
        const normalized = await normalizeStoredCombo({ ...merged, name: nextName }, transaction, [
          nextName,
        ]);
        validateComboInvariant({
          ...normalized,
          ...data,
          name: nextName,
          models: normalized.models,
        });
        await transaction.query(
          `UPDATE omniroute.combos SET name = $1, data = $2, sort_order = $3,
           updated_at = $4, context_cache_protection = $5 WHERE id = $6`,
          [
            nextName,
            JSON.stringify(normalized),
            Number.isNaN(sortOrder) ? null : sortOrder,
            normalized.updatedAt,
            normalized.context_cache_protection ? 1 : 0,
            id,
          ]
        );
        if (data.models !== undefined) {
          const cleared = await transaction.query(
            "DELETE FROM omniroute.session_model_history WHERE combo_name=$1",
            [currentName]
          );
          // Preserve the facade's existing rename predicate, including zero old pins.
          if ((cleared.rowCount ?? 0) > 0 && nextName !== currentName) {
            await transaction.query(
              "DELETE FROM omniroute.session_model_history WHERE combo_name=$1",
              [nextName]
            );
          }
        }
        await advanceApiKeyPolicyEpoch(transaction);
        return {
          combo: normalized,
          previousName: currentName,
          currentName: nextName,
          modelsFieldProvided: data.models !== undefined,
        };
      }),

    reorder: (comboIds) =>
      database.transaction(async (transaction) => {
        await lockComboMutation(transaction);
        // SQLite reorder does not select the dedicated context-cache column.
        const { rows } = await transaction.query<ComboRow>(
          `SELECT id, name, data, sort_order FROM omniroute.combos
           ORDER BY ${COMBO_ORDER_SQL} FOR UPDATE`
        );
        if (rows.length === 0) return { combos: [], rowsReordered: 0 };
        const existing = new Set(rows.map((row) => row.id));
        const seen = new Set<string>();
        const requested = comboIds.filter((id) => {
          if (!existing.has(id) || seen.has(id)) return false;
          seen.add(id);
          return true;
        });
        const ordered = [...requested, ...rows.map((row) => row.id).filter((id) => !seen.has(id))];
        const byId = new Map(rows.map((row) => [row.id, row]));
        const names = rows
          .map((row) => {
            const combo = parseComboRow(row);
            return combo && typeof combo.name === "string" ? combo.name.trim() : "";
          })
          .filter((name) => name.length > 0);
        const now = new Date().toISOString();
        for (const [index, id] of ordered.entries()) {
          const combo = parseComboRow(byId.get(id));
          if (!combo) continue;
          const sortOrder = index + 1;
          const updated = normalizeComboRecord(
            { ...combo, sortOrder, updatedAt: now },
            { allCombos: names }
          );
          await transaction.query(
            "UPDATE omniroute.combos SET data = $1, sort_order = $2, updated_at = $3 WHERE id = $4",
            [JSON.stringify(updated), sortOrder, now, id]
          );
        }
        await advanceApiKeyPolicyEpoch(transaction);
        return { combos: await listCombos(transaction), rowsReordered: ordered.length };
      }),

    async deleteById(id) {
      const receipt = await database.transaction(async (transaction) => {
        await lockComboMutation(transaction);
        const { rows } = await transaction.query<{ name: string }>(
          "DELETE FROM omniroute.combos WHERE id = $1 RETURNING name",
          [id]
        );
        if (rows.length === 0) return { deleted: false, purged: 0, keys: [] as string[] };
        // FK cascade removes mappings in the same transaction as this collateral state.
        const purged = await transaction.query(
          "DELETE FROM omniroute.proxy_assignments WHERE scope = 'combo' AND scope_id = $1",
          [id]
        );
        await transaction.query(
          "DELETE FROM omniroute.proxy_scope_rotation WHERE scope = 'combo' AND scope_id = $1",
          [id]
        );
        const name = rows[0].name;
        const lkgp = name
          ? await transaction.query<{ key: string }>(
              `DELETE FROM omniroute.key_value WHERE namespace = 'lkgp'
               AND starts_with(${asciiFoldSql("key")}, ${asciiFoldSql("$1::text")}) RETURNING key`,
              [`${name}:`]
            )
          : { rows: [] as Array<{ key: string }> };
        await advanceApiKeyPolicyEpoch(transaction);
        return {
          deleted: true,
          purged: purged.rowCount ?? 0,
          keys: lkgp.rows.map((row) => row.key).filter(Boolean),
        };
      });
      if (!receipt.deleted) return false;
      // COMMIT acknowledgement precedes effects. Unknown commit outcomes never replay work.
      const failed: string[] = [];
      if (receipt.purged > 0) {
        try {
          await effects.bumpProxyRegistryGeneration();
        } catch {
          failed.push("proxy_registry");
        }
      }
      // Independent invalidations must all run, even if an earlier callback rejects.
      for (const key of receipt.keys) {
        try {
          await effects.invalidateCachedLKGP(key);
        } catch {
          failed.push("lkgp");
        }
      }
      if (failed.length > 0) throw new PostgresComboEffectsError([...new Set(failed)]);
      return true;
    },
  };
}
