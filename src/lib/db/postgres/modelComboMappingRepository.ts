import { v4 as uuidv4 } from "uuid";
import type {
  ComboRecord,
  ModelComboMapping,
  ModelComboMappingRepository,
} from "@/domain/persistence/comboRepositories";
import { globToRegex } from "@/shared/utils/globPattern";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { lockRouting } from "./routingSemantics";
import { advanceApiKeyPolicyEpoch, lockApiKeyPolicy } from "./apiKeyPolicyState";

interface MappingRow {
  id: string;
  pattern: string;
  combo_id: string;
  combo_name?: string;
  priority: number;
  enabled: number;
  description: string;
  created_at: string;
  updated_at: string;
}

const MAPPING_COLUMNS = `m.id, m.pattern, m.combo_id, c.name AS combo_name,
  m.priority, m.enabled, m.description, m.created_at, m.updated_at`;
const MAPPING_ORDER =
  'm.priority DESC NULLS LAST, m.created_at COLLATE "C" ASC, m.insertion_order ASC';

function rowToMapping(row: MappingRow): ModelComboMapping {
  return {
    id: row.id,
    pattern: row.pattern,
    comboId: row.combo_id,
    comboName: row.combo_name || undefined,
    priority: row.priority,
    enabled: row.enabled === 1,
    description: row.description || "",
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function findMapping(
  transaction: PostgresTransaction,
  id: string,
  forUpdate = false
): Promise<ModelComboMapping | null> {
  const { rows } = await transaction.query<MappingRow>(
    `SELECT ${MAPPING_COLUMNS} FROM omniroute.model_combo_mappings m
     LEFT JOIN omniroute.combos c ON c.id = m.combo_id
     WHERE m.id = $1${forUpdate ? " FOR UPDATE OF m" : ""}`,
    [id]
  );
  return rows[0] ? rowToMapping(rows[0]) : null;
}

export function createPostgresModelComboMappingRepository(
  database: PostgresDatabase
): ModelComboMappingRepository {
  return {
    list: (options) =>
      database.transaction(async (transaction) => {
        // Prevent writes between the page and total queries, including an empty page.
        await lockRouting(transaction, true);
        let sql = `SELECT ${MAPPING_COLUMNS} FROM omniroute.model_combo_mappings m
          LEFT JOIN omniroute.combos c ON c.id = m.combo_id ORDER BY ${MAPPING_ORDER}`;
        const parameters: unknown[] = [];
        if (options?.limit !== undefined) {
          sql +=
            " LIMIT CASE WHEN $1::bigint < 0 THEN NULL ELSE $1::bigint END OFFSET GREATEST($2::bigint, 0)";
          parameters.push(options.limit, options.offset ?? 0);
        }
        const { rows } = await transaction.query<MappingRow>(sql, parameters);
        const count = await transaction.query<{ count: string }>(
          "SELECT count(*) AS count FROM omniroute.model_combo_mappings"
        );
        return { items: rows.map(rowToMapping), total: Number(count.rows[0].count) };
      }),

    findById: (id) => database.transaction((transaction) => findMapping(transaction, id)),

    create: (data) =>
      database.transaction(async (transaction) => {
        await lockRouting(transaction);
        await lockApiKeyPolicy(transaction);
        const now = new Date().toISOString();
        const id = uuidv4();
        await transaction.query(
          `INSERT INTO omniroute.model_combo_mappings
           (id, pattern, combo_id, priority, enabled, description, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            id,
            data.pattern,
            data.comboId,
            Number.isNaN(data.priority) ? null : (data.priority ?? 0),
            data.enabled !== false ? 1 : 0,
            data.description || "",
            now,
            now,
          ]
        );
        await advanceApiKeyPolicyEpoch(transaction);
        // The SQLite create result omits joined comboName; subsequent reads include it.
        return {
          id,
          pattern: data.pattern,
          comboId: data.comboId,
          priority: data.priority ?? 0,
          enabled: data.enabled !== false,
          description: data.description || "",
          createdAt: now,
          updatedAt: now,
        };
      }),

    update: (id, data) =>
      database.transaction(async (transaction) => {
        await lockRouting(transaction);
        await lockApiKeyPolicy(transaction);
        const existing = await findMapping(transaction, id, true);
        if (!existing) return null;
        await transaction.query(
          `UPDATE omniroute.model_combo_mappings SET pattern = $1, combo_id = $2, priority = $3,
           enabled = $4, description = $5, updated_at = $6 WHERE id = $7`,
          [
            data.pattern ?? existing.pattern,
            data.comboId ?? existing.comboId,
            Number.isNaN(data.priority) ? null : (data.priority ?? existing.priority),
            data.enabled !== undefined ? (data.enabled ? 1 : 0) : existing.enabled ? 1 : 0,
            data.description ?? existing.description,
            new Date().toISOString(),
            id,
          ]
        );
        await advanceApiKeyPolicyEpoch(transaction);
        return findMapping(transaction, id);
      }),

    deleteById: (id) =>
      database.transaction(async (transaction) => {
        await lockRouting(transaction);
        await lockApiKeyPolicy(transaction);
        const result = await transaction.query(
          "DELETE FROM omniroute.model_combo_mappings WHERE id = $1",
          [id]
        );
        if ((result.rowCount ?? 0) > 0) await advanceApiKeyPolicyEpoch(transaction);
        return (result.rowCount ?? 0) > 0;
      }),

    resolveForModel: (model) =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query<{ pattern: string; combo_data: string }>(
          `SELECT m.pattern, c.data AS combo_data FROM omniroute.model_combo_mappings m
           JOIN omniroute.combos c ON c.id = m.combo_id WHERE m.enabled = 1
           ORDER BY ${MAPPING_ORDER}`
        );
        for (const row of rows) {
          if (!globToRegex(row.pattern).test(model)) continue;
          try {
            // Deliberately return the stored blob, preserving legacy resolver semantics.
            const combo = JSON.parse(row.combo_data) as ComboRecord;
            if (combo.isActive === false) continue;
            return combo;
          } catch {
            // Malformed matching blobs are skipped so lower-priority mappings can serve.
          }
        }
        return null;
      }),
  };
}
