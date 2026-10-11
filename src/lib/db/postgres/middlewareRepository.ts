import type { HookConfig, HookConfigRow, HookLogEntry } from "@/lib/middleware/types";
import type { PostgresDatabase, PostgresTransaction } from "./database";

function decode(row: HookConfigRow): HookConfig {
  return {
    name: row.name,
    description: row.description,
    priority: row.priority,
    scope:
      row.scope_type === "combo" && row.combo_id
        ? { type: "combo", comboId: row.combo_id }
        : { type: "global" },
    enabled: row.enabled === 1,
    code: row.code,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    runCount: Number(row.run_count),
    lastError: row.last_error || undefined,
  };
}

async function get(transaction: PostgresTransaction, name: string) {
  const result = await transaction.query<HookConfigRow>(
    "SELECT * FROM omniroute.middleware_hooks WHERE name = $1",
    [name]
  );
  return result.rows[0] ? decode(result.rows[0]) : undefined;
}

export function createPostgresMiddlewareRepository(database: PostgresDatabase) {
  return {
    async list(enabledOnly = false): Promise<HookConfig[]> {
      return database.transaction(async (transaction) => {
        const result = await transaction.query<HookConfigRow>(
          `SELECT * FROM omniroute.middleware_hooks ${enabledOnly ? "WHERE enabled = 1" : ""}
           ORDER BY priority ASC, name COLLATE "C" ASC`
        );
        return result.rows.map(decode);
      });
    },
    get(name: string) {
      return database.transaction((transaction) => get(transaction, name));
    },
    async create(config: HookConfig): Promise<HookConfig> {
      return database.transaction(async (transaction) => {
        const now = new Date().toISOString();
        await transaction.query(
          `INSERT INTO omniroute.middleware_hooks
           (name, description, priority, scope_type, combo_id, enabled, code,
            created_at, updated_at, run_count, last_error)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$10)`,
          [
            config.name,
            config.description,
            config.priority,
            config.scope.type,
            config.scope.type === "combo" ? config.scope.comboId : null,
            config.enabled ? 1 : 0,
            config.code,
            now,
            config.runCount || 0,
            config.lastError ?? null,
          ]
        );
        return (await get(transaction, config.name))!;
      });
    },
    async update(name: string, updates: Partial<HookConfig>): Promise<HookConfig | undefined> {
      return database.transaction(async (transaction) => {
        const locked = await transaction.query<HookConfigRow>(
          "SELECT * FROM omniroute.middleware_hooks WHERE name = $1 FOR UPDATE",
          [name]
        );
        if (!locked.rows[0]) return undefined;
        const current = decode(locked.rows[0]);
        const next = { ...current, ...updates, name, updatedAt: new Date().toISOString() };
        await transaction.query(
          `UPDATE omniroute.middleware_hooks SET description = $2, priority = $3,
           scope_type = $4, combo_id = $5, enabled = $6, code = $7,
           updated_at = $8, run_count = $9, last_error = $10 WHERE name = $1`,
          [
            name,
            next.description,
            next.priority,
            next.scope.type,
            next.scope.type === "combo" ? next.scope.comboId : null,
            next.enabled ? 1 : 0,
            next.code,
            next.updatedAt,
            next.runCount,
            next.lastError ?? null,
          ]
        );
        return (await get(transaction, name))!;
      });
    },
    async delete(name: string): Promise<boolean> {
      return database.transaction(async (transaction) => {
        const result = await transaction.query(
          "DELETE FROM omniroute.middleware_hooks WHERE name = $1",
          [name]
        );
        return (result.rowCount ?? 0) > 0;
      });
    },
    async recordExecution(name: string, error?: string): Promise<void> {
      await database.transaction(async (transaction) => {
        await transaction.query(
          `UPDATE omniroute.middleware_hooks SET run_count = run_count + 1,
           last_error = $2, updated_at = $3 WHERE name = $1`,
          [name, error || null, new Date().toISOString()]
        );
      });
    },
    async logs(hookName?: string, limit = 50): Promise<HookLogEntry[]> {
      const boundedLimit = Number.isFinite(limit)
        ? Math.max(0, Math.min(1000, Math.floor(limit)))
        : 50;
      return database.transaction(async (transaction) => {
        const result = await transaction.query<{
          id: string;
          hook_name: string;
          request_id: string;
          duration_ms: number;
          mutated: number;
          skipped: number;
          error: string | null;
          timestamp: string;
        }>(
          `SELECT * FROM omniroute.middleware_logs
           ${hookName ? "WHERE hook_name = $2" : ""} ORDER BY timestamp DESC, id COLLATE "C" DESC LIMIT $1`,
          hookName ? [boundedLimit, hookName] : [boundedLimit]
        );
        return result.rows.map((row) => ({
          id: row.id,
          hookName: row.hook_name,
          requestId: row.request_id,
          durationMs: row.duration_ms,
          mutated: row.mutated === 1,
          skipped: row.skipped === 1,
          error: row.error ?? undefined,
          timestamp: row.timestamp,
        }));
      });
    },
  };
}
