import type { HookConfigRow } from "../../../src/lib/middleware/types.ts";
import {
  PostgresDatabase,
  type PostgresPool,
  type PostgresClient,
  type PostgresResult,
} from "../../../src/lib/db/postgres/database.ts";

export function barrier() {
  let resolve!: () => void;
  return {
    promise: new Promise<void>((done) => {
      resolve = done;
    }),
    resolve: () => resolve(),
  };
}
export class MiddlewareConflictPool implements PostgresPool {
  rows = new Map<string, HookConfigRow>();
  reads = 0;
  inserts = 0;
  commits = 0;
  releases = 0;
  readsEntered = barrier();
  releaseReads = barrier();
  blockTwoReads = false;
  conflictCommit = undefined as ReturnType<typeof barrier> | undefined;
  conflictCommitEntered = barrier();
  loseConflictCommit = false;
  loseInsertedCommit = false;
  insertSqlState = undefined as string | undefined;
  database = new PostgresDatabase(
    this,
    { maxConnections: 4, transactionTimeoutMs: 5000 },
    { warn() {} }
  );
  on(): void {}
  async end(): Promise<void> {}
  async connect(): Promise<PostgresClient> {
    let duplicate = false;
    let inserted = false;
    return {
      on() {},
      removeListener() {},
      release: () => {
        this.releases++;
      },
      query: async <Row>({
        text,
        values = [],
      }: {
        text: string;
        values?: unknown[];
      }): Promise<PostgresResult<Row>> => {
        if (text === "BEGIN" || text === "ROLLBACK")
          return { rows: [], rowCount: 0, command: text };
        if (text === "COMMIT") {
          this.commits++;
          if (duplicate && this.conflictCommit) {
            this.conflictCommitEntered.resolve();
            await this.conflictCommit.promise;
          }
          if ((duplicate && this.loseConflictCommit) || (inserted && this.loseInsertedCommit)) {
            throw Object.assign(new Error("Connection lost after sent COMMIT"), { code: "EPIPE" });
          }
          return { rows: [], rowCount: 0, command: "COMMIT" };
        }
        if (text.startsWith("SELECT * FROM omniroute.middleware_hooks WHERE name")) {
          const row = this.rows.get(String(values[0]));
          if (this.blockTwoReads && ++this.reads <= 2) {
            if (this.reads === 2) this.readsEntered.resolve();
            await this.releaseReads.promise;
          }
          return { rows: row ? ([row] as Row[]) : [], rowCount: row ? 1 : 0, command: "SELECT" };
        }
        if (text.startsWith("INSERT INTO omniroute.middleware_hooks")) {
          this.inserts++;
          if (this.insertSqlState)
            throw Object.assign(new Error("private driver detail"), {
              code: this.insertSqlState,
              severity: "ERROR",
              constraint: "unrelated_constraint",
            });
          const name = String(values[0]);
          if (this.rows.has(name)) {
            if (!text.includes("ON CONFLICT (name) DO NOTHING"))
              throw Object.assign(new Error("Duplicate name"), {
                code: "23505",
                severity: "ERROR",
                constraint: "middleware_hooks_pkey",
              });
            duplicate = true;
            return { rows: [], rowCount: 0, command: "INSERT" };
          }
          const row: HookConfigRow = {
            name,
            description: String(values[1]),
            priority: Number(values[2]),
            scope_type: values[3] as HookConfigRow["scope_type"],
            combo_id: values[4] === null ? null : String(values[4]),
            enabled: Number(values[5]),
            code: String(values[6]),
            created_at: String(values[7]),
            updated_at: String(values[7]),
            run_count: Number(values[8]),
            last_error: values[9] === null ? null : String(values[9]),
          };
          this.rows.set(name, row);
          inserted = true;
          return {
            rows: text.includes("RETURNING *") ? ([row] as Row[]) : [],
            rowCount: 1,
            command: "INSERT",
          };
        }
        throw new Error(`Unexpected middleware fixture query: ${text}`);
      },
    };
  }
}
// Authentication policy is real; inference-key lookup is the controlled port.
export const managementFixture = { validScopes: ["manage"] as string[], checks: 0 };
export async function getSettings() {
  return { requireLogin: true, setupComplete: true };
}
export function extractApiKey(request: Request): string | null {
  const value = request.headers.get("authorization");
  return value?.startsWith("Bearer ") ? value.slice(7) : null;
}
export async function isValidApiKey(key: string): Promise<boolean> {
  managementFixture.checks++;
  return key === "fixture-managed-key";
}
export async function getApiKeyMetadata(key: string) {
  return key === "fixture-managed-key" ? { scopes: managementFixture.validScopes } : null;
}
