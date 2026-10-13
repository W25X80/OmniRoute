import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase, getSelectedPersistenceIdentity } from "./controlPlaneContext";
import type { PostgresTransaction } from "./postgres/database";
import { acquirePostgresLock } from "./postgres/locks";

export interface SkillStorageRow {
  id: string;
  api_key_id: string;
  name: string;
  version: string;
  description: string | null;
  schema: string;
  handler: string;
  enabled: number;
  mode: string;
  source_provider: string | null;
  tags: string | null;
  install_count: number;
  created_at: string;
  updated_at: string;
}
export interface SkillExecutionStorageRow {
  id: string;
  skill_id: string;
  api_key_id: string;
  session_id: string | null;
  input: string;
  output: string | null;
  status: string;
  error_message: string | null;
  duration_ms: number | null;
  created_at: string;
}
const SKILL_COLUMNS =
  "id,api_key_id,name,version,description,schema,handler,enabled,mode,source_provider,tags,install_count,created_at,updated_at";
const EXECUTION_COLUMNS =
  "id,skill_id,api_key_id,session_id,input,output,status,error_message,duration_ms,created_at";
const sqliteAuthority = {};
/** Opaque cache partition token, never a driver/database escape hatch. */
export function getSkillStorageAuthority(): object {
  return getSelectedPersistenceIdentity() ?? sqliteAuthority;
}
async function native<T>(
  write: boolean,
  body: (tx: PostgresTransaction) => Promise<T>
): Promise<T> {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL skill storage is not selected");
  return database.transaction(async (tx) => {
    await acquirePostgresLock(tx, "skills", write ? "exclusive" : "shared");
    return body(tx);
  });
}

export async function insertSkillRowAsync(row: SkillStorageRow): Promise<void> {
  const values = [
    row.id,
    row.api_key_id,
    row.name,
    row.version,
    row.description,
    row.schema,
    row.handler,
    row.enabled,
    row.mode,
    row.source_provider,
    row.tags,
    row.install_count,
    row.created_at,
    row.updated_at,
  ];
  if (getSelectedPostgresDatabase()) {
    await native(true, async (tx) => {
      await tx.query(
        `INSERT INTO omniroute.skills(${SKILL_COLUMNS}) VALUES(${values.map((_, i) => `$${i + 1}`).join(",")})`,
        values
      );
    });
    return;
  }
  getDbInstance()
    .prepare(`INSERT INTO skills(${SKILL_COLUMNS}) VALUES(${values.map(() => "?").join(",")})`)
    .run(...values);
}
export async function deleteSkillRowAsync(id: string): Promise<number> {
  if (getSelectedPostgresDatabase())
    return native(
      true,
      async (tx) => (await tx.query("DELETE FROM omniroute.skills WHERE id=$1", [id])).rowCount ?? 0
    );
  return getDbInstance().prepare("DELETE FROM skills WHERE id=?").run(id).changes;
}
export async function deleteSkillsByNameAsync(name: string, apiKeyId?: string): Promise<number> {
  if (getSelectedPostgresDatabase())
    return native(
      true,
      async (tx) =>
        (
          await tx.query(
            "DELETE FROM omniroute.skills WHERE name=$1 AND ($2::text IS NULL OR api_key_id=$2)",
            [name, apiKeyId || null]
          )
        ).rowCount ?? 0
    );
  return getDbInstance()
    .prepare("DELETE FROM skills WHERE name=? AND (? IS NULL OR api_key_id=?)")
    .run(name, apiKeyId || null, apiKeyId || null).changes;
}
export async function listSkillRowsAsync(owners?: readonly string[]): Promise<SkillStorageRow[]> {
  if (getSelectedPostgresDatabase())
    return native(
      false,
      async (tx) =>
        (
          await tx.query<SkillStorageRow>(
            `SELECT ${SKILL_COLUMNS} FROM omniroute.skills ${owners ? "WHERE api_key_id=ANY($1::text[])" : ""} ORDER BY insertion_order`,
            owners ? [[...owners]] : []
          )
        ).rows
    );
  return (
    owners
      ? getDbInstance()
          .prepare(`SELECT * FROM skills WHERE api_key_id IN (${owners.map(() => "?").join(",")})`)
          .all(...owners)
      : getDbInstance().prepare("SELECT * FROM skills").all()
  ) as SkillStorageRow[];
}
export async function updateSkillEnabledByOwnerAsync(
  id: string,
  owner: string,
  enabled: boolean,
  time: string
): Promise<number> {
  const values = [enabled ? 1 : 0, enabled ? "on" : "off", time, id, owner];
  if (getSelectedPostgresDatabase())
    return native(
      true,
      async (tx) =>
        (
          await tx.query(
            "UPDATE omniroute.skills SET enabled=$1,mode=$2,updated_at=$3 WHERE id=$4 AND api_key_id=$5",
            values
          )
        ).rowCount ?? 0
    );
  return getDbInstance()
    .prepare("UPDATE skills SET enabled=?,mode=?,updated_at=? WHERE id=? AND api_key_id=?")
    .run(...values).changes;
}
export async function insertSkillExecutionAsync(
  row: Pick<
    SkillExecutionStorageRow,
    "id" | "skill_id" | "api_key_id" | "session_id" | "input" | "status" | "created_at"
  >
): Promise<void> {
  const values = [
    row.id,
    row.skill_id,
    row.api_key_id,
    row.session_id,
    row.input,
    row.status,
    row.created_at,
  ];
  if (getSelectedPostgresDatabase()) {
    await native(true, async (tx) => {
      await tx.query(
        "INSERT INTO omniroute.skill_executions(id,skill_id,api_key_id,session_id,input,status,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)",
        values
      );
    });
    return;
  }
  getDbInstance()
    .prepare(
      "INSERT INTO skill_executions(id,skill_id,api_key_id,session_id,input,status,created_at) VALUES(?,?,?,?,?,?,?)"
    )
    .run(...values);
}
export async function updateSkillExecutionAsync(
  id: string,
  patch: Partial<
    Pick<SkillExecutionStorageRow, "output" | "status" | "error_message" | "duration_ms">
  >
): Promise<void> {
  const values: unknown[] = [id],
    fields: string[] = [];
  for (const field of ["output", "status", "error_message", "duration_ms"] as const)
    if (patch[field] !== undefined) {
      values.push(patch[field]);
      fields.push(field);
    }
  if (!fields.length) return;
  if (getSelectedPostgresDatabase()) {
    await native(true, async (tx) => {
      await tx.query(
        `UPDATE omniroute.skill_executions SET ${fields.map((field, i) => `${field}=$${i + 2}`).join(",")} WHERE id=$1`,
        values
      );
    });
    return;
  }
  getDbInstance()
    .prepare(
      `UPDATE skill_executions SET ${fields.map((field) => `${field}=?`).join(",")} WHERE id=?`
    )
    .run(...values.slice(1), id);
}
export async function getSkillExecutionRowAsync(
  id: string
): Promise<SkillExecutionStorageRow | undefined> {
  if (getSelectedPostgresDatabase())
    return native(
      false,
      async (tx) =>
        (
          await tx.query<SkillExecutionStorageRow>(
            `SELECT ${EXECUTION_COLUMNS} FROM omniroute.skill_executions WHERE id=$1`,
            [id]
          )
        ).rows[0]
    );
  return getDbInstance().prepare("SELECT * FROM skill_executions WHERE id=?").get(id) as
    SkillExecutionStorageRow | undefined;
}
export async function listSkillExecutionRowsAsync(
  owner?: string,
  limit = 50,
  offset = 0
): Promise<SkillExecutionStorageRow[]> {
  if (getSelectedPostgresDatabase())
    return native(
      false,
      async (tx) =>
        (
          await tx.query<SkillExecutionStorageRow>(
            `SELECT ${EXECUTION_COLUMNS} FROM omniroute.skill_executions ${owner ? "WHERE api_key_id=$1" : ""} ORDER BY created_at DESC,insertion_order LIMIT $${owner ? 2 : 1} OFFSET $${owner ? 3 : 2}`,
            owner ? [owner, limit, offset] : [limit, offset]
          )
        ).rows
    );
  return (
    owner
      ? getDbInstance()
          .prepare(
            "SELECT * FROM skill_executions WHERE api_key_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?"
          )
          .all(owner, limit, offset)
      : getDbInstance()
          .prepare("SELECT * FROM skill_executions ORDER BY created_at DESC LIMIT ? OFFSET ?")
          .all(limit, offset)
  ) as SkillExecutionStorageRow[];
}
export async function countSkillExecutionRowsAsync(owner?: string): Promise<number> {
  if (getSelectedPostgresDatabase())
    return native(false, async (tx) =>
      Number(
        (
          await tx.query<{ count: string }>(
            `SELECT count(*) AS count FROM omniroute.skill_executions ${owner ? "WHERE api_key_id=$1" : ""}`,
            owner ? [owner] : []
          )
        ).rows[0].count
      )
    );
  const row = (
    owner
      ? getDbInstance()
          .prepare("SELECT count(*) AS count FROM skill_executions WHERE api_key_id=?")
          .get(owner)
      : getDbInstance().prepare("SELECT count(*) AS count FROM skill_executions").get()
  ) as { count: number };
  return row.count;
}

export function getSkillExecutionRow(id: string): SkillExecutionStorageRow | undefined {
  return getDbInstance().prepare("SELECT * FROM skill_executions WHERE id=?").get(id) as
    SkillExecutionStorageRow | undefined;
}
export function listSkillExecutionRows(
  owner?: string,
  limit = 50,
  offset = 0
): SkillExecutionStorageRow[] {
  return (
    owner
      ? getDbInstance()
          .prepare(
            "SELECT * FROM skill_executions WHERE api_key_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?"
          )
          .all(owner, limit, offset)
      : getDbInstance()
          .prepare("SELECT * FROM skill_executions ORDER BY created_at DESC LIMIT ? OFFSET ?")
          .all(limit, offset)
  ) as SkillExecutionStorageRow[];
}
export function countSkillExecutionRows(owner?: string): number {
  const row = (
    owner
      ? getDbInstance()
          .prepare("SELECT COUNT(*) AS count FROM skill_executions WHERE api_key_id=?")
          .get(owner)
      : getDbInstance().prepare("SELECT COUNT(*) AS count FROM skill_executions").get()
  ) as { count: number } | undefined;
  return row?.count ?? 0;
}
