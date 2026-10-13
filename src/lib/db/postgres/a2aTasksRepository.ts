import type {
  A2AHistoryFilter,
  A2ATaskHistoryRow,
  A2ATaskHistoryEventRow,
  UpsertA2ATaskInput,
} from "../a2aTasks";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

const COLUMNS =
  "id,state,skill_id,input_json,output_json,api_key_id,created_at,updated_at,completed_at";
function historyFilter(
  filter: Pick<A2AHistoryFilter, "from" | "to" | "skill" | "state" | "owner">
) {
  const values: unknown[] = [],
    clauses: string[] = [];
  for (const [field, column, operator] of [
    ["from", "created_at", ">="],
    ["to", "created_at", "<="],
    ["skill", "skill_id", "="],
    ["state", "state", "="],
  ] as const) {
    if (filter[field] !== undefined) {
      values.push(filter[field]);
      clauses.push(`${column} ${operator} $${values.length}`);
    }
  }
  if (filter.owner !== undefined) {
    values.push(filter.owner);
    clauses.push(`(api_key_id IS NULL OR api_key_id=$${values.length})`);
  }
  return { values, where: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "" };
}

export function createPostgresA2ATasksRepository(database: PostgresDatabase) {
  function operation<T>(write: boolean, body: (tx: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "a2a", write ? "exclusive" : "shared");
      return body(tx);
    });
  }
  return {
    persistA2ATaskHistory: (row: UpsertA2ATaskInput, eventType: string, dataJson?: string) =>
      operation(true, async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.a2a_tasks (${COLUMNS}) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT(id) DO UPDATE SET state=excluded.state,output_json=excluded.output_json,updated_at=excluded.updated_at,completed_at=excluded.completed_at`,
          [
            row.id,
            row.state,
            row.skillId,
            row.inputJson,
            row.outputJson,
            row.apiKeyId,
            row.createdAt,
            row.updatedAt,
            row.completedAt,
          ]
        );
        await tx.query(
          "INSERT INTO omniroute.a2a_task_events(task_id,event_type,data_json) VALUES($1,$2,$3)",
          [row.id, eventType, dataJson ?? null]
        );
      }),
    upsertA2ATask: (row: UpsertA2ATaskInput) =>
      operation(true, async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.a2a_tasks (${COLUMNS}) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT(id) DO UPDATE SET state=excluded.state,output_json=excluded.output_json,updated_at=excluded.updated_at,completed_at=excluded.completed_at`,
          [
            row.id,
            row.state,
            row.skillId,
            row.inputJson,
            row.outputJson,
            row.apiKeyId,
            row.createdAt,
            row.updatedAt,
            row.completedAt,
          ]
        );
      }),
    appendA2ATaskEvent: (taskId: string, eventType: string, dataJson?: string) =>
      operation(true, async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.a2a_task_events(task_id,event_type,data_json) VALUES($1,$2,$3)",
          [taskId, eventType, dataJson ?? null]
        );
      }),
    listA2ATaskEvents: (taskId: string) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<A2ATaskHistoryEventRow>(
              "SELECT event_type,data_json,created_at FROM omniroute.a2a_task_events WHERE task_id=$1 ORDER BY id",
              [taskId]
            )
          ).rows
      ),
    listA2ATaskHistory: (filter: A2AHistoryFilter) =>
      operation(false, async (tx) => {
        const { where, values } = historyFilter(filter);
        const total = Number(
          (
            await tx.query<{ count: string }>(
              `SELECT count(*) AS count FROM omniroute.a2a_tasks ${where}`,
              values
            )
          ).rows[0].count
        );
        const rows = (
          await tx.query<A2ATaskHistoryRow>(
            `SELECT ${COLUMNS} FROM omniroute.a2a_tasks ${where} ORDER BY created_at DESC,insertion_order LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
            [...values, filter.limit, filter.offset]
          )
        ).rows;
        return { rows, total };
      }),
    getA2ATaskHistoryById: (id: string, owner?: string): Promise<A2ATaskHistoryRow | null> =>
      operation(false, async (tx) => {
        const { where, values } = historyFilter({ owner });
        values.push(id);
        return (
          (
            await tx.query<A2ATaskHistoryRow>(
              `SELECT ${COLUMNS} FROM omniroute.a2a_tasks ${where ? `${where} AND` : "WHERE"} id=$${values.length}`,
              values
            )
          ).rows[0] ?? null
        );
      }),
    purgeA2AHistory: (days: number) =>
      !Number.isFinite(days) || days < 0
        ? Promise.resolve(0)
        : operation(true, async (tx) => {
            const cutoff = `to_char((CURRENT_TIMESTAMP - $1::double precision * INTERVAL '1 day') AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`;
            await tx.query(
              `DELETE FROM omniroute.a2a_task_events WHERE task_id IN (SELECT id FROM omniroute.a2a_tasks WHERE created_at < ${cutoff})`,
              [days]
            );
            return (
              (
                await tx.query(`DELETE FROM omniroute.a2a_tasks WHERE created_at < ${cutoff}`, [
                  days,
                ])
              ).rowCount ?? 0
            );
          }),
  };
}
