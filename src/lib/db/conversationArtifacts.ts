import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";

export async function getConversationArtifactPaths(
  correlationIds: readonly string[]
): Promise<string[]> {
  if (correlationIds.length === 0) return [];
  const database = getSelectedPostgresDatabase();
  let rows: Array<{ correlation_id: string; artifact_relpath: string }>;
  if (database) {
    rows = await database.transaction(
      async (transaction) =>
        (
          await transaction.query<{ correlation_id: string; artifact_relpath: string }>(
            `SELECT correlation_id,artifact_relpath FROM omniroute.call_logs
       WHERE correlation_id=ANY($1::text[]) AND artifact_relpath IS NOT NULL ORDER BY timestamp ASC`,
            [[...correlationIds]]
          )
        ).rows
    );
  } else {
    rows = getDbInstance()
      .prepare(
        `SELECT correlation_id,artifact_relpath FROM call_logs
      WHERE correlation_id IN (${correlationIds.map(() => "?").join(",")}) AND artifact_relpath IS NOT NULL ORDER BY timestamp ASC`
      )
      .all(...correlationIds) as typeof rows;
  }
  const first = new Map<string, string>();
  for (const row of rows)
    if (!first.has(row.correlation_id)) first.set(row.correlation_id, row.artifact_relpath);
  return [...first.values()];
}
