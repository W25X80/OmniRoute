import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { getDbInstance } from "./core";

type DbInstance = ReturnType<typeof getDbInstance>;

const ENCRYPTED_COLUMNS = ["api_key", "access_token", "refresh_token", "id_token"] as const;

const ENCRYPTED_PATTERN = "enc:v1:%";

function buildWhereClause(): string {
  return ENCRYPTED_COLUMNS.map((col) => `${col} LIKE '${ENCRYPTED_PATTERN}'`).join(" OR ");
}

export function countEncryptedCredentials(db: DbInstance = getDbInstance()): number {
  const where = buildWhereClause();
  const row = db
    .prepare(`SELECT COUNT(*) AS cnt FROM provider_connections WHERE ${where}`)
    .get() as { cnt: number } | undefined;
  return row?.cnt ?? 0;
}

export function resetEncryptedColumns(
  { dryRun }: { dryRun: boolean },
  db: DbInstance = getDbInstance()
): { affected: number } {
  const affected = countEncryptedCredentials(db);
  if (dryRun || affected === 0) return { affected };

  const nullCols = ENCRYPTED_COLUMNS.map((col) => `${col} = NULL`).join(", ");
  const where = buildWhereClause();
  db.prepare(`UPDATE provider_connections SET ${nullCols} WHERE ${where}`).run();

  return { affected };
}

export async function countEncryptedCredentialsAsync(): Promise<number> {
  const database = getSelectedPostgresDatabase();
  if (!database) return countEncryptedCredentials();
  return database.transaction(async (transaction) => {
    const result = await transaction.query<{ count: string }>(
      "SELECT COUNT(*)::text AS count FROM omniroute.provider_connections WHERE " +
        buildWhereClause()
    );
    return Number(result.rows[0]?.count ?? 0);
  });
}

export async function resetEncryptedColumnsAsync({
  dryRun,
}: {
  dryRun: boolean;
}): Promise<{ affected: number }> {
  const database = getSelectedPostgresDatabase();
  if (!database) return resetEncryptedColumns({ dryRun });
  return database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 4)");
    const count = await transaction.query<{ count: string }>(
      "SELECT COUNT(*)::text AS count FROM omniroute.provider_connections WHERE " +
        buildWhereClause()
    );
    const affected = Number(count.rows[0]?.count ?? 0);
    if (!dryRun && affected > 0) {
      await transaction.query(
        "UPDATE omniroute.provider_connections SET " +
          ENCRYPTED_COLUMNS.map((column) => `${column} = NULL`).join(", ") +
          " WHERE " +
          buildWhereClause()
      );
    }
    return { affected };
  });
}
