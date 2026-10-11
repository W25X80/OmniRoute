import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";

export interface ProviderStatusRow {
  id: string;
  provider: string;
  is_active: number | boolean | null;
  test_status: string | null;
  last_error: string | null;
}

/** Raw operational projection preserves nullable active-state policy without credentials. */
export async function getProviderConnectionStatusRowsAsync(): Promise<ProviderStatusRow[]> {
  const database = getSelectedPostgresDatabase();
  if (database) {
    return database.transaction(async (transaction) => {
      const result = await transaction.query<ProviderStatusRow>(
        `SELECT id, provider, is_active, test_status, last_error
         FROM omniroute.provider_connections ORDER BY insertion_order`
      );
      return result.rows;
    });
  }
  return getDbInstance()
    .prepare("SELECT id, provider, is_active, test_status, last_error FROM provider_connections")
    .all() as ProviderStatusRow[];
}
