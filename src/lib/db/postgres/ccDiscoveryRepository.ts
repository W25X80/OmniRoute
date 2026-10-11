import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import { queryUsageRows, bindSelectedUsageRepository } from "./usageQueries";
export class PostgresCcDiscoveryRepository {
  constructor(private readonly database: PostgresDatabase) {}
  async increment(keys: string[]): Promise<void> {
    await this.database.transaction(async (transaction) => {
      for (const key of keys)
        await transaction.query(
          `INSERT INTO omniroute.key_value(namespace,key,value)
    VALUES('ccDiscoveryMetrics',$1,'1') ON CONFLICT(namespace,key) DO UPDATE SET value=
    ((CASE WHEN omniroute.key_value.value ~ '^[+-]?[0-9]+$' THEN omniroute.key_value.value::numeric ELSE 0 END)+1)::text`,
          [key]
        );
    });
  }
  async rows(): Promise<Array<{ key: string; value: string }>> {
    return queryUsageRows(
      this.database,
      "SELECT key,value FROM omniroute.key_value WHERE namespace='ccDiscoveryMetrics'"
    );
  }
}
export function getSelectedCcDiscoveryRepository(): PostgresCcDiscoveryRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresCcDiscoveryRepository(database))
    : undefined;
}
