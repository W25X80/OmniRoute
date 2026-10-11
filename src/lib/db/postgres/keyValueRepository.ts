import type { PostgresDatabase, PostgresTransaction } from "./database";

export interface PostgresKeyValueRow {
  key: string;
  value: string;
}

/** Internal durable KV primitives. Namespace owners retain their policy and decoding. */
export async function readPostgresKeyValue(
  transaction: PostgresTransaction,
  namespace: string,
  key: string
): Promise<string | undefined> {
  const { rows } = await transaction.query<{ value: string }>(
    "SELECT value FROM omniroute.key_value WHERE namespace = $1 AND key = $2",
    [namespace, key]
  );
  return rows[0]?.value;
}

export async function listPostgresKeyValues(
  transaction: PostgresTransaction,
  namespace: string
): Promise<PostgresKeyValueRow[]> {
  const { rows } = await transaction.query<PostgresKeyValueRow>(
    "SELECT key, value FROM omniroute.key_value WHERE namespace = $1",
    [namespace]
  );
  return rows;
}

export async function writePostgresKeyValue(
  transaction: PostgresTransaction,
  namespace: string,
  key: string,
  value: string
): Promise<void> {
  await transaction.query(
    `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ($1, $2, $3)
     ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value`,
    [namespace, key, value]
  );
}

export async function deletePostgresKeyValue(
  transaction: PostgresTransaction,
  namespace: string,
  key: string
): Promise<boolean> {
  const result = await transaction.query(
    "DELETE FROM omniroute.key_value WHERE namespace = $1 AND key = $2",
    [namespace, key]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function deletePostgresKeyValueNamespace(
  transaction: PostgresTransaction,
  namespace: string
): Promise<number> {
  const result = await transaction.query("DELETE FROM omniroute.key_value WHERE namespace = $1", [
    namespace,
  ]);
  return result.rowCount ?? 0;
}

/** Same leased transaction for read/modify/write, including an absent row. No nested tx/effects. */
export async function lockPostgresKeyValueMutation(
  transaction: PostgresTransaction
): Promise<void> {
  await transaction.query("SELECT pg_advisory_xact_lock(18765, 20)");
}

export function withPostgresKeyValueMutation<T>(
  database: PostgresDatabase,
  body: (transaction: PostgresTransaction) => Promise<T>
): Promise<T> {
  return database.transaction(async (transaction) => {
    await lockPostgresKeyValueMutation(transaction);
    return body(transaction);
  });
}

export function getPostgresKeyValue(
  database: PostgresDatabase,
  namespace: string,
  key: string
): Promise<string | undefined> {
  return database.transaction((transaction) => readPostgresKeyValue(transaction, namespace, key));
}

export function listPostgresKeyValueNamespace(
  database: PostgresDatabase,
  namespace: string
): Promise<PostgresKeyValueRow[]> {
  return database.transaction((transaction) => listPostgresKeyValues(transaction, namespace));
}
