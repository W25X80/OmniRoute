import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import { withPostgresCatalogSnapshot as prepareSnapshot } from "../../../src/lib/db/postgres/catalogSnapshot.ts";

interface ImportGate {
  entered(): void;
  release: Promise<void>;
}

declare global {
  var __ccDiscoveryCatalogImportGate: ImportGate | undefined;
  var __ccDiscoveryCatalogReadGate: ImportGate | undefined;
}

// Only the runtime's dynamic import is redirected here. The real plane, schema,
// snapshot query and counter transaction retain their production implementations.
const importGate = globalThis.__ccDiscoveryCatalogImportGate;
importGate?.entered();
await importGate?.release;

export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const readGate = globalThis.__ccDiscoveryCatalogReadGate;
  readGate?.entered();
  await readGate?.release;
  return prepareSnapshot(database, body);
}
