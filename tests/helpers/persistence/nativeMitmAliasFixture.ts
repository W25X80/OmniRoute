import {
  fixture,
  withPostgresCatalogSnapshot as withFixtureSnapshot,
} from "./runtimeOwnedWorkFixture.ts";
import { getSelectedPostgresDatabase } from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";

const values = new Map<string, string>();
export const aliasAdmission = {
  gate: undefined as Promise<void> | undefined,
  entered: undefined as (() => void) | undefined,
};
export function resetAliasFixture(): void {
  values.clear();
  aliasAdmission.gate = undefined;
  aliasAdmission.entered = undefined;
}
export function getDbInstance(): never {
  throw new Error("Native MITM attempted stale application SQLite");
}
export function backupDbFile(): never {
  throw new Error("Native MITM attempted application SQLite backup");
}
function assertScope(): void {
  if (!getSelectedPostgresDatabase() || fixture.selectedGeneration() === undefined) {
    throw new Error("MITM catalog scope is absent");
  }
}
export function catalogValue(namespace: string, key: string) {
  assertScope();
  const value = values.get(`${namespace}:${key}`);
  return value === undefined ? undefined : { namespace, key, value };
}
export function catalogNamespaceRows(namespace: string) {
  assertScope();
  return [...values]
    .filter(([key]) => key.startsWith(`${namespace}:`))
    .map(([key, value]) => ({
      namespace,
      key: key.slice(namespace.length + 1),
      value,
    }));
}
export async function putCatalogValue(namespace: string, key: string, value: string) {
  assertScope();
  values.set(`${namespace}:${key}`, value);
}
export function hasPostgresCatalogSnapshot() {
  return fixture.selectedGeneration() !== undefined;
}
export function isPostgresCatalogMutation() {
  return false;
}
export function mutatePostgresCatalog<T>(_database: PostgresDatabase, body: () => Promise<T>) {
  assertScope();
  return body();
}
export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
) {
  const gate = aliasAdmission.gate;
  aliasAdmission.gate = undefined;
  if (gate) {
    aliasAdmission.entered?.();
    await gate;
  }
  return withFixtureSnapshot(database, body);
}
