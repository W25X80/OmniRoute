import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresMemoryRepository } from "./postgres/memoryRepository";
import { PostgresMemoryVectorStore } from "./postgres/memoryVectorRepository";

/** Domain-only selection; callers never receive SQL primitives or a driver. */
export function getSelectedMemoryRepository() {
  const database = getSelectedPostgresDatabase();
  return database ? createPostgresMemoryRepository(database) : undefined;
}

export function getSelectedMemoryVectorStore() {
  const database = getSelectedPostgresDatabase();
  return database ? new PostgresMemoryVectorStore(database) : undefined;
}
