import { AsyncResource } from "node:async_hooks";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

export const scopeFixture = {
  generation: 0,
  statements: [] as Array<{ generation: number; sql: string }>,
  closed: false,
  closeCalls: 0,
  heartbeatStops: 0,
  barrier: null as Promise<void> | null,
  resources: [] as AsyncResource[],
};

export type ScopeFixtureBinding = PostgresControlPlaneBinding & { catalogGeneration: number };

export async function ensurePersistenceReady(): Promise<void> {}

export async function closePersistenceRuntime(): Promise<void> {
  scopeFixture.closed = true;
  scopeFixture.closeCalls++;
}

/** Scope composition oracle only: real database behavior belongs to PostgreSQL integration tests. */
export async function runPersistenceRequest<T>(body: () => T | Promise<T>): Promise<T> {
  if (scopeFixture.closed) throw new Error("Persistence fixture is closed");
  const generation = ++scopeFixture.generation;
  let alive = true;
  const transaction: PostgresTransaction = {
    async query<Row>(sql: string) {
      scopeFixture.statements.push({ generation, sql });
      return { rows: [] as Row[], rowCount: 0, command: "SELECT" };
    },
  };
  const database = {
    transaction<TValue>(operation: (tx: PostgresTransaction) => Promise<TValue>) {
      return operation(transaction);
    },
  } as unknown as PostgresDatabase;
  const binding = {
    catalogGeneration: generation,
    database,
    databaseIdentity: {},
    assertReady() {
      if (!alive) throw new Error("Persistence fixture scope has ended");
    },
  } as unknown as ScopeFixtureBinding;
  try {
    return await withPostgresControlPlane(binding, async () => {
      scopeFixture.resources.push(new AsyncResource("mcp-constructor-scope"));
      if (scopeFixture.barrier) await scopeFixture.barrier;
      return body();
    });
  } finally {
    alive = false;
  }
}
