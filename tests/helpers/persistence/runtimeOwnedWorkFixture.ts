import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresControlPlane } from "../../../src/lib/db/postgres/controlPlane.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  const gate = { promise, resolve };
  fixture.barriers.push(gate);
  return gate;
}

const catalog = new AsyncLocalStorage<number>();
const authority = Object.freeze({});
export const fixture = {
  events: [] as string[],
  resources: [] as AsyncResource[],
  barriers: [] as Array<{ promise: Promise<void>; resolve(): void }>,
  generation: 0,
  queries: 0,
  environmentReads: 0,
  planeCreates: 0,
  artifactCreates: 0,
  artifactCloses: 0,
  planeCloses: 0,
  poolCloses: 0,
  applicationReleases: 0,
  runFailure: undefined as Error | undefined,
  catalogFailure: undefined as Error | undefined,
  flushFailure: undefined as Error | undefined,
  artifactFailure: undefined as Error | undefined,
  planeFailure: undefined as Error | undefined,
  startupBarrier: undefined as Promise<void> | undefined,
  startupEntered: undefined as (() => void) | undefined,
  selectedGeneration: () => catalog.getStore(),
  authority,
  releaseBarriers() {
    for (const gate of fixture.barriers) gate.resolve();
  },
  reset() {
    for (const resource of fixture.resources) resource.emitDestroy();
    fixture.resources = [];
    fixture.barriers = [];
    fixture.events = [];
    fixture.generation = 0;
    fixture.queries = 0;
    fixture.environmentReads = 0;
    fixture.planeCreates = 0;
    fixture.artifactCreates = 0;
    fixture.artifactCloses = 0;
    fixture.planeCloses = 0;
    fixture.poolCloses = 0;
    fixture.applicationReleases = 0;
    fixture.runFailure = undefined;
    fixture.catalogFailure = undefined;
    fixture.flushFailure = undefined;
    fixture.artifactFailure = undefined;
    fixture.planeFailure = undefined;
    fixture.startupBarrier = undefined;
    fixture.startupEntered = undefined;
  },
};

export async function readPostgresEnvironment() {
  fixture.environmentReads++;
  return {};
}
export function readS3ArtifactConfiguration() {
  return {};
}

/** Only transport/boot dependencies are substituted. The runtime and authority context are real. */
export async function initializePostgresControlPlane(): Promise<PostgresControlPlane> {
  fixture.planeCreates++;
  let state: "ready" | "closing" | "closed" = "ready";
  let active = 0;
  let runId = 0;
  let close: Promise<void> | undefined;
  let drained: (() => void) | undefined;
  const plane: PostgresControlPlane = {
    get state() {
      return state;
    },
    bootstrapSecrets: Object.freeze({
      jwtSecret: "test-jwt-secret",
      apiKeySecret: "test-api-secret",
    }),
    async run(body) {
      if (state !== "ready") throw new Error("Fixture plane admission ended");
      if (fixture.runFailure) {
        const failure = fixture.runFailure;
        fixture.runFailure = undefined;
        throw failure;
      }
      active++;
      const id = ++runId;
      let alive = true;
      const assertReady = () => {
        if (!alive || state === "closed") throw new Error("Fixture native scope ended");
      };
      const transaction: PostgresTransaction = {
        async query<Row>() {
          assertReady();
          fixture.queries++;
          return { rows: [] as Row[], rowCount: 0, command: "SELECT" };
        },
      };
      const database = {
        async transaction<T>(operation: (tx: PostgresTransaction) => Promise<T>) {
          assertReady();
          return operation(transaction);
        },
      } as unknown as PostgresDatabase;
      const binding = {
        database,
        databaseIdentity: authority,
        assertReady,
      } as unknown as PostgresControlPlaneBinding;
      try {
        return await withPostgresControlPlane(binding, () => body(binding));
      } finally {
        alive = false;
        active--;
        if (id === 1) {
          fixture.applicationReleases++;
          fixture.events.push("application-release");
        }
        if (!active) drained?.();
      }
    },
    close() {
      close ??= (async () => {
        fixture.planeCloses++;
        fixture.events.push("plane-close");
        state = "closing";
        if (active)
          await new Promise<void>((done) => {
            drained = done;
          });
        fixture.poolCloses++;
        fixture.events.push("pool-close");
        state = "closed";
        if (fixture.planeFailure) throw fixture.planeFailure;
      })();
      return close;
    },
  };
  return plane;
}

export function createS3ArtifactStore(): NativeArtifactStore {
  fixture.artifactCreates++;
  let close: Promise<void> | undefined;
  return {
    async put() {
      throw new Error("Artifact writes are outside this composition oracle");
    },
    async get() {
      throw new Error("Artifact reads are outside this composition oracle");
    },
    async delete() {
      throw new Error("Artifact deletes are outside this composition oracle");
    },
    close() {
      close ??= (async () => {
        fixture.artifactCloses++;
        fixture.events.push("artifact-close");
        if (fixture.artifactFailure) throw fixture.artifactFailure;
      })();
      return close;
    },
  };
}

export async function initializeCircuitBreakerPersistence() {
  fixture.startupEntered?.();
  if (fixture.startupBarrier) await fixture.startupBarrier;
}
export async function initializeQuotaSnapshotCache() {}
export async function flushCircuitBreakerPersistence() {
  fixture.events.push("sql-flush");
  if (fixture.flushFailure) throw fixture.flushFailure;
}
export async function withPostgresCatalogSnapshot<T>(
  _database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const generation = ++fixture.generation;
  fixture.resources.push(new AsyncResource("runtime-catalog-owned-resource"));
  if (fixture.catalogFailure) {
    const failure = fixture.catalogFailure;
    fixture.catalogFailure = undefined;
    throw failure;
  }
  return catalog.run(generation, body);
}

export function hasPostgresCatalogSnapshot(): boolean {
  return catalog.getStore() !== undefined;
}
