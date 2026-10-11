import { AsyncLocalStorage } from "node:async_hooks";

interface Pool {
  id: string;
  name: string;
  connectionId: string;
  connectionIds: string[];
  groupId: string;
  createdAt: string;
  allocations: unknown[];
}
interface Gate {
  started: boolean;
  ready: Promise<void>;
  enter(): void;
  waiting: Promise<void>;
  release(): void;
}
export interface FixturePlane {
  readonly identity: object;
  readonly rows: Map<string, Pool>;
  readonly gates: Gate[];
}
interface FixtureScope {
  readonly plane: FixturePlane;
  alive: boolean;
}
const scopes = new AsyncLocalStorage<FixtureScope>();

function scope(): FixtureScope {
  const current = scopes.getStore();
  if (!current?.alive) throw new Error("Isolated native scope has ended");
  return current;
}

export function fixturePlane(): FixturePlane {
  return {
    identity: Object.freeze({}),
    rows: new Map([
      [
        "pool",
        {
          id: "pool",
          name: "original",
          connectionId: "connection",
          connectionIds: ["connection"],
          groupId: "group",
          createdAt: "fixture",
          allocations: [],
        },
      ],
    ]),
    gates: [],
  };
}

export async function runFixture<T>(plane: FixturePlane, body: () => Promise<T>): Promise<T> {
  const current: FixtureScope = { plane, alive: true };
  try {
    return await scopes.run(current, body);
  } finally {
    current.alive = false;
  }
}

export function pauseNextHook(plane: FixturePlane): Gate {
  let entered!: () => void;
  let released!: () => void;
  const gate: Gate = {
    started: false,
    ready: new Promise<void>((resolve) => {
      entered = resolve;
    }),
    enter() {
      gate.started = true;
      entered();
    },
    waiting: new Promise<void>((resolve) => {
      released = resolve;
    }),
    release() {
      released();
    },
  };
  plane.gates.push(gate);
  return gate;
}

// Each call returns a distinct guarded proxy, matching separate admitted native runs.
export function getSelectedPostgresDatabase(): FixturePlane {
  return new Proxy(scope().plane, {});
}
export function getSelectedPersistenceIdentity(): object {
  return scope().plane.identity;
}

/** Isolated domain port; real native SQL and COMMIT oracles live in the integration suite. */
export class PostgresQuotaRepository {
  constructor(private readonly plane: FixturePlane) {}
  async getPool(id: string): Promise<Pool | null> {
    scope();
    return this.plane.rows.get(id) ?? null;
  }
  async updatePool(id: string, input: { name?: string }): Promise<Pool | null> {
    scope();
    const row = this.plane.rows.get(id);
    if (!row) return null;
    if (input.name !== undefined) row.name = input.name;
    return row;
  }
  async deletePool(id: string): Promise<boolean> {
    scope();
    return this.plane.rows.delete(id);
  }
  async upsertAllocations(id: string, values: unknown[]): Promise<void> {
    scope();
    const row = this.plane.rows.get(id);
    if (row) row.allocations = values;
  }
}

export async function syncQuotaCombos(): Promise<void> {
  const current = scope();
  const gate = current.plane.gates.shift();
  if (gate) {
    gate.enter();
    await gate.waiting;
  }
  scope();
}
export const removeQuotaCombosForPool = syncQuotaCombos;

// Explicit isolated effect substitutions; no production cache/no-log behavior is claimed here.
export function clearApiKeyCaches(): void {}
export function invalidateModelCatalogCache(): void {}
export async function getCachedProviderConnectionById() {
  return { provider: "fixture" };
}
export function quotaAllocationFingerprint(value: unknown): string {
  return JSON.stringify(value) ?? "";
}
export function getDbInstance(): never {
  throw new Error("Unexpected SQLite fallback");
}
export const rowToQuotaAllocation = getDbInstance;
export const deleteSchedulesForPool = getDbInstance;
