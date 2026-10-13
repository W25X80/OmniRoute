import type { PostgresDatabase, PostgresTransaction } from "../../../src/lib/db/postgres/database";
import { CATALOG_TABLES } from "../../../src/lib/db/postgres/catalogSnapshot";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext";

export { isCloudEnabled } from "../../../src/lib/db/settings";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => (resolve = done));
  return { promise, resolve };
}

type Row = Record<string, unknown>;
type Binding = ReturnType<typeof getSelectedControlPlane>;
const settings = {
  requireLogin: true,
  password: "configured-test-password",
  setupComplete: true,
  cloudEnabled: false,
};

export const fixture = {
  events: [] as string[],
  unexpected: [] as string[],
  rows: new Map<string, Row>(),
  generation: 1,
  creates: 0,
  resultWrites: 0,
  resultBindings: [] as Binding[],
  reads: [] as Binding[],
  validators: [] as Array<{ provider: string; allowLocalSpawn: boolean; binding: Binding }>,
  runtimeProbes: [] as string[],
  leased: false,
  result: { valid: true } as { valid: boolean; unsupported?: boolean; error?: string },
  entered: barrier(),
  release: barrier(),
  resultAck: barrier(),
  failed: barrier(),
  failures: [] as string[],
  reset() {
    this.events = [];
    this.unexpected = [];
    this.rows = new Map();
    this.generation = 1;
    this.creates = 0;
    this.resultWrites = 0;
    this.resultBindings = [];
    this.reads = [];
    this.validators = [];
    this.runtimeProbes = [];
    this.leased = false;
    this.result = { valid: true };
    this.entered = barrier();
    this.release = barrier();
    this.resultAck = barrier();
    this.failed = barrier();
    this.failures = [];
  },
};

/** A strict SQL transport fixture, not an engine oracle. Repositories and scope guards are real. */
export async function createPostgresDatabase(): Promise<
  Pick<PostgresDatabase, "transaction" | "close">
> {
  let closed = false;
  return {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      if (closed) throw new Error("Fixture pool is closed");
      const rows = structuredClone(fixture.rows);
      let changed = false;
      let inserted = false;
      let resultWrite = false;
      let generation = fixture.generation;
      const tx: PostgresTransaction = {
        async query<R>(sql: string, values: unknown[] = []) {
          const text = sql.replace(/\s+/g, " ").trim();
          let found: Row[] = [];
          let count = 0;
          if (text.includes(" AS tables,")) {
            found = [
              {
                values: Object.entries(settings).map(([key, value]) => ({
                  namespace: "settings",
                  key,
                  value: JSON.stringify(value),
                })),
                tables: Object.fromEntries(CATALOG_TABLES.map((table) => [table, []])),
                authority: "provider-lifetime-fixture",
                generation: String(generation),
              },
            ];
          } else if (text.startsWith("SELECT pg_advisory_xact_lock(")) {
            found = [];
          } else if (
            text ===
            "SELECT name,description,pipeline FROM omniroute.compression_combos WHERE id=$1 FOR UPDATE"
          ) {
            // This transport fixture has no seeded compression combo; boot's
            // explicit upgrade owner consequently has no row to transform.
            found = [];
          } else if (
            text === "SELECT key, value FROM omniroute.key_value WHERE namespace = 'settings'"
          ) {
            found = Object.entries(settings).map(([key, value]) => ({
              key,
              value: JSON.stringify(value),
            }));
          } else if (text.startsWith("UPDATE omniroute.api_key_policy_epoch SET generation")) {
            generation++;
            changed = true;
            found = [{ authority_id: "provider-lifetime-fixture", generation: String(generation) }];
          } else if (text.startsWith("SELECT * FROM omniroute.provider_connections WHERE id=$1")) {
            fixture.reads.push(getSelectedControlPlane());
            fixture.events.push("connection-read");
            const row = rows.get(String(values[0]));
            found = row ? [row] : [];
          } else if (
            text.startsWith("SELECT * FROM omniroute.provider_connections WHERE provider=$1")
          ) {
            found = [...rows.values()].filter((row) => row.provider === values[0]);
          } else if (
            text.startsWith("SELECT MAX(priority) AS max FROM omniroute.provider_connections")
          ) {
            found = [{ max: 0 }];
          } else if (
            text.startsWith("SELECT id FROM omniroute.provider_connections WHERE provider=$1")
          ) {
            found = [...rows.values()]
              .filter((row) => row.provider === values[0])
              .map(({ id }) => ({ id }));
          } else if (text.startsWith("INSERT INTO omniroute.provider_connections (")) {
            const columns = text
              .match(/^INSERT INTO omniroute\.provider_connections \(([^)]+)\)/)![1]
              .split(", ")
              .map((column) => column.replaceAll('"', ""));
            const row = Object.fromEntries(columns.map((column, index) => [column, values[index]]));
            rows.set(String(row.id), row);
            changed = inserted = true;
            count = 1;
          } else if (text === "UPDATE omniroute.provider_connections SET priority=$1 WHERE id=$2") {
            const row = rows.get(String(values[1]))!;
            row.priority = values[0];
            changed = true;
            count = 1;
          } else if (text.startsWith('UPDATE omniroute.provider_connections SET "')) {
            const row = rows.get(String(values.at(-1)))!;
            const columns = [...text.matchAll(/"([^"]+)"=\$\d+/g)].map((match) => match[1]);
            for (const [index, column] of columns.entries()) row[column] = values[index];
            resultWrite = row.last_tested != null || row.is_active === 1;
            changed = true;
            count = 1;
          } else {
            fixture.unexpected.push(text);
            throw new Error(`Unexpected lifetime fixture SQL: ${text}`);
          }
          return {
            rows: structuredClone(found) as R[],
            rowCount: count || found.length,
            command: text.split(" ")[0],
          };
        },
      };
      const result = await body(tx);
      if (changed) {
        fixture.rows = rows;
        fixture.generation = generation;
      }
      if (inserted) {
        fixture.creates++;
        fixture.events.push("create-ack");
      }
      if (resultWrite) {
        fixture.resultWrites++;
        fixture.resultBindings.push(getSelectedControlPlane());
        fixture.events.push("result-ack");
        fixture.resultAck.resolve();
      }
      return result;
    },
    async close() {
      closed = true;
      fixture.events.push("pool-close");
    },
  };
}

// Bootstrap/independent sinks have no bearing on the provider ownership chain.
export async function migratePostgresSchema() {}
export async function readPostgresEnvironment() {
  return {};
}
export function readS3ArtifactConfiguration() {
  return {};
}
export function createS3ArtifactStore() {
  return {
    async close() {
      fixture.events.push("artifacts-close");
    },
  };
}
export async function initializeCircuitBreakerPersistence() {}
export async function initializeQuotaSnapshotCache() {}
export async function flushCircuitBreakerPersistence() {}
export function getAuditRequestContext() {
  return {};
}
export function logAuditEvent() {
  fixture.events.push("audit");
}
export async function shouldHideLogs() {
  return true;
}
export function logProxyEvent() {
  fixture.events.push("probe-log");
}
export async function resolveProxyForConnection() {
  return null;
}
export async function isConnectionUnavailableToAuxiliaryActivity() {
  return fixture.leased;
}
export async function getCliRuntimeStatus(toolId: string) {
  fixture.runtimeProbes.push(toolId);
  return { installed: true, runnable: true };
}
export async function validateProviderApiKey(input: {
  provider: string;
  allowLocalSpawn: boolean;
}) {
  fixture.validators.push({
    provider: input.provider,
    allowLocalSpawn: input.allowLocalSpawn,
    binding: getSelectedControlPlane(),
  });
  fixture.events.push("probe-entered");
  fixture.entered.resolve();
  await fixture.release.promise;
  fixture.events.push("probe-released");
  return fixture.result;
}
