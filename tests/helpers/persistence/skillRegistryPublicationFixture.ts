import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import type { SkillStorageRow } from "../../../src/lib/db/skillStorage.ts";
import type { SkillPatch } from "../../../src/lib/db/skills.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const sqliteAuthority = {};
export const getSkillStorageAuthority = () => getSelectedPersistenceIdentity() ?? sqliteAuthority;

export function createStore() {
  return {
    rows: new Map<string, SkillStorageRow>(),
    reads: 0,
    activeReads: 0,
    maxActiveReads: 0,
    inserts: 0,
    updates: 0,
    deletes: 0,
    readFailure: undefined as Error | undefined,
    writeFailure: undefined as Error | undefined,
    commitBeforeInsertFailure: false,
    denied: false,
    onRead: undefined as (() => void) | undefined,
    gates: [] as Array<{
      entered: ReturnType<typeof deferred>;
      release: ReturnType<typeof deferred>;
    }>,
    holdNextRead() {
      const gate = { entered: deferred(), release: deferred() };
      this.gates.push(gate);
      return { entered: gate.entered.promise, release: gate.release.resolve };
    },
  };
}
const stores = new WeakMap<object, ReturnType<typeof createStore>>();
export function store() {
  const authority = getSkillStorageAuthority();
  let value = stores.get(authority);
  if (!value) stores.set(authority, (value = createStore()));
  return value;
}

/** Snapshot capture followed by delayed completion models a committed read ACK, not a SQL engine. */
export async function listSkillRowsAsync(owners?: readonly string[]): Promise<SkillStorageRow[]> {
  const current = store();
  current.reads++;
  current.activeReads++;
  current.maxActiveReads = Math.max(current.maxActiveReads, current.activeReads);
  const rows = structuredClone(
    [...current.rows.values()].filter((row) => !owners || owners.includes(row.api_key_id))
  );
  const gate = current.gates.shift();
  try {
    gate?.entered.resolve();
    await gate?.release.promise;
    current.onRead?.();
    if (current.readFailure) throw current.readFailure;
    return rows;
  } finally {
    current.activeReads--;
  }
}
export async function insertSkillRowAsync(row: SkillStorageRow): Promise<void> {
  const current = store();
  current.inserts++;
  if (current.writeFailure && !current.commitBeforeInsertFailure) throw current.writeFailure;
  current.rows.set(row.id, structuredClone(row));
  if (current.writeFailure) throw current.writeFailure;
}
export async function deleteSkillRowAsync(id: string): Promise<number> {
  const current = store();
  current.deletes++;
  if (current.writeFailure) throw current.writeFailure;
  return Number(current.rows.delete(id));
}
export async function deleteSkillsByNameAsync(name: string, owner?: string): Promise<number> {
  const current = store();
  current.deletes++;
  if (current.writeFailure) throw current.writeFailure;
  let count = 0;
  for (const row of current.rows.values()) {
    if (row.name === name && (!owner || row.api_key_id === owner)) {
      current.rows.delete(row.id);
      count++;
    }
  }
  return count;
}
export async function updateSkillEnabledByOwnerAsync(
  id: string,
  owner: string,
  enabled: boolean,
  time: string
): Promise<number> {
  const current = store();
  current.updates++;
  if (current.writeFailure) throw current.writeFailure;
  const row = current.rows.get(id);
  if (!row || row.api_key_id !== owner) return 0;
  Object.assign(row, { enabled: enabled ? 1 : 0, mode: enabled ? "on" : "off", updated_at: time });
  return 1;
}

/** Route boundary doubles keep the real PUT parsing/control flow and actual registry. */
export async function updateSkillAsync(id: string, patch: SkillPatch): Promise<number> {
  const current = store();
  current.updates++;
  if (current.writeFailure) throw current.writeFailure;
  const row = current.rows.get(id);
  if (!row) return 0;
  Object.assign(row, patch);
  return 1;
}
export async function requireManagementAuth(): Promise<Response | null> {
  return store().denied ? Response.json({ error: "fixture unauthorized" }, { status: 401 }) : null;
}
export const logger = () => ({ debug() {}, info() {}, error() {} });
export const sanitizeErrorMessage = (message: string) => message;
