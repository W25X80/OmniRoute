import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresCallLogRepository } from "../../../src/lib/db/postgres/callLogRepository.ts";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

type Gate = ReturnType<typeof barrier>;
export const rotationFixture = {
  events: [] as Array<{ kind: "delete" | "trim"; identity: object }>,
  gates: new Map<object, { entered: Gate; release: Gate }>(),
  sqliteCalls: 0,
  arm(identity: object) {
    const gate = { entered: barrier(), release: barrier() };
    this.gates.set(identity, gate);
    return gate;
  },
  releaseAll() {
    for (const gate of this.gates.values()) gate.release.resolve();
  },
  reset() {
    this.events = [];
    this.gates.clear();
    this.sqliteCalls = 0;
  },
};

/** Only the repository I/O is replaced; production authority selection/admission remain real. */
export function getSelectedCallLogRepository(): PostgresCallLogRepository | undefined {
  const identity = getSelectedPersistenceIdentity();
  if (!identity) return undefined;
  return {
    async deleteBefore() {
      rotationFixture.events.push({ kind: "delete", identity });
      const gate = rotationFixture.gates.get(identity);
      gate?.entered.resolve();
      await gate?.release.promise;
      return { deletedRows: 0, paths: [] };
    },
    async trim() {
      rotationFixture.events.push({ kind: "trim", identity });
      return { deletedRows: 0, paths: [] };
    },
  } as unknown as PostgresCallLogRepository;
}

export function getDbInstance(): never {
  rotationFixture.sqliteCalls++;
  throw new Error("Unexpected SQLite I/O");
}
export function findReferencedArtifacts(): never {
  throw new Error("Unexpected SQLite query");
}
export function selectCallLogIdsBefore(): never {
  throw new Error("Unexpected SQLite query");
}
export function selectOverflowArtifactPaths(): never {
  throw new Error("Unexpected SQLite query");
}
export const CALL_LOGS_DIR = "";
export function deleteCallArtifact() {
  return false;
}
export async function deleteCallArtifactAsync() {
  return false;
}
export function getCallLogMaxEntries() {
  return 5000;
}
export function getCallLogRetentionDays() {
  return 90;
}
export function getCallLogsTableMaxRows() {
  return 5000;
}
export function isSqlitePagerCorruptError() {
  return false;
}
export function notePagerCorruption() {
  throw new Error("Unexpected SQLite corruption report");
}
