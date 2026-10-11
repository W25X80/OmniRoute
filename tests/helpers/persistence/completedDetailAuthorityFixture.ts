import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import { getPersistenceArtifactStore } from "../../../src/lib/db/persistenceRuntime.ts";
import type { PostgresCallLogRepository } from "../../../src/lib/db/postgres/callLogRepository.ts";
import { isSizeLimitOmissionMarker as isOmissionMarker } from "../../../src/shared/constants/callLogSizeLimitMarkers.ts";
import { barrier } from "./runtimeOwnedWorkFixture.ts";

export const detailFixture = {
  queries: 0,
  cutoffs: [] as string[],
  queriedAt: [] as number[],
  reads: 0,
  sqliteCalls: 0,
  paths: [] as Array<{ artifact_relpath: string | null }>,
  payloads: new Map<string, unknown>(),
  entered: undefined as ReturnType<typeof barrier> | undefined,
  release: undefined as ReturnType<typeof barrier> | undefined,
  queryFailure: undefined as Error | undefined,
  reset() {
    this.queries = 0;
    this.cutoffs = [];
    this.queriedAt = [];
    this.reads = 0;
    this.sqliteCalls = 0;
    this.paths = [];
    this.payloads.clear();
    this.entered = undefined;
    this.release = undefined;
    this.queryFailure = undefined;
  },
};

/** Repository I/O is controlled; application admission, identity and artifact authorization are real. */
export function getSelectedCallLogRepository(): PostgresCallLogRepository | undefined {
  if (!getSelectedPersistenceIdentity()) return undefined;
  return {
    async requestArtifactPaths(_connection: string, _model: string, since: string) {
      detailFixture.queries++;
      detailFixture.cutoffs.push(since);
      detailFixture.queriedAt.push(Date.now());
      detailFixture.entered?.resolve();
      await detailFixture.release?.promise;
      if (detailFixture.queryFailure) throw detailFixture.queryFailure;
      return detailFixture.paths;
    },
  } as unknown as PostgresCallLogRepository;
}
export async function readCallArtifactAsync(reference: string) {
  getPersistenceArtifactStore();
  detailFixture.reads++;
  const artifact = detailFixture.payloads.get(reference);
  return artifact ? { state: "ready", artifact } : { state: "missing", artifact: null };
}
export const isSizeLimitOmissionMarker = isOmissionMarker;
export function readCallArtifact(): never {
  throw new Error("Unexpected SQLite artifact read");
}
export function getDbInstance(): never {
  detailFixture.sqliteCalls++;
  throw new Error("Unexpected SQLite I/O");
}
