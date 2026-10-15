import { isArtifactStorageErrorKind } from "@/domain/persistence/artifactStore";

export function artifactFailureDiagnostic(error: unknown): {
  kind: string;
  name?: string;
  status?: number;
  requestId?: string;
} {
  for (const kind of [
    "unavailable",
    "put_unknown",
    "deadline",
    "integrity",
    "invalid_reference",
    "overloaded",
    "closed",
  ] as const) {
    if (isArtifactStorageErrorKind(error, kind)) return { kind, ...error.diagnostic };
  }
  return { kind: "reconciliation_failed" };
}

/** Object-specific corruption must not hide neighbours; transport/auth failures stop the pass. */
export function isSharedArtifactFailure(error: unknown): boolean {
  return (
    !isArtifactStorageErrorKind(error, "integrity") &&
    !isArtifactStorageErrorKind(error, "invalid_reference")
  );
}

export type ArtifactStorageOperation = "get" | "put" | "delete";
export type ArtifactReconciliationOptions = {
  /** Admission deadline; admitted raw I/O remains owned until the store settles it. */
  deadline?: number;
  onFailure?(error: unknown, operation?: ArtifactStorageOperation): void;
  onStorageSuccess?(operation: ArtifactStorageOperation): void;
};

type Status = {
  state: "starting" | "healthy" | "pending" | "degraded";
  lastPassAt: string | null;
  lastFailure: ReturnType<typeof artifactFailureDiagnostic> | null;
  lastFailureOperation: ArtifactStorageOperation | null;
  lastFailureAt: string | null;
  degradedSince: string | null;
  pending: number;
  uncertain: number;
  legacy: number;
  foreignScope: number;
  stagedBytes: number;
  oldestPendingAt: string | null;
};
const key = Symbol.for("omniroute.artifact.reconciliation.status.v1");
const root = globalThis as typeof globalThis & { [key]?: Status };
const status = (root[key] ??= {
  state: "starting",
  lastPassAt: null,
  lastFailure: null,
  lastFailureOperation: null,
  lastFailureAt: null,
  degradedSince: null,
  pending: 0,
  uncertain: 0,
  legacy: 0,
  foreignScope: 0,
  stagedBytes: 0,
  oldestPendingAt: null,
});

/** Cheap management-only snapshot; no storage identities or I/O. */
export function getArtifactReconciliationStatus() {
  return { ...status, lastFailure: status.lastFailure && { ...status.lastFailure } };
}
export function recordArtifactReconciliationPass(
  counts: Pick<
    Status,
    "pending" | "uncertain" | "legacy" | "foreignScope" | "stagedBytes" | "oldestPendingAt"
  >,
  evidence: {
    failure?: unknown;
    operation?: ArtifactStorageOperation;
    succeeded?: ReadonlySet<ArtifactStorageOperation>;
  } = {}
) {
  const now = new Date().toISOString();
  if (evidence.failure) {
    status.lastFailure = artifactFailureDiagnostic(evidence.failure);
    status.lastFailureOperation = evidence.operation ?? null;
    status.lastFailureAt = now;
    status.degradedSince ??= now;
  } else if (
    counts.pending === 0 ||
    (status.lastFailureOperation &&
      evidence.succeeded?.has(status.lastFailureOperation) &&
      !["integrity", "invalid_reference"].includes(status.lastFailure?.kind ?? ""))
  ) {
    status.lastFailure = null;
    status.lastFailureOperation = null;
    status.lastFailureAt = null;
    status.degradedSince = null;
  }
  Object.assign(status, counts, {
    state: status.lastFailure
      ? "degraded"
      : counts.legacy || counts.foreignScope
        ? "degraded"
        : counts.pending
          ? "pending"
          : "healthy",
    lastPassAt: now,
  });
  return status.lastFailure === null;
}
