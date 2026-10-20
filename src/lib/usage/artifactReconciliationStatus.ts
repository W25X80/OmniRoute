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
  publishing: number;
  deletePending: number;
  oldestUncertainCreatedAt: string | null;
  reviewRequired: boolean;
};
const UNCERTAINTY_REVIEW_AGE_MS = 30 * 60_000;
const REVIEW_WARNING_INTERVAL_MS = 30 * 60_000;
const key = Symbol.for("omniroute.artifact.reconciliation.status.v1");
const root = globalThis as typeof globalThis & { [key]?: Status & { reviewWarningAfter?: number } };
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
  publishing: 0,
  deletePending: 0,
  oldestUncertainCreatedAt: null,
  reviewRequired: false,
});

/** Cheap management-only snapshot; no storage identities or I/O. */
export function getArtifactReconciliationStatus() {
  const { reviewWarningAfter: _internal, ...snapshot } = status;
  return { ...snapshot, lastFailure: status.lastFailure && { ...status.lastFailure } };
}
export function recordArtifactReconciliationPass(
  counts: Pick<
    Status,
    "pending" | "uncertain" | "legacy" | "foreignScope" | "stagedBytes" | "oldestPendingAt"
  > &
    Partial<Pick<Status, "publishing" | "deletePending" | "oldestUncertainCreatedAt">>,
  evidence: {
    failure?: unknown;
    operation?: ArtifactStorageOperation;
    succeeded?: ReadonlySet<ArtifactStorageOperation>;
  } = {}
) {
  const nowMs = Date.now();
  const now = new Date(nowMs).toISOString();
  const oldestUncertainCreatedAt = counts.oldestUncertainCreatedAt ?? null;
  const oldest = oldestUncertainCreatedAt === null ? NaN : Date.parse(oldestUncertainCreatedAt);
  // Entry age is not a durable last-progress clock. Successful GET404 is still uncertain.
  const reviewRequired = counts.uncertain > 0 && nowMs - oldest >= UNCERTAINTY_REVIEW_AGE_MS;
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
    publishing: counts.publishing ?? 0,
    deletePending: counts.deletePending ?? 0,
    oldestUncertainCreatedAt,
    reviewRequired,
    state: status.lastFailure
      ? "degraded"
      : counts.legacy || counts.foreignScope
        ? "degraded"
        : counts.pending
          ? "pending"
          : "healthy",
    lastPassAt: now,
  });
  if (!reviewRequired) status.reviewWarningAfter = 0;
  else if (nowMs >= (status.reviewWarningAfter ?? 0)) {
    status.reviewWarningAfter = nowMs + REVIEW_WARNING_INTERVAL_MS;
    console.warn("[callLogs] Aged uncertain artifact operations require review:", {
      publishing: status.publishing,
      deletePending: status.deletePending,
      uncertain: status.uncertain,
      oldestUncertainCreatedAt,
    });
  }
  return status.lastFailure === null;
}
