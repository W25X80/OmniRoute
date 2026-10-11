export interface StoredArtifact {
  reference: string;
  sizeBytes: number;
  checksum: string;
}

/** Binary object storage only; application serialization remains with its owning domain. */
export interface NativeArtifactStore {
  put(bytes: Uint8Array): Promise<StoredArtifact>;
  get(reference: string): Promise<Uint8Array | null>;
  /** True means deletion was acknowledged; an already absent key may also be acknowledged. */
  delete(reference: string): Promise<boolean>;
  /** Reject new operations and drain admitted work before successful resource teardown. */
  close(): Promise<void>;
}

export type ArtifactStorageFailure =
  | "configuration"
  | "invalid_reference"
  | "too_large"
  | "overloaded"
  | "closed"
  | "unavailable"
  | "deadline"
  | "integrity"
  | "put_unknown";

const MESSAGES: Record<ArtifactStorageFailure, string> = {
  configuration: "Invalid artifact storage configuration",
  invalid_reference: "Invalid artifact reference",
  too_large: "Artifact exceeds the storage size limit",
  overloaded: "Artifact storage operation limit reached",
  closed: "Artifact storage is closing",
  unavailable: "Artifact storage is unavailable",
  deadline: "Artifact storage deadline exceeded",
  integrity: "Artifact integrity validation failed",
  put_unknown: "Artifact upload outcome is unknown",
};

const ARTIFACT_ERROR = Symbol.for("omniroute.artifact.storage.error");

export class ArtifactStorageError extends Error {
  constructor(
    readonly kind: ArtifactStorageFailure,
    /** Internal reconciliation identity; never include it or the SDK error in HTTP error bodies. */
    readonly reference?: string
  ) {
    super(MESSAGES[kind]);
    this.name = "ArtifactStorageError";
    Object.defineProperty(this, ARTIFACT_ERROR, { value: true });
  }
}

/** The store and its callers can live in separate Next server bundles. */
export function isArtifactStorageErrorKind(
  error: unknown,
  kind: ArtifactStorageFailure
): error is ArtifactStorageError {
  if (!error || typeof error !== "object") return false;
  const candidate = error as Partial<ArtifactStorageError> & Record<symbol, unknown>;
  return candidate[ARTIFACT_ERROR] === true && candidate.kind === kind;
}
