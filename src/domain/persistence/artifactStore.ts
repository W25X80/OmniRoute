export interface StoredArtifact {
  reference: string;
  sizeBytes: number;
  checksum: string;
}

export interface PreparedArtifact extends Readonly<StoredArtifact> {
  readonly storageScope: string;
  /** The allocated key receives one atomic PUT attempt and is never replayed. */
  readonly singleAttempt: true;
}

export interface ArtifactPutOptions {
  /** Durable caller ownership must be acknowledged before any external PUT. */
  onPrepared?(artifact: PreparedArtifact): Promise<void>;
}

/** Binary object storage only; application serialization remains with its owning domain. */
export interface NativeArtifactStore {
  put(bytes: Uint8Array, options?: ArtifactPutOptions): Promise<StoredArtifact>;
  /** Optional on non-native test adapters; native publication fails closed if absent. */
  readonly storageScope?: string;
  /** Actual raw PUT/preparation lifetime, independent from the deadline-facing promise. */
  isPutActive?(reference: string): boolean;
  get(reference: string): Promise<Uint8Array | null>;
  /** True is acknowledged deletion; false is confirmed already-absent. Failures reject. */
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
    readonly reference?: string,
    /** False proves dispatch never began; absence provides no such guarantee. */
    readonly putStarted?: boolean
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
