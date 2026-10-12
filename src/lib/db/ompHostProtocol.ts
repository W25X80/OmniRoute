export const OMP_HOST_MAX_JOBS = 32;
export const OMP_HOST_MAX_FRAME_BYTES = 64 * 1024;
export const OMP_HOST_MAX_PENDING_BYTES = 2 * 1024 * 1024;
export const OMP_HOST_OPERATION_TIMEOUT_MS = 10_000;

export type OmpHostOperation =
  | { kind: "get"; providerId: string }
  | { kind: "save"; providerId: string; apiKey: string; baseUrl: string }
  | { kind: "delete"; providerId: string };

// Existing host data may contain non-string JSON values; preserve the source projection.
export interface OmpHostCredentials {
  hasOmniRoute: boolean;
  apiKey: unknown;
  baseUrl: unknown;
}
export type OmpHostRequest =
  { kind: "operation"; id: number; operation: OmpHostOperation } | { kind: "close" };
export type OmpHostReply =
  | { kind: "result"; id: number; ok: true; credentials?: OmpHostCredentials }
  | { kind: "result"; id: number; ok: false };

export function ompHostFrameBytes(message: unknown): number {
  return Buffer.byteLength(JSON.stringify(message), "utf8");
}
export function isOmpHostOperation(value: unknown): value is OmpHostOperation {
  if (!value || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.providerId === "string" &&
    (row.kind === "get" ||
      row.kind === "delete" ||
      (row.kind === "save" && typeof row.apiKey === "string" && typeof row.baseUrl === "string"))
  );
}
