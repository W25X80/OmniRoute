import { createHash } from "node:crypto";
import { decrypt, requireExplicitStorageEncryptionKey } from "../db/encryption.ts";
import {
  MAX_CALL_LOG_DECODE_BYTES,
  MAX_CALL_LOG_STAGED_CIPHER_CHARS,
  type CallLogArtifactDecodeResult,
  type StagedCallLogDecodeInput,
} from "./callLogArtifactProtocol.ts";
import type { CallLogArtifact } from "./callLogArtifacts.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** CPU-actor entry point only: no plaintext or crypto work on the serving thread. */
export function decodeStagedCallLog(input: StagedCallLogDecodeInput): CallLogArtifactDecodeResult {
  const corrupt = { artifact: null, state: "corrupt" as const };
  if (
    !Number.isFinite(input.expiresAt) ||
    Date.now() >= input.expiresAt ||
    typeof input.ciphertext !== "string" ||
    input.ciphertext.length > MAX_CALL_LOG_STAGED_CIPHER_CHARS ||
    !Number.isSafeInteger(input.sizeBytes) ||
    input.sizeBytes <= 0 ||
    input.sizeBytes > MAX_CALL_LOG_DECODE_BYTES ||
    !/^[0-9a-f]{64}$/.test(input.sha256) ||
    typeof input.callId !== "string" ||
    !input.callId
  )
    return corrupt;
  const parts = input.ciphertext.split(":");
  if (
    parts.length !== 5 ||
    parts[0] !== "enc" ||
    parts[1] !== "v1" ||
    !/^[0-9a-f]{32}$/.test(parts[2]) ||
    parts[3].length % 2 !== 0 ||
    !/^[0-9a-f]+$/.test(parts[3]) ||
    !/^[0-9a-f]{32}$/.test(parts[4])
  )
    return corrupt;
  try {
    requireExplicitStorageEncryptionKey();
    const base64 = decrypt(input.ciphertext, { quiet: true });
    if (base64.length !== Math.ceil(input.sizeBytes / 3) * 4) return corrupt;
    const bytes = Buffer.from(base64, "base64");
    if (
      bytes.byteLength !== input.sizeBytes ||
      bytes.toString("base64") !== base64 ||
      createHash("sha256").update(bytes).digest("hex") !== input.sha256
    )
      return corrupt;
    const artifact: unknown = JSON.parse(bytes.toString("utf8"));
    if (
      Date.now() >= input.expiresAt ||
      !isRecord(artifact) ||
      artifact.schemaVersion !== 5 ||
      !isRecord(artifact.summary) ||
      artifact.summary.id !== input.callId ||
      !["requestBody", "responseBody", "error"].every((key) => Object.hasOwn(artifact, key)) ||
      (artifact.pipeline !== undefined && !isRecord(artifact.pipeline))
    )
      return corrupt;
    return { artifact: artifact as CallLogArtifact, state: "ready" };
  } catch {
    return corrupt;
  }
}
