import type { CallLogArtifact, CallLogArtifactWriteResult } from "./callLogArtifacts.ts";

export interface CallLogArtifactEnvironment {
  pipelineMaxSizeKb?: string;
  chatDebugFile?: string;
  appLogLevel?: string;
}

export const MAX_CALL_LOG_DECODE_BYTES = 8 * 1024 * 1024;
export const MAX_CALL_LOG_STAGED_CIPHER_CHARS = 22369720;

export interface StagedCallLogDecodeInput {
  ciphertext: string;
  sizeBytes: number;
  sha256: string;
  callId: string;
  expiresAt: number;
}

export interface CallLogArtifactDecodeResult {
  artifact: CallLogArtifact | null;
  state: "ready" | "corrupt";
}

export type CallLogArtifactWorkerRequest =
  | {
      id: number;
      kind: "write" | "serialize";
      artifact: CallLogArtifact;
      environment: CallLogArtifactEnvironment;
    }
  | { id: number; kind: "deserialize"; bytes: Uint8Array }
  | { id: number; kind: "deserialize-staged"; staging: StagedCallLogDecodeInput };

export type CallLogArtifactWorkerReply =
  | { id: number; kind: "write"; result: CallLogArtifactWriteResult | null }
  | { id: number; kind: "serialize"; bytes: Uint8Array | null }
  | {
      id: number;
      kind: "deserialize" | "deserialize-staged";
      result: CallLogArtifactDecodeResult | null;
    };
