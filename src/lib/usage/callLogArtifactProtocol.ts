import type { CallLogArtifact, CallLogArtifactWriteResult } from "./callLogArtifacts.ts";

export interface CallLogArtifactEnvironment {
  pipelineMaxSizeKb?: string;
  chatDebugFile?: string;
  appLogLevel?: string;
}

export const MAX_CALL_LOG_DECODE_BYTES = 8 * 1024 * 1024;

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
  | { id: number; kind: "deserialize"; bytes: Uint8Array };

export type CallLogArtifactWorkerReply =
  | { id: number; kind: "write"; result: CallLogArtifactWriteResult | null }
  | { id: number; kind: "serialize"; bytes: Uint8Array | null }
  | { id: number; kind: "deserialize"; result: CallLogArtifactDecodeResult | null };
