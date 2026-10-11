import { randomUUID } from "node:crypto";
import {
  sanitizeErrorMessage,
  sanitizeUpstreamDetails,
} from "@omniroute/open-sse/utils/errorSanitization";
import type { ServerToolClaim } from "../skillExecutionFence";
import type { PostgresDatabase } from "./database";

interface ToolExecutionRow {
  id: string;
  tool_name: string;
  input_digest: string;
  status: string;
  output: string | null;
  error_message: string | null;
  claim_expires_at: string;
}
export interface ServerToolClaimInput {
  apiKeyId: string;
  requestIdentity: string;
  toolCallId: string;
  toolName: string;
  inputDigest: string;
  leaseExpiresAt: string;
}
export interface ServerToolFinalizeInput {
  executionId: string;
  status: "success" | "error" | "timeout";
  output: unknown | null;
  errorMessage: string | null;
  durationMs: number;
}

function sanitizedBounded(value: unknown, limit: number): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return sanitizeErrorMessage(value).slice(0, limit);
  let serialized: string;
  try {
    serialized = JSON.stringify(sanitizeUpstreamDetails(value));
  } catch {
    serialized = JSON.stringify({ error: "Value is not JSON-serializable" });
  }
  if (serialized.length <= limit) return serialized;
  for (let length = limit - 40; length > 0; length -= 20) {
    const envelope = JSON.stringify({ truncated: true, preview: serialized.slice(0, length) });
    if (envelope.length <= limit) return envelope;
  }
  return JSON.stringify({ truncated: true, preview: serialized.slice(0, 20) });
}
function classify(
  row: ToolExecutionRow,
  input: ServerToolClaimInput,
  now: number
): ServerToolClaim {
  if (row.tool_name !== input.toolName || row.input_digest !== input.inputDigest)
    return { kind: "identity_conflict", executionId: row.id };
  if (row.status === "success" || row.status === "error" || row.status === "timeout") {
    let output: unknown = row.output;
    if (row.output !== null) {
      try {
        output = JSON.parse(row.output);
      } catch {
        output = row.output;
      }
    }
    return {
      kind: "replay",
      executionId: row.id,
      status: row.status,
      output,
      errorMessage: row.error_message,
    };
  }
  return {
    kind: Date.parse(row.claim_expires_at) <= now ? "unknown" : "in_progress",
    executionId: row.id,
  };
}

export function createPostgresServerToolExecutionRepository(database: PostgresDatabase) {
  return {
    readRow: (id: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<ToolExecutionRow>(
              "SELECT id,tool_name,input_digest,status,output,error_message,claim_expires_at FROM omniroute.server_tool_executions WHERE id=$1",
              [id]
            )
          ).rows[0] ?? null
      ),
    claimServerToolExecution: (
      input: ServerToolClaimInput,
      nowMs?: number
    ): Promise<ServerToolClaim> =>
      database.transaction(async (tx) => {
        const id = randomUUID();
        const inserted = await tx.query(
          `INSERT INTO omniroute.server_tool_executions(id,api_key_id,request_identity,tool_call_id,tool_name,input_digest,status,claim_expires_at)
        VALUES($1,$2,$3,$4,$5,$6,'running',$7) ON CONFLICT(api_key_id,request_identity,tool_call_id) DO NOTHING RETURNING id`,
          [
            id,
            input.apiKeyId,
            input.requestIdentity,
            input.toolCallId,
            input.toolName,
            input.inputDigest,
            input.leaseExpiresAt,
          ]
        );
        if (inserted.rowCount === 1) return { kind: "claimed", executionId: id };
        const existing = (
          await tx.query<ToolExecutionRow>(
            "SELECT id,tool_name,input_digest,status,output,error_message,claim_expires_at FROM omniroute.server_tool_executions WHERE api_key_id=$1 AND request_identity=$2 AND tool_call_id=$3",
            [input.apiKeyId, input.requestIdentity, input.toolCallId]
          )
        ).rows[0];
        return existing
          ? classify(existing, input, nowMs ?? Date.now())
          : { kind: "unknown", executionId: id };
      }),
    finalizeServerToolExecution: (input: ServerToolFinalizeInput) =>
      database.transaction(async (tx) => {
        const result = await tx.query(
          `UPDATE omniroute.server_tool_executions SET status=$1,output=$2,error_message=$3,duration_ms=$4,
        completed_at=to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$5 AND status='running'`,
          [
            input.status,
            sanitizedBounded(input.output, 32768),
            sanitizedBounded(input.errorMessage, 4096),
            input.durationMs,
            input.executionId,
          ]
        );
        return (result.rowCount ?? 0) > 0;
      }),
  };
}
