import { isResponsesTerminalEventType } from "../../utils/responsesTerminalEvent.ts";

// Two legal SSE line endings. Unlike repeating a CR/LF alternation, this
// cannot backtrack into treating a single CRLF as an empty line.
export const CODEX_SSE_BLOCK_SEP_RE = /\r\n\r\n|\r\n[\r\n]|\r\r\n?|\n\r\n?|\n\n/;

function hasOutputProgress(data: string): boolean {
  try {
    const payload: unknown = JSON.parse(data);
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
    const event = payload as Record<string, unknown>;
    if (isResponsesTerminalEventType(event.type)) return true;
    if (typeof event.type !== "string" || event.error) return false;
    // Responses text, reasoning, and tool argument/input deltas all carry a
    // non-empty string delta. Lifecycle/heartbeat/empty frames are not output.
    return (
      event.type.startsWith("response.") &&
      event.type.endsWith(".delta") &&
      typeof event.delta === "string" &&
      event.delta.length > 0
    );
  } catch {
    return false;
  }
}

/** Release the pre-read on complete output frames, not merely response.created.
 * Lifecycle-only handoff would lose created -> overload fallback across chunks.
 * Parse SSE/JSON rather than depending on compact JSON substring formatting.
 */
export function hasCodexSsePeekProgress(text: string): boolean {
  // The final split member is an unfinished frame (or empty after a separator).
  const frames = text.split(CODEX_SSE_BLOCK_SEP_RE).slice(0, -1);
  return frames.some((frame) => {
    const data = frame
      .split(/\r\n|\r|\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n");
    return hasOutputProgress(data);
  });
}
