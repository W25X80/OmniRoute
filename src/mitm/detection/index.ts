import { nativeMitmEnabled, assertNativeMitmNotAborted } from "../nativeHostIo.ts";
/**
 * Detection dispatcher.
 *
 * `detectAgent(id)` returns whether the given AgentBridge target is installed
 * on the current machine. All detection probes are filesystem-only — they
 * never spawn shells or interpolate runtime paths (Hard Rule #13).
 *
 * Trae is intentionally absent from the dispatch table: its viability is still
 * under investigation, so callers receive `{ installed: false }` until the
 * upstream surface is confirmed (see `targets/trae.ts`).
 */
import type { AgentId, DetectionResult } from "../types";
import { detectAntigravity, detectAntigravityAsync } from "./antigravity";
import { detectKiro, detectKiroAsync } from "./kiro";
import { detectCopilot, detectCopilotAsync } from "./copilot";
import { detectCodex, detectCodexAsync } from "./codex";
import { detectCursor, detectCursorAsync } from "./cursor";
import { detectZed, detectZedAsync } from "./zed";
import { detectClaudeCode, detectClaudeCodeAsync } from "./claudeCode";
import { detectOpenCode, detectOpenCodeAsync } from "./openCode";

export const DETECTORS: Record<AgentId, () => DetectionResult> = {
  antigravity: detectAntigravity,
  kiro: detectKiro,
  copilot: detectCopilot,
  // Enterprise and public Copilot use the same locally installed extension.
  "ghe-copilot": detectCopilot,
  codex: detectCodex,
  cursor: detectCursor,
  zed: detectZed,
  "claude-code": detectClaudeCode,
  "open-code": detectOpenCode,
  trae: () => ({ installed: false }),
};

export function detectAgent(id: AgentId): DetectionResult {
  const fn = DETECTORS[id];
  if (!fn) return { installed: false };
  try {
    return fn();
  } catch {
    return { installed: false };
  }
}

export {
  detectAntigravity,
  detectKiro,
  detectCopilot,
  detectCodex,
  detectCursor,
  detectZed,
  detectClaudeCode,
  detectOpenCode,
};

const ASYNC_DETECTORS: Record<AgentId, () => Promise<DetectionResult>> = {
  antigravity: detectAntigravityAsync,
  kiro: detectKiroAsync,
  copilot: detectCopilotAsync,
  "ghe-copilot": detectCopilotAsync,
  codex: detectCodexAsync,
  cursor: detectCursorAsync,
  zed: detectZedAsync,
  "claude-code": detectClaudeCodeAsync,
  "open-code": detectOpenCodeAsync,
  trae: async () => ({ installed: false }),
};
export async function detectAgentAsync(id: AgentId): Promise<DetectionResult> {
  if (!nativeMitmEnabled()) return detectAgent(id);
  try {
    return await (ASYNC_DETECTORS[id]?.() ?? { installed: false });
  } catch {
    assertNativeMitmNotAborted();
    return { installed: false };
  }
}
