import { scopeFixture } from "./mcpRuntimeScopeFixture.ts";

/** Record lifetime ownership independently of filesystem speed. Real heartbeat ticks have their own tests. */
export function startMcpHeartbeat(_config: unknown): () => Promise<void> {
  let stopped = false;
  return async () => {
    if (stopped) return;
    stopped = true;
    scopeFixture.heartbeatStops++;
  };
}
