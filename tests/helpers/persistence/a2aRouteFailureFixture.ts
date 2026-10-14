import { A2ATaskManager, type A2APersistence } from "../../../src/lib/a2a/taskManager.ts";
import type { TaskState } from "../../../src/lib/a2a/taskManager.ts";
import { createA2AStream as stream } from "../../../src/lib/a2a/streaming.ts";
export { SSE_HEADERS } from "../../../src/lib/a2a/streaming.ts";
export { executeA2ATaskWithState } from "../../../src/lib/a2a/taskExecution.ts";

// This fixture isolates task-write outcomes under an explicit test plane. Real admission,
// HTTP cancellation and drain are exercised by nativeHttpHandlerLifetime.test.ts.
export async function runPersistenceAttempt<T>(body: () => T | Promise<T>): Promise<T> {
  return body();
}

export const a2aRouteFixture = {
  manager: undefined as A2ATaskManager | undefined,
  writes: [] as string[],
  handlerCalls: 0,
  streamCalls: 0,
  failureState: undefined as TaskState | undefined,
  failure: undefined as unknown,
  handlerFailure: undefined as unknown,
  reset() {
    this.manager?.destroy();
    this.manager = undefined;
    this.writes = [];
    this.handlerCalls = 0;
    this.streamCalls = 0;
    this.failureState = undefined;
    this.failure = undefined;
    this.handlerFailure = undefined;
  },
  initialize() {
    const persistence: A2APersistence = {
      upsert() {},
      appendEvent() {},
      purge() {
        return 0;
      },
      async persist(row) {
        a2aRouteFixture.writes.push(row.state);
        if (row.state === a2aRouteFixture.failureState) throw a2aRouteFixture.failure;
      },
    };
    this.manager = new A2ATaskManager(5, persistence);
  },
};
export function getTaskManager(): A2ATaskManager {
  if (!a2aRouteFixture.manager) throw new Error("A2A fixture manager is not initialized");
  return a2aRouteFixture.manager;
}
export const A2A_SKILL_HANDLERS = {
  async fixture() {
    a2aRouteFixture.handlerCalls++;
    if (a2aRouteFixture.handlerFailure) throw a2aRouteFixture.handlerFailure;
    return { artifacts: [{ type: "text", content: "completed" }], metadata: {} };
  },
};
export function createA2AStream(...args: Parameters<typeof stream>) {
  a2aRouteFixture.streamCalls++;
  return stream(...args);
}
export function logRoutingDecision(): void {}
export async function getSettings() {
  return { a2aEnabled: true, requireLogin: true, setupComplete: true };
}

// Keep the real A2A authentication/owner code; only its key-transport port is substituted.
export function extractApiKey(request: Request): string | null {
  const value = request.headers.get("authorization");
  return value?.startsWith("Bearer ") ? value.slice(7) : null;
}
export async function isValidApiKey(): Promise<boolean> {
  throw new Error("Explicit A2A-key authentication must not consult inference-key validation");
}
// The native flag-storage port publishes the admitted explicit A2A-key posture.
// The real resolver still applies its DB > environment > default priority and auth policy.
export function getFeatureFlagOverride(key: string): string | undefined {
  return key === "REQUIRE_API_KEY" ? "false" : undefined;
}
export async function getFeatureFlagOverrideAsync(key: string) {
  return getFeatureFlagOverride(key);
}
export async function getFeatureFlagOverridesAsync() {
  return { REQUIRE_API_KEY: "false" };
}
