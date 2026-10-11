import { AsyncLocalStorage } from "node:async_hooks";

interface PluginExecutionGuard {
  assertIdle(): void;
  assertIdleAsync?: () => Promise<void>;
  reject(): never;
}

const requestGuard = new AsyncLocalStorage<PluginExecutionGuard>();

/** Internal request capability; never derived from request headers or JSON fields. */
export function withPluginExecutionGuard<T>(guard: PluginExecutionGuard, operation: () => T): T {
  return requestGuard.run(guard, operation);
}

/** Strict requests may proceed only with no configured/runtime plugin to initialize. */
export function canInitializeRequestPlugins(): boolean {
  const guard = requestGuard.getStore();
  if (!guard) return true;
  guard.assertIdle();
  return false;
}

/** Call immediately before a real handler, outside catches that normally ignore plugin errors. */
export function assertPluginHandlerAllowed(): void {
  requestGuard.getStore()?.reject();
}

/** Re-check the latched policy at each dispatch and before receipt persistence. */
export function assertRequestPluginsIdle(): void {
  requestGuard.getStore()?.assertIdle();
}

export async function canInitializeRequestPluginsAsync(): Promise<boolean> {
  const guard = requestGuard.getStore();
  if (!guard) return true;
  if (guard.assertIdleAsync) await guard.assertIdleAsync();
  else guard.assertIdle();
  return false;
}
export async function assertRequestPluginsIdleAsync(): Promise<void> {
  const guard = requestGuard.getStore();
  if (guard?.assertIdleAsync) await guard.assertIdleAsync();
  else guard?.assertIdle();
}
