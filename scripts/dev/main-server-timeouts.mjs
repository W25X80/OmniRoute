// Main-server keepAlive/headers timeouts (#7003) — SIBLING module of
// standalone-server-ws.mjs. The shipped server-ws.mjs may only import
// siblings copied next to it by assembleStandalone (peer-stamp, tls-options,
// the guards): a ../../src/... import resolves OUTSIDE the package after the
// copy to the dist root and crashes boot with ERR_MODULE_NOT_FOUND (caught
// live by check:pack-boot on 2026-07-15 — the #7065 class).
// Parity with src/shared/utils/runtimeTimeouts.ts#getMainServerTimeoutConfig
// is enforced by tests/unit/main-server-timeouts-parity.test.ts.

export const DEFAULT_MAIN_SERVER_KEEPALIVE_TIMEOUT_MS = 65_000;
export const DEFAULT_MAIN_SERVER_HEADERS_TIMEOUT_MS = 66_000;

// Keep in parity with runtimeTimeouts.ts; native shutdown stays below ECS's 120 s limit.
export function getShutdownBudgetConfig(env = process.env) {
  const backend = env.OMNIROUTE_DATABASE_BACKEND ?? "postgres";
  if (backend !== "sqlite" && backend !== "postgres") {
    throw new Error("OMNIROUTE_DATABASE_BACKEND must be sqlite or postgres");
  }
  const native = backend === "postgres";
  const parsed = Number(env.SHUTDOWN_TIMEOUT_MS);
  const requested =
    env.SHUTDOWN_TIMEOUT_MS?.trim() && Number.isFinite(parsed) && parsed >= 0
      ? Math.floor(parsed)
      : native
        ? 90_000
        : 30_000;
  const drainMs = native ? Math.min(requested, 90_000) : requested;
  const cleanupMs = native ? 20_000 : 5_000;
  return { drainMs, cleanupMs, forceExitMs: drainMs + cleanupMs };
}

function readTimeoutMs(env, name, defaultValue, { allowZero = false, logger } = {}) {
  const raw = env[name];
  if (raw == null || raw.trim() === "") return defaultValue;
  const parsed = Number(raw);
  const isValid = Number.isFinite(parsed) && (allowZero ? parsed >= 0 : parsed > 0);
  if (!isValid) {
    logger?.(`Invalid ${name}="${raw}". Using default ${defaultValue}ms.`);
    return defaultValue;
  }
  return Math.floor(parsed);
}

export function getMainServerTimeoutConfig(env = process.env, logger) {
  const keepAliveTimeoutMs = readTimeoutMs(
    env,
    "MAIN_SERVER_KEEPALIVE_TIMEOUT_MS",
    DEFAULT_MAIN_SERVER_KEEPALIVE_TIMEOUT_MS,
    { allowZero: true, logger }
  );
  const headersTimeoutMs = readTimeoutMs(
    env,
    "MAIN_SERVER_HEADERS_TIMEOUT_MS",
    DEFAULT_MAIN_SERVER_HEADERS_TIMEOUT_MS,
    { allowZero: true, logger }
  );
  return {
    keepAliveTimeoutMs,
    // Node requires headersTimeout > keepAliveTimeout; keep both configurable
    // but always coherent (mirrors the canonical TS implementation).
    headersTimeoutMs:
      headersTimeoutMs > 0 && keepAliveTimeoutMs > 0
        ? Math.max(headersTimeoutMs, keepAliveTimeoutMs + 1_000)
        : headersTimeoutMs,
  };
}
