import { pingDbAsync } from "@/lib/db/core";
import { getProviderConnectionStatusRowsAsync } from "@/lib/db/providerStatus";
import { listPoolsAsync as listPools } from "@/lib/db/quotaPools";

export async function buildOmniRouteStatus() {
  const [connections, circuitModule, quotaMonitorModule] = await Promise.all([
    getProviderConnectionStatusRowsAsync(),
    import("@/shared/utils/circuitBreaker").catch(() => null),
    import("../../open-sse/services/quotaMonitor").catch(() => null),
  ]);
  const pools = (await listPools()).items;
  const circuitStatuses = circuitModule?.getAllCircuitBreakerStatuses() ?? null;
  const quotaSummary = quotaMonitorModule?.getQuotaMonitorSummary() ?? null;
  const active = connections.filter(
    (connection) => connection.is_active !== 0 && connection.is_active !== false
  );
  const disabled = connections.filter(
    (connection) => connection.is_active === 0 || connection.is_active === false
  );
  const healthy = active.filter((connection) => connection.test_status === "active");

  return {
    gateway: (await pingDbAsync()) ? "healthy" : "degraded",
    catalog: { available: true },
    providers: {
      configured: connections.length,
      active: active.length,
      healthy: healthy.length,
      disabled: disabled.length,
      connections: connections.map((connection) => ({
        id: connection.id,
        provider: connection.provider,
        active: connection.is_active !== 0 && connection.is_active !== false,
        health:
          connection.is_active === 0 || connection.is_active === false
            ? "disabled"
            : connection.test_status === "active"
              ? "healthy"
              : "unknown",
        failureState: connection.last_error ? "recent_error" : "none",
      })),
    },
    pools: {
      count: pools.length,
      allocations: pools.reduce((count, pool) => count + pool.allocations.length, 0),
      items: pools.map((pool) => ({
        id: pool.id,
        name: pool.name,
        connectionIds: pool.connectionIds,
        allocationCount: pool.allocations.length,
      })),
    },
    quotaMonitoring: {
      authoritative: quotaSummary?.active ?? 0,
      headerBased: 0,
      configured: 0,
      unsupported: quotaSummary ? Math.max(0, active.length - quotaSummary.active) : null,
      status: quotaSummary?.active ? "partial" : "unknown",
    },
    circuits: circuitStatuses
      ? {
          open: circuitStatuses.filter((c) => c.state === "OPEN").length,
          halfOpen: circuitStatuses.filter((c) => c.state === "HALF_OPEN").length,
          closed: circuitStatuses.filter((c) => c.state === "CLOSED").length,
          source: "persisted circuit breaker registry",
        }
      : { status: "unknown", source: "resilience subsystem unavailable" },
    usage: { source: "usage_history and call_logs", liveRequestsExecuted: false },
    budgets: { source: "internal governance limits", upstreamQuotaClaims: false },
  };
}
