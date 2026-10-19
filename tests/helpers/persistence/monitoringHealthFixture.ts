import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";

export const monitoringFixture = {
  builds: 0,
  reads: 0,
  probes: 0,
  breakerVersion: 1,
  probeVersion: 200,
  readBarrier: undefined as Promise<void> | undefined,
  buildBarrier: undefined as Promise<void> | undefined,
  probeBarrier: undefined as Promise<void> | undefined,
  buildEntered: undefined as (() => void) | undefined,
  probeEntered: undefined as (() => void) | undefined,
};

function assertScope() {
  getSelectedControlPlane()!.assertReady();
  monitoringFixture.reads++;
}

export async function getCachedSettings() {
  assertScope();
  if (monitoringFixture.readBarrier) await monitoringFixture.readBarrier;
  assertScope();
  return { deepHealthUrl: "https://probe.invalid/v1/chat/completions" };
}

export async function getProviderConnections() {
  assertScope();
  return [];
}

export function getAllCircuitBreakerStatuses() {
  assertScope();
  return [{ provider: "fixture", state: "CLOSED", version: monitoringFixture.breakerVersion }];
}

export function resetAllCircuitBreakers() {
  assertScope();
  monitoringFixture.breakerVersion++;
}

export async function readRunningBuildShaAsync() {
  const gate = monitoringFixture.buildBarrier;
  monitoringFixture.buildBarrier = undefined;
  monitoringFixture.buildEntered?.();
  if (gate) await gate;
  return "test-build";
}

export function buildHealthPayload(input: Record<string, unknown>) {
  monitoringFixture.builds++;
  return {
    status: "healthy",
    timestamp: new Date(Date.now()).toISOString(),
    providerBreakers: input.circuitBreakers,
  };
}

export async function probeDeepHealth() {
  monitoringFixture.probes++;
  const status = monitoringFixture.probeVersion;
  const gate = monitoringFixture.probeBarrier;
  monitoringFixture.probeBarrier = undefined;
  monitoringFixture.probeEntered?.();
  if (gate) await gate;
  return { ok: status === 200, failover: false, status };
}

export const DEEP_HEALTH_VERDICT_TTL_MS = 30000;
export const DEEP_HEALTH_PROBE_TIMEOUT_MS = 3000;
export const APP_CONFIG = { version: "test" };
export const AI_PROVIDERS = {};
export const requireManagementAuth = async (request: Request) =>
  request.headers.has("x-test-management") ? null : new Response(null, { status: 401 });
export const isAuthenticated = async () => true;
export const getWalMaintenanceState = () => null;
export const getArtifactReconciliationStatus = () => ({ state: "idle" });
export const getAllRateLimitStatus = () => ({});
export const getLearnedLimits = () => ({});
export const getAllModelLockouts = () => [];
export const getInflightCount = () => 0;
export const getQuotaMonitorSummary = () => ({});
export const getQuotaMonitorSnapshots = () => [];
export const getActiveSessions = () => [];
export const getAllActiveSessionCountsByKey = () => ({});
export const getCachedCredentialHealthSummary = () => undefined;
export const getAllHealthStatuses = () => ({});
export const getAdaptiveAdmissionRuntime = () => ({ snapshot: () => null });
export const perConnectionAdmissionController = { snapshot: () => null };
