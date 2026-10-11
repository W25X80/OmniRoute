import type { Server } from "node:http";

export const liveOwnerFixture = {
  identity: {},
  events: [] as string[],
  enabled: true,
  startBarrier: undefined as Promise<void> | undefined,
  frameBarrier: undefined as Promise<void> | undefined,
  failStart: false,
  server: {} as Server,
};
export function getSelectedPersistenceIdentity(): object {
  return liveOwnerFixture.identity;
}
export async function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  liveOwnerFixture.events.push("owned:start");
  return body();
}
export function isLiveWsEnabled(): boolean {
  return liveOwnerFixture.enabled;
}
export async function startLiveDashboardServer(port: number, host: string): Promise<Server> {
  liveOwnerFixture.events.push("listen:" + host + ":" + port);
  await liveOwnerFixture.startBarrier;
  if (liveOwnerFixture.failStart) throw new Error("Fixture listener failed");
  liveOwnerFixture.events.push("listen:ready");
  return liveOwnerFixture.server;
}
export async function stopLiveDashboardServer(server: Server): Promise<void> {
  if (server !== liveOwnerFixture.server) throw new Error("The actual listener was not retained");
  liveOwnerFixture.events.push("stop:latch");
  await liveOwnerFixture.frameBarrier;
  liveOwnerFixture.events.push("stop:drained");
}
export function warnIfNonLoopbackWithoutApiKey(_label: string, host: string): void {
  liveOwnerFixture.events.push("warning:" + host);
}
