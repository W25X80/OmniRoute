import { EventEmitter } from "node:events";
import { spawn as actualSpawn, type ChildProcess } from "node:child_process";
import { PassThrough } from "node:stream";
import fs from "node:fs";
import { observeNativeServiceWork as actualObserveServiceWork } from "../../../src/lib/services/nativeLifecycle.ts";
import type { NativeEmbeddedServices } from "../../../src/lib/services/nativeLifecycle.ts";
import { writeFile as actualWriteFile } from "node:fs/promises";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import {
  fixture as runtimeFixture,
  withPostgresCatalogSnapshot as actualCatalogSnapshot,
} from "./runtimeOwnedWorkFixture.ts";
import { decidePreSpawn } from "../../../src/lib/services/portProbe.ts";
import type { PreSpawnProbe } from "../../../src/lib/services/portProbe.ts";
import type { ServiceModel } from "../../../src/lib/db/serviceModels.ts";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

export class FakeServiceChild extends EventEmitter {
  pid: number | undefined = 42000;
  exitCode: number | null = null;
  signalCode: NodeJS.Signals | null = null;
  killed = false;
  stdout = new PassThrough();
  stderr = new PassThrough();
  signals: NodeJS.Signals[] = [];
  ignoreTerm = false;
  delayExit = false;
  kill(signal: NodeJS.Signals = "SIGTERM"): boolean {
    this.killed = true;
    this.signals.push(signal);
    if (signal === "SIGTERM" && this.ignoreTerm) return true;
    if (!this.delayExit) queueMicrotask(() => this.ackExit(signal));
    return true;
  }
  ackExit(signal: NodeJS.Signals): void {
    this.signalCode = signal;
    this.emit("exit", null, signal);
    this.stdout.end();
    this.stderr.end();
  }
}

export const serviceFixture = {
  children: [] as Array<FakeServiceChild | ChildProcess>,
  realChild: false,
  probe: { healthy: false, portInUse: false } as PreSpawnProbe,
  adoptEnabled: false,
  childFactory: () => new FakeServiceChild(),
  installed: new Set<string>(),
  rowBarrier: undefined as Promise<void> | undefined,
  statusBarrier: undefined as Promise<void> | undefined,
  statusEntered: undefined as (() => void) | undefined,
  writeBarrier: undefined as Promise<void> | undefined,
  writeEntered: undefined as (() => void) | undefined,
  realReady: barrier(),
  calls: [] as Array<{ operation: string; tool: string; generation: number }>,
  nextStateEffectBarrier: undefined as Promise<void> | undefined,
  stateEffectEntered: undefined as (() => void) | undefined,
  nextCatalogBarrier: undefined as Promise<void> | undefined,
  catalogEntered: undefined as (() => void) | undefined,
  fileWrites: 0,
  syncWrites: 0,
  flushPath: null as string | null,
  models: new Map<string, ServiceModel[]>(),
  reset() {
    this.children = [];
    this.realChild = false;
    this.probe = { healthy: false, portInUse: false };
    this.adoptEnabled = false;
    this.childFactory = () => new FakeServiceChild();
    this.installed = new Set();
    this.rowBarrier = undefined;
    this.statusBarrier = undefined;
    this.statusEntered = undefined;
    this.writeBarrier = undefined;
    this.writeEntered = undefined;
    this.realReady = barrier();
    this.calls = [];
    this.nextStateEffectBarrier = undefined;
    this.stateEffectEntered = undefined;
    this.nextCatalogBarrier = undefined;
    this.catalogEntered = undefined;
    this.fileWrites = 0;
    this.syncWrites = 0;
    this.flushPath = null;
    this.models = new Map();
  },
};

function record(operation: string, tool: string): void {
  if (process.env.OMNIROUTE_DATABASE_BACKEND === "sqlite") {
    serviceFixture.calls.push({ operation, tool, generation: 0 });
    return;
  }
  getSelectedControlPlane()!.assertReady();
  const generation = runtimeFixture.selectedGeneration();
  if (!generation) throw new Error("Embedded callback lacks a fresh catalog snapshot");
  serviceFixture.calls.push({ operation, tool, generation });
}
export async function getServiceRow(tool: string) {
  record("service-row", tool);
  if (serviceFixture.rowBarrier) await serviceFixture.rowBarrier;
  return { logsBufferPath: serviceFixture.flushPath, autoRestartAdopted: false };
}
export async function getVersionManagerTool(tool: string) {
  record("version-row", tool);
  return serviceFixture.installed.has(tool) ? { status: "installed", autoStart: false } : null;
}
export async function setToolStatus(tool: string, status: string) {
  record(`status:${status}`, tool);
  serviceFixture.statusEntered?.();
  if (serviceFixture.statusBarrier) await serviceFixture.statusBarrier;
}
export async function updateServiceField() {}
export async function updateVersionManagerTool(tool: string) {
  record("last-sync", tool);
}
export async function getSettings() {
  record("settings", "");
  return {};
}
export async function getOrCreateApiKey() {
  return "fixture-service-key";
}
export async function markAllUnavailableAsync(tool: string) {
  record("unavailable", tool);
}
export async function saveServiceModelsAsync(tool: string, models: ServiceModel[]) {
  record("models", tool);
  serviceFixture.models.set(tool, models);
}
export function getServiceModels(tool: string) {
  return serviceFixture.models.get(tool) ?? [];
}

export const CLIPROXY_DEFAULT_PORT = 8317,
  MUX_DEFAULT_PORT = 8322,
  BIFROST_DEFAULT_PORT = 8080,
  DARIO_DEFAULT_PORT = 3456,
  OPENWA_DEFAULT_PORT = 8323,
  LLMLINGUA_DEFAULT_PORT = 20135;
export function getServiceProviderPlugin(tool: string) {
  return {
    tool,
    port: { envVar: "FIXTURE_SERVICE_PORT", default: 20130 },
    healthPath: "/health",
    healthIntervalMs: 500,
    stopTimeoutMs: 50,
    logsBufferBytes: 4096,
    needsApiKey: true,
  };
}
export function resolveSpawnArgs() {
  return { command: process.execPath, args: [], env: process.env, cwd: process.cwd() };
}

export function spawn(...args: Parameters<typeof actualSpawn>): ChildProcess {
  const child = serviceFixture.realChild ? actualSpawn(...args) : serviceFixture.childFactory();
  serviceFixture.children.push(child);
  if (serviceFixture.realChild)
    child.stdout?.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("child-ready")) serviceFixture.realReady.resolve();
    });
  return child as unknown as ChildProcess;
}

export async function writeFile(...args: Parameters<typeof actualWriteFile>): Promise<void> {
  serviceFixture.fileWrites++;
  serviceFixture.writeEntered?.();
  if (serviceFixture.writeBarrier) await serviceFixture.writeBarrier;
  return actualWriteFile(...args);
}
const synchronousFiles = {
  ...fs,
  writeFileSync(...args: Parameters<typeof fs.writeFileSync>) {
    serviceFixture.syncWrites++;
    if (process.env.OMNIROUTE_DATABASE_BACKEND === "sqlite") return fs.writeFileSync(...args);
    throw new Error("Native ring flush called synchronous filesystem");
  },
};
export default synchronousFiles;

export { decidePreSpawn };
export async function probeBeforeSpawn(): Promise<PreSpawnProbe> {
  return serviceFixture.probe;
}
export async function resolvePortPid(): Promise<number> {
  return 42424;
}
export function isAdoptExistingEnabled(): boolean {
  return serviceFixture.adoptEnabled;
}

export async function withPostgresCatalogSnapshot<T>(
  database: Parameters<typeof actualCatalogSnapshot>[0],
  body: () => Promise<T>
): Promise<T> {
  const gate = serviceFixture.nextCatalogBarrier;
  serviceFixture.nextCatalogBarrier = undefined;
  if (gate) {
    serviceFixture.catalogEntered?.();
    await gate;
  }
  return actualCatalogSnapshot(database, body);
}

export {
  captureNativeEmbeddedServices,
  assertNativeServiceActivation,
} from "../../../src/lib/services/nativeLifecycle.ts";
export function observeNativeServiceWork(
  owner: NativeEmbeddedServices,
  body: () => Promise<void>
): void {
  const gate = serviceFixture.nextStateEffectBarrier;
  serviceFixture.nextStateEffectBarrier = undefined;
  actualObserveServiceWork(owner, async () => {
    if (gate) {
      serviceFixture.stateEffectEntered?.();
      await gate;
    }
    await body();
  });
}
