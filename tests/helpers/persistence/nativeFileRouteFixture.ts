import { AsyncLocalStorage } from "node:async_hooks";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { NativeFileWork, getNativeFileHttpWork } from "../../../src/lib/db/nativeFileWork.ts";
import {
  createBodyTooLargeResponse,
  readFormDataWithLimit as readFormDataOwner,
  RequestBodyTooLargeError,
} from "../../../src/shared/middleware/bodySizeGuard";
import {
  resolveEffectiveApiKeyId,
  resolveListScope,
} from "../../../src/app/api/v1/_helpers/apiKeyScope";
import { formatFileResponse } from "../../../src/lib/db/files";
import type { createFileAsync as createOwner } from "../../../src/lib/db/files";
export {
  createBodyTooLargeResponse,
  RequestBodyTooLargeError,
  resolveEffectiveApiKeyId,
  resolveListScope,
  formatFileResponse,
};
export const fileRouteFixture = {
  limit: 1024,
  records: [] as Parameters<typeof createOwner>[0][],
  settingsCalls: 0,
  parserCalls: 0,
  parserErrors: [] as unknown[],
  paths: [] as string[],
  policies: [] as Request[],
  settings: { maxBodySizeMb: 17 },
};
export async function getCachedSettings() {
  fileRouteFixture.settingsCalls++;
  return fileRouteFixture.settings;
}
export function getBodySizeLimit(path: string, settings: unknown) {
  if (settings !== fileRouteFixture.settings) throw new Error("Missing exact settings snapshot");
  fileRouteFixture.paths.push(path);
  return fileRouteFixture.limit;
}
export async function getApiKeyRequestScope() {
  return { apiKey: "presented", apiKeyId: "scope-owner", rejection: null };
}
export async function enforceApiKeyPolicy(request: Request) {
  fileRouteFixture.policies.push(request);
  return { rejection: null, apiKeyInfo: { id: "policy-owner" } };
}
export async function createFileAsync(input: Parameters<typeof createOwner>[0]) {
  fileRouteFixture.records.push(input);
  return {
    ...input,
    id: "file-fixture",
    createdAt: 1700000000,
    mimeType: input.mimeType ?? null,
    apiKeyId: input.apiKeyId ?? null,
    expiresAt: input.expiresAt ?? null,
    deletedAt: null,
  };
}
export async function listFilesAsync() {
  return [];
}
export async function countFilesAsync() {
  return 0;
}

/** Controlled application authority; the actual file capacity owner still admits and accounts bytes. */
export async function withNativeFileFixtureAuthority<T>(body: () => Promise<T>): Promise<T> {
  const previousRuntime = globalThis.__omniroutePersistenceRuntime;
  const identity = Object.freeze({});
  const owners = new AsyncLocalStorage<{ active: boolean }>();
  const owner = { active: true };
  const binding = {
    databaseIdentity: identity,
    assertReady() {
      if (!owner.active) throw new Error("File fixture scope has ended");
    },
  } as PostgresControlPlaneBinding;
  globalThis.__omniroutePersistenceRuntime = {
    backend: "postgres",
    phase: "ready",
    applicationIdentity: identity,
    owners,
    requests: new Set(),
    background: new Set(),
    tails: new Set(),
  } as NonNullable<typeof globalThis.__omniroutePersistenceRuntime>;
  try {
    return await withPostgresControlPlane(binding, () => owners.run(owner, body));
  } finally {
    owner.active = false;
    globalThis.__omniroutePersistenceRuntime = previousRuntime;
  }
}

/** Direct route fixture owns a small, already-built Request; real HTTP tests meter each wire chunk. */
export async function runNativeUploadRouteFixture<T>(
  request: Request,
  body: () => Promise<T>
): Promise<T> {
  const wireBytes = (await request.clone().arrayBuffer()).byteLength;
  return withNativeFileFixtureAuthority(async () => {
    const work = NativeFileWork.acquire("upload");
    work.observeWire(wireBytes, 500 * 1024 * 1024);
    try {
      return await globalThis.__omnirouteNativeFileWork!.http.run(work, body);
    } finally {
      work.release();
    }
  });
}

export async function readFormDataWithLimit(...args: Parameters<typeof readFormDataOwner>) {
  getNativeFileHttpWork().assertActive();
  fileRouteFixture.parserCalls++;
  try {
    return await readFormDataOwner(...args);
  } catch (error) {
    fileRouteFixture.parserErrors.push(error);
    throw error;
  }
}
