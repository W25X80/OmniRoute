import {
  createBodyTooLargeResponse,
  readFormDataWithLimit,
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
  readFormDataWithLimit,
  RequestBodyTooLargeError,
  resolveEffectiveApiKeyId,
  resolveListScope,
  formatFileResponse,
};
export const fileRouteFixture = {
  limit: 1024,
  records: [] as Parameters<typeof createOwner>[0][],
  settingsCalls: 0,
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
