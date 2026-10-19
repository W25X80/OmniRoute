import { ApiKeyScopeConflictError } from "@/domain/persistence/apiKeyRepository";
import { NextResponse } from "next/server";
import { getApiKeyById, regenerateApiKey } from "@/lib/db/apiKeys";
import {
  requireCredentialForManagementScopedKey,
  requireManagementAuth,
} from "@/lib/api/requireManagementAuth";
import * as log from "@/sse/utils/logger";

/**
 * POST /api/keys/[id]/regenerate
 *
 * Regenerates the API key value for a given ID.
 * The old key is immediately invalidated.
 */
export async function POST(request, { params }) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;

  try {
    const { id } = await params;
    if (!id) {
      return NextResponse.json({ error: "Missing key ID" }, { status: 400 });
    }

    const existing = await getApiKeyById(id);
    if (!existing) return NextResponse.json({ error: "Key not found" }, { status: 404 });
    const denied = await requireCredentialForManagementScopedKey(request, existing.scopes);
    if (denied) return denied;
    const result = await regenerateApiKey(id, { expectedScopes: existing.scopes });
    if (!result) {
      return NextResponse.json({ error: "Key not found" }, { status: 404 });
    }

    return NextResponse.json({
      message: "API key regenerated successfully",
      key: result.key,
      id: result.id,
    });
  } catch (error) {
    if (error instanceof ApiKeyScopeConflictError)
      return NextResponse.json(
        { error: "API key scopes changed; refresh before regenerating" },
        { status: 409 }
      );
    log.error("keys", "Error regenerating key", error);
    return NextResponse.json({ error: "Failed to regenerate key" }, { status: 500 });
  }
}
