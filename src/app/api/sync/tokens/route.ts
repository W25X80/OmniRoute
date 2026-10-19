import { authorizeSyncTokenIssuer } from "@/lib/sync/issuer";
import { NextResponse } from "next/server";
import { getAuditRequestContext, logAuditEvent } from "@/lib/compliance/index";
import { createErrorResponse, createErrorResponseFromUnknown } from "@/lib/api/errorResponse";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { createSyncTokenSchema, parseSyncTokenTtlMs } from "@/shared/validation/schemas";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";
import {
  issueSyncToken,
  listSyncTokenSummaries,
  resolveSyncApiKeyIdFromManagementRequest,
} from "@/lib/sync/tokens";

export async function GET(request: Request) {
  const authError = await requireManagementAuth(request, { alwaysRequireAuth: true });
  if (authError) return authError;

  try {
    const tokens = await listSyncTokenSummaries();
    return NextResponse.json({
      tokens,
      total: tokens.length,
    });
  } catch (error) {
    return createErrorResponseFromUnknown(error, "Failed to list sync tokens");
  }
}

export async function POST(request: Request) {
  const issuer = await authorizeSyncTokenIssuer(request);
  if (issuer.ok === false) return issuer.response;

  const auditContext = getAuditRequestContext(request);

  let rawBody;
  try {
    rawBody = await request.json();
  } catch {
    return createErrorResponse({
      status: 400,
      message: "Invalid JSON body",
    });
  }

  try {
    const validation = validateBody(createSyncTokenSchema, rawBody);
    if (isValidationFailure(validation)) {
      return NextResponse.json({ error: validation.error }, { status: 400 });
    }

    const syncApiKeyId = await resolveSyncApiKeyIdFromManagementRequest(request);
    const issued = await issueSyncToken({
      name: validation.data.name,
      syncApiKeyId,
      issuedBy: issuer.issuer,
      authGeneration: issuer.generation,
      expiresAt: validation.data.ttl
        ? new Date(Date.now() + parseSyncTokenTtlMs(validation.data.ttl)!).toISOString()
        : undefined,
    });

    const tokenSummary = {
      id: issued.record.id,
      name: issued.record.name,
      syncApiKeyId: issued.record.syncApiKeyId,
      issuedBy: issued.record.issuedBy,
      expiresAt: issued.record.expiresAt,
      revokedAt: issued.record.revokedAt,
      lastUsedAt: issued.record.lastUsedAt,
      createdAt: issued.record.createdAt,
      updatedAt: issued.record.updatedAt,
    };

    logAuditEvent({
      action: "sync.token.created",
      actor: "admin",
      target: issued.record.name,
      resourceType: "sync_token",
      status: "success",
      ipAddress: auditContext.ipAddress || undefined,
      requestId: auditContext.requestId,
      metadata: tokenSummary,
    });

    return NextResponse.json(
      {
        token: issued.token,
        syncToken: tokenSummary,
      },
      { status: 201 }
    );
  } catch (error) {
    return createErrorResponseFromUnknown(error, "Failed to create sync token");
  }
}
