import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { requireOperatorCredential } from "@/lib/api/operatorCredential";
import { createErrorResponseFromUnknown } from "@/lib/api/errorResponse";
import { NextResponse } from "next/server";
import { z } from "zod";
import { getRelayTokensAsync, createRelayTokenAsync } from "@/lib/db/relayProxies";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";
const relayTokenInputSchema = z.object({
  name: z.string().trim().min(1, "name is required"),
  description: z.string().optional(),
  comboId: z.string().trim().min(1).optional(),
  allowedModels: z.array(z.string().trim().min(1)).optional(),
  maxTokensPerRequest: z.number().int().positive().optional(),
  maxRequestsPerMinute: z.number().int().positive().optional(),
  maxRequestsPerDay: z.number().int().positive().optional(),
  maxCostPerDay: z.number().nonnegative().optional(),
  expiresAt: z.number().int().positive().optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export async function GET(request: Request) {
  const denied = await requireManagementAuth(request, { alwaysRequireAuth: true });
  if (denied) return denied;
  const tokens = await getRelayTokensAsync();
  // Strip hash from response
  const safe = tokens.map((t) => ({
    id: t.id,
    name: t.name,
    tokenPrefix: t.tokenPrefix,
    description: t.description,
    comboId: t.comboId,
    allowedModels: t.allowedModels,
    maxTokensPerRequest: t.maxTokensPerRequest,
    maxRequestsPerMinute: t.maxRequestsPerMinute,
    maxRequestsPerDay: t.maxRequestsPerDay,
    maxCostPerDay: t.maxCostPerDay,
    enabled: t.enabled,
    createdAt: t.createdAt,
    updatedAt: t.updatedAt,
    expiresAt: t.expiresAt,
    lastUsedAt: t.lastUsedAt,
  }));
  return NextResponse.json(safe);
}

export async function POST(request: Request) {
  const verdict = await requireOperatorCredential(request, {
    alwaysRequireAuth: true,
    forbiddenMessage: "Minting relay tokens requires an operator credential.",
  });
  if (verdict.ok === false) return verdict.response;
  try {
    const rawBody = await request.json();
    const validation = validateBody(relayTokenInputSchema, rawBody);
    if (isValidationFailure(validation)) {
      return NextResponse.json({ error: validation.error }, { status: 400 });
    }
    const token = await createRelayTokenAsync(validation.data);

    return NextResponse.json({
      id: token.id,
      name: token.name,
      rawToken: token.rawToken,
      tokenPrefix: token.tokenPrefix,
    });
  } catch (error) {
    if (error instanceof SyntaxError)
      return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
    return createErrorResponseFromUnknown(error, "Failed to create relay token");
  }
}
