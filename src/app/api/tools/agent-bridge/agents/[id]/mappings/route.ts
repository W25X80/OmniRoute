/**
 * GET /api/tools/agent-bridge/agents/[id]/mappings   — list model mappings
 * PUT /api/tools/agent-bridge/agents/[id]/mappings   — replace all mappings
 * LOCAL_ONLY: registered in routeGuard.ts
 */
import { AgentBridgeMappingPutSchema } from "@/shared/schemas/agentBridge";
import {
  getMappingsForAgentAsync,
  setMappingsAsync,
  syncAgentBridgeMappingsToMitmAliasAsync,
} from "@/lib/db/agentBridgeMappings";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { createErrorResponse } from "@/lib/api/errorResponse";

type Params = { params: Promise<{ id: string }> };

export async function GET(_request: Request, { params }: Params): Promise<Response> {
  try {
    const { id } = await params;
    const mappings = await getMappingsForAgentAsync(id);
    return Response.json({ mappings });
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 500, message: msg });
  }
}

export async function PUT(request: Request, { params }: Params): Promise<Response> {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return createErrorResponse({ status: 400, message: "Invalid JSON body" });
  }

  const parsed = AgentBridgeMappingPutSchema.safeParse(body);
  if (!parsed.success) {
    return createErrorResponse({
      status: 400,
      message: "Invalid request body",
      details: parsed.error.flatten(),
    });
  }

  try {
    const { id } = await params;
    await setMappingsAsync(id, parsed.data.mappings);
    // Fix #8656: Sync to mitmAlias in key_value table so the MITM proxy
    // (server.cjs) reads user-configured mappings during interception.
    await syncAgentBridgeMappingsToMitmAliasAsync(id);
    const mappings = await getMappingsForAgentAsync(id);
    return Response.json({ ok: true, mappings });
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 500, message: msg });
  }
}
