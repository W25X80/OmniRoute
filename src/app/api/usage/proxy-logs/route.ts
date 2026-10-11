import {
  getProxyLogsAsync as getProxyLogs,
  clearProxyLogsAsync as clearProxyLogs,
  getProxyLogBufferStatus,
} from "@/lib/proxyLogger";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";

function proxyBufferHeaders(): Record<string, string> | undefined {
  const status = getProxyLogBufferStatus();
  if (!status) return undefined;
  return {
    "X-OmniRoute-Proxy-Log-Buffer-Rows": String(status.bufferedRows),
    "X-OmniRoute-Proxy-Log-Buffer-Bytes": String(status.bufferedBytes),
    "X-OmniRoute-Proxy-Log-Dropped": String(status.droppedEvents),
    "X-OmniRoute-Proxy-Log-Oversized": String(status.oversizedEvents),
    "X-OmniRoute-Proxy-Log-Row-Limit": String(status.rowLimit),
    "X-OmniRoute-Proxy-Log-Byte-Limit": String(status.byteLimit),
    "X-OmniRoute-Proxy-Log-Event-Byte-Limit": String(status.eventByteLimit),
    "X-OmniRoute-Proxy-Log-Flush-Failures": String(status.consecutiveFlushFailures),
    "X-OmniRoute-Proxy-Log-Next-Flush-At": String(status.nextFlushAt),
  };
}

function serverErrorResponse(error: unknown): Response {
  return Response.json(
    {
      error: {
        message: sanitizeErrorMessage(error instanceof Error ? error.message : String(error)),
        type: "server_error",
      },
    },
    { status: 500, headers: proxyBufferHeaders() }
  );
}

/**
 * GET /api/usage/proxy-logs — get proxy usage logs
 * Query params: ?status=ok|error|timeout&type=http|socks5&provider=xxx&level=global|provider|combo|key&search=xxx&limit=300
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);

    const filters: Record<string, any> = {};
    if (searchParams.get("status")) filters.status = searchParams.get("status");
    if (searchParams.get("type")) filters.type = searchParams.get("type");
    if (searchParams.get("provider")) filters.provider = searchParams.get("provider");
    if (searchParams.get("level")) filters.level = searchParams.get("level");
    if (searchParams.get("search")) filters.search = searchParams.get("search");
    if (searchParams.get("limit")) filters.limit = parseInt(searchParams.get("limit"), 10);

    const logs = await getProxyLogs(filters);
    return Response.json(logs, { headers: proxyBufferHeaders() });
  } catch (error) {
    return serverErrorResponse(error);
  }
}

/**
 * DELETE /api/usage/proxy-logs — clear all proxy logs
 */
export async function DELETE() {
  try {
    await clearProxyLogs();
    return Response.json({ cleared: true }, { headers: proxyBufferHeaders() });
  } catch (error) {
    return serverErrorResponse(error);
  }
}
