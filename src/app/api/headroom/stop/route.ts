import { stopHeadroomProxy } from "@/lib/headroom/process";
import { stopHeadroomProxyAsync } from "@/lib/headroom/nativeProcess";
import { usesNativeHostProfile } from "@/lib/nativeHostProbes";
import { createErrorResponse } from "@/lib/api/errorResponse";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";

export const dynamic = "force-dynamic";

export async function POST(request?: Request): Promise<Response> {
  try {
    const result = usesNativeHostProfile()
      ? await stopHeadroomProxyAsync(request?.signal)
      : stopHeadroomProxy();
    const status = result.stopped ? 200 : 409;
    return Response.json(result, { status });
  } catch (error) {
    return createErrorResponse({
      status: 500,
      message: sanitizeErrorMessage(error),
      type: "server_error",
    });
  }
}
