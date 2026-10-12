import { mitmFileExists, readMitmFile } from "@/mitm/nativeHostIo";
/**
 * GET /api/tools/agent-bridge/cert/download
 * Streams the PEM certificate file.
 * LOCAL_ONLY: registered in routeGuard.ts
 */
import { resolveMitmDataDir } from "@/mitm/dataDir";
import path from "path";
import { createErrorResponse } from "@/lib/api/errorResponse";

export async function GET(): Promise<Response> {
  const crtPath = path.join(resolveMitmDataDir(), "mitm", "server.crt");

  if (!(await mitmFileExists(crtPath))) {
    return createErrorResponse({
      status: 404,
      message:
        "Certificate not found. Generate one first via POST /api/tools/agent-bridge/cert/regenerate",
    });
  }

  try {
    const pem = await readMitmFile(crtPath);
    return new Response(Uint8Array.from(pem), {
      status: 200,
      headers: {
        "Content-Type": "application/x-pem-file",
        "Content-Disposition": 'attachment; filename="omniroute-mitm.crt"',
        "Content-Length": String(pem.length),
      },
    });
  } catch {
    return createErrorResponse({ status: 500, message: "Failed to read certificate file" });
  }
}
