import {
  mitmFileExists,
  mkdirMitmDirectory,
  readMitmFile,
  writeMitmFile,
} from "@/mitm/nativeHostIo";
/**
 * GET  /api/tools/agent-bridge/upstream-ca   — returns current upstream CA path
 * POST /api/tools/agent-bridge/upstream-ca   — validates + persists a new path
 * LOCAL_ONLY: registered in routeGuard.ts
 *
 * Persistence: <dataDir>/mitm/upstream-ca.path  (one-line text file)
 * After persisting, configureUpstreamCaAsync() is called immediately so the new CA
 * takes effect without a reboot. Spec: plan 11 §4.7.
 */
import { AgentBridgeUpstreamCaPostSchema } from "@/shared/schemas/agentBridge";
import { resolveMitmDataDir } from "@/mitm/dataDir";
import { configureUpstreamCaAsync } from "@/mitm/upstreamTrust";
import path from "path";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { createErrorResponse } from "@/lib/api/errorResponse";

const CA_PATH_FILE = path.join(resolveMitmDataDir(), "mitm", "upstream-ca.path");

async function readStoredCaPath(): Promise<string | null> {
  try {
    if (!(await mitmFileExists(CA_PATH_FILE))) return null;
    const raw = (await readMitmFile(CA_PATH_FILE, "utf8")).trim();
    return raw || null;
  } catch {
    return null;
  }
}

async function writeStoredCaPath(caPath: string): Promise<void> {
  const dir = path.dirname(CA_PATH_FILE);
  if (!(await mitmFileExists(dir))) await mkdirMitmDirectory(dir);
  await writeMitmFile(CA_PATH_FILE, caPath + "\n");
}

export async function GET(): Promise<Response> {
  try {
    const stored = await readStoredCaPath();
    // Prefer env var; file is secondary
    const active = process.env.AGENTBRIDGE_UPSTREAM_CA_CERT || stored || null;
    return Response.json({ path: active });
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 500, message: msg });
  }
}

export async function POST(request: Request): Promise<Response> {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return createErrorResponse({ status: 400, message: "Invalid JSON body" });
  }

  const parsed = AgentBridgeUpstreamCaPostSchema.safeParse(body);
  if (!parsed.success) {
    return createErrorResponse({
      status: 400,
      message: "Invalid request body",
      details: parsed.error.flatten(),
    });
  }

  const { path: caPath } = parsed.data;

  // Validate the file actually exists (plan 11 §4.7)
  if (!(await mitmFileExists(caPath))) {
    return createErrorResponse({
      status: 400,
      message: `Upstream CA file not found: ${caPath}`,
    });
  }

  try {
    await writeStoredCaPath(caPath);
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 500, message: msg });
  }

  // Activate the new CA immediately so it takes effect without a reboot.
  try {
    await configureUpstreamCaAsync(caPath);
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 400, message: msg });
  }

  return Response.json({ ok: true, path: caPath });
}
