import { persistenceCapabilityResponse } from "@/lib/db/persistenceCapabilities";
import { NextResponse } from "next/server";
import { runManagedDatabaseHealthCheck } from "@/lib/db/healthCheck";
import { isAuthenticated } from "@/shared/utils/apiAuth";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";

export async function GET(request: Request) {
  if (!(await isAuthenticated(request))) {
    return NextResponse.json({ error: { message: "Authentication required" } }, { status: 401 });
  }

  try {
    return NextResponse.json(
      await runManagedDatabaseHealthCheck({ autoRepair: false, skipIntegrityCheck: true })
    );
  } catch (error) {
    const unavailable = persistenceCapabilityResponse(error);
    if (unavailable) return NextResponse.json(unavailable, { status: 501 });
    const message = error instanceof Error ? error.message : String(error);
    console.error("[API] DB health diagnosis failed:", message);
    return NextResponse.json(
      { error: { message: sanitizeErrorMessage(message) } },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  if (!(await isAuthenticated(request))) {
    return NextResponse.json({ error: { message: "Authentication required" } }, { status: 401 });
  }

  try {
    return NextResponse.json(await runManagedDatabaseHealthCheck({ autoRepair: true }));
  } catch (error) {
    const unavailable = persistenceCapabilityResponse(error);
    if (unavailable) return NextResponse.json(unavailable, { status: 501 });
    const message = error instanceof Error ? error.message : String(error);
    console.error("[API] DB health repair failed:", message);
    return NextResponse.json(
      { error: { message: sanitizeErrorMessage(message) } },
      { status: 500 }
    );
  }
}
