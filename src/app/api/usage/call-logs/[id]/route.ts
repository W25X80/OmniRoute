import { NextResponse } from "next/server";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { getManagementCallLogById } from "@/lib/usage/callLogs";

export async function GET(request, { params }) {
  try {
    const authError = await requireManagementAuth(request);
    if (authError) return authError;

    const { id } = await params;
    const log = await getManagementCallLogById(id, request?.signal);

    if (!log) {
      return NextResponse.json({ error: "Log not found" }, { status: 404 });
    }

    return NextResponse.json(log);
  } catch (error) {
    console.error("[API ERROR] /api/usage/call-logs/[id] failed:", error);
    return NextResponse.json({ error: "Failed to fetch log" }, { status: 500 });
  }
}
