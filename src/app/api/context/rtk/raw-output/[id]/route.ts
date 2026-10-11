import { NextResponse } from "next/server";
import { readRtkRawOutputAsync as readRtkRawOutput } from "@omniroute/open-sse/services/compression/engines/rtk";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";

export async function GET(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;

  const { id } = await params;
  if (!/^[a-f0-9]{24}$/.test(id)) {
    return NextResponse.json({ error: "Invalid raw output id" }, { status: 400 });
  }

  const content = await readRtkRawOutput(id);
  if (content === null) {
    return NextResponse.json({ error: "Raw output not found" }, { status: 404 });
  }

  return new NextResponse(content, {
    status: 200,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}
