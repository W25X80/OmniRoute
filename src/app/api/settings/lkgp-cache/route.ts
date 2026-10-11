import { NextResponse } from "next/server";
import { clearAllLKGPAsync } from "@/lib/db/settings/lkgp";
import { isAuthenticated } from "@/shared/utils/apiAuth";

export async function DELETE(request: Request) {
  if (!(await isAuthenticated(request))) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  try {
    await clearAllLKGPAsync();
    return NextResponse.json({ cleared: true });
  } catch (err: unknown) {
    const error = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error }, { status: 500 });
  }
}
