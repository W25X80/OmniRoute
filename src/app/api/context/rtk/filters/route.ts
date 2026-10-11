import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { NextResponse } from "next/server";
import {
  getRtkFilterCatalog,
  getRtkFilterCatalogSnapshotAsync,
  getRtkFilterLoadDiagnostics,
} from "@omniroute/open-sse/services/compression/engines/rtk/filterLoader";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";

export async function GET(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;
  if (parseDatabaseBackend() === "postgres")
    return NextResponse.json(await getRtkFilterCatalogSnapshotAsync());
  return NextResponse.json({
    filters: getRtkFilterCatalog(),
    diagnostics: getRtkFilterLoadDiagnostics(),
  });
}
