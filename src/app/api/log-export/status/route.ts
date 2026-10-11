/**
 * API: Log export status
 * GET — Scheduler state (cron, enabled, recent runs) plus the current backlog, so the
 *       dashboard can answer "is the hourly export healthy?" in one request.
 */

import { NextResponse } from "next/server";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { getJobRegistry } from "@/lib/jobRegistry";
import { LOG_EXPORT_JOB_ID } from "@/lib/jobs/logExportJob";
import { getLogExportDestinationsAsync } from "@/lib/db/logExportDestinations";
import { getMaxCallLogRowIdAsync } from "@/lib/usage/callLogExportSource";
import { toDestinationViewAsync } from "@/lib/logExport/presenter";

export async function GET(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;

  try {
    const destinations = await Promise.all(
      (await getLogExportDestinationsAsync()).map(toDestinationViewAsync)
    );
    const registry = getJobRegistry();
    const job =
      (await registry.listJobsAsync()).find((entry) => entry.id === LOG_EXPORT_JOB_ID) ?? null;

    return NextResponse.json({
      job: job
        ? {
            id: job.id,
            enabled: job.enabled,
            cron: job.cron,
            timezone: (job.config?.timezone as string) ?? "UTC",
          }
        : null,
      runs: job ? await registry.getRunsAsync(LOG_EXPORT_JOB_ID, 20) : [],
      maxCallLogRowId: await getMaxCallLogRowIdAsync(),
      destinations,
    });
  } catch (error: any) {
    return NextResponse.json(
      { error: sanitizeErrorMessage(error) || "Failed to read log export status" },
      { status: 500 }
    );
  }
}
