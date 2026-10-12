import { getCachedSettings } from "@/lib/db/settings";
import {
  DEFAULT_HEADROOM_URL,
  getHeadroomStatus,
  getHeadroomStatusAsync,
} from "@/lib/headroom/detect";
import { getManagedPid } from "@/lib/headroom/process";
import { getManagedHeadroomPidAsync } from "@/lib/headroom/nativeProcess";
import { usesNativeHostProfile } from "@/lib/nativeHostProbes";
import { createErrorResponse } from "@/lib/api/errorResponse";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";

export const dynamic = "force-dynamic";

export async function GET(request?: Request): Promise<Response> {
  try {
    const settings = await getCachedSettings();
    const url =
      typeof settings.headroomUrl === "string" && settings.headroomUrl
        ? settings.headroomUrl
        : DEFAULT_HEADROOM_URL;
    const native = usesNativeHostProfile();
    const status = await (native
      ? getHeadroomStatusAsync(url, { signal: request?.signal })
      : getHeadroomStatus(url));
    const managedPid = native ? await getManagedHeadroomPidAsync(request?.signal) : getManagedPid();
    return Response.json({ ...status, url, managedPid });
  } catch (error) {
    return createErrorResponse({
      status: 500,
      message: sanitizeErrorMessage(error),
      type: "server_error",
    });
  }
}
