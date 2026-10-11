import { NextResponse } from "next/server";
import {
  applyIPFilterUpdateAsync,
  getIPFilterConfig,
} from "@omniroute/open-sse/services/ipFilter.ts";
import { updateIpFilterSchema } from "@/shared/validation/schemas";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";

export async function GET(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;
  try {
    return NextResponse.json(getIPFilterConfig());
  } catch (error) {
    console.error("Error getting IP filter config:", error);
    return NextResponse.json({ error: "Failed to get IP filter config" }, { status: 500 });
  }
}

export async function PUT(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;
  let rawBody;
  try {
    rawBody = await request.json();
  } catch {
    return NextResponse.json(
      {
        error: {
          message: "Invalid request",
          details: [{ field: "body", message: "Invalid JSON body" }],
        },
      },
      { status: 400 }
    );
  }

  try {
    const validation = validateBody(updateIpFilterSchema, rawBody);
    if (isValidationFailure(validation)) {
      return NextResponse.json({ error: validation.error }, { status: 400 });
    }
    const body = validation.data;

    await applyIPFilterUpdateAsync(body);

    return NextResponse.json(getIPFilterConfig());
  } catch (error) {
    console.error("Error updating IP filter config:", error);
    return NextResponse.json({ error: "Failed to update IP filter config" }, { status: 500 });
  }
}
