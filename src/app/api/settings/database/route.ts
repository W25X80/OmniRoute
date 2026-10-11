import { persistenceCapabilityResponse } from "@/lib/db/persistenceCapabilities";
import { NextRequest, NextResponse } from "next/server";
import { isAuthenticated } from "@/shared/utils/apiAuth";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";
import { databaseSettingsSchema } from "@/shared/validation/settingsSchemas";
import { getDatabaseSettingsAsync, updateDatabaseSettingsAsync } from "@/lib/db/databaseSettings";
import { redactCacheSecrets } from "@/lib/db/cacheSecrets";

async function publicDatabaseSettings() {
  const settings = await getDatabaseSettingsAsync();
  return { ...settings, cache: redactCacheSecrets(settings.cache) };
}

const databaseSettingsPatchSchema = databaseSettingsSchema.partial().strict();

export async function GET(request: NextRequest) {
  if (!(await isAuthenticated(request))) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    return NextResponse.json(await publicDatabaseSettings());
  } catch (error) {
    const unavailable = persistenceCapabilityResponse(error);
    if (unavailable) return NextResponse.json(unavailable, { status: 501 });
    console.error("Error getting database settings:", error);
    return NextResponse.json({ error: "Failed to load database settings" }, { status: 500 });
  }
}

export async function PATCH(request: NextRequest) {
  if (!(await isAuthenticated(request))) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const rawBody = await request.json();
    const validation = validateBody(databaseSettingsPatchSchema, rawBody);

    if (isValidationFailure(validation)) {
      return NextResponse.json({ error: validation.error }, { status: 400 });
    }

    await updateDatabaseSettingsAsync(validation.data);

    // Return merged settings (GET response pattern)
    return NextResponse.json(await publicDatabaseSettings());
  } catch (error) {
    const unavailable = persistenceCapabilityResponse(error);
    if (unavailable) return NextResponse.json(unavailable, { status: 501 });
    console.error("Error updating database settings:", error);
    return NextResponse.json({ error: "Failed to update database settings" }, { status: 500 });
  }
}

export async function PUT(request: NextRequest) {
  return PATCH(request);
}
