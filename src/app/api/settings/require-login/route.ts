import { randomUUID } from "node:crypto";
import { NextResponse } from "next/server";
import { cookies } from "next/headers";
import { getAuthSettingsSnapshot } from "@/lib/db/authSettings";
import { AUTH_GENERATION_SETTING } from "@/lib/db/authSettingsSemantics";
import { isPostgresErrorKind } from "@/lib/db/postgres/database";
import { authorizeLoginSettingsWrite } from "@/lib/api/loginSettingsAuth";
import { createErrorResponseFromUnknown } from "@/lib/api/errorResponse";
import {
  DASHBOARD_SESSION_COOKIE,
  SESSIONS_VALID_AFTER_SETTING,
  getDashboardJwtSecret,
  mintDashboardSessionToken,
  verifyDashboardSessionToken,
} from "@/shared/utils/dashboardSessionToken";
import { isFeatureFlagEnabled } from "@/shared/utils/featureFlags";
import { getSettings, updateSettings, SettingsRevisionConflictError } from "@/lib/db/settings";
import {
  ensurePersistentManagementPasswordHash,
  getStoredManagementPassword,
  verifyManagementPassword,
  hasManagementPasswordConfigured,
  hashManagementPassword,
} from "@/lib/auth/managementPassword";
import { consumeBootstrapToken } from "@/lib/auth/bootstrapToken";
import { BOOTSTRAP_TOKEN_HEADER } from "@/server/authz/headers";
import { getNodeRuntimeSupport } from "@/shared/utils/nodeRuntimeSupport.ts";
import { updateRequireLoginSchema } from "@/shared/validation/schemas";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";

async function checkSessionAuthenticated(): Promise<boolean> {
  try {
    const cookieStore = await cookies();
    const token = cookieStore.get(DASHBOARD_SESSION_COOKIE)?.value;
    return (await verifyDashboardSessionToken(token, getDashboardJwtSecret())) !== null;
  } catch {
    return false;
  }
}

// Node.js compatibility check — reflect the supported secure runtime floors used by CLI/CI.
function getNodeCompatibility() {
  const { nodeVersion, nodeCompatible } = getNodeRuntimeSupport();
  return { nodeVersion, nodeCompatible };
}

export async function GET() {
  const nodeInfo = getNodeCompatibility();
  try {
    const settings = await getSettings();
    const requireLogin = settings.requireLogin !== false;
    const authenticated = await checkSessionAuthenticated();
    const hasPassword = hasManagementPasswordConfigured(settings);
    const setupComplete = !!settings.setupComplete;
    const oidcEnabled = !!settings.oidcEnabled;
    const oidcDisablePasswordLogin =
      oidcEnabled &&
      (settings.oidcDisablePasswordLogin === true ||
        isFeatureFlagEnabled("OMNIROUTE_OIDC_DISABLE_PASSWORD_LOGIN") ||
        process.env.OMNIROUTE_OIDC_DISABLE_PASSWORD_LOGIN === "true" ||
        process.env.OIDC_DISABLE_PASSWORD_LOGIN === "true");
    return NextResponse.json({
      authenticated,
      requireLogin,
      hasPassword,
      setupComplete,
      oidcEnabled,
      oidcDisablePasswordLogin,
      ...nodeInfo,
    });
  } catch (error) {
    console.error("[API] Error fetching require-login settings:", error);
    return NextResponse.json(
      {
        authenticated: false,
        requireLogin: true,
        hasPassword: true,
        setupComplete: true,
        oidcEnabled: false,
        oidcDisablePasswordLogin: false,
        ...nodeInfo,
      },
      { status: 200 }
    );
  }
}

/**
 * POST /api/settings/require-login — Set password and/or toggle requireLogin.
 * Unauthenticated writes are only allowed during the initial bootstrap window.
 */
export async function POST(request: Request) {
  let authority: Awaited<ReturnType<typeof authorizeLoginSettingsWrite>> | undefined;
  let mutationStarted = false;
  try {
    const snapshot = await getAuthSettingsSnapshot();
    const rawBody = await request.json();
    const validation = validateBody(updateRequireLoginSchema, rawBody);
    if (isValidationFailure(validation))
      return NextResponse.json({ error: validation.error }, { status: 400 });
    authority = await authorizeLoginSettingsWrite(request, snapshot);
    if (authority.ok === false) return authority.response;
    const { requireLogin, password, currentPassword } = validation.data;
    let revision = snapshot.revision;
    if (password && authority.operator === "dashboard_session") {
      if (!hasManagementPasswordConfigured(snapshot.settings)) {
        authority.proof?.release();
        return NextResponse.json(
          { error: "Password recovery requires the local CLI" },
          { status: 403 }
        );
      }
      const state = await ensurePersistentManagementPasswordHash({
        settings: snapshot.settings,
        revision,
        source: "settings.require_login_password_change",
      });
      revision = state.revision;
      if (
        !currentPassword ||
        !(await verifyManagementPassword(
          currentPassword,
          getStoredManagementPassword(state.settings)
        ))
      ) {
        return NextResponse.json({ error: "Invalid current password" }, { status: 403 });
      }
    }
    const updates: Record<string, unknown> = {};
    if (typeof requireLogin === "boolean") updates.requireLogin = requireLogin;
    if (password) {
      updates.password = await hashManagementPassword(password);
      updates[AUTH_GENERATION_SETTING] = randomUUID();
      updates[SESSIONS_VALID_AFTER_SETTING] = Math.floor(Date.now() / 1000);
    }
    mutationStarted = true;
    await updateSettings(updates, { expectedRevision: revision });
    authority.proof?.consume();
    consumeBootstrapToken(request.headers.get(BOOTSTRAP_TOKEN_HEADER));
    const response = NextResponse.json({ success: true });
    const secret = getDashboardJwtSecret();
    if (password && authority.operator === "dashboard_session" && secret) {
      const forwardedProto = (request.headers.get("x-forwarded-proto") || "")
        .split(",")[0]
        .trim()
        .toLowerCase();
      response.cookies.set(
        DASHBOARD_SESSION_COOKIE,
        await mintDashboardSessionToken(secret, updates[AUTH_GENERATION_SETTING] as string),
        {
          httpOnly: true,
          secure:
            process.env.AUTH_COOKIE_SECURE === "true" ||
            forwardedProto === "https" ||
            new URL(request.url).protocol === "https:",
          sameSite: "lax",
          path: "/",
          maxAge: 30 * 86400,
        }
      );
    }
    return response;
  } catch (error) {
    if (authority?.ok) {
      if (
        !mutationStarted ||
        error instanceof SettingsRevisionConflictError ||
        isPostgresErrorKind(error, "transaction_failed")
      )
        authority.proof?.release();
      else authority.proof?.consume();
    }
    if (error instanceof SyntaxError)
      return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
    if (error instanceof SettingsRevisionConflictError)
      return NextResponse.json(
        { error: "Settings changed; refresh before retrying" },
        { status: 409 }
      );
    return createErrorResponseFromUnknown(error, "Failed to update login settings");
  }
}
