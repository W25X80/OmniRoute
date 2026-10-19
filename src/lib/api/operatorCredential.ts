import { readAuthGeneration } from "@/lib/db/authSettingsSemantics";
import { createErrorResponse } from "@/lib/api/errorResponse";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { isTrustedLoopbackInternalServiceRequestAsync } from "@/lib/api/internalServiceAuth";
import { isCliTokenAuthValid } from "@/lib/middleware/cliTokenAuth";
import { AUTHZ_HEADER_AUTH_KIND, AUTHZ_HEADER_AUTH_LABEL } from "@/server/authz/headers";
import {
  isAuthRequired,
  isDashboardSessionAuthenticated,
  getDashboardSessionPayload,
} from "@/shared/utils/apiAuth";

export type OperatorCredentialKind =
  "open_instance" | "dashboard_session" | "local_cli" | "internal_service";

/**
 * Is the caller the instance operator, as opposed to a machine credential?
 *
 * Operator credentials are the ones only the owner of the instance holds:
 *   - an authenticated dashboard session (password / OIDC login),
 *   - the machine-bound local CLI credential — the subject stamped by the authz pipeline,
 *     or validated here (loopback-only) for raw callers that skip the pipeline.
 *
 * API keys (whatever their scope) and `oma_` access tokens are machine credentials: they
 * can be handed to automation, so actions that hand out top-level privilege must not be
 * reachable with them alone. Sensitive callers set alwaysRequireAuth to exclude the
 * compatibility open-instance verdict. Returns `null` when the caller is not an operator.
 *
 * Throws when the auth backend fails — callers map that to a 503, never to an allow.
 */
export async function resolveOperatorCredential(
  request: Request,
  options: { alwaysRequireAuth?: boolean; allowInternalService?: boolean } = {}
): Promise<OperatorCredentialKind | null> {
  if (!options.alwaysRequireAuth && !(await isAuthRequired(request, { ignorePublicRoute: true })))
    return "open_instance";
  if (await isDashboardSessionAuthenticated(request)) return "dashboard_session";
  if (options.allowInternalService && (await isTrustedLoopbackInternalServiceRequestAsync(request)))
    return "internal_service";
  if (
    request.headers.get(AUTHZ_HEADER_AUTH_KIND) === "management_key" &&
    request.headers.get(AUTHZ_HEADER_AUTH_LABEL) === "local-cli-token"
  ) {
    return "local_cli";
  }
  if (await isCliTokenAuthValid(request)) return "local_cli";
  return null;
}

export type OperatorAuthVerdict =
  | { ok: true; operator: OperatorCredentialKind; generation?: string }
  | { ok: false; response: Response };

/** Sensitive callers opt out of open-instance authority explicitly. */
export async function requireOperatorCredential(
  request: Request,
  options: { forbiddenMessage: string; alwaysRequireAuth?: boolean; allowInternalService?: boolean }
): Promise<OperatorAuthVerdict> {
  try {
    const response = await requireManagementAuth(request, {
      alwaysRequireAuth: options.alwaysRequireAuth,
    });
    if (response) return { ok: false, response };
    const operator = await resolveOperatorCredential(request, options);
    if (operator === "dashboard_session") {
      const payload = await getDashboardSessionPayload(request);
      const generation = payload && readAuthGeneration(payload);
      if (generation) return { ok: true, operator, generation };
      return {
        ok: false,
        response: createErrorResponse({ status: 401, message: "Authentication required" }),
      };
    }
    if (operator) return { ok: true, operator };
    return {
      ok: false,
      response: createErrorResponse({
        status: 403,
        message: options.forbiddenMessage,
        type: "invalid_request",
      }),
    };
  } catch {
    return {
      ok: false,
      response: createErrorResponse({
        status: 503,
        message: "Service temporarily unavailable",
        type: "server_error",
      }),
    };
  }
}

export const CREDENTIAL_EXPORT_FORBIDDEN_MESSAGE =
  "Exporting stored credentials requires a signed-in dashboard session or the local CLI.";
