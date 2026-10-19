import { requireOperatorCredential } from "@/lib/api/operatorCredential";
import { getAuthSettingsSnapshot } from "@/lib/db/authSettings";
import { createErrorResponse } from "@/lib/api/errorResponse";

export async function authorizeSyncTokenIssuer(request: Request) {
  const verdict = await requireOperatorCredential(request, {
    alwaysRequireAuth: true,
    allowInternalService: true,
    forbiddenMessage: "Issuing sync tokens requires an operator credential.",
  });
  if (verdict.ok === false) return verdict;
  let snapshot: Awaited<ReturnType<typeof getAuthSettingsSnapshot>>;
  try {
    snapshot = await getAuthSettingsSnapshot();
  } catch {
    return {
      ok: false as const,
      response: createErrorResponse({
        status: 503,
        message: "Service temporarily unavailable",
        type: "server_error",
      }),
    };
  }
  if (
    !snapshot.generation ||
    (verdict.operator === "dashboard_session" && verdict.generation !== snapshot.generation)
  ) {
    return {
      ok: false as const,
      response: createErrorResponse({
        status: 409,
        message: "Authentication changed; sign in again",
      }),
    };
  }
  const issuer = { dashboard_session: "session", local_cli: "cli", internal_service: "internal" }[
    verdict.operator
  ];
  if (!issuer)
    return {
      ok: false as const,
      response: createErrorResponse({ status: 403, message: "Operator credential required" }),
    };
  return { ok: true as const, issuer, generation: snapshot.generation };
}
