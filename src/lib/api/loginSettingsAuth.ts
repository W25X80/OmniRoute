import { isStrictLoopbackRequest } from "@/shared/utils/apiAuth";
import { BOOTSTRAP_TOKEN_HEADER } from "@/server/authz/headers";
import { reserveBootstrapToken } from "@/lib/auth/bootstrapToken";
import { hasManagementPasswordConfigured } from "@/lib/auth/managementPassword";
import { requireOperatorCredential, type OperatorCredentialKind } from "./operatorCredential";
import type { AuthSettingsSnapshot } from "@/lib/db/authSettingsSemantics";

export type LoginSettingsAuthority =
  | {
      ok: true;
      operator: OperatorCredentialKind | null;
      proof?: NonNullable<ReturnType<typeof reserveBootstrapToken>>;
    }
  | { ok: false; response: Response };

/** No password alone is insufficient: even incomplete enabled OIDC ends bootstrap. */
export async function authorizeLoginSettingsWrite(
  request: Request,
  snapshot: AuthSettingsSnapshot
): Promise<LoginSettingsAuthority> {
  const verdict = await requireOperatorCredential(request, {
    alwaysRequireAuth: true,
    forbiddenMessage: "Changing login settings requires an operator credential.",
  });
  if (verdict.ok === true) {
    if (verdict.operator !== "dashboard_session" || verdict.generation === snapshot.generation)
      return verdict;
    return { ok: false, response: new Response(null, { status: 409 }) };
  }
  if (verdict.response.status >= 500) return verdict;
  const settings = snapshot.settings;
  if (
    request.method !== "POST" ||
    new URL(request.url).pathname !== "/api/settings/require-login" ||
    hasManagementPasswordConfigured(settings) ||
    settings.oidcEnabled === true
  )
    return verdict;
  if (isStrictLoopbackRequest(request)) return { ok: true, operator: null };
  const proof = reserveBootstrapToken(request.headers.get(BOOTSTRAP_TOKEN_HEADER));
  return proof ? { ok: true, operator: null, proof } : verdict;
}
