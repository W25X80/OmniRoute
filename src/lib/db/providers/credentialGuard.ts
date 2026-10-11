import {
  getStoredManagementPassword,
  isBcryptHash,
  verifyManagementPassword,
} from "@/lib/auth/managementPassword";

/** Thrown when a write would store the dashboard login password as a provider credential. */
export class ManagementPasswordAsCredentialError extends Error {
  readonly code = "MANAGEMENT_PASSWORD_AS_CREDENTIAL" as const;

  constructor() {
    super(
      "That value is the dashboard login password, not a provider API key. Storing it would " +
        "send it upstream on every request routed through this connection."
    );
    this.name = "ManagementPasswordAsCredentialError";
  }
}

/**
 * Refuse to store the dashboard login password as a connection API key.
 *
 * A browser that autofills the management password into the API-key field
 * produces a connection whose credential authenticates against nothing, and
 * every request routed through it comes back 401. Rejecting it in the form
 * would not be enough: the same autofill fires again while an operator is
 * repairing the connection by hand, so the refusal has to sit on the write
 * path that all of those forms funnel into.
 *
 * Only an actual match blocks the write. A settings row that cannot be read,
 * or a bcrypt call that throws, logs and allows -- a guard against one specific
 * operator mistake must not become a way to lock out every connection write.
 *
 * Deliberately narrower than CONNECTION_CREDENTIAL_FIELDS. The OAuth tokens
 * arrive from a provider's token endpoint, and the refresh path writes them
 * back through updateProviderConnection on every renewal, so checking them
 * would put a bcrypt round on a renewal path to defend a field no autofill
 * reaches. apiKey is the only credential an operator types into a form.
 */
export async function assertApiKeyIsNotManagementPassword(
  apiKey: unknown,
  getSettings: () => Promise<Record<string, unknown>>
): Promise<void> {
  if (typeof apiKey !== "string") return;
  const trimmed = apiKey.trim();
  if (!trimmed) return;

  try {
    const settings = (await getSettings()) as Record<string, unknown>;
    const stored = getStoredManagementPassword(settings);
    // Only a stored bcrypt hash is comparable. A fresh install that has never
    // bootstrapped a password has nothing to collide with.
    if (!isBcryptHash(stored)) return;
    // Both forms of the value, because neither the login route nor the
    // set-password route trims: a paste carries whitespace the password does
    // not have, and a password is allowed to carry whitespace of its own. The
    // second comparison only runs when the first fails on a different string.
    const matches =
      (await verifyManagementPassword(trimmed, stored)) ||
      (trimmed !== apiKey && (await verifyManagementPassword(apiKey, stored)));
    if (!matches) return;
  } catch (err) {
    console.warn(
      "[Providers] could not check the credential against the dashboard password:",
      err instanceof Error ? err.message : String(err)
    );
    return;
  }

  throw new ManagementPasswordAsCredentialError();
}
