/** Client-safe inventory shared by authenticated writers and legacy import filtering. */
export const SECURITY_IMPACTING_SETTINGS_KEYS = [
  "localOnlyManageScopeBypassEnabled",
  "localOnlyManageScopeBypassPrefixes",
  "requireLogin",
  "newPassword",
  "oidcEnabled",
  "oidcDisablePasswordLogin",
  "oidcClientSecret",
  "oidcIssuer",
  "oidcClientId",
  "oidcAllowedSubjects",
  "oidcRedirectPath",
  "oidcScopes",
  "corsOrigins",
  "bruteForceProtection",
] as const;

export const DASHBOARD_SESSION_STATE_SETTINGS_KEYS = [
  "sessionsValidAfter",
  "revokedDashboardSessions",
  "managementAuthGeneration",
] as const;

export const IMPORT_STRIPPED_SETTINGS_KEYS = [
  "password",
  "currentPassword",
  ...SECURITY_IMPACTING_SETTINGS_KEYS,
  ...DASHBOARD_SESSION_STATE_SETTINGS_KEYS,
] as const;
