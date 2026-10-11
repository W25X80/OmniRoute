import type {
  GroupModelPermission,
  KeyGroup,
  KeyGroupMember,
  ModelAccessCheck,
} from "@/domain/persistence/apiKeyGroupsRepository";

export function hydrateKeyGroup(row: Record<string, unknown>): KeyGroup {
  return {
    id: String(row.id),
    name: String(row.name),
    description: String(row.description || ""),
    isActive: row.is_active === 1,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

export function hydrateGroupPermission(row: Record<string, unknown>): GroupModelPermission {
  return {
    id: String(row.id),
    groupId: String(row.group_id),
    modelPattern: String(row.model_pattern),
    provider: typeof row.provider === "string" && row.provider ? row.provider : null,
    accessType: row.access_type as "allow" | "deny",
    createdAt: String(row.created_at),
  };
}

export function hydrateKeyGroupMember(row: Record<string, unknown>): KeyGroupMember {
  return {
    keyId: String(row.key_id),
    groupId: String(row.group_id),
    createdAt: String(row.created_at),
  };
}

/** Shared SQLite group syntax: only '*' is special, case sensitive, and spans path separators. */
export function matchesGroupModelPattern(pattern: string, model: string): boolean {
  if (pattern === "*") return true;
  if (!pattern.includes("*")) return pattern === model;
  const escaped = pattern.replace(/[.+^${}()|[\]\\?]/g, "\\$&").replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`).test(model);
}

export function evaluateKeyGroupAccess(
  hasGroups: boolean,
  permissions: GroupModelPermission[],
  model: string,
  provider?: string
): ModelAccessCheck {
  if (!hasGroups) return { allowed: true, matchedRules: [], deniedBy: null };
  const matches = (rule: GroupModelPermission) =>
    matchesGroupModelPattern(rule.modelPattern, model) &&
    (!rule.provider || rule.provider === provider);
  const deniedBy = permissions.find((rule) => rule.accessType === "deny" && matches(rule)) || null;
  return {
    allowed: !deniedBy && permissions.some((rule) => rule.accessType === "allow" && matches(rule)),
    matchedRules: permissions,
    deniedBy,
  };
}
