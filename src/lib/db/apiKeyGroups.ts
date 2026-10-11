/**
 * API Key Groups DB — CRUD operations for team/enterprise key grouping
 *
 * Tables: key_groups, group_model_permissions, key_group_members
 * Migration: 065_api_key_groups.sql
 *
 * Enables team-level API key management with model-level access control.
 */

import { getDbInstance } from "@/lib/db/core";
import { randomUUID } from "crypto";
import { invalidateModelCatalogCache } from "./readCache";
import { getSelectedControlPlane } from "./controlPlaneContext";
import {
  evaluateKeyGroupAccess,
  hydrateKeyGroup as rowToGroup,
  hydrateGroupPermission as rowToPermission,
  hydrateKeyGroupMember as rowToMember,
} from "./apiKeys/groupSemantics";

// ── Types ────────────────────────────────────────────────────────────────

export interface KeyGroup {
  id: string;
  name: string;
  description: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface GroupModelPermission {
  id: string;
  groupId: string;
  modelPattern: string;
  provider: string | null;
  accessType: "allow" | "deny";
  createdAt: string;
}

export interface KeyGroupMember {
  keyId: string;
  groupId: string;
  createdAt: string;
}

export interface KeyGroupWithPermissions extends KeyGroup {
  permissions: GroupModelPermission[];
  memberCount: number;
}

// ── Key Groups CRUD ──────────────────────────────────────────────────────

export function getAllKeyGroups(): KeyGroup[] {
  const db = getDbInstance() as any;
  const rows = db.prepare("SELECT * FROM key_groups ORDER BY name ASC").all() as any[];
  return rows.map(rowToGroup);
}

export function getKeyGroup(id: string): KeyGroup | undefined {
  const db = getDbInstance() as any;
  const row = db.prepare("SELECT * FROM key_groups WHERE id = ?").get(id) as any;
  return row ? rowToGroup(row) : undefined;
}

export function getKeyGroupWithPermissions(id: string): KeyGroupWithPermissions | undefined {
  const group = getKeyGroup(id);
  if (!group) return undefined;

  const permissions = getGroupPermissions(id);
  const memberCount = getGroupMemberCount(id);

  return { ...group, permissions, memberCount };
}

export function createKeyGroup(name: string, description = ""): KeyGroup {
  const db = getDbInstance() as any;
  const id = randomUUID();
  const now = new Date().toISOString();

  db.prepare(
    "INSERT INTO key_groups (id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)"
  ).run(id, name, description, now, now);

  return getKeyGroup(id)!;
}

export function updateKeyGroup(
  id: string,
  updates: { name?: string; description?: string; isActive?: boolean }
): KeyGroup | undefined {
  const existing = getKeyGroup(id);
  if (!existing) return undefined;

  const db = getDbInstance() as any;
  const sets: string[] = [];
  const params: Record<string, unknown> = { id };

  if (updates.name !== undefined) {
    sets.push("name = @name");
    params.name = updates.name;
  }
  if (updates.description !== undefined) {
    sets.push("description = @description");
    params.description = updates.description;
  }
  if (updates.isActive !== undefined) {
    sets.push("is_active = @isActive");
    params.isActive = updates.isActive ? 1 : 0;
  }

  if (sets.length === 0) return existing;
  const catalogInvalidationNeeded =
    updates.isActive !== undefined && updates.isActive !== existing.isActive;

  sets.push("updated_at = datetime('now')");

  const result = db.prepare(`UPDATE key_groups SET ${sets.join(", ")} WHERE id = @id`).run(params);
  if (catalogInvalidationNeeded && result.changes > 0) {
    invalidateModelCatalogCache();
  }
  return result.changes > 0 ? getKeyGroup(id) : existing;
}

export function deleteKeyGroup(id: string): boolean {
  const db = getDbInstance() as any;
  // CASCADE deletes permissions and members
  const result = db.prepare("DELETE FROM key_groups WHERE id = ?").run(id);
  if (result.changes > 0) {
    invalidateModelCatalogCache();
    return true;
  }
  return false;
}

// ── Group Permissions ────────────────────────────────────────────────────

export function getGroupPermissions(groupId: string): GroupModelPermission[] {
  const db = getDbInstance() as any;
  const rows = db
    .prepare(
      "SELECT * FROM group_model_permissions WHERE group_id = ? ORDER BY access_type ASC, model_pattern ASC"
    )
    .all(groupId) as any[];
  return rows.map(rowToPermission);
}

export function addGroupPermission(
  groupId: string,
  modelPattern: string,
  accessType: "allow" | "deny",
  provider?: string
): GroupModelPermission {
  const db = getDbInstance() as any;
  const id = randomUUID();
  const now = new Date().toISOString();

  const result = db
    .prepare(
      "INSERT INTO group_model_permissions (id, group_id, model_pattern, provider, access_type, created_at) VALUES (?, ?, ?, ?, ?, ?)"
    )
    .run(id, groupId, modelPattern, provider || null, accessType, now);

  if (result.changes > 0) {
    invalidateModelCatalogCache();
  }

  return getGroupPermissions(groupId).find((p) => p.id === id)!;
}

export function removeGroupPermission(permissionId: string): boolean {
  const db = getDbInstance() as any;
  const result = db.prepare("DELETE FROM group_model_permissions WHERE id = ?").run(permissionId);
  if (result.changes > 0) {
    invalidateModelCatalogCache();
  }
  return result.changes > 0;
}
export function getGroupMembers(groupId: string): KeyGroupMember[] {
  const db = getDbInstance() as any;
  const rows = db
    .prepare("SELECT * FROM key_group_members WHERE group_id = ? ORDER BY created_at ASC")
    .all(groupId) as any[];
  return rows.map(rowToMember);
}

export function getKeyGroupsForApiKey(keyId: string): KeyGroup[] {
  const db = getDbInstance() as any;
  const rows = db
    .prepare(
      `
    SELECT g.* FROM key_groups g
    INNER JOIN key_group_members m ON g.id = m.group_id
    WHERE m.key_id = ? AND g.is_active = 1
    ORDER BY g.name ASC
  `
    )
    .all(keyId) as any[];
  return rows.map(rowToGroup);
}

export function addKeyToGroup(keyId: string, groupId: string): boolean {
  const db = getDbInstance() as any;
  try {
    const result = db
      .prepare("INSERT OR IGNORE INTO key_group_members (key_id, group_id) VALUES (?, ?)")
      .run(keyId, groupId);
    if (result.changes > 0) {
      invalidateModelCatalogCache();
    }
    return true;
  } catch {
    return false;
  }
}

export function removeKeyFromGroup(keyId: string, groupId: string): boolean {
  const db = getDbInstance() as any;
  const result = db
    .prepare("DELETE FROM key_group_members WHERE key_id = ? AND group_id = ?")
    .run(keyId, groupId);
  if (result.changes > 0) {
    invalidateModelCatalogCache();
  }
  return result.changes > 0;
}

function getGroupMemberCount(groupId: string): number {
  const db = getDbInstance() as any;
  const row = db
    .prepare("SELECT COUNT(*) as count FROM key_group_members WHERE group_id = ?")
    .get(groupId) as any;
  return row?.count || 0;
}

// ── Authorization Helper ────────────────────────────────────────────────

export interface ModelAccessCheck {
  allowed: boolean;
  matchedRules: GroupModelPermission[];
  deniedBy: GroupModelPermission | null;
}

/**
 * Check if an API key has access to a specific model.
 * Deny rules override allow rules. If no rules match, access is allowed by default.
 */
export function checkKeyModelAccess(
  keyId: string,
  model: string,
  provider?: string
): ModelAccessCheck {
  const groups = getKeyGroupsForApiKey(keyId);
  if (groups.length === 0) {
    // No groups = no restrictions
    return { allowed: true, matchedRules: [], deniedBy: null };
  }

  const db = getDbInstance() as any;
  const groupIds = groups.map((g) => g.id);
  const placeholders = groupIds.map(() => "?").join(",");

  const rules = db
    .prepare(
      `
    SELECT * FROM group_model_permissions
    WHERE group_id IN (${placeholders})
    ORDER BY access_type ASC
  `
    )
    .all(...groupIds) as any[];

  const permissions = rules.map(rowToPermission);

  return evaluateKeyGroupAccess(true, permissions, model, provider);
}

// Async domain facades preserve the synchronous SQLite API for existing embedded callers.

export async function getAllKeyGroupsAsync(
  ...args: Parameters<typeof getAllKeyGroups>
): Promise<ReturnType<typeof getAllKeyGroups>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.getAllKeyGroups(...args) : getAllKeyGroups(...args);
}

export async function getKeyGroupAsync(
  ...args: Parameters<typeof getKeyGroup>
): Promise<ReturnType<typeof getKeyGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.getKeyGroup(...args) : getKeyGroup(...args);
}

export async function getKeyGroupWithPermissionsAsync(
  ...args: Parameters<typeof getKeyGroupWithPermissions>
): Promise<ReturnType<typeof getKeyGroupWithPermissions>> {
  const selected = getSelectedControlPlane();
  return selected
    ? selected.keyGroups.getKeyGroupWithPermissions(...args)
    : getKeyGroupWithPermissions(...args);
}

export async function createKeyGroupAsync(
  ...args: Parameters<typeof createKeyGroup>
): Promise<ReturnType<typeof createKeyGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.createKeyGroup(...args) : createKeyGroup(...args);
}

export async function updateKeyGroupAsync(
  ...args: Parameters<typeof updateKeyGroup>
): Promise<ReturnType<typeof updateKeyGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.updateKeyGroup(...args) : updateKeyGroup(...args);
}

export async function deleteKeyGroupAsync(
  ...args: Parameters<typeof deleteKeyGroup>
): Promise<ReturnType<typeof deleteKeyGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.deleteKeyGroup(...args) : deleteKeyGroup(...args);
}

export async function getGroupPermissionsAsync(
  ...args: Parameters<typeof getGroupPermissions>
): Promise<ReturnType<typeof getGroupPermissions>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.getGroupPermissions(...args) : getGroupPermissions(...args);
}

export async function addGroupPermissionAsync(
  ...args: Parameters<typeof addGroupPermission>
): Promise<ReturnType<typeof addGroupPermission>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.addGroupPermission(...args) : addGroupPermission(...args);
}

export async function removeGroupPermissionAsync(
  ...args: Parameters<typeof removeGroupPermission>
): Promise<ReturnType<typeof removeGroupPermission>> {
  const selected = getSelectedControlPlane();
  return selected
    ? selected.keyGroups.removeGroupPermission(...args)
    : removeGroupPermission(...args);
}

export async function getGroupMembersAsync(
  ...args: Parameters<typeof getGroupMembers>
): Promise<ReturnType<typeof getGroupMembers>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.getGroupMembers(...args) : getGroupMembers(...args);
}

export async function getKeyGroupsForApiKeyAsync(
  ...args: Parameters<typeof getKeyGroupsForApiKey>
): Promise<ReturnType<typeof getKeyGroupsForApiKey>> {
  const selected = getSelectedControlPlane();
  return selected
    ? selected.keyGroups.getKeyGroupsForApiKey(...args)
    : getKeyGroupsForApiKey(...args);
}

export async function addKeyToGroupAsync(
  ...args: Parameters<typeof addKeyToGroup>
): Promise<ReturnType<typeof addKeyToGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.addKeyToGroup(...args) : addKeyToGroup(...args);
}

export async function removeKeyFromGroupAsync(
  ...args: Parameters<typeof removeKeyFromGroup>
): Promise<ReturnType<typeof removeKeyFromGroup>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.removeKeyFromGroup(...args) : removeKeyFromGroup(...args);
}

export async function checkKeyModelAccessAsync(
  ...args: Parameters<typeof checkKeyModelAccess>
): Promise<ReturnType<typeof checkKeyModelAccess>> {
  const selected = getSelectedControlPlane();
  return selected ? selected.keyGroups.checkKeyModelAccess(...args) : checkKeyModelAccess(...args);
}
