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

export interface ModelAccessCheck {
  allowed: boolean;
  matchedRules: GroupModelPermission[];
  deniedBy: GroupModelPermission | null;
}

/** Key/group membership and permissions are part of the same durable policy authority. */
export interface ApiKeyGroupsRepository {
  getAllKeyGroups(): Promise<KeyGroup[]>;
  getKeyGroup(id: string): Promise<KeyGroup | undefined>;
  getKeyGroupWithPermissions(id: string): Promise<KeyGroupWithPermissions | undefined>;
  createKeyGroup(name: string, description?: string): Promise<KeyGroup>;
  updateKeyGroup(
    id: string,
    update: { name?: string; description?: string; isActive?: boolean }
  ): Promise<KeyGroup | undefined>;
  deleteKeyGroup(id: string): Promise<boolean>;
  getGroupPermissions(groupId: string): Promise<GroupModelPermission[]>;
  addGroupPermission(
    groupId: string,
    modelPattern: string,
    accessType: "allow" | "deny",
    provider?: string
  ): Promise<GroupModelPermission>;
  removeGroupPermission(permissionId: string): Promise<boolean>;
  getGroupMembers(groupId: string): Promise<KeyGroupMember[]>;
  getKeyGroupsForApiKey(keyId: string): Promise<KeyGroup[]>;
  addKeyToGroup(keyId: string, groupId: string): Promise<boolean>;
  removeKeyFromGroup(keyId: string, groupId: string): Promise<boolean>;
  checkKeyModelAccess(keyId: string, model: string, provider?: string): Promise<ModelAccessCheck>;
}
