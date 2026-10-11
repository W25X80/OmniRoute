/** Native migration statements derived from core.ts, API_KEY_COLUMN_FALLBACKS and migrations 066/147/205. */
export const API_KEY_SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE omniroute.api_keys (
    id text PRIMARY KEY, name text NOT NULL, key text NOT NULL UNIQUE, machine_id text,
    allowed_models text DEFAULT '[]', no_log integer NOT NULL DEFAULT 0, created_at text NOT NULL,
    codex_service_mode text NOT NULL DEFAULT 'inherit' CHECK (codex_service_mode IN ('inherit','default','priority','flex')),
    model_access_mode text NOT NULL DEFAULT 'all' CHECK (model_access_mode IN ('all','restricted')),
    blocked_models text, allowed_combos text, allowed_connections text,
    auto_resolve integer NOT NULL DEFAULT 0, is_active integer NOT NULL DEFAULT 1,
    access_schedule text, max_requests_per_day integer, max_requests_per_minute integer,
    throttle_delay_ms integer, max_sessions integer NOT NULL DEFAULT 0,
    revoked_at text, expires_at text, last_used_at text, key_prefix text,
    ip_allowlist text, scopes text, rate_limits text, is_banned integer NOT NULL DEFAULT 0,
    key_hash text, proxy_id text, allowed_endpoints text,
    allowed_quotas text NOT NULL DEFAULT '[]', stream_default_mode text NOT NULL DEFAULT 'legacy',
    cache_default_mode text NOT NULL DEFAULT 'legacy', disable_non_public_models integer NOT NULL DEFAULT 0,
    allow_usage_command integer NOT NULL DEFAULT 0, usage_limit_enabled integer NOT NULL DEFAULT 0,
    daily_usage_limit_usd double precision, weekly_usage_limit_usd double precision,
    chaos_mode_enabled integer NOT NULL DEFAULT 0, compression_enabled integer NOT NULL DEFAULT 1,
    allow_auto_combos integer NOT NULL DEFAULT 1,
    catalog_scope text NOT NULL DEFAULT 'all' CHECK (catalog_scope IN ('all','combos','models')),
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `CREATE INDEX api_keys_hash ON omniroute.api_keys(key_hash)`,
  `CREATE INDEX api_keys_revoked_at ON omniroute.api_keys(revoked_at)`,
  `CREATE INDEX api_keys_expires_at ON omniroute.api_keys(expires_at)`,
  `CREATE TABLE omniroute.api_key_policy_epoch (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    authority_id uuid NOT NULL DEFAULT gen_random_uuid(), generation bigint NOT NULL DEFAULT 0
  )`,
  `INSERT INTO omniroute.api_key_policy_epoch(singleton) VALUES (true)`,
  `CREATE TABLE omniroute.key_groups (
    id text PRIMARY KEY, name text NOT NULL, description text NOT NULL DEFAULT '',
    is_active integer NOT NULL DEFAULT 1,
    created_at text NOT NULL DEFAULT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),
    updated_at text NOT NULL DEFAULT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `CREATE TABLE omniroute.group_model_permissions (
    id text PRIMARY KEY, group_id text NOT NULL REFERENCES omniroute.key_groups(id) ON DELETE CASCADE,
    model_pattern text NOT NULL, provider text,
    access_type text NOT NULL DEFAULT 'allow' CHECK (access_type IN ('allow','deny')),
    created_at text NOT NULL DEFAULT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `CREATE TABLE omniroute.key_group_members (
    key_id text NOT NULL REFERENCES omniroute.api_keys(id) ON DELETE CASCADE,
    group_id text NOT NULL REFERENCES omniroute.key_groups(id) ON DELETE CASCADE,
    created_at text NOT NULL DEFAULT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    PRIMARY KEY(key_id,group_id)
  )`,
  `CREATE INDEX group_permissions_group ON omniroute.group_model_permissions(group_id)`,
  `CREATE INDEX group_members_group ON omniroute.key_group_members(group_id)`,
];
