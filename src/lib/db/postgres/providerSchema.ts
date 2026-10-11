const asciiWhitespace = [9, 10, 11, 12, 13, 32].map((code) => `chr(${code})`).join(" || ");
const ecmaWhitespace = [
  9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201,
  8202, 8232, 8233, 8239, 8287, 12288, 65279,
]
  .map((code) => `chr(${code})`)
  .join(" || ");

/** Columns and active provider-row policies from the current SQLite migration state. */
export const POSTGRES_PROVIDER_SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE omniroute.provider_connections (
    id text COLLATE "C" PRIMARY KEY, provider text COLLATE "C" NOT NULL,
    auth_type text COLLATE "C", name text COLLATE "C", email text COLLATE "C",
    priority double precision DEFAULT 0, is_active integer DEFAULT 1,
    access_token text, refresh_token text, expires_at text, token_expires_at text,
    scope text, project_id text, test_status text, error_code text, last_error text,
    last_error_at text, last_error_type text, last_error_source text,
    backoff_level double precision DEFAULT 0, rate_limited_until text,
    health_check_interval double precision, last_health_check_at text, last_tested text,
    api_key text, id_token text, provider_specific_data text, expires_in double precision,
    display_name text, global_priority double precision, default_model text, token_type text,
    consecutive_use_count double precision DEFAULT 0, rate_limit_protection integer DEFAULT 0,
    last_used_at text, "group" text COLLATE "C", max_concurrent double precision,
    proxy_enabled integer NOT NULL DEFAULT 1, per_key_proxy_enabled integer NOT NULL DEFAULT 0,
    quota_visible integer NOT NULL DEFAULT 1, quota_window_thresholds_json text,
    rate_limit_overrides_json text, created_at text COLLATE "C" NOT NULL,
    updated_at text COLLATE "C" NOT NULL, last_ping_at text, last_pinged_reset_key text,
    synced_models_at text, insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  "CREATE INDEX idx_pc_provider ON omniroute.provider_connections(provider)",
  "CREATE INDEX idx_pc_active ON omniroute.provider_connections(is_active)",
  "CREATE INDEX idx_pc_priority ON omniroute.provider_connections(provider, priority)",
  "CREATE INDEX idx_pc_max_concurrent ON omniroute.provider_connections(provider, max_concurrent)",
  `CREATE TABLE omniroute.provider_nodes (
    id text COLLATE "C" PRIMARY KEY, type text COLLATE "C" NOT NULL,
    name text NOT NULL, prefix text, api_type text, base_url text,
    chat_path text, models_path text, custom_headers_json text, icon_url text,
    daily_quota_reset_timezone text, daily_quota_reset_hour double precision,
    created_at text NOT NULL, updated_at text NOT NULL
  )`,
  // Migrations 164/165/167/168/171: tombstones and immutable audited identities.
  // Lease policies remain with their unported owner. Migration 166's one-time data update
  // is intentionally not promoted to a permanent trigger.
  `CREATE FUNCTION omniroute.enforce_provider_connection_policy() RETURNS trigger
   LANGUAGE plpgsql AS $$
   DECLARE
     current_id text := lower(btrim(NEW.provider, ${ecmaWhitespace}));
     designer_id text := lower(btrim(NEW.provider, ${asciiWhitespace}));
     previous_provider text;
     previous_id text;
     retired_source text;
     retired_message text;
     now_text text := to_char(statement_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS');
   BEGIN
     IF TG_OP = 'INSERT' THEN
       SELECT provider INTO previous_provider FROM omniroute.provider_connections WHERE id = NEW.id;
     ELSE
       previous_provider := OLD.provider;
     END IF;
     previous_id := lower(btrim(previous_provider, ${ecmaWhitespace}));
     IF (previous_id IN ('felo-web', 'felo') AND current_id NOT IN ('felo-web', 'felo'))
       OR (previous_id IN ('chatgpt-web', 'cgpt-web') AND current_id NOT IN ('chatgpt-web', 'cgpt-web')) THEN
       RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'Retired provider connection identity cannot be changed';
     END IF;
     IF designer_id IN ('microsoft-designer-web', 'msdesigner') THEN
       IF TG_OP = 'INSERT' OR COALESCE(NEW.is_active, 0) <> 0
         OR COALESCE(NEW.test_status, '') <> 'unavailable'
         OR COALESCE(NEW.last_error, '') <> 'Provider retired from OmniRoute runtime.'
         OR COALESCE(NEW.last_error_type, '') <> 'provider_retired'
         OR COALESCE(NEW.last_error_source, '') <> 'migration:retire-microsoft-designer-web' THEN
         NEW.is_active := 0;
         NEW.test_status := 'unavailable';
         NEW.last_error := 'Provider retired from OmniRoute runtime.';
         NEW.last_error_at := COALESCE(NEW.last_error_at, now_text);
         NEW.last_error_type := 'provider_retired';
         NEW.last_error_source := 'migration:retire-microsoft-designer-web';
       END IF;
     ELSE
       IF current_id IN ('felo-web', 'felo') THEN
         retired_source := 'migration:retire-felo-web';
         retired_message := 'Provider integration retired from OmniRoute v3.8.50';
       ELSIF current_id IN ('qwen-web', 'qw') THEN
         retired_source := 'migration:retire-qwen-web';
         retired_message := 'Provider integration retired from OmniRoute v3.8.51';
       ELSIF current_id = 'cgpt-web' THEN
         retired_source := 'migration:retire-chatgpt-web';
         retired_message := 'Provider integration retired from OmniRoute v3.8.51';
       END IF;
       IF retired_source IS NOT NULL AND (
         NEW.is_active IS DISTINCT FROM 0
         OR NEW.test_status IS DISTINCT FROM 'unavailable'
         OR NEW.error_code IS DISTINCT FROM 'PROVIDER_REMOVED'
         OR NEW.last_error IS DISTINCT FROM retired_message
         OR NEW.last_error_type IS DISTINCT FROM 'provider_removed'
         OR NEW.last_error_source IS DISTINCT FROM retired_source
         OR NEW.last_error_at IS NULL) THEN
         NEW.is_active := 0;
         NEW.test_status := 'unavailable';
         NEW.error_code := 'PROVIDER_REMOVED';
         NEW.last_error := retired_message;
         NEW.last_error_type := 'provider_removed';
         NEW.last_error_source := retired_source;
         NEW.last_error_at := now_text;
         NEW.updated_at := now_text;
       END IF;
     END IF;
     RETURN NEW;
   END $$`,
  `CREATE TRIGGER provider_connection_policy_insert BEFORE INSERT
    ON omniroute.provider_connections FOR EACH ROW
    EXECUTE FUNCTION omniroute.enforce_provider_connection_policy()`,
  `CREATE TRIGGER provider_connection_policy_update BEFORE UPDATE OF provider, is_active,
    test_status, error_code, last_error, last_error_type, last_error_source, last_error_at
    ON omniroute.provider_connections FOR EACH ROW
    EXECUTE FUNCTION omniroute.enforce_provider_connection_policy()`,
];
