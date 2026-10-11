/** Portable quota metadata and rolling counters. No SQLite migration is reinterpreted. */
export const POSTGRES_QUOTA_SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE omniroute.quota_groups (
    id text PRIMARY KEY, name text NOT NULL, created_at text NOT NULL,
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `INSERT INTO omniroute.quota_groups(id,name,created_at)
    VALUES ('group-demo','GroupDemo',CURRENT_TIMESTAMP::text)`,
  `CREATE TABLE omniroute.quota_pools (
    id text PRIMARY KEY, connection_id text NOT NULL, name text NOT NULL,
    group_id text, created_at text NOT NULL,
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `CREATE INDEX quota_pools_connection ON omniroute.quota_pools(connection_id)`,
  `CREATE INDEX quota_pools_group ON omniroute.quota_pools(group_id)`,
  `CREATE TABLE omniroute.quota_pool_connections (
    pool_id text NOT NULL, connection_id text NOT NULL, created_at text NOT NULL,
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    PRIMARY KEY(pool_id,connection_id)
  )`,
  `CREATE TABLE omniroute.quota_allocations (
    pool_id text NOT NULL REFERENCES omniroute.quota_pools(id) ON DELETE CASCADE,
    api_key_id text NOT NULL, weight double precision NOT NULL CHECK(weight >= 0 AND weight <= 100),
    cap_value double precision, cap_unit text CHECK(cap_unit IN ('percent','requests','tokens','usd')),
    policy text NOT NULL DEFAULT 'hard' CHECK(policy IN ('hard','soft','burst')),
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    PRIMARY KEY(pool_id,api_key_id)
  )`,
  `CREATE INDEX quota_allocations_apikey ON omniroute.quota_allocations(api_key_id)`,
  `CREATE TABLE omniroute.quota_consumption (
    api_key_id text NOT NULL, dimension_key text NOT NULL,
    bucket_index bigint NOT NULL, consumed double precision NOT NULL DEFAULT 0,
    updated_at bigint NOT NULL, insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    PRIMARY KEY(api_key_id,dimension_key,bucket_index)
  )`,
  `CREATE INDEX quota_consumption_dimension ON omniroute.quota_consumption(dimension_key,bucket_index)`,
  `CREATE INDEX quota_consumption_updated_at ON omniroute.quota_consumption(updated_at)`,
  `CREATE TABLE omniroute.provider_plans (
    connection_id text PRIMARY KEY, provider text NOT NULL, dimensions_json text NOT NULL,
    source text NOT NULL DEFAULT 'manual' CHECK(source IN ('auto','manual')), updated_at text NOT NULL,
    insertion_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE
  )`,
  `CREATE INDEX provider_plans_provider ON omniroute.provider_plans(provider)`,
  `CREATE TABLE omniroute.quota_allocation_model_caps (
    pool_id text NOT NULL, api_key_id text NOT NULL, model text NOT NULL,
    cap_value double precision NOT NULL CHECK(cap_value > 0),
    cap_unit text NOT NULL CHECK(cap_unit IN ('percent','requests','tokens','usd')),
    PRIMARY KEY(pool_id,api_key_id,model)
  )`,
  `CREATE TABLE omniroute.quota_schedules (
    id text PRIMARY KEY, pool_id text NOT NULL, label text, days text NOT NULL,
    start_minute integer NOT NULL CHECK(start_minute >= 0 AND start_minute < 1440),
    end_minute integer NOT NULL CHECK(end_minute > 0 AND end_minute <= 1440),
    timezone text, mode text NOT NULL DEFAULT 'allow' CHECK(mode IN ('allow','block')),
    reserves text, budget_value double precision CHECK(budget_value IS NULL OR budget_value > 0),
    budget_unit text CHECK(budget_unit IS NULL OR budget_unit IN ('requests','tokens','usd')),
    budget_window text NOT NULL DEFAULT 'daily'
      CHECK(budget_window IN ('hourly','5h','daily','weekly','monthly')),
    priority integer NOT NULL DEFAULT 0, enabled integer NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at text NOT NULL
  )`,
  `CREATE INDEX quota_schedules_pool ON omniroute.quota_schedules(pool_id,enabled)`,
];
