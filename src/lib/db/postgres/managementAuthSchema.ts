/** Forward-only migration; existing migrations and issuer provenance remain unchanged. */
export const POSTGRES_MANAGEMENT_AUTH_STATEMENTS = [
  "ALTER TABLE omniroute.sync_tokens ADD COLUMN issued_by text, ADD COLUMN expires_at text, ADD COLUMN auth_generation text",
  `INSERT INTO omniroute.key_value(namespace,key,value)
   VALUES('settings','managementAuthGeneration',to_json(gen_random_uuid()::text)::text)
   ON CONFLICT(namespace,key) DO NOTHING`,
] as const;
