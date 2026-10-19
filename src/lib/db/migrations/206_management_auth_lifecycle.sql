ALTER TABLE sync_tokens ADD COLUMN issued_by TEXT;
ALTER TABLE sync_tokens ADD COLUMN expires_at TEXT;
ALTER TABLE sync_tokens ADD COLUMN auth_generation TEXT;

INSERT OR IGNORE INTO key_value(namespace,key,value)
VALUES('settings','managementAuthGeneration',
  '"' || lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
  substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',1+(random() & 3),1) ||
  substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))) || '"');
