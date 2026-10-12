---
title: "PostgreSQL operations"
lastUpdated: 2026-10-12
---

# PostgreSQL operations

This runbook describes the current native server implementation. SQLite remains the default.
`OMNIROUTE_DATABASE_BACKEND=postgres` selects a different durable authority at startup; changing
it requires migration and restart. There is no fallback to SQLite after a PostgreSQL failure.
Implementation details and limits are in the
[native persistence README](../../src/lib/db/postgres/README.md).

## Operator profile

Use one active writer process/task for the first ECS profile. Do not overlap old and replacement
writers during cutover. SQL transactions and leases do not establish multi-replica/HA qualification
for process-local caches, producer state and buffers. RDS and private S3 are external services;
this repository does not create them or their IAM/network policies.

The target engine is RDS for PostgreSQL 18.6, matching the pinned local TLS test engine.
Confirm availability in the target AWS Region before provisioning. PostgreSQL 17 is the minimum
server feature level required by `transaction_timeout`; it is not the deployment recommendation
or a claim that the current integration campaign tested version 17.

Provide an explicit nonempty writable local ephemeral `DATA_DIR` for scratch and workers; native
startup rejects an absent value. Set `APP_LOG_TO_FILE=false`
and collect stdout/stderr through the task's logging configuration. Local scratch is not the
application database or durable call-log object store. Optional host integrations may need separate
files; OMP retains its own `~/.omp/agent/agent.db` capability.

Inject the unchanged `STORAGE_ENCRYPTION_KEY` used by the source data. Preserve existing explicit
`JWT_SECRET` and `API_KEY_SECRET`; when absent, imported durable values are reused, or a fresh
empty database creates them atomically. Keep these values with the protected backup/recovery set.
Do not rotate them as an incidental consequence of switching backends.

Supply these PostgreSQL fields through the process environment:

| Variable                                                  | Requirement                                                                     |
| --------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `OMNIROUTE_POSTGRES_HOST`                                 | RDS endpoint whose certificate hostname is verified                             |
| `OMNIROUTE_POSTGRES_PORT`                                 | Defaults to 5432                                                                |
| `OMNIROUTE_POSTGRES_DATABASE`                             | Nonempty target database                                                        |
| `OMNIROUTE_POSTGRES_USER` / `OMNIROUTE_POSTGRES_PASSWORD` | Nonempty credentials; migration privileges for the dedicated `omniroute` schema |
| `OMNIROUTE_POSTGRES_TLS`                                  | `verify-full` for RDS; this is the default                                      |
| `OMNIROUTE_POSTGRES_CA_FILE`                              | Readable mounted CA bundle for the chosen RDS certificate chain                 |

No database URL is consumed. Plaintext `disable-local` is accepted only for literal loopback IPs,
not RDS. The server profile defaults to 16 admitted SQL operations, 256 FIFO waiters and a 3-second
admission wait. Connection/lock/statement/query/transaction/idle deadlines default to
3/1/5/6/15/5 seconds. Tune only with the documented ordering constraints; pool limits apply per
process. PostgreSQL 17+ is required by `transaction_timeout`.

Configure `AWS_REGION`, `OMNIROUTE_ARTIFACT_S3_BUCKET`, `OMNIROUTE_ARTIFACT_S3_PREFIX` and
`OMNIROUTE_ARTIFACT_S3_EXPECTED_BUCKET_OWNER`. The expected owner is a 12-digit account ID.
The task IAM role must permit object PUT/GET/DELETE in that private prefix. If
`OMNIROUTE_ARTIFACT_S3_KMS_KEY_ID` is configured, authorize the required KMS use too. Provision and
verify private bucket/public-access policy separately. Startup validates configuration but does
not test bucket access, IAM or KMS. Call artifacts have an 8 MiB limit; application file BLOBs
have a separate 512 MiB logical content boundary. New files use transactional 8 MiB PostgreSQL
chunks with a final size/SHA marker; callers retain the logical file API. This size limit does not
guarantee completion within the configured SQL deadlines.

## Offline SQLite cutover

The following commands run from a source checkout with its project dependencies installed.
They are verified CLI argument forms, not proof that a packaged installation includes the
maintenance carrier. Supply database/storage credentials through the environment, not command
arguments. Create a fresh private bundle directory before capture/export; the parent directory
must exist.
Capture/export never overwrites an existing binary carrier.

1. Stop admission and drain all source writers, workers, buffers and streams. Preserve a complete
   recoverable SQLite backup and its encryption/authentication secrets. `--offline` acknowledges
   operator intent; it does not detect or prove that other processes stopped.
2. Capture an existing consistent source file. `--artifact-dir` is the base for its stored relative
   call-artifact references. Keep the bundle, its sibling `objects/` and `blobs/`, and the source backup
   together. Contents are sensitive despite private file modes.

```sh
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action capture --offline --source /path/to/storage.sqlite --bundle /private/migration/bundle.json --artifact-dir /path/to/source-artifact-base --large-files
```

3. With all target application writers stopped, import into an empty target. The CLI applies native
   migrations before import; it recognizes only declared bootstrap seeds. Keep source and uploaded
   objects after a failed or uncertain action.

```sh
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action import --offline --bundle /private/migration/bundle.json --large-files --memory-budget-mib 4096
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action verify --offline --bundle /private/migration/bundle.json --large-files
```

4. Inspect the sanitized count/digest result and retain the private bundle/receipt lineage. On
   `commit_unknown`, rerun with exactly the same bundle to reconcile; do not recapture into a new
   bundle or blindly replay domain writes. On `committed=true`/`artifact_readback`, repair/verify
   object access without treating the SQL write as rolled back.
5. Set `OMNIROUTE_DATABASE_BACKEND=postgres`, preserve secrets, and restart through the supported
   server transport. In a built source checkout the command is `npm run start`; the assembled
   standalone runtime uses its persistence-aware server wrapper. A bare Next/server entry without
   that boundary is rejected. Verify startup, authentication, representative writes and a clean
   shutdown before accepting traffic.

Capture fails instead of truncating beyond 250,000 rows, 512 MiB JSON, 10 GiB binary carrier data
or 512 MiB per file. Chunks are 8 MiB; ordinary non-file BLOBs and call artifacts remain limited to
8 MiB. Large-file import requires at least 2048 MiB declared memory budget; 4096 MiB is used by the
maximum-file witness. Provision actual process memory and scratch separately. Unknown durable
source tables/columns, corrupt required secrets, unsupported virtual data and invalid numeric/time
representations block qualification; there is no automatic data repair. Import preserves retained
records and summaries but cannot reconstruct history or objects already deleted from the source;
it does not invent historical per-key archives.

The maximum-file maintenance witness uses explicit PostgreSQL statement/query/idle/transaction
budgets of 60000/90000/90000/300000 milliseconds through
`OMNIROUTE_POSTGRES_STATEMENT_TIMEOUT_MS`, `OMNIROUTE_POSTGRES_QUERY_TIMEOUT_MS`,
`OMNIROUTE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS` and `OMNIROUTE_POSTGRES_TRANSACTION_TIMEOUT_MS`.
Large offline imports can exceed the gateway's
default deadlines. Configure maintenance budgets separately and restore the gateway defaults
before serving requests; these values do not guarantee completion for every dataset.

## Logical export, restore and failure recovery

With application writers drained, export the native dataset and objects to a new private directory:

```sh
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action export --offline --bundle /private/native-backup/bundle.json --large-files
```

Restore to a separately configured empty target, keeping its writers stopped:

```sh
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action restore --offline --bundle /private/native-backup/bundle.json --large-files --memory-budget-mib 4096
node --import tsx/esm scripts/ops/postgres-migrate-storage.ts --action verify --offline --bundle /private/native-backup/bundle.json --large-files
```

Restore starts a fresh policy authority UUID/generation 0 and records the old authority/epoch as
lineage. Business rows, identities and ciphertext are retained with declared receipt transformations.
Private chunk tables are verified physical state, not portable row owners. NULL/empty content,
tombstones and independent file metadata survive logical export/restore. Receipts bind a schema
fingerprint covering column definitions, primary/foreign-key topology, migration checksums,
constraint definitions and trigger declarations. Standalone index definitions and trigger-function
bodies are outside this fingerprint; it is not a complete physical-schema integrity check. A receipt
with a different bound fingerprint is rejected.
For a schema transition, verify and export the current logical dataset, then restore to a fresh
target and retain both recovery sets until cutover is verified.
Logical export is bounded maintenance, not an RDS physical snapshot/PITR implementation. SQLite
file backup, WAL/PRAGMA optimization, file import/export and legacy JSON repair are unavailable on
PostgreSQL; guarded APIs report 501 `persistence_capability_unavailable`.

Send graceful termination and inspect cleanup completion. Producer stop, durable buffer flush,
request/background/SSE drain and pool/object-client close have explicit owners. The wrappers force
exit after their shutdown budget, so provision the task stop grace accordingly. Forced exit or an
ambiguous COMMIT requires reconciliation, not an assumption that writes failed. Pending S3 deletion
work remains in the SQL queue until object deletion and queue acknowledgement succeed.

Changing the selector back to SQLite is not reverse migration: the old file lacks PostgreSQL-era
writes. Preserve the native dataset/objects and explicitly reconcile those writes before a backout
that must retain them. No PostgreSQL-to-SQLite data conversion command is provided here.

## Qualification pending for the release artifact

Run the existing local checks against the exact release inputs:

```sh
npm run typecheck:core
npm run test:postgres
RUN_PG_IMPORT_MAX_BLOB=1 npm run test:postgres -- postgresApplicationMaxBlob.test.ts postgresFileContentStorage.test.ts
npm run check:docs-all
```

The PostgreSQL runner creates a disposable local TLS PostgreSQL 18.6 instance using Docker and
OpenSSL, isolates suites and removes its resources. The explicit maximum-file test needs at least
2 GiB scratch disk and a provisioned memory budget. It is skipped without its opt-in. The native
maximum-file create/read witness has a separate explicit 60-second transaction budget; ordinary
statement/query/lock/idle limits stay unchanged. The offline maximum-file restore witness uses
the maintenance budgets above. Neither test promises maximum-file completion at the gateway's
15-second default or under arbitrary load. Local SDK transport fixtures are not live S3 evidence.

Before deployment, record pending exact-artifact qualification for packaged dependency/carrier
availability; cold start, graceful stop and restart with stable credentials; full source migration
and maximum-file logical restore; RDS CA/network/privileges and backup/PITR recovery; private
S3 IAM/expected-owner/KMS and uncertain-PUT reconciliation; queue saturation, network loss and
COMMIT ambiguity; and a single-writer ECS replacement without overlap. This runbook records none
of those operational runs as complete and makes no live AWS, HA, production or deploy-ready claim.
