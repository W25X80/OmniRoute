# Native PostgreSQL persistence

The server can select `OMNIROUTE_DATABASE_BACKEND=postgres`; an unset value or `sqlite`
keeps SQLite as the default. Selection happens at process startup. Changing authority requires
an offline data migration and restart; a PostgreSQL error never selects SQLite as a fallback.
This is implemented server behavior, not a deployment-readiness or multi-writer certificate.
See the [operations runbook](../../../../docs/ops/POSTGRESQL.md) and the upstream
[persistence proposal](../../../../docs/architecture/persistence-backend-boundary.md).

## Architecture and lifecycle

Domain callers use their owning `src/lib/db/` modules. Native implementations in this directory
own PostgreSQL SQL; they do not emulate the synchronous `SqliteAdapter`. The implementation
covers control-plane configuration and credentials, routing/catalog/policy, leases and sessions,
usage/logs/audit, cleanup, agents/memory and optional-service records. Backend-specific host and
SQLite file operations retain explicit capability boundaries.

[`persistenceRuntime.ts`](../persistenceRuntime.ts) awaits connection configuration, checksummed
migrations, encrypted bootstrap secrets and settings before installing the selected application
binding. Explicit nonempty `JWT_SECRET` and `API_KEY_SECRET` keep precedence; otherwise their
imported or atomically created durable secrets are used. Startup requires a stable explicit
`STORAGE_ENCRYPTION_KEY` and a nonempty explicit `DATA_DIR` for local scratch. Cold imports do not
initialize the application SQLite database. Corrupt or undecryptable bootstrap secrets fail startup rather than
being replaced. The runtime installs the chosen authentication secrets before consumers start.

The supported HTTP wrappers in `scripts/dev/run-next.mjs` and
`scripts/dev/standalone-server-ws.mjs` enter owned request scopes. Instrumentation rejects a
PostgreSQL boot without that transport boundary. Requests prepare a native catalog/settings/flag
snapshot; asynchronous event callbacks, background work and SSE accounting tails retain or
re-enter the admitted authority. Ended scopes and direct application SQLite access are rejected.
There is no dashboard switch or implicit mixed-backend operation.

[`postgresShutdown.ts`](../../startup/postgresShutdown.ts) stops producers, joins startup,
flushes durable buffers and drains requests, background work and stream tails before closing
object storage and the database pool. The shutdown wrapper also has a force-exit budget:
cleanup is not guaranteed after a forced kill or expired stop grace period. PostgreSQL hot reload
polls fresh native settings (5 seconds by default) without watching `storage.sqlite`.
Process-local caches, runtime settings and producer ownership still require the single-writer
operational profile; durable SQL locks do not establish active-active readiness.

## Configuration and admission

[`environment.ts`](environment.ts) reads the server profile below. Host, database, user and password
must be nonempty. It reads separate fields, not `DATABASE_URL` or another connection string.
`OMNIROUTE_POSTGRES_TLS` defaults to `verify-full`; certificate chain and hostname are verified.
`OMNIROUTE_POSTGRES_CA_FILE` supplies readable CA bundle contents, such as the chosen RDS CA.
`disable-local` permits plaintext only for literal `127.0.0.1` or `::1`.

| Server environment variable                      |                           Default |
| ------------------------------------------------ | --------------------------------: |
| `OMNIROUTE_POSTGRES_PORT`                        |                              5432 |
| `OMNIROUTE_POSTGRES_MAX_CONNECTIONS`             |                                16 |
| `OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS`       |                               256 |
| `OMNIROUTE_POSTGRES_CONNECTION_TIMEOUT_MS`       |                              3000 |
| `OMNIROUTE_POSTGRES_ADMISSION_WAIT_TIMEOUT_MS`   | connection timeout, normally 3000 |
| `OMNIROUTE_POSTGRES_LOCK_TIMEOUT_MS`             |                              1000 |
| `OMNIROUTE_POSTGRES_STATEMENT_TIMEOUT_MS`        |                              5000 |
| `OMNIROUTE_POSTGRES_QUERY_TIMEOUT_MS`            |                              6000 |
| `OMNIROUTE_POSTGRES_TRANSACTION_TIMEOUT_MS`      |                             15000 |
| `OMNIROUTE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS` |                              5000 |

[`config.ts`](config.ts) requires lock < statement < query < transaction deadlines and an idle
transaction deadline no greater than the transaction deadline. It sets UTC and uses the server
`transaction_timeout`, requiring PostgreSQL 17 or newer. The integration runner pins PostgreSQL
18.6. Driver numeric parsers retain their defaults; native owners validate their representations.

Admission in [`database.ts`](database.ts) reserves a slot before connection acquisition.
The server profile allows 16 admitted operations and a bounded FIFO of 256 waiting operations.
A full queue or expired admission wait produces `overloaded`; new work cannot overtake queued
work. Closing rejects new admission and drains admitted/queued operations before `pool.end()`.
These are per-process SQL-operation limits, not HTTP concurrency or global RDS connection limits.
Explicit low-level factories retain defaults of 4 connections and no waiting queue unless configured.

## Transaction and publication contract

A transaction leases one client, uses parameterized single-statement extended-protocol queries,
and must await each query. Normal repository callbacks contain database work only: no nested
transactions, transaction-control SQL or external effects. Failure taints the transaction even
if a callback catches the query error; expired scopes cannot issue later SQL.

Only a `COMMIT` command tag acknowledges success. Ambiguous/lost acknowledgement yields
`PostgresError.kind = "commit_unknown"` and destroys the uncertain client. The callback is never
automatically replayed. A timeout or closed socket does not prove that a sent COMMIT failed;
retry needs domain-specific receipts or reconciliation. Driver errors retain fixed messages and
SQLSTATE rather than credentials, SQL text or server row details.

Required publication/cache effects run after acknowledged COMMIT. Their failures carry
`committed = true` and require effect recovery, not transaction replay. Settings use revision CAS;
key/group policy and relevant catalog/settings writes advance a durable policy epoch. Native
model/auth reads avoid positive cross-request authorization caching. Privacy publication is ordered
by its durable authority UUID and epoch; process-local publication is not a multi-process cache bus.

## Artifacts and local capabilities

The PostgreSQL server requires the S3 configuration in
[`s3ArtifactConfiguration.ts`](../../artifacts/s3ArtifactConfiguration.ts): `AWS_REGION`,
`OMNIROUTE_ARTIFACT_S3_BUCKET`, `OMNIROUTE_ARTIFACT_S3_PREFIX` and
`OMNIROUTE_ARTIFACT_S3_EXPECTED_BUCKET_OWNER`; `OMNIROUTE_ARTIFACT_S3_KMS_KEY_ID` is optional.
The SDK loads lazily and uses its credential chain, including task IAM credentials. Configuration
validation at startup does not prove bucket reachability or IAM permissions. The application does
not provision a private bucket, public-access controls, IAM policy or RDS resources.

[`s3ArtifactStore.ts`](../../artifacts/s3ArtifactStore.ts) stores immutable call-log objects with
SHA-256 references, conditional PUT, expected-owner checks and AES256 or configured KMS encryption.
GET validates length and digest. It admits at most 8 operations with a 5-second deadline and an
8 MiB object limit. Unknown PUT results retain their internal reference for reconciliation; they
are not evidence that no object exists. Log cleanup queues deletion in SQL when the last reference
is removed; acknowledged object deletion is followed by queue acknowledgement. SQL and S3 are
separate commit domains. The store does not expose a production endpoint override or filesystem fallback.

Application files retain their existing logical `files.content` contract and 512 MiB content
boundary. Migration 4 adds private `files_content_state` and `files_content_chunks` tables.
New content is stored as at most 64 bytea chunks of 8 MiB inside the metadata transaction; the
whole-file marker is inserted last. Parent row locks serialize replacement, reading and deletion.
Mapped readers check sizes, sequential indexes, chunk hashes and the whole SHA before returning the
last chunk. NULL, an empty file, soft-deleted rows and the independent metadata `files.bytes`
value remain distinct. Legacy inline bytea rows are read explicitly; neither representation
falls back to a local file. This file boundary is separate from the 8 MiB call-log S3 object limit.
Size limits do not guarantee completion before the configured transaction deadline.

[`nativeFileWork.ts`](../nativeFileWork.ts) bounds the existing application file consumers to
64 active consumers and 4 GiB of retained payload credit per process identity. Native HTTP
uploads reserve eight times actual accepted wire bytes before Next.js clones the body. Downloads
reserve the larger of three content copies or output plus a bounded chunk-codec allowance before
allocation. Credit remains until accepted handler/SQL work and HTTP consumption both settle;
internal batch consumers retain it through their awaited consumption. A disconnect does not prove
that an accepted SQL write failed. Capacity admission fails with a sanitized 503 and never waits
while holding a SQL client. The ordinary file HTTP wire limit is 500 MiB; it is separate from the
512 MiB logical content limit. These bounds cover the inspected parser/binary/response payload,
not every metadata or JSON graph, process RSS, GC, native or socket allocation.

Local writable scratch remains necessary for workers, temporary artifacts and optional host
integrations. OMP's `~/.omp/agent/agent.db` is an explicit host SQLite capability, not application
fallback. In the native profile, [`ompHostClient.ts`](../ompHostClient.ts) delegates fixed host
operations to one owned worker with bounded job count, payload and deadlines. An uncertain
mutation acknowledgement is not retried. Headroom's native process/probe path uses asynchronous
child processes and filesystem calls. Shutdown stops new producers, drains admitted work, then
joins these resources. Those host integrations are distinct from application PostgreSQL state.

[`persistenceCapabilities.ts`](../persistenceCapabilities.ts) reports unavailable SQLite file
backup/restore/import/export, optimization, legacy JSON import and integrity repair as capability
errors; guarded HTTP handlers return 501 with `persistence_capability_unavailable`. SQLite WAL,
PRAGMA and physical backup controls do not operate on RDS. PostgreSQL physical backup/PITR and S3
recovery require separate operational qualification.

## Offline migration and logical restore

[`postgres-migrate-storage.ts`](../../../../scripts/ops/postgres-migrate-storage.ts) provides
`capture`, `import`, `verify`, `export` and `restore`, with explicit `--offline` acknowledgement.
It captures a consistent read-only SQLite transaction without initializing or migrating the source.
The owner manifest rejects unknown durable tables/columns, including unknown empty tables; only
evidenced generated indexes/views and engine metadata have declared handling. IDs, insertion order,
identity highwater, ciphertext and source fields are preserved. Declared usage timestamp fields are
converted to canonical UTC and recorded as transformations; invalid instants and unsafe/fractional
token counters fail rather than being repaired. Standalone FTS and unsupported quantized vectors
are not silently discarded. Previously purged source history/objects cannot be reconstructed;
retained summaries are preserved without inventing per-key archive history.

The bounded profile permits 250,000 rows, 512 MiB JSON, ordinary BLOBs/artifacts up to 8 MiB and
100-row SQL batches. `--large-files` captures file BLOBs into separate 8 MiB binary chunks with
size/SHA manifests, up to 512 MiB per file and 10 GiB aggregate carrier data. Large bytes are never
base64-embedded in the JSON snapshot. Import validates all chunks before SQL and streams verified
chunks into the same file transaction without assembling a whole 512 MiB file. Export and
verification expose logical content, validate every stored chunk and omit private physical tables
from the portable row owners. `--memory-budget-mib` must declare at least 2048 MiB. That declaration is
not an OS-enforced RSS ceiling. The max-file qualification witness declares 4096 MiB.

Import/restore requires an empty target apart from recognized migration seeds, uses offline table
locks and checks every copied row before COMMIT. A canonical-JSON SHA-256 receipt binds source,
plan and target schema. Reusing the same bundle reconciles that receipt and verifies current rows;
it does not replay writes. Unversioned experimental receipts from the former hash algorithm fail
closed. The schema fingerprint binds column definitions, primary/foreign-key topology,
migration checksums, constraint definitions and trigger declarations. It does not include standalone
index definitions or trigger-function bodies. A receipt with a different bound fingerprint is not
silently reinterpreted: use a verified logical export and restore to a fresh target for a schema transition.
A confirmed import followed by failed artifact readback reports `committed = true` with
`artifact_readback`. Historical native restore creates a fresh authority UUID/generation 0 and
retains source epoch lineage in the receipt. The older `postgres-import-routing.ts` remains a
routing-slice utility, not the complete migration command.

## Qualification boundary

Source implementation and local test tooling do not certify the installed release or deployment.
The [runbook](../../../../docs/ops/POSTGRESQL.md) lists pending exact-artifact qualification for
startup/shutdown, real TLS PostgreSQL, maximum-file migration/restore, private S3/IAM/KMS, RDS
recovery and a single ECS writer. No live AWS, HA, production, packaged-install or deploy-ready
claim follows from this README.
