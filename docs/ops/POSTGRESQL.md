---
title: "PostgreSQL operations"
lastUpdated: 2026-10-21
---

# PostgreSQL operations

This fork uses native PostgreSQL by default when `OMNIROUTE_DATABASE_BACKEND` is absent.
Explicit `postgres` selects the same authority; only `OMNIROUTE_DATABASE_BACKEND=sqlite` selects
legacy SQLite. Empty or invalid values fail startup. Changing authority requires migration and
restart; existing SQLite data is never imported automatically. Missing PostgreSQL configuration
or a PostgreSQL failure never opens SQLite or generates replacement local secrets.
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

Native HTTP admission allows at most 64 active persistence requests. Resource-pressure
admission runs before catalog preparation and body consumption, returning a sanitized 503
with `Retry-After: 5` when capacity is unavailable. Accepted work retains its cleanup authority.
The RSS fuse uses the smaller of its configured threshold and 90% of the finite container
limit. The sampler supports cgroup v2 and cgroup v1, including a finite hierarchical limit
when the task's leaf limit is unlimited. These bounds reduce amplification; they do not
guarantee that one large accepted request cannot exhaust memory.

Pending Responses continuations are a temporary bridge to durable call artifacts, not a second
database. One process-wide bridge admits at most 256 entries, with estimated limits of 8 MiB
per entry and 64 MiB total. It owns detached payloads and expires them after 60 seconds; a
startup-owned one-second sweep releases expired entries even without another lookup. Over-budget
entries are refused whole. Durable continuation lookup and current key privacy checks remain
authoritative. Shutdown drains accepted work before releasing this bridge.

The artifact worker queue reserves estimated input memory before cloning or worker transfer:
32 MiB per raw artifact and 128 MiB across active/waiting inputs in all process bundles.
Decode views are copied at their exact size under the existing 8 MiB limit; staged ciphertext
keeps its existing size limit and consumes the shared budget. Caller mutation cannot enlarge
an admitted graph. Refusal leaves PostgreSQL scalar call metadata with
`detailReason: "preparation_failed"`; it does not create a publication intent or change usage
accounting. Existing stored-payload truncation remains unchanged. Estimated input retention
does not bound worker serialization temporaries or total process RSS.

Every 30 seconds, the `runtime.resources` log records process/container memory, event-loop
delay/utilization, persistence admission counts, pending-continuation counts/estimated bytes
and process-wide artifact serialization retained-byte/refusal counters.
These aggregate observations contain no request identities or payloads. They support diagnosis;
they are not a heap-retention profile.

Configure `AWS_REGION`, `OMNIROUTE_ARTIFACT_S3_BUCKET`, `OMNIROUTE_ARTIFACT_S3_PREFIX` and
`OMNIROUTE_ARTIFACT_S3_EXPECTED_BUCKET_OWNER`. The expected owner is a 12-digit account ID.
The task IAM role must permit object PUT/GET/DELETE in that private prefix. If
`OMNIROUTE_ARTIFACT_S3_KMS_KEY_ID` is configured, authorize the required KMS use too. Provision and
verify private bucket/public-access policy separately. Startup validates configuration but does
not test bucket access, IAM or KMS. Call artifacts have an 8 MiB limit; application file BLOBs
have a separate 512 MiB logical content boundary. New files use transactional 8 MiB PostgreSQL
chunks with a final size/SHA marker; callers retain the logical file API. This size limit does not
guarantee completion within the configured SQL deadlines.

### History concurrency and artifact recovery

Native migration 9 adds a root `history_revision` and indexes for session-specific latest-log
lookups and fingerprint candidates. History reads use MVCC without the global history advisory
lock; compound reads request a read-only repeatable-read transaction from the database owner.
Ordinary writers take a shared reset gate and lock their affected parent rows before node rows.
Only an explicit whole-history reset takes the gate exclusively. Classification runs outside SQL,
then append/count changes commit atomically if both the parent incarnation and revision match.
An acknowledged pre-write conflict can be reclassified twice; an unknown COMMIT is never replayed.
The process-local fingerprint fence has bounded waiting and cancellation. This remains a
single-writer deployment profile, not multi-replica qualification.

Tracking reads at most 32,769 index rows and refuses histories above 32,768 nodes instead of
passing a truncated graph to reconnection matching. Stored history is retained. Conversation
canonicalization starts only after fingerprint admission; hashing and reconnect traversal yield
between bounded work slices while preserving candidate order and matching semantics.

Independent session-affinity pins use a shared sessions gate followed by a per-pin exclusive
fence; model-history appends share the group gate. Cleanup and existing combo model edits keep
their exclusive gate. A drained, single-writer replacement is required in either direction
when deploying this lock-protocol change.

Transaction diagnostics use one structured log record and a unique transaction identifier to
separate concurrent operations. A server `FATAL 25P03` during COMMIT means the idle transaction
was terminated before command execution and is classified as a known failure without replay.
Connection loss, an acknowledgement lost after COMMIT, and other fatal errors remain unknown
outcomes. No automatic retry is added.

LKGP record and clear operations acquire background ownership before suspension, retain the
existing 64-operation limit, and drain during shutdown. Related hints commit together. Delayed
clears compare the observed persisted value, including its private revision token, so a newer
success on the same provider/account survives an older failure. The public pin shape is unchanged.

The existing 30-second artifact recovery producer serves publication and deletion queues in
alternating order, within one 10-second admission budget. An admitted storage operation retains
its actual I/O lifetime; the pass does not abandon raw work at its admission deadline. Shared
transport/authentication failures stop the pass with bounded backoff (30 seconds to 5 minutes).
Object-specific integrity/reference failures defer that item and allow neighbours to proceed.
The six-hour retention schedule is unchanged. Only matching, non-NULL storage scopes are eligible
for automated object deletion; legacy and foreign-scope entries need explicit operator review.

S3 requires bucket-level `s3:ListBucket` to distinguish a missing object from forbidden access
using GET. Provision this separately for the dedicated artifact bucket and the actual task role,
with an explicit decision about exposing object names outside the call-log prefix. Validate an
existing object and a random missing key under that role with ExpectedBucketOwner. A 403 is an
availability/access failure, never evidence of absence. A negative GET after an uncertain PUT
does not prove that the PUT cannot complete later; its journal identity remains pending.

The two authenticated management detail routes (`/api/logs/[id]` and
`/api/usage/call-logs/[id]`) can read an intact retained encrypted staging payload while
publication is pending. They recheck the original call incarnation, runtime identity and current
key privacy policy after bounded worker-thread decryption and integrity validation. Responses
identify `detailSource: "staging"` and `artifactPublicationState`; persisted `detailState` and
the journal remain unchanged. This capability does not extend to exports or inference
continuation. At most two staged reads are admitted per process, with a ten-second response
deadline; cancelled work retains its slot until its actual SQL/CPU lifetime ends.

The authenticated management health payload includes `artifactReconciliation` (pending/uncertain/
legacy/foreign-scope counts, publication/deletion counts, staged bytes, oldest pending timestamp,
`oldestUncertainCreatedAt`, `reviewRequired` and safe failure diagnostics)
and `persistenceBackground` counters for the declared LKGP/reconciliation owners. Anonymous
liveness responses do not expose these details. PostgreSQL diagnostics separate advisory lock,
query and COMMIT timing; slow successful operations are rate-limited by static operation name.
`npm run check:persistence-background-owners` is an AST admission ratchet with a frozen legacy
inventory. It does not prove transitive callback safety or the absence of all blocking code;
scenario tests and native runtime qualification remain required.

`reviewRequired` marks uncertain entries whose creation time is at least 30 minutes old; a safe
count/timestamp warning is emitted at most once per 30 minutes while that condition persists.
This measures the age of an unresolved entry, not the time since its last positive progress.
Neither a negative GET nor a successful staging read completes publication or retirement.

Before upgrading, drain the sole writer and capture a verified recoverable database backup plus
the unchanged encryption/authentication secrets. Apply migration 9 offline and verify its history
and indexes before opening traffic. An older image does not recognize the new migration history:
image-only rollback is unsupported. Rehearse a database-aware restore on an isolated target.
Restoring the pre-upgrade backup after traffic resumes loses later database writes; obtain an
explicit recovery-point decision before doing so. Prefer a compatible corrective image when
retaining those writes is required. Do not delete or edit migration-history rows to bypass checks.

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

For a SQLite source whose `conversation_turn_nodes` history exceeds the inline row budget,
add `--large-rows` to **capture, import and verify**. This optional profile writes history into
private `rows/` files next to the bundle; retain those files with the other carriers. It preserves
source rowids as native insertion order and binds every chunk's size, count and SHA-256 to the
snapshot and import receipt. Import, full row verification, identity restart and receipt publication
share the same transaction. Missing, reordered or modified chunks reject the action; reruns use
the same bundle and verify the acknowledged receipt rather than replaying writes.

The history carrier allows at most 10,000,000 rows, 10 GiB and 10,000 chunks. Each chunk contains
at most 5,000 rows and 8 MiB; capture reduces the batch size using a conservative row-size bound
before loading rows. SQLite and PostgreSQL use keyset reads rather than growing offsets for this
table. Other tables retain the original inline limits. This profile is specific to SQLite history
import; it does not enlarge native logical export/restore limits. With `--large-rows`, the CLI uses
the greater of the configured transaction deadline and 300,000 milliseconds. Configure the offline
deadline explicitly for large datasets, up to 1,800,000 milliseconds. This expands the parser's
accepted explicit transaction configuration; its 15,000-millisecond default and the other timeout
bounds and ordering requirements remain unchanged. Live gateway deadlines remain unchanged. These bounds are
not an RSS ceiling or a guarantee of completion within any deadline.

When referenced call-log files exceed the ordinary 512 MiB aggregate carrier budget, add
`--large-artifacts` to capture/export and import/restore/verify. This opt-in permits up to
10 GiB of referenced artifact bytes stored outside the JSON bundle. Each object remains limited
to 8 MiB and is read, hashed and verified individually; the JSON snapshot limit is unchanged.
Without the flag, both capture and bundle loading reject larger aggregate declarations.
Retain the entire private `objects/` directory with the bundle. Large aggregate capacity does
not change serving object limits, bypass checksums or guarantee completion before a deadline.

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
Legacy local CLI commands (`auth export --force`, local provider/key/setup operations,
`reset-encrypted-columns`, `backup`, `restore`, and `backup auto enable/disable/status`) require
explicit `OMNIROUTE_DATABASE_BACKEND=sqlite`. They refuse PostgreSQL/default-native execution
instead of opening a local database or reporting a successful local backup. Use the native offline
export/restore procedure above for PostgreSQL maintenance.

Send graceful termination and inspect cleanup completion. Producer stop, durable buffer flush,
request/background/SSE drain and pool/object-client close have explicit owners. The wrappers force
exit after their shutdown budget, so provision the task stop grace accordingly. Forced exit or an
ambiguous COMMIT requires reconciliation, not an assumption that writes failed. Pending S3 deletion
work remains in the SQL queue until object deletion and queue acknowledgement succeed.

Changing the selector back to SQLite is not reverse migration: the old file lacks PostgreSQL-era
writes. Preserve the native dataset/objects and explicitly reconcile those writes before a backout
that must retain them. No PostgreSQL-to-SQLite data conversion command is provided here.

## Qualification pending for the release artifact

### Offline schema upgrade for indexed history retention

Stop all application writers before upgrading this lock protocol. Migration 7 adds a derived
`timestamptz` column, validates and backfills existing `conversation_turn_nodes.last_seen_at`
values, and creates an ordered retention index in one transaction. Public timestamp text stays
unchanged; a trigger maintains the indexed value. Invalid timestamps and relative literals such
as `now` or `today` reject the migration atomically and require an explicit data repair. Retention
duration and the strict cutoff remain unchanged. Cleanup admits at most 100 batches of 1,000
rows per pass and stops admitting batches after one second; a batch already running can extend
that duration up to its SQL deadline. Locked rows are revisited by later passes.

For a populated database, run the schema-only command during the maintenance window with
explicitly measured offline timeout budgets:

```sh
node --import tsx/esm scripts/ops/postgres-migrate-schema.ts --offline
```

The command reads the documented PostgreSQL environment, applies checksummed migrations and
the legacy compression seed upgrade, then closes the pool. It starts no application producers
and uses no object store. `--offline` acknowledges that the operator has stopped writers; the
command cannot itself establish ECS quiescence. Configure statement/query/transaction deadlines
for the measured backfill and index duration, preserving their ordering, and restore live
deadlines for the gateway. Migration 8 adds durable encrypted artifact staging and privacy
revocation. Verify migration history and restart behavior before allowing traffic. Do not roll
back to a binary that rejects the newer schema; retain a verified pre-upgrade recovery set.

### Local code qualification

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
