import { readFileSync } from "node:fs";
import ts from "typescript";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  fixture,
  configuration,
  barrier,
} from "../helpers/persistence/artifactPublicationFixture.ts";
import {
  prepareApplicationArtifacts,
  assertApplicationArtifactPublicationScope,
  type OfflineArtifactCheckpoint,
} from "../../src/lib/db/postgres/applicationImportArtifacts.ts";
const source = new URL("../../src/lib/artifacts/s3ArtifactStore.ts", import.meta.url).href;
const mock = new URL("../helpers/persistence/artifactPublicationFixture.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL?.split("?")[0] === source && specifier === "@aws-sdk/client-s3")
      return { url: mock, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const { createS3ArtifactStore } = await import(source);
const bytes = Buffer.from("offline journal bytes"),
  checksum = createHash("sha256").update(bytes).digest("hex");
const snapshot = {
  format: "omniroute-application-storage-v1",
  source: "sqlite",
  tables: [
    {
      name: "call_logs",
      columns: ["artifact_relpath", "artifact_size_bytes", "artifact_sha256"],
      rowOrder: "rowid",
      rows: [{ rowid: "1", values: ["source/detail.json", bytes.length, checksum] }],
    },
  ],
  metadataTables: [],
  definitions: [],
  excluded: [],
};
let disk: OfflineArtifactCheckpoint[] = [];
const clone = (entries: readonly OfflineArtifactCheckpoint[]) =>
  JSON.parse(JSON.stringify(entries)) as OfflineArtifactCheckpoint[];
function io(store: ReturnType<typeof createS3ArtifactStore>) {
  return {
    target: store,
    async readSource() {
      return bytes;
    },
    async checkpoint(entries: readonly OfflineArtifactCheckpoint[]) {
      disk = clone(entries);
      fixture.events.push("checkpoint:" + entries[0].state + ":" + entries[0].uploadState);
    },
  };
}
test.after(() => hooks.deregister());
test("offline pre-send journal survives ACK loss and failed readback; rerun only GETs same key", async () => {
  fixture.reset();
  disk = [];
  fixture.putMode = "lost";
  fixture.getFailure = true;
  const store = createS3ArtifactStore(configuration);
  fixture.beforePut = () => {
    assert.equal(disk.length, 1);
    assert.equal(disk[0].state, "pending");
    assert.equal(disk[0].singleAttempt, true);
  };
  await assert.rejects(prepareApplicationArtifacts(snapshot, io(store)));
  assert.equal(disk[0].state, "pending");
  assert.equal(disk[0].uploadState, "uncertain");
  const reference = disk[0].targetReference;
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  fixture.getFailure = false;
  const mappings = await prepareApplicationArtifacts(snapshot, io(store), disk);
  assert.equal(mappings[0].targetReference, reference);
  assert.equal(disk[0].state, "ready");
  assert.equal(mappings[0].singleAttempt, true);
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  await store.close();
});
test("acknowledged upload is checkpointed before a failed GET and no new PUT is needed", async () => {
  fixture.reset();
  disk = [];
  fixture.getFailure = true;
  const store = createS3ArtifactStore(configuration);
  await assert.rejects(prepareApplicationArtifacts(snapshot, io(store)));
  assert.equal(disk[0].uploadState, "acknowledged");
  assert.equal(disk[0].state, "pending");
  fixture.getFailure = false;
  await prepareApplicationArtifacts(snapshot, io(store), disk);
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  await store.close();
});
test("checkpoint refusal before dispatch produces no PUT and no object", async () => {
  fixture.reset();
  const store = createS3ArtifactStore(configuration);
  await assert.rejects(
    prepareApplicationArtifacts(snapshot, {
      target: store,
      async readSource() {
        return bytes;
      },
      async checkpoint() {
        throw new Error("durability not acknowledged");
      },
    })
  );
  assert.equal(fixture.requests.length, 0);
  assert.equal(fixture.objects.size, 0);
  await store.close();
});
test("offline pending404 is retained while a sole PUT completes late, then resumed without replay", async () => {
  fixture.reset();
  disk = [];
  fixture.writeGate = barrier();
  fixture.putEntered = barrier();
  const store = createS3ArtifactStore(configuration, { deadlineMs: 25 });
  const preparation = prepareApplicationArtifacts(snapshot, io(store));
  void preparation.catch(() => {});
  await fixture.putEntered.promise;
  await assert.rejects(preparation);
  const original = clone(disk);
  assert.equal(original.length, 1);
  assert.equal(original[0].state, "pending");
  await assert.rejects(prepareApplicationArtifacts(snapshot, io(store), disk));
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  assert.deepEqual(disk, original);
  fixture.writeGate.resolve();
  await store.close();
  const fresh = createS3ArtifactStore(configuration);
  const mappings = await prepareApplicationArtifacts(snapshot, io(fresh), disk);
  assert.equal(mappings[0].targetReference, original[0].targetReference);
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  await fresh.close();
});
test("old ready arrays remain readable without inventing single-attempt provenance", async () => {
  fixture.reset();
  disk = [];
  const store = createS3ArtifactStore(configuration),
    stored = await store.put(bytes);
  const mapping = {
    sourceReference: "source/detail.json",
    targetReference: stored.reference,
    sizeBytes: bytes.length,
    checksum,
  };
  const ready = await prepareApplicationArtifacts(snapshot, io(store), [mapping]);
  assert.equal(ready[0].singleAttempt, undefined);
  assert.equal(ready[0].storageScope, store.storageScope);
  assert.equal(disk[0].singleAttempt, undefined);
  await store.close();
});
test("unresolved foreign-scope queue fails before new uploads while settled attached scope may be rewritten", async () => {
  fixture.reset();
  const store = createS3ArtifactStore(configuration);
  const scoped = {
    ...snapshot,
    source: "postgres",
    tables: [
      ...snapshot.tables,
      {
        name: "artifact_deletion_queue",
        columns: ["relpath", "publication_state", "upload_state", "store_scope"],
        rowOrder: "native",
        rows: [
          {
            rowid: null,
            values: ["source/detail.json", "publishing", "uncertain", "a".repeat(64)],
          },
        ],
      },
    ],
  };
  await assert.rejects(prepareApplicationArtifacts(scoped, io(store)));
  assert.equal(fixture.requests.length, 0);
  scoped.tables[1].rows[0].values[1] = "attached";
  scoped.tables[1].rows[0].values[2] = "acknowledged";
  assert.doesNotThrow(() =>
    assertApplicationArtifactPublicationScope(
      scoped as unknown as Parameters<typeof assertApplicationArtifactPublicationScope>[0],
      store
    )
  );
  await store.close();
});

// Exact source-body projections qualify private pure planning/FS boundaries only.
function ownerProjection(
  url: URL,
  functions: readonly string[],
  variables: readonly string[],
  imports: string
): string {
  const original = readFileSync(url, "utf8"),
    source = ts.createSourceFile(url.pathname, original, ts.ScriptTarget.Latest, true);
  const bodies: string[] = [];
  for (const name of variables) {
    const found = source.statements.filter(
      (node) =>
        ts.isVariableStatement(node) &&
        node.declarationList.declarations.some((entry) => entry.name.getText(source) === name)
    );
    assert.equal(found.length, 1, "actual owner variable " + name);
    bodies.push(found[0].getText(source));
  }
  for (const name of functions) {
    const found = source.statements.filter(
      (node) => ts.isFunctionDeclaration(node) && node.name?.text === name
    );
    assert.equal(found.length, 1, "actual owner function " + name);
    bodies.push("export " + found[0].getText(source));
  }
  return ts.transpileModule(imports + bodies.join("\n"), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  }).outputText;
}
const planUrl = new URL("../../src/lib/db/postgres/applicationImport.ts", import.meta.url),
  checkpointUrl = new URL("../../scripts/ops/postgres-migrate-storage.ts", import.meta.url);
const planProjection = planUrl.href + "?artifact-maintenance-contract",
  checkpointProjection = checkpointUrl.href + "?artifact-maintenance-contract";
const planCode = ownerProjection(
  planUrl,
  [
    "validUsageTimestamp",
    "checkedCell",
    "columnOrder",
    "buildPlan",
    "addArtifactPublicationPlan",
    "artifactPublicationValues",
  ],
  ["integerTypes", "logicalTables", "controlTables"],
  `
import {applicationImportDigest as canonical} from "./applicationImportDigest.ts";
import {APPLICATION_IMPORT_NATIVE_ONLY,APPLICATION_IMPORT_TABLES} from "./applicationImportManifest.ts";
import {ApplicationImportError,applicationSnapshotDigest} from "./applicationSnapshot.ts";
`
);
const checkpointCode = ownerProjection(
  checkpointUrl,
  ["atomicJson"],
  [],
  `
import {randomUUID} from "node:crypto";
import {open,rename,link,unlink} from "node:fs/promises";
import path from "node:path";
`
);
const projectionHooks = registerHooks({
  load(url, context, nextLoad) {
    if (url === planProjection || url === checkpointProjection)
      return {
        format: "module",
        source: url === planProjection ? planCode : checkpointCode,
        shortCircuit: true,
      };
    return nextLoad(url, context);
  },
});
test.after(() => projectionHooks.deregister());
interface MaintenancePlan {
  planDigest: string;
  tables: { name: string; columns: { name: string }[]; rows: unknown[][] }[];
}
const planOwner = (await import(planProjection)) as {
  buildPlan(snapshot: unknown, schema: typeof targetSchema, artifacts: unknown[]): MaintenancePlan;
};
const checkpointOwner = (await import(checkpointProjection)) as {
  atomicJson(filename: string, value: unknown, replace?: boolean): Promise<void>;
};
const scope = "b".repeat(64),
  target = "s3/01234567-0123-4123-8123-012345678901_" + checksum + ".json",
  createdAt = "2026-10-12T12:00:00.000Z";
const mapping = {
  sourceReference: "source/detail.json",
  targetReference: target,
  sizeBytes: bytes.length,
  checksum,
  storageScope: scope,
  singleAttempt: true as const,
  preparedAt: createdAt,
};
const column = (name: string, type = "text", nullable = false) => ({
  name,
  type,
  nullable,
  defaultExpression: null,
  identity: "",
  generated: "",
});
const queueColumns = [
  column("relpath"),
  column("created_at", "timestamp with time zone"),
  column("publication_state"),
  column("upload_state"),
  column("publication_owner", "uuid", true),
  column("store_scope", "text", true),
  column("expected_size_bytes", "bigint", true),
  column("expected_sha256", "text", true),
  column("next_attempt_at", "timestamp with time zone"),
];
const targetSchema = {
  digest: "synthetic-schema",
  tables: [
    {
      name: "call_logs",
      columns: [
        column("id"),
        column("artifact_relpath"),
        column("artifact_size_bytes", "bigint", true),
        column("artifact_sha256", "text", true),
      ],
      primary: ["id"],
      parents: [],
    },
    { name: "artifact_deletion_queue", columns: queueColumns, primary: ["relpath"], parents: [] },
  ],
};
const fullSnapshot = {
  ...snapshot,
  tables: [
    {
      ...snapshot.tables[0],
      columns: ["id", ...snapshot.tables[0].columns],
      rows: [{ rowid: "1", values: ["restored-log", ...snapshot.tables[0].rows[0].values] }],
    },
  ],
};
test("actual private import plan atomically binds remapped payload and scoped attached lifecycle to the same digest", () => {
  const plan = planOwner.buildPlan(
    fullSnapshot as unknown as Parameters<typeof planOwner.buildPlan>[0],
    targetSchema,
    [mapping]
  );
  const queue = plan.tables.find((table) => table.name === "artifact_deletion_queue")!;
  const value = (name: string) =>
    queue.rows[0][queue.columns.findIndex((column) => column.name === name)];
  assert.equal(value("relpath"), target);
  assert.equal(value("store_scope"), scope);
  assert.equal(value("publication_state"), "attached");
  assert.equal(value("upload_state"), "acknowledged");
  assert.equal(plan.tables.find((table) => table.name === "call_logs")!.rows[0][1], target);
  assert.equal(
    plan.planDigest,
    planOwner.buildPlan(
      fullSnapshot as unknown as Parameters<typeof planOwner.buildPlan>[0],
      targetSchema,
      [mapping]
    ).planDigest
  );
  const legacy = planOwner.buildPlan(
    fullSnapshot as unknown as Parameters<typeof planOwner.buildPlan>[0],
    targetSchema,
    [{ ...mapping, singleAttempt: undefined }]
  );
  const row = legacy.tables.find((table) => table.name === "artifact_deletion_queue")!;
  assert.equal(
    row.rows[0][row.columns.findIndex((column) => column.name === "upload_state")],
    "legacy_unknown"
  );
});
test("actual private restore plan rewrites settled attached scope but preserves unresolved identities and states", () => {
  const pending = "s3/11111111-1111-4111-8111-111111111111_" + checksum + ".json";
  const values = (reference: string, phase: string, outcome: string) => [
    reference,
    createdAt,
    phase,
    outcome,
    null,
    "c".repeat(64),
    { kind: "integer" as const, value: String(bytes.length) },
    checksum,
    createdAt,
  ];
  const withQueue = {
    ...fullSnapshot,
    source: "postgres",
    tables: [
      ...fullSnapshot.tables,
      {
        name: "artifact_deletion_queue",
        columns: queueColumns.map((column) => column.name),
        rowOrder: "native",
        rows: [
          { rowid: null, values: values("source/detail.json", "attached", "acknowledged") },
          { rowid: null, values: values(pending, "publishing", "uncertain") },
        ],
      },
    ],
  };
  const plan = planOwner.buildPlan(
    withQueue as unknown as Parameters<typeof planOwner.buildPlan>[0],
    targetSchema,
    [mapping]
  );
  const queue = plan.tables.find((table) => table.name === "artifact_deletion_queue")!;
  assert.equal(queue.rows.length, 2);
  const row = queue.rows.find((row) => row[0] === pending)!;
  assert.equal(row[2], "publishing");
  assert.equal(row[3], "uncertain");
  assert.equal(row[5], "c".repeat(64));
  const attached = queue.rows.find((row) => row[0] === target)!;
  assert.equal(attached[5], scope);
  assert.equal(attached[2], "attached");
  withQueue.tables[1].rows[0].values[3] = "legacy_unknown";
  const legacy = planOwner.buildPlan(
    withQueue as unknown as Parameters<typeof planOwner.buildPlan>[0],
    targetSchema,
    [mapping]
  );
  assert.equal(
    legacy.tables.find((table) => table.name === "artifact_deletion_queue")!.rows.length,
    3
  );
});
test("actual CLI checkpoint writes/fsyncs file before rename, fsyncs directory, and removes failed-sync temporary files", async (t) => {
  const fsPromises = await import("node:fs/promises");
  const { syncBuiltinESMExports } = await import("node:module");
  const root = new URL("../../scripts/ad-hoc/checkpoint-", import.meta.url).pathname;
  const directory = await fsPromises.mkdtemp(root),
    filename = directory + "/uploads.json",
    events: string[] = [];
  const originalOpen = fsPromises.default.open,
    originalRename = fsPromises.default.rename;
  let fail = false;
  t.mock.method(fsPromises.default, "open", async (...args: Parameters<typeof originalOpen>) => {
    const handle = await originalOpen(...args),
      isDirectory = String(args[0]) === directory;
    return new Proxy(handle, {
      get(target, property) {
        if (property === "sync")
          return async () => {
            events.push(isDirectory ? "directory-sync" : "file-sync");
            if (fail && !isDirectory) throw new Error("sync refused");
            return target.sync();
          };
        if (property === "writeFile")
          return async (...values: Parameters<typeof handle.writeFile>) => {
            events.push("file-write");
            return target.writeFile(...values);
          };
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  });
  t.mock.method(
    fsPromises.default,
    "rename",
    async (...args: Parameters<typeof originalRename>) => {
      events.push("rename");
      return originalRename(...args);
    }
  );
  syncBuiltinESMExports();
  try {
    await checkpointOwner.atomicJson(filename, [mapping], true);
    assert.deepEqual(events, ["file-write", "file-sync", "rename", "directory-sync"]);
    assert.deepEqual(JSON.parse(await fsPromises.readFile(filename, "utf8")), [mapping]);
    fail = true;
    await assert.rejects(
      checkpointOwner.atomicJson(filename, [{ changed: true }], true),
      /sync refused/
    );
    assert.deepEqual(await fsPromises.readdir(directory), ["uploads.json"]);
    assert.deepEqual(JSON.parse(await fsPromises.readFile(filename, "utf8")), [mapping]);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    await fsPromises.rm(directory, { recursive: true, force: true });
  }
});
