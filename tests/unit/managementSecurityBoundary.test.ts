import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, after, test } from "node:test";
import { SignJWT } from "jose";
import { NextRequest } from "next/server";

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "management-boundary-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DATA_DIR = scratch;
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.API_KEY_SECRET = "management-tests-api-key-secret";
process.env.JWT_SECRET = "management-boundary-test-secret";
process.env.OMNIROUTE_PEER_STAMP_TOKEN = "management-boundary-peer-stamp";
delete process.env.INITIAL_PASSWORD;
const core = await import("../../src/lib/db/core.ts");
const settings = await import("../../src/lib/db/settings.ts");
const authState = await import("../../src/lib/db/authSettings.ts");
const password = await import("../../src/lib/auth/managementPassword.ts");
const session = await import("../../src/shared/utils/dashboardSessionToken.ts");
const bootstrap = await import("../../src/lib/auth/bootstrapToken.ts");
const keys = await import("../../src/lib/db/apiKeys.ts");
const loginSettings = await import("../../src/app/api/settings/require-login/route.ts");
const mainSettings = await import("../../src/app/api/settings/route.ts");
const relay = await import("../../src/app/api/relay/tokens/route.ts");
const sync = await import("../../src/app/api/sync/tokens/route.ts");
const exports = await import("../../src/app/api/settings/export-json/route.ts");
const imports = await import("../../src/app/api/settings/import-json/route.ts");
const backupExport = await import("../../src/app/api/db-backups/export/route.ts");
const backupExportAll = await import("../../src/app/api/db-backups/exportAll/route.ts");
const backupImport = await import("../../src/app/api/db-backups/import/route.ts");
const backups = await import("../../src/app/api/db-backups/route.ts");
const cli = await import("../../src/app/api/cli/tokens/route.ts");
const cliKeys = await import("../../src/app/api/cli-tools/keys/route.ts");
const accessTokens = await import("../../src/lib/db/accessTokens.ts");
const pipeline = await import("../../src/server/authz/pipeline.ts");
const headers = await import("../../src/server/authz/headers.ts");

beforeEach(async () => {
  core.getDbInstance().prepare("DELETE FROM key_value WHERE namespace='settings'").run();
  await settings.updateSettings({
    requireLogin: false,
    setupComplete: true,
    managementAuthGeneration: randomUUID(),
  });
  bootstrap.__resetBootstrapTokenForTest();
});
after(() => {
  core.resetDbInstance();
  fs.rmSync(scratch, { recursive: true, force: true });
});

function request(route: string, body?: unknown, extra: HeadersInit = {}, method = "POST") {
  return new Request("http://management.test/api/" + route, {
    method,
    headers: { "content-type": "application/json", ...Object.fromEntries(new Headers(extra)) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
}
async function authenticated() {
  const state = await authState.getAuthSettingsSnapshot();
  return {
    cookie:
      "auth_token=" +
      (await session.mintDashboardSessionToken(
        session.getDashboardJwtSecret()!,
        state.generation!
      )),
  };
}

test("open mode cannot mint protected credentials or export raw configuration", async () => {
  assert.equal((await relay.POST(request("relay/tokens", { name: "anonymous" }))).status, 401);
  assert.equal((await sync.POST(request("sync/tokens", { name: "anonymous" }))).status, 401);
  assert.equal(
    (await cli.POST(request("cli/tokens", { name: "anonymous", scope: "admin" }))).status,
    401
  );
  assert.equal(
    (await exports.GET(request("settings/export-json", undefined, {}, "GET"))).status,
    401
  );
});

test("manage key cannot convert open or closed mode into operator identity", async () => {
  const key = await keys.createApiKey("machine", "management-test-machine", ["manage"]);
  for (const requireLogin of [false, true]) {
    await settings.updateSettings({
      requireLogin,
      password: await password.hashManagementPassword("original-password"),
    });
    const credential = { authorization: "Bearer " + key.key };
    const before = await authState.getAuthSettingsSnapshot();
    assert.equal(
      (
        await loginSettings.POST(
          request(
            "settings/require-login",
            { requireLogin: false, password: "attacker-password" },
            credential
          )
        )
      ).status,
      403
    );
    assert.equal(
      (
        await mainSettings.PATCH(
          request(
            "settings",
            { oidcEnabled: true, currentPassword: "original-password" },
            credential,
            "PATCH"
          )
        )
      ).status,
      403
    );
    assert.equal(
      (await relay.POST(request("relay/tokens", { name: "machine" }, credential))).status,
      403
    );
    assert.equal(
      (await sync.POST(request("sync/tokens", { name: "machine" }, credential))).status,
      403
    );
    assert.equal(
      (await exports.GET(request("settings/export-json", undefined, credential, "GET"))).status,
      403
    );
    assert.equal(
      (await cli.POST(request("cli/tokens", { name: "machine", scope: "admin" }, credential)))
        .status,
      403
    );
    assert.equal((await authState.getAuthSettingsSnapshot()).revision, before.revision);
  }
});

test("open and machine key listings stay masked while the operator may recover raw keys", async () => {
  const key = await keys.createApiKey("list-manager", "management-test-machine", ["manage"]);
  const machine = await accessTokens.createAccessTokenAsync({ name: "read-list", scope: "read" });
  for (const credential of [{}, { authorization: "Bearer " + machine.secret }]) {
    const response = await cliKeys.GET(request("cli-tools/keys", undefined, credential, "GET"));
    assert.equal(response.status, 200);
    const body = await response.json();
    const listed = body.keys.find((value: { id: string }) => value.id === key.id);
    assert.ok(listed);
    assert.equal(listed.rawKey, undefined);
    assert.notEqual(listed.key, key.key);
    assert.equal(JSON.stringify(body).includes(key.key), false);
  }
  const response = await cliKeys.GET(
    request("cli-tools/keys", undefined, await authenticated(), "GET")
  );
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.keys.find((value: { id: string }) => value.id === key.id).rawKey, key.key);
});

test("enabled OIDC without a password is not bootstrap", async () => {
  const key = await keys.createApiKey("machine", "management-test-machine", ["manage"]);
  await settings.updateSettings({ requireLogin: true, oidcEnabled: true });
  const before = await authState.getAuthSettingsSnapshot();
  assert.equal(
    (
      await loginSettings.POST(
        request(
          "settings/require-login",
          { password: "attacker-password" },
          { authorization: "Bearer " + key.key }
        )
      )
    ).status,
    403
  );
  assert.equal((await authState.getAuthSettingsSnapshot()).revision, before.revision);
});

test("raw export and SQLite restore guards reject open and machine callers before effects", async () => {
  const key = await keys.createApiKey("backup-manager", "management-test-machine", ["manage"]);
  for (const credential of [{}, { authorization: "Bearer " + key.key }]) {
    const expected = "authorization" in credential ? 403 : 401;
    assert.equal(
      (await imports.POST(request("settings/import-json", {}, credential))).status,
      expected
    );
    assert.equal(
      (await backupImport.POST(request("db-backups/import", {}, credential))).status,
      expected
    );
    assert.equal((await backups.POST(request("db-backups", {}, credential))).status, expected);
    assert.equal(
      (await backupExport.GET(request("db-backups/export", undefined, credential, "GET"))).status,
      expected
    );
    assert.equal(
      (await backupExportAll.GET(request("db-backups/exportAll", undefined, credential, "GET")))
        .status,
      expected
    );
  }
  assert.equal(
    (await exports.GET(request("settings/export-json", undefined, await authenticated(), "GET")))
      .status,
    200
  );
});

test("Host and forged locality do not establish bootstrap; actual proof is reserved once", async () => {
  const forged = request(
    "settings/require-login",
    { password: "attacker-password" },
    { host: "localhost", [headers.AUTHZ_HEADER_PEER_LOCALITY]: "loopback" }
  );
  const verdict = await pipeline.runAuthzPipeline(new NextRequest(forged));
  const forwarded = new Headers(forged.headers);
  for (const [key, value] of verdict.headers)
    if (key.startsWith("x-middleware-request-"))
      forwarded.set(key.slice("x-middleware-request-".length), value);
  assert.equal(
    (
      await loginSettings.POST(
        request("settings/require-login", { password: "attacker-password" }, forwarded)
      )
    ).status,
    401
  );
  const token = bootstrap.getOrCreateBootstrapToken({ log() {} });
  const lease = bootstrap.reserveBootstrapToken(token)!;
  assert.ok(lease);
  assert.equal(bootstrap.reserveBootstrapToken(token), null);
  assert.equal(bootstrap.peekBootstrapToken(token), false);
  lease.release();
  const response = await loginSettings.POST(
    request(
      "settings/require-login",
      { password: "bootstrap-password" },
      { [headers.BOOTSTRAP_TOKEN_HEADER]: token }
    )
  );
  assert.equal(response.status, 200);
  assert.equal(bootstrap.peekBootstrapToken(token), false);
  assert.equal(
    (
      await loginSettings.POST(
        request(
          "settings/require-login",
          { password: "changed-password" },
          { [headers.BOOTSTRAP_TOKEN_HEADER]: token }
        )
      )
    ).status,
    401
  );
});

test("both password writers revoke same-second sessions and never upgrade an old epoch", async () => {
  await settings.updateSettings({
    requireLogin: true,
    password: await password.hashManagementPassword("original-password"),
  });
  const state = await authState.getAuthSettingsSnapshot(),
    credential = await authenticated();
  const old = credential.cookie.slice("auth_token=".length);
  const response = await loginSettings.POST(
    request(
      "settings/require-login",
      { currentPassword: "original-password", password: "second-password" },
      credential
    )
  );
  assert.equal(response.status, 200);
  assert.equal(await session.verifyDashboardSessionToken(old), null);
  const paused = await session.mintDashboardSessionToken(
    session.getDashboardJwtSecret()!,
    state.generation!
  );
  assert.equal(await session.verifyDashboardSessionToken(paused), null);
  const renewed = response.headers.get("set-cookie")!.match(/auth_token=([^;]+)/)![1];
  assert.ok(await session.verifyDashboardSessionToken(renewed));
  const main = await mainSettings.PATCH(
    request(
      "settings",
      { currentPassword: "second-password", newPassword: "third-password" },
      { cookie: "auth_token=" + renewed },
      "PATCH"
    )
  );
  assert.equal(main.status, 200);
  assert.equal(await session.verifyDashboardSessionToken(renewed), null);
  const legacy = await new SignJWT({ authenticated: true })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime("1h")
    .sign(session.getDashboardJwtSecret()!);
  assert.equal(await session.verifyDashboardSessionToken(legacy), null);
});

test("ordinary operator relay/sync and scoped API-key workflows remain available", async () => {
  const credential = await authenticated();
  assert.equal(
    (await relay.POST(request("relay/tokens", { name: "operator" }, credential))).status,
    200
  );
  assert.equal(
    (await sync.POST(request("sync/tokens", { name: "operator", ttl: "1h" }, credential))).status,
    201
  );
  const api = await import("../../src/app/api/keys/route.ts");
  assert.equal(
    (await api.POST(request("keys", { name: "inference", scopes: ["read:usage"] }))).status,
    201
  );
  assert.equal(
    (await api.POST(request("keys", { name: "anonymous-manager", scopes: ["manage"] }))).status,
    401
  );
  const key = await keys.createApiKey("manager", "management-test-machine", ["manage"]);
  assert.equal(
    (
      await api.POST(
        request(
          "keys",
          { name: "managed", scopes: ["manage"] },
          { authorization: "Bearer " + key.key }
        )
      )
    ).status,
    201
  );
});
