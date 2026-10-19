import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { registerHooks } from "node:module";
import { after, beforeEach, test } from "node:test";
import * as settings from "../../src/lib/db/settings.ts";
import * as core from "../../src/lib/db/core.ts";
import * as auth from "../../src/lib/db/authSettings.ts";
import * as bootstrap from "../../src/lib/auth/bootstrapToken.ts";
import * as password from "../../src/lib/auth/managementPassword.ts";
import * as session from "../../src/shared/utils/dashboardSessionToken.ts";
import * as fixture from "../fixtures/managementBootstrapSettings.ts";

const routeUrl = new URL("../../src/app/api/settings/require-login/route.ts", import.meta.url).href;
const fixtureUrl = new URL("../fixtures/managementBootstrapSettings.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === routeUrl && specifier === "@/lib/db/settings")
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const route = await import(routeUrl);

beforeEach(async () => {
  delete process.env.INITIAL_PASSWORD;
  delete process.env.AUTH_COOKIE_SECURE;
  process.env.JWT_SECRET = "private-bootstrap-outcome-fixture";
  core.getDbInstance().prepare("DELETE FROM key_value WHERE namespace='settings'").run();
  await settings.updateSettings({
    requireLogin: false,
    setupComplete: true,
    managementAuthGeneration: randomUUID(),
  });
  bootstrap.__resetBootstrapTokenForTest();
});
after(() => {
  hooks.deregister();
  core.resetDbInstance();
});

for (const [mode, reusable] of [
  ["acknowledged", false],
  ["conflict", true],
  ["rollback", true],
  ["commit_unknown", false],
  ["committed_effect", false],
  ["unbranded", false],
  ["unknown", false],
] as const) {
  test(`bootstrap proof ${reusable ? "survives" : "is consumed after"} ${mode}`, async () => {
    fixture.selectBootstrapMutationMode(mode);
    const token = bootstrap.getOrCreateBootstrapToken({ log() {} });
    const before = await auth.getAuthSettingsSnapshot();
    const response = await route.POST(
      new Request("http://management.test/api/settings/require-login", {
        method: "POST",
        headers: { "content-type": "application/json", "x-omniroute-bootstrap-token": token },
        body: JSON.stringify({ requireLogin: false }),
      })
    );
    assert.equal(response.status, mode === "acknowledged" ? 200 : mode === "conflict" ? 409 : 500);
    assert.equal(fixture.writes, 1, "an uncertain or failed mutation is never replayed");
    assert.equal(bootstrap.peekBootstrapToken(token), reusable);
    assert.equal(response.headers.get("set-cookie"), null);
    const after = await auth.getAuthSettingsSnapshot();
    const durable = ["acknowledged", "commit_unknown", "committed_effect"].includes(mode);
    assert.equal(after.revision, before.revision + Number(durable));
  });
}

test("password renewal keeps a Secure cookie when HTTPS terminates at the proxy", async () => {
  fixture.selectBootstrapMutationMode("acknowledged");
  await settings.updateSettings({
    password: await password.hashManagementPassword("previous-password"),
  });
  const generation = (await auth.getAuthSettingsSnapshot()).generation!;
  const token = await session.mintDashboardSessionToken(
    session.getDashboardJwtSecret()!,
    generation
  );
  const response = await route.POST(
    new Request("http://management.test/api/settings/require-login", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-forwarded-proto": "https",
        cookie: "auth_token=" + token,
      },
      body: JSON.stringify({
        password: "replacement-password",
        currentPassword: "previous-password",
      }),
    })
  );
  assert.equal(response.status, 200);
  assert.match(response.headers.get("set-cookie")!, /; Secure(?:;|$)/i);
  assert.equal(await session.verifyDashboardSessionToken(token), null);
  const renewed = response.headers.get("set-cookie")!.match(/auth_token=([^;]+)/)![1];
  assert.ok(await session.verifyDashboardSessionToken(renewed));
});
