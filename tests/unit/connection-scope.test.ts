import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  connectionScopeFromConfiguredIds as configured,
  connectionScopeFromResolvedIds as resolved,
  connectionScopeIds,
  connectionScopePermits,
  intersectConnectionScopes as intersect,
  UNRESTRICTED_CONNECTION_SCOPE,
  type ConnectionScope,
} from "../../open-sse/services/connectionScope.ts";
import { quotaScopeFixture as quota } from "../helpers/routing/connectionScopeQuotaFixture.ts";

const policyUrl = new URL("../../src/lib/api/keyConnectionScope.ts", import.meta.url).href;
const fixtureUrl = new URL("../helpers/routing/connectionScopeQuotaFixture.ts", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL === policyUrl && specifier === "@/lib/quota/quotaKey")
      return { url: fixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
const { resolveKeyConnectionScope } = await import(policyUrl);
test.after(() => hooks.deregister());

test("raw all-mode stays unrestricted while computed empty sets remain denied", () => {
  for (const input of [undefined, null, []]) assert.equal(configured(input).kind, "unrestricted");
  assert.equal(resolved([]).kind, "deny");
  const deny = intersect(configured(["A"]), configured(["B"]));
  assert.equal(deny.kind, "deny");
  assert.equal(intersect(deny, resolved(["A", "B"])).kind, "deny");
  assert.equal(connectionScopePermits(deny, "A"), false);
  assert.equal(connectionScopePermits(resolved(["A"]), undefined), false);
  const scoped = resolved(["A", "A", "B"]);
  assert.notEqual(scoped.kind, "deny");
  if (scoped.kind !== "deny") assert.deepEqual(connectionScopeIds(scoped), ["A", "B"]);
});

test("scope intersection is associative, commutative and never expands either operand", () => {
  const universe = ["A", "B", "C"];
  const scopes: ConnectionScope[] = [UNRESTRICTED_CONNECTION_SCOPE];
  for (let bits = 0; bits < 8; bits++)
    scopes.push(resolved(universe.filter((_, index) => bits & (1 << index))));
  const members = (scope: ConnectionScope) =>
    universe.filter((id) => connectionScopePermits(scope, id));
  for (const a of scopes)
    for (const b of scopes) {
      const actual = members(intersect(a, b));
      assert.deepEqual(
        actual,
        members(a).filter((id) => members(b).includes(id))
      );
      assert.deepEqual(actual, members(intersect(b, a)));
      for (const c of scopes)
        assert.deepEqual(
          members(intersect(intersect(a, b), c)),
          members(intersect(a, intersect(b, c)))
        );
    }
});

test("key, target and active quota narrowing cannot resurrect a denied target", async () => {
  quota.reads = 0;
  quota.failure = null;
  quota.ids = ["A", "B"];
  const key = { allowedConnections: ["A"], allowedQuotas: ["pool"] };
  assert.equal((await resolveKeyConnectionScope(key, ["B"])).kind, "deny");
  assert.equal(quota.reads, 1);
  quota.ids = [];
  assert.equal((await resolveKeyConnectionScope(key)).kind, "deny");
  quota.ids = ["B"];
  const scoped = await resolveKeyConnectionScope({
    allowedConnections: [],
    allowedQuotas: ["pool"],
  });
  assert.deepEqual(scoped, resolved(["B"]));
  quota.reads = 0;
  assert.equal((await resolveKeyConnectionScope({ allowedConnections: [] })).kind, "unrestricted");
  assert.equal(quota.reads, 0);
  const failure = new Error("controlled quota resolution failure");
  quota.failure = failure;
  try {
    await assert.rejects(resolveKeyConnectionScope(key), (error) => error === failure);
  } finally {
    quota.failure = null;
  }
});
