import assert from "node:assert/strict";
import test from "node:test";
import { normalizeStoredOperatorEgressAddress } from "../../src/lib/db/proxies/egressAddress.ts";

test("stored operator addresses retain IPv4 and IPv4-mapped values", () => {
  assert.equal(normalizeStoredOperatorEgressAddress("203.0.113.7"), "203.0.113.7");
  assert.equal(normalizeStoredOperatorEgressAddress("::ffff:203.0.113.7"), "203.0.113.7");
});

test("raw and stored IPv6 prefixes resolve to the same canonical address", () => {
  assert.equal(normalizeStoredOperatorEgressAddress("2001:4860:4860::8888"), "v6:2001:4860:4860:0");
  assert.equal(normalizeStoredOperatorEgressAddress("v6:2001:4860:4860:0"), "v6:2001:4860:4860:0");
  assert.equal(
    normalizeStoredOperatorEgressAddress(" V6:2001:4860:4860:0000 "),
    "v6:2001:4860:4860:0"
  );
});

test("malformed prefixes and hostnames are never accepted as stored IP addresses", () => {
  for (const value of [
    null,
    1,
    "proxy.example",
    "https://203.0.113.7",
    "v6:2001:4860:0",
    "v6:2001:4860:4860:0:1",
    "v6:2001:4860:4860:00000",
    "v6:2001:4860:bad-host:0",
    "v6:2001:4860:4860:0%zone",
  ]) {
    assert.equal(normalizeStoredOperatorEgressAddress(value), null);
  }
});
