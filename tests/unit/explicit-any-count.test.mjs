import assert from "node:assert/strict";
import test from "node:test";
import { countExplicitAny } from "../../scripts/check/explicitAnyCount.mjs";

test("type annotations, assertions and generic bounds consume the explicit-any budget", () => {
  assert.equal(
    countExplicitAny(`
      type Values = Array<any>;
      let value: any;
      const cast = value as any;
      function read<T extends any>(input: T): any { return input; }
    `),
    5
  );
});

test("property names, identifiers, strings and comments do not consume a type budget", () => {
  assert.equal(
    countExplicitAny(`
      // any is a property name, not a type here.
      const any = "any";
      const options = { any };
      AbortSignal.any([]);
      const pattern = /any/;
      interface Options { any: string; }
    `),
    0
  );
});

test("TSX attributes stay distinct from actual any types", () => {
  assert.equal(countExplicitAny('const view = <Widget any="any" />;', "view.tsx"), 0);
  assert.equal(countExplicitAny('const view: any = <Widget any="any" />;', "view.tsx"), 1);
});

test("invalid syntax cannot silently pass a type budget", () => {
  assert.throws(() => countExplicitAny("const value: any = ;"), /invalid syntax/);
});
