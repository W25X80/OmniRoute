import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import ts from "typescript";

import {
  breakerCountsAsFailure,
  logBreakerStateChange,
} from "../../src/sse/handlers/providerBreakerCallbacks.ts";

test("the process-owned failure callback excludes local lifecycle errors and ingress deadlines", () => {
  for (const local of [
    new DOMException("Request cancelled", "AbortError"),
    new Error("Invalid state: Controller is already closed"),
    new Error("request_signal_aborted"),
    new Error("Client disconnected"),
  ]) {
    assert.equal(breakerCountsAsFailure(local), false);
  }
  for (const upstream of [new Error("upstream unavailable"), { statusCode: 503 }, null]) {
    assert.equal(breakerCountsAsFailure(upstream), true);
  }
  assert.equal(typeof logBreakerStateChange, "function");
  const deadline = new Error("Gateway request exceeded its configured ingress deadline");
  deadline.name = "TimeoutError";
  Object.defineProperty(deadline, Symbol.for("omniroute.request-ingress-deadline.v1"), {
    value: true,
  });
  assert.equal(breakerCountsAsFailure(deadline), false);
  assert.equal(
    breakerCountsAsFailure(new DOMException("Upstream timed out", "TimeoutError")),
    true
  );
});

// This structural guard concerns callback ownership, not HTTP behavior. Actual
// route tests independently verify the callbacks installed in the live registry.
for (const name of ["chat.ts", "chatHelpers.ts"]) {
  test(`${name} gives the process breaker module-owned callbacks`, () => {
    const source = ts.createSourceFile(
      name,
      readFileSync(new URL(`../../src/sse/handlers/${name}`, import.meta.url), "utf8"),
      ts.ScriptTarget.Latest,
      true
    );
    const imports = new Set<string>();
    for (const statement of source.statements) {
      if (
        ts.isImportDeclaration(statement) &&
        ts.isStringLiteral(statement.moduleSpecifier) &&
        statement.moduleSpecifier.text === "./providerBreakerCallbacks" &&
        statement.importClause?.namedBindings &&
        ts.isNamedImports(statement.importClause.namedBindings)
      ) {
        for (const binding of statement.importClause.namedBindings.elements) {
          imports.add(binding.name.text);
        }
      }
    }
    let registrations = 0;
    function visit(node: ts.Node) {
      if (
        ts.isCallExpression(node) &&
        ts.isIdentifier(node.expression) &&
        node.expression.text === "getCircuitBreaker" &&
        node.arguments[1] &&
        ts.isObjectLiteralExpression(node.arguments[1])
      ) {
        registrations += 1;
        for (const [option, callback] of [
          ["isFailure", "breakerCountsAsFailure"],
          ["onStateChange", "logBreakerStateChange"],
        ]) {
          const property = node.arguments[1].properties.find(
            (p) => ts.isPropertyAssignment(p) && p.name.getText(source) === option
          );
          assert.ok(property && ts.isPropertyAssignment(property), `${option} must be installed`);
          assert.ok(
            ts.isIdentifier(property.initializer),
            `${option} must not capture request state`
          );
          assert.equal(property.initializer.text, callback);
          assert.ok(imports.has(callback), `${callback} must be imported from the shared owner`);
        }
      }
      ts.forEachChild(node, visit);
    }
    visit(source);
    assert.equal(registrations, 1, "the actual provider registration must be inspected");
  });
}
