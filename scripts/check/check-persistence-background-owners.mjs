import { readFile, readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";

/** A ratchet for explicit lifecycle ownership, not a proof of transitive async safety. */
export function inspectBackgroundOwners(text, file = "source.ts") {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
  const names = new Set(["runPersistenceBackgroundWork"]);
  for (const statement of source.statements) {
    if (
      !ts.isImportDeclaration(statement) ||
      !statement.importClause?.namedBindings ||
      !ts.isNamedImports(statement.importClause.namedBindings)
    )
      continue;
    for (const entry of statement.importClause.namedBindings.elements)
      if ((entry.propertyName ?? entry.name).text === "runPersistenceBackgroundWork")
        names.add(entry.name.text);
  }
  let unnamed = 0;
  const invalid = [];
  function visit(node) {
    if (
      ts.isCallExpression(node) &&
      ((ts.isIdentifier(node.expression) && names.has(node.expression.text)) ||
        (ts.isPropertyAccessExpression(node.expression) && names.has(node.expression.name.text)))
    ) {
      const options = node.arguments[1];
      const owner =
        options &&
        ts.isObjectLiteralExpression(options) &&
        options.properties.find(
          (property) =>
            ts.isPropertyAssignment(property) &&
            property.name.getText(source).replaceAll(/["']/g, "") === "owner"
        );
      if (!owner) unnamed++;
      else if (
        !ts.isStringLiteral(owner.initializer) ||
        !["lkgp.clear", "lkgp.record", "artifact.reconcile"].includes(owner.initializer.text)
      )
        invalid.push(source.getLineAndCharacterOfPosition(node.getStart()).line + 1);
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  return { unnamed, invalid };
}

export async function inventoryBackgroundOwners(root) {
  const result = {};
  async function visit(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile() && /\.[cm]?[jt]sx?$/.test(entry.name)) {
        const facts = inspectBackgroundOwners(await readFile(path, "utf8"), path);
        if (facts.unnamed || facts.invalid.length)
          result[relative(root, path).replaceAll("\\", "/")] = facts;
      }
    }
  }
  await visit(join(root, "src"));
  await visit(join(root, "open-sse"));
  return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const root = process.cwd();
  const baseline = JSON.parse(
    await readFile(join(root, "config/quality/persistence-background-owners.json"), "utf8")
  );
  const actual = await inventoryBackgroundOwners(root);
  const violations = [];
  for (const [file, facts] of Object.entries(actual)) {
    if (facts.unnamed > (baseline.unnamedByFile[file] ?? 0))
      violations.push(`${file}: new background entry needs a static lifecycle owner`);
    for (const line of facts.invalid) violations.push(`${file}:${line}: invalid background owner`);
  }
  if (violations.length) {
    console.error(violations.join("\n"));
    process.exitCode = 1;
  } else
    console.log(
      "Background owner ratchet passed; legacy/transitive effects require scenario review."
    );
}
