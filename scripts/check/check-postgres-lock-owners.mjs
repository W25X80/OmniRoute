import { readdir, readFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";

const ownerModule = "src/lib/db/postgres/locks.ts";

/** Detect literal advisory SQL outside its semantic owner registry, excluding comments. */
export function hasAdvisorySql(source) {
  const scanner = ts.createScanner(
    ts.ScriptTarget.Latest,
    true,
    ts.LanguageVariant.Standard,
    source
  );
  for (let kind = scanner.scan(); kind !== ts.SyntaxKind.EndOfFileToken; kind = scanner.scan()) {
    if (
      [
        ts.SyntaxKind.StringLiteral,
        ts.SyntaxKind.NoSubstitutionTemplateLiteral,
        ts.SyntaxKind.TemplateHead,
        ts.SyntaxKind.TemplateMiddle,
        ts.SyntaxKind.TemplateTail,
      ].includes(kind) &&
      /\bpg_(?:try_)?advisory_/i.test(scanner.getTokenText())
    )
      return true;
  }
  return false;
}

export async function checkPostgresLockOwners(root) {
  const violations = [];
  async function visit(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile() && /\.(?:[cm]?[jt]sx?)$/.test(entry.name)) {
        const local = relative(root, path).replaceAll("\\", "/");
        if (local !== ownerModule && hasAdvisorySql(await readFile(path, "utf8")))
          violations.push(local);
      }
    }
  }
  await visit(join(root, "src"));
  await visit(join(root, "open-sse"));
  return violations.sort();
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const violations = await checkPostgresLockOwners(process.cwd());
  if (violations.length) {
    console.error(`Advisory SQL must use ${ownerModule}:\n${violations.join("\n")}`);
    process.exitCode = 1;
  } else console.log("PostgreSQL advisory SQL uses the semantic lock registry.");
}
