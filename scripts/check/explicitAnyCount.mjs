import ts from "typescript";

export function countExplicitAny(content, fileName = "source.ts") {
  const source = ts.createSourceFile(
    fileName,
    content,
    ts.ScriptTarget.Latest,
    true,
    fileName.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS
  );
  if (source.parseDiagnostics.length > 0) {
    throw new Error(`Cannot count explicit any types in invalid syntax: ${fileName}`);
  }
  let count = 0;
  const visit = (node) => {
    if (node.kind === ts.SyntaxKind.AnyKeyword) count++;
    ts.forEachChild(node, visit);
  };
  visit(source);
  return count;
}
