import fs from "node:fs";
import { registerHooks } from "node:module";
import path from "node:path";
import ts from "typescript";

// Opt-in causal runs load the exact saved owner bytes at their normal module
// URLs. Ordinary runs retain every production import and statement unchanged.
const preimageRoot = process.env.CLAUDE_TOOL_LIFECYCLE_PREIMAGE;
if (preimageRoot) {
  const targets = new Map(
    [
      "open-sse/translator/response/openai-to-claude.ts",
      "open-sse/handlers/responseTranslator.ts",
    ].map((relative) => [new URL(`../../../${relative}`, import.meta.url).href, relative])
  );
  const hooks = registerHooks({
    load(url, context, nextLoad) {
      const relative = targets.get(url.split("?")[0]);
      if (!relative) return nextLoad(url, context);
      return {
        format: "module",
        shortCircuit: true,
        source: ts.transpileModule(fs.readFileSync(path.join(preimageRoot, relative), "utf8"), {
          compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
        }).outputText,
      };
    },
  });
  process.on("exit", () => hooks.deregister());
}
