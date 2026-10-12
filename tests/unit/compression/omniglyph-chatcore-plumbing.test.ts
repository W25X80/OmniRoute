import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import { test } from "node:test";
import ts from "typescript";
import { resolveOmniGlyphTransport } from "../../../open-sse/services/compression/imageTransportPolicy.ts";

test("chatCore uses the measured Anthropic byte-preserving transport policy", () => {
  const chatCore = readFileSync("open-sse/handlers/chatCore.ts", "utf8");

  assert.match(chatCore, /resolveOmniGlyphTransport\(provider\)/);
  const parsed = ts.createSourceFile("chatCore.ts", chatCore, ts.ScriptTarget.Latest, true);
  let translation: ts.BinaryExpression | undefined;
  let postCompression: ts.IfStatement | undefined;
  const walk = (node: ts.Node) => {
    if (
      ts.isBinaryExpression(node) &&
      node.left.getText(parsed) === "translatedBody" &&
      node.operatorToken.kind === ts.SyntaxKind.EqualsToken &&
      ts.isAwaitExpression(node.right) &&
      ts.isCallExpression(node.right.expression) &&
      node.right.expression.expression.getText(parsed) === "translateRequestAsync"
    )
      translation = node;
    if (
      ts.isIfStatement(node) &&
      node.expression.getText(parsed).startsWith("runPostTranslationCompression &&")
    )
      postCompression = node;
    ts.forEachChild(node, walk);
  };
  walk(parsed);
  assert.ok(translation, "chatCore must await the actual translatedBody translation owner");
  assert.ok(postCompression, "the guarded target-wire stage must remain present");
  assert.ok(
    postCompression.pos > translation.end,
    "target-wire compression must follow translation"
  );
  assert.match(
    postCompression.thenStatement.getText(parsed),
    /await runPostTranslationCompression\(postInput\)/
  );
  assert.match(chatCore, /compressionStage: "post-translation"/);
  assert.deepEqual(resolveOmniGlyphTransport("anthropic"), {
    providerTransport: "direct",
    imageTransportFidelity: "byte-preserving",
  });
  assert.deepEqual(resolveOmniGlyphTransport("claude"), {
    providerTransport: "direct",
    imageTransportFidelity: "byte-preserving",
  });
  assert.deepEqual(resolveOmniGlyphTransport("openai"), {
    providerTransport: "aggregator",
    imageTransportFidelity: "unknown",
  });
});
