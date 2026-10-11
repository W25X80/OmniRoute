import type { CodeGraphQueryResult } from "./codegraphKnowledge";

export function formatCodeGraphResult(result: CodeGraphQueryResult): string {
  if (!result.success) {
    if (result.engine === "none") {
      return `CodeGraph not available: ${result.error || "DB not found"}. The app runs without the development code index in production.`;
    }
    return `CodeGraph query error: ${result.error}`;
  }

  const rows = result.data as Record<string, unknown>[];
  if (!rows || rows.length === 0) return "No results found.";

  return (
    JSON.stringify(rows.slice(0, 30), null, 2) +
    (rows.length > 30 ? `\n... and ${rows.length - 30} more` : "")
  );
}
