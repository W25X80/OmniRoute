/**
 * GET /api/agent-skills/coverage
 *
 * Returns SkillCoverage: how many skills have a SKILL.md on filesystem vs catalog total.
 *
 * Response: SkillCoverage
 */
import { NextResponse } from "next/server";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";

import { buildErrorBody } from "@omniroute/open-sse/utils/error.ts";
import { computeCoverage, computeCoverageAsync } from "@/lib/agentSkills/catalog";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const coverage =
      parseDatabaseBackend() === "postgres" ? await computeCoverageAsync() : computeCoverage();
    return NextResponse.json(coverage);
  } catch (error) {
    console.error("[API] GET /api/agent-skills/coverage error:", error);
    return NextResponse.json(buildErrorBody(500, "Failed to compute skill coverage"), {
      status: 500,
    });
  }
}
