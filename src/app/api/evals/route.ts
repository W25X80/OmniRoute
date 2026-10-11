import { randomUUID } from "node:crypto";
import { NextResponse } from "next/server";
import { getEvalScorecardAsync, listEvalRunsAsync } from "@/lib/db/evals";
import { getApiKeys } from "@/lib/db/apiKeys";
import { listSuitesAsync, runSuiteAsync, createScorecard } from "@/lib/evals/evalRunner";
import { buildEvalTargetOptions, runEvalSuiteAgainstTarget } from "@/lib/evals/runtime";
import { requireManagementAuth } from "@/lib/api/requireManagementAuth";
import { evalRunSuiteSchema } from "@/shared/validation/schemas";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";

export async function GET(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;

  try {
    const suites = await listSuitesAsync();
    const recentRuns = await listEvalRunsAsync({ limit: 20 });
    const scorecard = await getEvalScorecardAsync({ limit: 50 });
    const targets = await buildEvalTargetOptions();
    const apiKeys = await getApiKeys();

    return NextResponse.json({
      suites,
      recentRuns,
      scorecard,
      targets,
      apiKeys: apiKeys.map((key) => ({
        id: key.id,
        name: key.name,
        isActive: key.isActive !== false,
      })),
    });
  } catch (error: unknown) {
    return NextResponse.json({ error: sanitizeErrorMessage(error) }, { status: 500 });
  }
}

export async function POST(request: Request) {
  const authError = await requireManagementAuth(request);
  if (authError) return authError;

  let rawBody;
  try {
    rawBody = await request.json();
  } catch {
    return NextResponse.json(
      {
        error: {
          message: "Invalid request",
          details: [{ field: "body", message: "Invalid JSON body" }],
        },
      },
      { status: 400 }
    );
  }

  try {
    const validation = validateBody(evalRunSuiteSchema, rawBody);
    if (isValidationFailure(validation)) {
      return NextResponse.json({ error: validation.error }, { status: 400 });
    }

    const { suiteId, outputs, target, compareTarget, apiKeyId } = validation.data;

    if (outputs && Object.keys(outputs).length > 0) {
      const result = await runSuiteAsync(suiteId, outputs);
      return NextResponse.json(result);
    }

    const targetsToRun = [target || { type: "suite-default" as const, id: null }];
    if (compareTarget) {
      targetsToRun.push(compareTarget);
    }

    const runGroupId = targetsToRun.length > 1 ? randomUUID() : null;
    const runs = await Promise.all(
      targetsToRun.map((entry) =>
        runEvalSuiteAgainstTarget({
          suiteId,
          target: entry,
          apiKeyId,
          runGroupId,
        })
      )
    );

    const scorecard =
      runs.length > 0
        ? createScorecard(
            runs.map((run) => ({
              suiteId: `${run.suiteId}:${run.target.key}`,
              suiteName: `${run.suiteName} · ${run.target.label}`,
              results: run.results,
              summary: run.summary,
            }))
          )
        : null;

    return NextResponse.json({
      suiteId,
      runGroupId,
      runs,
      scorecard,
      recentRuns: await listEvalRunsAsync({ limit: 20 }),
      historyScorecard: await getEvalScorecardAsync({ limit: 50 }),
    });
  } catch (error: unknown) {
    return NextResponse.json({ error: sanitizeErrorMessage(error) }, { status: 500 });
  }
}
