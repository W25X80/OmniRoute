import type {
  QuotaSchedule,
  ReserveRule,
  ScheduleBudgetUnit,
  ScheduleMode,
} from "@/lib/quota/schedules";
import type { QuotaWindow } from "@/lib/quota/dimensions";
import type { PoolAllocation } from "./quotaPools";
import type { ProviderPlan, QuotaDimension } from "./providerPlans";

export interface QuotaAllocationRow {
  pool_id: string;
  api_key_id: string;
  weight: number;
  cap_value: number | null;
  cap_unit: string | null;
  policy: string;
}

export function rowToQuotaAllocation(row: QuotaAllocationRow): PoolAllocation {
  const allocation: PoolAllocation = {
    apiKeyId: row.api_key_id,
    weight: row.weight,
    policy: ["hard", "soft", "burst"].includes(row.policy)
      ? (row.policy as PoolAllocation["policy"])
      : "hard",
  };
  if (row.cap_value != null) allocation.capValue = row.cap_value;
  if (row.cap_unit != null) allocation.capUnit = row.cap_unit as PoolAllocation["capUnit"];
  return allocation;
}

export interface QuotaPlanRow {
  connection_id: string;
  provider: string;
  dimensions_json: string;
  source: string;
  updated_at: string;
}

export function rowToQuotaPlan(row: QuotaPlanRow): ProviderPlan {
  let dimensions: QuotaDimension[] = [];
  try {
    dimensions = JSON.parse(row.dimensions_json) as QuotaDimension[];
  } catch {
    dimensions = [];
  }
  return {
    connectionId: row.connection_id,
    provider: row.provider,
    dimensions,
    source: row.source as ProviderPlan["source"],
  };
}

export interface QuotaScheduleRow {
  id: string;
  pool_id: string;
  label: string | null;
  days: string;
  start_minute: number;
  end_minute: number;
  timezone: string | null;
  mode: string;
  reserves: string | null;
  budget_value: number | null;
  budget_unit: string | null;
  budget_window: string;
  priority: number;
  enabled: number;
}

const VALID_WINDOWS = new Set(["hourly", "5h", "daily", "weekly", "monthly"]);
const VALID_RESERVE_WINDOWS = new Set([...VALID_WINDOWS, "any"]);
const VALID_BUDGET_UNITS = new Set(["requests", "tokens", "usd"]);

export function parseDays(csv: string): number[] {
  const days = new Set<number>();
  for (const part of csv.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const day = Number(trimmed);
    if (Number.isInteger(day) && day >= 0 && day <= 6) days.add(day);
  }
  return [...days].sort((a, b) => a - b);
}

export function parseReserves(raw: string | null): ReserveRule[] {
  if (!raw) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const rules: ReserveRule[] = [];
  for (const entry of parsed) {
    if (typeof entry !== "object" || entry === null) continue;
    const { window, percent } = entry as { window?: unknown; percent?: unknown };
    if (typeof window !== "string" || !VALID_RESERVE_WINDOWS.has(window)) continue;
    if (typeof percent !== "number" || !Number.isFinite(percent)) continue;
    if (percent < 0 || percent > 100) continue;
    rules.push({ window: window as ReserveRule["window"], percent });
  }
  return rules;
}

export function serializeQuotaDays(days: number[]): string {
  return [...new Set(days.filter((day) => Number.isInteger(day) && day >= 0 && day <= 6))]
    .sort((a, b) => a - b)
    .join(",");
}

export function serializeQuotaReserves(reserves: ReserveRule[] | undefined): string | null {
  return reserves?.length ? JSON.stringify(reserves) : null;
}

export function rowToQuotaSchedule(row: QuotaScheduleRow): QuotaSchedule {
  const schedule: QuotaSchedule = {
    id: row.id,
    poolId: row.pool_id,
    days: parseDays(row.days),
    startMinute: row.start_minute,
    endMinute: row.end_minute,
    timezone: row.timezone,
    mode: ["allow", "block"].includes(row.mode) ? (row.mode as ScheduleMode) : "block",
    reserves: parseReserves(row.reserves),
    budgetWindow: VALID_WINDOWS.has(row.budget_window)
      ? (row.budget_window as QuotaWindow)
      : "daily",
    priority: row.priority,
    enabled: row.enabled === 1,
  };
  if (row.label != null) schedule.label = row.label;
  if (
    row.budget_value != null &&
    row.budget_unit != null &&
    VALID_BUDGET_UNITS.has(row.budget_unit)
  ) {
    schedule.budgetValue = row.budget_value;
    schedule.budgetUnit = row.budget_unit as ScheduleBudgetUnit;
  }
  return schedule;
}

export function quotaAllocationFingerprint(allocations: PoolAllocation[] = []): string {
  return JSON.stringify(
    allocations
      .map((allocation) => ({
        apiKeyId: allocation.apiKeyId,
        weight: allocation.weight,
        capValue: allocation.capValue ?? null,
        capUnit: allocation.capUnit ?? null,
        policy: allocation.policy,
      }))
      .sort((left, right) => left.apiKeyId.localeCompare(right.apiKeyId))
  );
}
