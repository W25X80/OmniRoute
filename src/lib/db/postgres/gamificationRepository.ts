import type {
  BadgeDefinition,
  CommunityServer,
  InviteToken,
  LeaderboardRow,
  TokenLedgerEntry,
  UserBadge,
  UserLevelRow,
} from "../gamification";
import { rowToCamel } from "../caseMapping";
import { calculateLevel } from "../../gamification/xp";
import type { PostgresDatabase, PostgresTransaction } from "./database";

const NOW = "to_char(statement_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')";
const TODAY = "to_char(statement_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD')";
const HOUR_START =
  "to_char((statement_timestamp()-interval '1 hour') AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')";
type Row = Record<string, unknown>;
const numeric = (value: number): number | null => (Number.isNaN(value) ? null : value);
function camel<T>(row: Row): T {
  const { insertion_order: _order, ...record } = row;
  return rowToCamel(record) as T;
}
function weight(metadata?: string): number {
  let value: unknown;
  try {
    value = metadata == null ? null : JSON.parse(metadata)?.amount;
  } catch {
    throw new Error("Gamification metadata is invalid JSON");
  }
  if (value == null) return 1;
  let number: number;
  if (typeof value === "number") number = value;
  else if (typeof value === "boolean") number = value ? 1 : 0;
  else if (typeof value === "string") number = Number(value.match(/^\s*[+-]?\d+/)?.[0] ?? 0);
  else number = 0;
  return Math.max(-9223372036854775808, Math.min(9223372036854775807, Math.trunc(number)));
}
async function award(
  tx: PostgresTransaction,
  key: string,
  action: string,
  amount: number,
  metadata?: string
): Promise<void> {
  const count = weight(metadata);
  await tx.query(
    "INSERT INTO omniroute.xp_audit_log(api_key_id,action,xp_earned,metadata) VALUES($1,$2,$3,$4)",
    [key, action, numeric(amount), metadata ?? null]
  );
  await tx.query(
    `INSERT INTO omniroute.xp_action_counts(api_key_id,action,count,updated_at) VALUES($1,$2,$3,${NOW})
    ON CONFLICT(api_key_id,action) DO UPDATE SET count=xp_action_counts.count+EXCLUDED.count,updated_at=EXCLUDED.updated_at`,
    [key, action, count]
  );
  await tx.query(
    `INSERT INTO omniroute.user_levels(api_key_id,total_xp,current_level,updated_at) VALUES($1,$2,$3,${NOW})
    ON CONFLICT(api_key_id) DO UPDATE SET total_xp=user_levels.total_xp+EXCLUDED.total_xp,updated_at=EXCLUDED.updated_at`,
    [key, numeric(amount), calculateLevel(amount)]
  );
}
async function balance(tx: PostgresTransaction, key: string): Promise<number> {
  const { rows } = await tx.query<{ balance: number }>(
    `SELECT COALESCE(SUM(CASE WHEN to_api_key_id=$1 THEN amount ELSE 0 END),0)-
    COALESCE(SUM(CASE WHEN from_api_key_id=$1 THEN amount ELSE 0 END),0) AS balance FROM omniroute.token_ledger WHERE from_api_key_id=$1 OR to_api_key_id=$1`,
    [key]
  );
  return rows[0].balance;
}

/** Native gamification durable owner. All compound writes hold the same transaction lock. */
export function createPostgresGamificationRepository(database: PostgresDatabase) {
  async function rows<T extends Row = Row>(text: string, values: unknown[] = []): Promise<T[]> {
    return (await database.transaction((tx) => tx.query<T>(text, values))).rows;
  }
  async function mutation<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    return database.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(18765,17)");
      return body(tx);
    });
  }
  async function write(text: string, values: unknown[] = []): Promise<void> {
    await mutation((tx) => tx.query(text, values));
  }
  const badgesSql = `SELECT ub.api_key_id,ub.badge_id,ub.unlocked_at,bd.name AS "badgeName",bd.description AS "badgeDescription",bd.icon AS "badgeIcon",bd.category AS "badgeCategory",bd.rarity AS "badgeRarity" FROM omniroute.user_badges ub JOIN omniroute.badge_definitions bd ON bd.id=ub.badge_id`;
  return {
    updateScore: (key: string, scope: string, points: number) =>
      write(
        `INSERT INTO omniroute.leaderboard(api_key_id,scope,score,updated_at) VALUES($1,$2,$3,${NOW}) ON CONFLICT(api_key_id,scope) DO UPDATE SET score=leaderboard.score+EXCLUDED.score,updated_at=EXCLUDED.updated_at`,
        [key, scope, numeric(points)]
      ),
    async getRank(key: string, scope: string): Promise<number> {
      const result = await rows<{ rank: string }>(
        `SELECT count(*)+1 AS rank FROM omniroute.leaderboard WHERE scope=$2 AND score>(SELECT score FROM omniroute.leaderboard WHERE api_key_id=$1 AND scope=$2) HAVING EXISTS(SELECT 1 FROM omniroute.leaderboard WHERE api_key_id=$1 AND scope=$2)`,
        [key, scope]
      );
      return Number(result[0]?.rank ?? 0);
    },
    async getTopN(scope: string, limit: number, offset = 0): Promise<LeaderboardRow[]> {
      const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.trunc(limit), 0), 200) : 0;
      const safeOffset = Number.isFinite(offset) ? Math.max(Math.trunc(offset), 0) : 0;
      return (
        await rows(
          `SELECT api_key_id,scope,score,updated_at FROM omniroute.leaderboard WHERE scope=$1 ORDER BY score DESC,api_key_id ASC LIMIT $2 OFFSET $3`,
          [scope, safeLimit, safeOffset]
        )
      ).map((row) => camel<LeaderboardRow>(row));
    },
    addXp: (key: string, action: string, amount: number, metadata?: string) =>
      mutation((tx) => award(tx, key, action, amount, metadata)),
    async awardXpOncePerDay(
      key: string,
      action: string,
      amount: number,
      metadata?: string
    ): Promise<boolean> {
      return mutation(async (tx) => {
        const existing = await tx.query(
          `SELECT 1 FROM omniroute.xp_audit_log WHERE api_key_id=$1 AND action=$2 AND created_at>=${TODAY} LIMIT 1`,
          [key, action]
        );
        if (existing.rows.length) return false;
        await award(tx, key, action, amount, metadata);
        return true;
      });
    },
    async synchronizeLevel(key: string): Promise<{ oldLevel: number; newLevel: number } | null> {
      return mutation(async (tx) => {
        const { rows } = await tx.query<{
          total_xp: number;
          current_level: number;
        }>(
          "SELECT total_xp,current_level FROM omniroute.user_levels WHERE api_key_id=$1 FOR UPDATE",
          [key]
        );
        if (!rows[0]) return null;
        const newLevel = calculateLevel(rows[0].total_xp);
        const oldLevel = rows[0].current_level;
        if (newLevel === oldLevel) return null;
        await tx.query(
          `UPDATE omniroute.user_levels SET current_level=$2,updated_at=${NOW} WHERE api_key_id=$1`,
          [key, newLevel]
        );
        return { oldLevel, newLevel };
      });
    },
    async getXp(key: string): Promise<UserLevelRow | null> {
      const result = await rows(
        "SELECT api_key_id,total_xp,current_level,updated_at FROM omniroute.user_levels WHERE api_key_id=$1",
        [key]
      );
      return result[0] ? camel<UserLevelRow>(result[0]) : null;
    },
    updateLevel: (key: string, level: number) =>
      write(
        `INSERT INTO omniroute.user_levels(api_key_id,total_xp,current_level,updated_at) VALUES($1,0,$2,${NOW}) ON CONFLICT(api_key_id) DO UPDATE SET current_level=EXCLUDED.current_level,updated_at=EXCLUDED.updated_at`,
        [key, numeric(level)]
      ),
    async unlockBadge(key: string, badge: string): Promise<boolean> {
      const result = await mutation((tx) =>
        tx.query(
          "INSERT INTO omniroute.user_badges(api_key_id,badge_id) VALUES($1,$2) ON CONFLICT(api_key_id,badge_id) DO NOTHING RETURNING badge_id",
          [key, badge]
        )
      );
      return result.rows.length > 0;
    },
    async hasBadge(key: string, badge: string): Promise<boolean> {
      return (
        (
          await rows(
            "SELECT 1 FROM omniroute.user_badges WHERE api_key_id=$1 AND badge_id=$2 LIMIT 1",
            [key, badge]
          )
        ).length > 0
      );
    },
    async hasXpActionToday(key: string, action: string): Promise<boolean> {
      return (
        (
          await rows(
            `SELECT 1 FROM omniroute.xp_audit_log WHERE api_key_id=$1 AND action=$2 AND created_at>=${TODAY} LIMIT 1`,
            [key, action]
          )
        ).length > 0
      );
    },
    async getBadges(key: string): Promise<UserBadge[]> {
      return (await rows(`${badgesSql} WHERE ub.api_key_id=$1 ORDER BY ub.badge_id`, [key])).map(
        (row) => camel<UserBadge>(row)
      );
    },
    async getBadgeDefinitions(category?: string): Promise<BadgeDefinition[]> {
      return (
        await rows(
          `SELECT * FROM omniroute.badge_definitions${category ? " WHERE category=$1" : ""} ORDER BY insertion_order`,
          category ? [category] : []
        )
      ).map((row) => camel<BadgeDefinition>(row));
    },
    async getAggregateXp(): Promise<UserLevelRow> {
      const result = await rows<{
        total_xp: number;
        updated_at: string | null;
      }>(
        "SELECT COALESCE(SUM(total_xp),0) AS total_xp,MAX(updated_at) AS updated_at FROM omniroute.user_levels"
      );
      const totalXp = result[0].total_xp;
      return {
        apiKeyId: "*",
        totalXp,
        currentLevel: calculateLevel(totalXp),
        updatedAt: result[0].updated_at ?? "",
      };
    },
    async getAllEarnedBadges(): Promise<UserBadge[]> {
      return (
        await rows(
          `SELECT '*' AS api_key_id,ub.badge_id,MIN(ub.unlocked_at) AS unlocked_at,bd.name AS "badgeName",bd.description AS "badgeDescription",bd.icon AS "badgeIcon",bd.category AS "badgeCategory",bd.rarity AS "badgeRarity" FROM omniroute.user_badges ub JOIN omniroute.badge_definitions bd ON bd.id=ub.badge_id GROUP BY ub.badge_id,bd.id ORDER BY ub.badge_id`
        )
      ).map((row) => camel<UserBadge>(row));
    },
    async transferTokens(
      from: string,
      to: string,
      amount: number,
      reason: string,
      idempotencyKey: string
    ): Promise<{ success: boolean; error?: string }> {
      return mutation(async (tx) => {
        const existing = await tx.query(
          "SELECT id FROM omniroute.token_ledger WHERE idempotency_key=$1",
          [idempotencyKey]
        );
        if (existing.rows.length) return { success: true };
        if ((await balance(tx, from)) < amount)
          return { success: false, error: "insufficient_balance" };
        await tx.query(
          "INSERT INTO omniroute.token_ledger(from_api_key_id,to_api_key_id,amount,reason,idempotency_key) VALUES($1,$2,$3,$4,$5)",
          [from, to, numeric(amount), reason, idempotencyKey]
        );
        return { success: true };
      });
    },
    getBalance: (key: string) => database.transaction((tx) => balance(tx, key)),
    async getHistory(key: string, limit: number): Promise<TokenLedgerEntry[]> {
      const result = await rows(
        "SELECT * FROM omniroute.token_ledger WHERE from_api_key_id=$1 OR to_api_key_id=$1 ORDER BY created_at DESC,id ASC LIMIT $2",
        [key, limit < 0 ? null : limit]
      );
      return result.map((row) => ({
        ...camel<TokenLedgerEntry>(row),
        id: Number(row.id),
      }));
    },
    createInviteToken: (
      id: string,
      code: string,
      hash: string,
      createdBy: string,
      url?: string,
      maxUses?: number
    ) =>
      write(
        "INSERT INTO omniroute.invite_tokens(id,code,token_hash,created_by,server_url,max_uses) VALUES($1,$2,$3,$4,$5,$6)",
        [id, code, hash, createdBy, url ?? null, numeric(maxUses ?? 1)]
      ),
    async getInviteByCode(code: string): Promise<InviteToken | null> {
      const result = await rows("SELECT * FROM omniroute.invite_tokens WHERE code=$1", [code]);
      return result[0] ? camel<InviteToken>(result[0]) : null;
    },
    async redeemInvite(code: string, usedBy: string): Promise<boolean> {
      const result = await mutation((tx) =>
        tx.query(
          `UPDATE omniroute.invite_tokens SET use_count=use_count+1,used_by=$2 WHERE code=$1 AND revoked_at IS NULL AND use_count<max_uses AND (expires_at IS NULL OR expires_at>${NOW}) RETURNING id`,
          [code, usedBy]
        )
      );
      return result.rows.length > 0;
    },
    revokeInvite: (id: string) =>
      write(`UPDATE omniroute.invite_tokens SET revoked_at=${NOW} WHERE id=$1`, [id]),
    async getConnectedServerByKeyHash(hash: string): Promise<{ id: string } | undefined> {
      return (
        await rows<{ id: string }>(
          "SELECT id FROM omniroute.community_servers WHERE api_key_hash=$1 AND status='connected' ORDER BY insertion_order LIMIT 1",
          [hash]
        )
      )[0];
    },
    connectServer: (id: string, name: string, url: string, hash: string) =>
      write(
        `INSERT INTO omniroute.community_servers(id,name,url,api_key_hash) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,url=EXCLUDED.url,api_key_hash=EXCLUDED.api_key_hash,connected_at=EXCLUDED.connected_at,last_sync_at=NULL,status='connected',error_message=NULL,insertion_order=DEFAULT`,
        [id, name, url, hash]
      ),
    disconnectServer: (id: string) =>
      write("UPDATE omniroute.community_servers SET status='disconnected' WHERE id=$1", [id]),
    async listServers(): Promise<Omit<CommunityServer, "apiKeyHash">[]> {
      return (
        await rows(
          "SELECT id,name,url,connected_at,last_sync_at,status,error_message FROM omniroute.community_servers ORDER BY insertion_order"
        )
      ).map((row) => camel<Omit<CommunityServer, "apiKeyHash">>(row));
    },
    async getLeaderboardNeighbors(
      key: string,
      scope: string,
      radius = 5
    ): Promise<{
      above: Array<{ apiKeyId: string; score: number }>;
      below: Array<{ apiKeyId: string; score: number }>;
    }> {
      // Anchor and neighbors share one statement snapshot. A concurrent score update
      // cannot turn this user into their own neighbor or mix the two sides' epochs.
      const neighbors = await rows<{
        side: "above" | "below";
        api_key_id: string;
        score: number;
      }>(
        `WITH current_score AS (
          SELECT score FROM omniroute.leaderboard WHERE api_key_id=$1 AND scope=$2
        ), above AS (
          SELECT api_key_id,score FROM omniroute.leaderboard
          WHERE scope=$2 AND score>(SELECT score FROM current_score)
          ORDER BY score ASC,api_key_id DESC LIMIT $3
        ), below AS (
          SELECT api_key_id,score FROM omniroute.leaderboard
          WHERE scope=$2 AND score<(SELECT score FROM current_score)
          ORDER BY score DESC,api_key_id ASC LIMIT $3
        )
        SELECT 'above' AS side,api_key_id,score FROM above
        UNION ALL SELECT 'below' AS side,api_key_id,score FROM below
        ORDER BY side ASC,score DESC,api_key_id ASC`,
        [key, scope, radius < 0 ? null : radius]
      );
      const project = (side: "above" | "below") =>
        neighbors
          .filter((row) => row.side === side)
          .map((row) => ({ apiKeyId: row.api_key_id, score: row.score }));
      return { above: project("above"), below: project("below") };
    },
    rotateLeaderboardScope: (scope: "weekly" | "monthly") =>
      mutation(async (tx) => {
        const suffix =
          scope === "weekly"
            ? `week_${new Date().toISOString().slice(0, 10)}`
            : `month_${new Date().toISOString().slice(0, 7)}`;
        const existing = await tx.query(
          "SELECT 1 FROM omniroute.leaderboard WHERE scope=$1 LIMIT 1",
          [suffix]
        );
        if (existing.rows.length) return;
        await tx.query(
          "INSERT INTO omniroute.leaderboard(api_key_id,scope,score,updated_at) SELECT api_key_id,$1,score,updated_at FROM omniroute.leaderboard WHERE scope=$2 ON CONFLICT(api_key_id,scope) DO NOTHING",
          [suffix, scope]
        );
        await tx.query("DELETE FROM omniroute.leaderboard WHERE scope=$1", [scope]);
      }),
    async getActionCount(key: string, action: string): Promise<number> {
      return Number(
        (
          await rows<{ count: number }>(
            "SELECT count FROM omniroute.xp_action_counts WHERE api_key_id=$1 AND action=$2",
            [key, action]
          )
        )[0]?.count ?? 0
      );
    },
    async getBadgeDetails(id: string): Promise<
      | {
          name: string;
          description: string | null;
          icon: string | null;
          rarity: string;
        }
      | undefined
    > {
      return (
        await rows<{
          name: string;
          description: string | null;
          icon: string | null;
          rarity: string;
        }>("SELECT name,description,icon,rarity FROM omniroute.badge_definitions WHERE id=$1", [id])
      )[0];
    },
    async getFirstLogDate(key: string): Promise<string | null> {
      return (
        await rows<{ first_at: string | null }>(
          "SELECT MIN(created_at) AS first_at FROM omniroute.xp_audit_log WHERE api_key_id=$1",
          [key]
        )
      )[0].first_at;
    },
    async getUniqueCount(key: string, field: string): Promise<number> {
      const result = await rows<{ total: string }>(
        String.raw`SELECT COUNT(DISTINCT CASE json_typeof(extracted)
         WHEN 'null' THEN NULL
         WHEN 'boolean' THEN CASE extracted::text WHEN 'true' THEN 'number:1' ELSE 'number:0' END
         WHEN 'number' THEN 'number:'||trim_scale((extracted::text)::numeric)::text
         WHEN 'string' THEN 'string:'||(extracted#>>'{}')
         ELSE 'string:'||(SELECT string_agg(parts[1],'' ORDER BY ordinal)
           FROM regexp_matches(extracted::text,$pattern$("(?:[^"\\]|\\.)*"|[^[:space:]"]+)$pattern$,'g') WITH ORDINALITY AS tokens(parts,ordinal))
         END) AS total FROM (SELECT json_extract_path(metadata::json,VARIADIC string_to_array($2,'.')) AS extracted FROM omniroute.xp_audit_log WHERE api_key_id=$1 AND metadata IS NOT NULL) entries`,
        [key, field]
      );
      return Number(result[0].total);
    },
    seedBadges: (badges: readonly Omit<BadgeDefinition, "createdAt">[]) =>
      mutation(async (tx) => {
        for (const badge of badges)
          await tx.query(
            "INSERT INTO omniroute.badge_definitions(id,name,description,icon,category,rarity,criteria,hidden) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO NOTHING",
            [
              badge.id,
              badge.name,
              badge.description,
              badge.icon,
              badge.category,
              badge.rarity,
              badge.criteria,
              badge.hidden,
            ]
          );
      }),
    async listInvites(key: string) {
      return (
        await rows(
          "SELECT id,code,server_url,max_uses,use_count,expires_at,revoked_at,created_at FROM omniroute.invite_tokens WHERE created_by=$1 ORDER BY created_at DESC,insertion_order ASC",
          [key]
        )
      ).map((row) =>
        camel<{
          id: string;
          code: string;
          serverUrl: string | null;
          maxUses: number;
          useCount: number;
          expiresAt: string | null;
          revokedAt: string | null;
          createdAt: string;
        }>(row)
      );
    },
    async getServerEndpoint(
      id: string,
      connectedOnly = true
    ): Promise<{ url: string; apiKeyHash: string } | undefined> {
      const result = await rows(
        `SELECT url,api_key_hash FROM omniroute.community_servers WHERE id=$1${connectedOnly ? " AND status='connected'" : ""}`,
        [id]
      );
      return result[0] ? camel<{ url: string; apiKeyHash: string }>(result[0]) : undefined;
    },
    applyRemoteScores: (id: string, entries: readonly { apiKeyId: string; score: number }[]) =>
      mutation(async (tx) => {
        for (const entry of entries)
          await tx.query(
            `INSERT INTO omniroute.leaderboard(api_key_id,scope,score,updated_at) VALUES($1,'global',$2,${NOW}) ON CONFLICT(api_key_id,scope) DO UPDATE SET score=EXCLUDED.score,updated_at=EXCLUDED.updated_at`,
            [entry.apiKeyId, numeric(entry.score)]
          );
        await tx.query(
          `UPDATE omniroute.community_servers SET last_sync_at=${NOW},error_message=NULL WHERE id=$1`,
          [id]
        );
      }),
    setServerSyncError: (id: string, message: string) =>
      write("UPDATE omniroute.community_servers SET status='error',error_message=$2 WHERE id=$1", [
        id,
        message,
      ]),
    async getRecentXp(key: string, windowMs: number): Promise<number> {
      const result = await rows<{ total: number }>(
        `SELECT COALESCE(SUM(xp_earned),0) AS total FROM omniroute.xp_audit_log WHERE api_key_id=$1 AND created_at>to_char((statement_timestamp()-make_interval(secs=>$2)) AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
        [key, Math.ceil(windowMs / 1000)]
      );
      return result[0].total;
    },
    async getHighXpKeys(): Promise<Array<{ api_key_id: string; hourly_total: number }>> {
      return rows<{ api_key_id: string; hourly_total: number }>(
        `SELECT api_key_id,SUM(xp_earned) AS hourly_total FROM omniroute.xp_audit_log WHERE created_at>${HOUR_START} GROUP BY api_key_id HAVING SUM(xp_earned)>1000 ORDER BY api_key_id`
      );
    },
    async getXpDistribution(key: string): Promise<{
      total: number;
      mean: number | null;
      variance: number | null;
    }> {
      const result = await rows<{
        total: number;
        mean: number | null;
        variance: number | null;
      }>(
        `WITH totals AS (SELECT api_key_id,SUM(xp_earned) AS total FROM omniroute.xp_audit_log WHERE created_at>${HOUR_START} GROUP BY api_key_id) SELECT COALESCE((SELECT total FROM totals WHERE api_key_id=$1),0) AS total,AVG(total) AS mean,CASE WHEN AVG(total)=0 THEN 1 ELSE AVG(total*total)-AVG(total)*AVG(total) END AS variance FROM totals`,
        [key]
      );
      return result[0];
    },
    async readStreak(key: string): Promise<string | null> {
      return (
        (
          await rows<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='gamification:streaks' AND key=$1",
            [key]
          )
        )[0]?.value ?? null
      );
    },
    listStreaks: () =>
      rows<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='gamification:streaks' ORDER BY key"
      ),
    async advanceStreak<T>(
      key: string,
      derive: (raw: string | null) => { value?: string; result: T }
    ): Promise<T> {
      return mutation(async (tx) => {
        const row = await tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='gamification:streaks' AND key=$1 FOR UPDATE",
          [key]
        );
        const next = derive(row.rows[0]?.value ?? null);
        if (next.value !== undefined)
          await tx.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('gamification:streaks',$1,$2) ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value",
            [key, next.value]
          );
        return next.result;
      });
    },
  };
}
