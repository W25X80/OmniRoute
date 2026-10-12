import { nativeMitmEnabled, existsAsync, withNativeMitmCancellation } from "@/mitm/nativeHostIo";
/**
 * GET /api/tools/agent-bridge/state
 * Returns global MITM server status + per-agent detection/status.
 * LOCAL_ONLY: registered in routeGuard.ts
 *
 * Fix #8656: Now returns the full payload shape the UI expects:
 * { serverState, agentStates, bypassPatterns, mappings } while maintaining
 * backward-compat legacy keys { server, agents } for integration tests.
 */
import { getMitmStatus, getAllAgentsStatusAsync, getCachedPassword } from "@/mitm/manager";
import { isSudoPasswordRequiredAsync, checkDNSEntryForAgentAsync } from "@/mitm/dns/dnsConfig";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { createErrorResponse } from "@/lib/api/errorResponse";
import { getAllAgentBridgeStatesAsync } from "@/lib/db/agentBridgeState";
import { getAllBypassPatternsAsync } from "@/lib/db/agentBridgeBypass";
import { getMappingsForAgentAsync } from "@/lib/db/agentBridgeMappings";
import { checkCertInstalled } from "@/mitm/cert/install";
import { resolveActiveCertPathAsync } from "@/mitm/cert/activeCert";
import { resolveMitmDataDir } from "@/mitm/dataDir";
import { ALL_TARGETS } from "@/mitm/targets/index";
import path from "path";
import fs from "fs";

export async function GET(request?: Request): Promise<Response> {
  try {
    return await withNativeMitmCancellation(request?.signal, getState);
  } catch (error) {
    return createErrorResponse({ status: 500, message: sanitizeErrorMessage(error) });
  }
}

async function getState(): Promise<Response> {
  try {
    // Fetch all data in parallel for performance
    const [serverStatus, agents, agentStates, bypassPatterns] = await Promise.all([
      getMitmStatus(),
      getAllAgentsStatusAsync(),
      getAllAgentBridgeStatesAsync(),
      getAllBypassPatternsAsync(),
    ]);

    // Load mappings for all registered agents
    const mappingsEntries = await Promise.all(
      ALL_TARGETS.map(async (t) => {
        const mappings = await getMappingsForAgentAsync(t.id);
        return [
          t.id,
          mappings.map((m) => ({ source: m.source_model, target: m.target_model })),
        ] as const;
      })
    );
    const mappings = Object.fromEntries(mappingsEntries);

    // Compute REAL certTrusted (OS trust store check, not just file exists).
    // #14070: resolve the file the active migration decision actually
    // installs (ca.crt under the root-CA model) instead of always
    // hard-coding the legacy server.crt path.
    const certDir = path.join(resolveMitmDataDir(), "mitm");
    const rootCaEnabled = process.env.MITM_ROOT_CA_ENABLED === "true";
    const { certPath } = await resolveActiveCertPathAsync(certDir, rootCaEnabled);
    const certExists = nativeMitmEnabled() ? await existsAsync(certPath) : fs.existsSync(certPath);
    const certTrusted = certExists ? await checkCertInstalled(certPath) : false;

    // Compute aggregate dnsConfigured: true if ANY agent has hosts spoofed
    // This fixes the Maintenance card "dns-configured" showing ❌ for non-Antigravity agents
    let dnsConfigured = false;
    for (const state of agentStates) {
      if (state.dns_enabled && (await checkDNSEntryForAgentAsync(state.agent_id))) {
        dnsConfigured = true;
        break;
      }
    }

    const isWin = process.platform === "win32";
    const hasCachedPassword = !!getCachedPassword();
    const needsSudoPassword = !isWin && !hasCachedPassword && (await isSudoPasswordRequiredAsync());

    // Build enriched server state
    const enrichedServer = {
      ...serverStatus,
      certExists,
      certTrusted,
      dnsConfigured,
      hasCachedPassword,
      needsSudoPassword,
      isWin,
    };

    return Response.json({
      // Legacy keys for backward compat (integration tests + settings/mitm depend on these)
      server: enrichedServer,
      agents,
      // New keys the UI actually reads (fix #8656)
      serverState: enrichedServer,
      agentStates,
      bypassPatterns: bypassPatterns.map((b) => b.pattern),
      mappings,
    });
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    return createErrorResponse({ status: 500, message: msg });
  }
}
