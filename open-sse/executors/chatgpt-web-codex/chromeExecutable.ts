import { access } from "node:fs/promises";

/** The existing executor candidate priority, shared by synchronous and native checks. */
export function chromeExecutableCandidates(explicit?: string): Array<string | undefined> {
  return [
    explicit,
    process.env.CHATGPT_WEB_CODEX_CHROME_PATH,
    process.env.CHROME_PATH,
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ];
}

/** No positive cache: each native configuration check qualifies its current ordered paths. */
export async function detectChromeExecutableAsync(explicit?: string): Promise<string | undefined> {
  for (const candidate of chromeExecutableCandidates(explicit)) {
    if (!candidate) continue;
    try {
      await access(candidate);
      return candidate;
    } catch {
      // The source selector tries the next candidate when this path does not exist.
    }
  }
  return undefined;
}
