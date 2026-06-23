// Build-time identifiers injected by `scripts/package.ts` via Bun
// `--define`. In dev (`bun run dev`) these are undefined and we fall back
// to readable placeholders so logs still show something useful.
declare const BUILD_COMMIT: string | undefined;
declare const BUILD_TIME: string | undefined;
declare const BUILD_VERSION: string | undefined;

export const BUILD_INFO = {
  commit: typeof BUILD_COMMIT === "string" ? BUILD_COMMIT : "dev",
  buildTime: typeof BUILD_TIME === "string" ? BUILD_TIME : new Date().toISOString(),
  version: typeof BUILD_VERSION === "string" ? BUILD_VERSION : "0.0.0-dev",
} as const;
