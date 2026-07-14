// Stub. Populated at build time by `scripts/package.ts`, which rewrites
// this file before invoking `bun build` and restores the empty
// stub afterwards. The runtime path in `db/index.ts` uses this map
// exclusively whenever it is populated (packaged builds) or the binary is
// compiled; only dev/test runs with the empty stub read the on-disk
// `drizzle/` folder (HOST-M9-13).
//
// If `bun run check` (or any Bun-direct entry point) somehow lands here with
// the stub still in place AND no on-disk `drizzle/`, the boot guard in
// `runMigrations` throws with a remediation hint.
export const embeddedMigrations = new Map<string, string>([
]);
