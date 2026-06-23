// Stub. Populated at compile time by `scripts/compile.ts`, which rewrites
// this file before invoking `bun build --compile` and restores the empty
// stub afterwards. The runtime path in `db/index.ts` prefers the on-disk
// `drizzle/` folder when present and falls back to this map only inside the
// compiled binary, where the folder has been excluded.
//
// If `bun run check` (or any Bun-direct entry point) somehow lands here with
// the stub still in place AND no on-disk `drizzle/`, the boot guard in
// `runMigrations` throws with a remediation hint.
export const embeddedMigrations = new Map<string, string>([
]);
