// MEMORY STORE — helpers shared by the per-repo adapters.

/** Mirror a sqlite unique-constraint failure so the domain's `/UNIQUE/` checks hold across adapters. */
export function uniqueViolation(constraint: string): Error {
  return new Error(`UNIQUE constraint failed: ${constraint}`);
}

/** Stable sort, `key` descending — rows with an equal key keep insertion order. */
export function newestFirst<T>(rows: T[], key: (row: T) => number): T[] {
  return [...rows].sort((a, b) => key(b) - key(a));
}
