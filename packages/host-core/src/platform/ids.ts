// M9 — PLATFORM / ids. Re-exports the canonical id generators from the kept
// `src/shared/lib/id` home (no reimplementation) and adds the `generateId`
// convenience used for prefixed, opaque resource ids.
import { nanoid, ulid } from "../shared/lib/id";

export { nanoid, ulid };

/**
 * Opaque resource id. With a `prefix` (e.g. "agent", "task") returns
 * `"<prefix>_<nanoid>"` for human-greppable ids; without one, a bare nanoid.
 * For time-ordered ids (creation-order invariant) use `ulid()` instead.
 */
export function generateId(prefix?: string): string {
  return prefix ? `${prefix}_${nanoid()}` : nanoid();
}
