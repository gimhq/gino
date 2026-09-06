// PLATFORM (cross-cutting infrastructure): errors, ids/crypto, logger seam.
//
// Thin facade: the cross-cutting concerns keep their homes
// (`src/shared/lib/{errors,id}.ts`, `src/logger.ts`); this barrel CENTRALISES
// the import surface so the rest of the package can `import { ... } from
// "../platform"`. It only ADDS ids/crypto (generateId, ULID, safeEqual,
// hashToken/verifyToken) — no behavioural change to `src/shared/lib`.

// logger seam (the structural interface the domain code logs through).
export type { Logger } from "../logger";
// errors (re-export — the typed AppError hierarchy + envelope).
export {
  AppError,
  ForbiddenError,
  NotFoundError,
  UnauthorizedError,
  ValidationError,
} from "../shared/lib/errors";

export { hashToken, safeEqual, verifyToken } from "./crypto";

// ids/crypto (the platform's own surface).
export { generateId, nanoid, ulid } from "./ids";

export const MODULE = "platform" as const;
