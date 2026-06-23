// M9 — PLATFORM (cross-cutting infrastructure): env/config, logger
// (redaction), errors, ids/crypto, persistence, bootstrap, observability.
//
// Thin facade: the cross-cutting concerns keep their bun-tpl idiom homes
// (`src/config{,.ts}`, `src/shared/lib/{logger,errors,id}.ts`, `src/db/**`,
// `src/app.ts`); this barrel CENTRALISES the import surface so the rest of
// `src/**` can `import { ... } from "@/platform"`. It only ADDS ids/crypto
// (generateId, ULID, safeEqual, hashToken/verifyToken) — no behavioural change
// to `src/shared/lib`. Persistence + bootstrap consolidate here as later
// stages land (docs/host-v2-design.md §M9).

export { hashToken, safeEqual, verifyToken } from "./crypto";
// ids/crypto (ADDED by M9 — the platform's own surface).
export { generateId, nanoid, ulid } from "./ids";

// env/config (re-export — `src/config` is the sole config source).
export { loadConfig, loadConfigStrict } from "@/config";
export type { Config } from "@/config";
export { configSchema } from "@/config/schema";

// errors (re-export — the typed AppError hierarchy + envelope).
export {
  AppError,
  ForbiddenError,
  NotFoundError,
  UnauthorizedError,
  ValidationError,
} from "@/shared/lib/errors";
// logger (re-export — secret-redacting structured logger).
export { createLogger } from "@/shared/lib/logger";

export type { Logger } from "@/shared/lib/logger";

export const MODULE = "platform" as const;
