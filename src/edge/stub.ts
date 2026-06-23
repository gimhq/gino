import type { Context } from "hono";
import type { AppEnv } from "@/shared/lib/types";
import { describeRoute, errors } from "@/shared/lib/openapi";

// Shared helpers for the M1 edge skeleton. Each endpoint is a documented
// placeholder returning 501 — the real handlers land in B1–B6. Keeping the
// 501 envelope shape identical to the scaffold error envelope (`{ success,
// error }`) means clients see one consistent contract during the build-out.

/** 501 Not Implemented, in the standard error envelope. */
export function notImplemented(c: Context<AppEnv>, detail: string) {
  return c.json(
    { success: false, error: { code: "NOT_IMPLEMENTED", message: detail } },
    501,
  );
}

/**
 * `describeRoute` for a skeleton stub so the v2 route topology is visible in
 * `/openapi.json` + `/docs`. `authStatuses` documents the auth rejection code
 * (401) alongside the 501 placeholder.
 */
export function stubRoute(tag: string, summary: string, ...authStatuses: number[]) {
  return describeRoute({
    tags: [tag],
    summary: `${summary} (stub — 501 until implemented)`,
    responses: errors(...authStatuses, 501),
  });
}
