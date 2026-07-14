import type { Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import type { AppEnv } from "@/shared/lib/types";
import { ZodError } from "zod";
import { AppError } from "@/shared/lib/errors";

export function errorHandler(err: Error, c: Context<AppEnv>) {
  if (err instanceof AppError) {
    return c.json(err.toJSON(), err.statusCode as ContentfulStatusCode);
  }

  if (err instanceof ZodError) {
    return c.json({
      success: false,
      error: { code: "VALIDATION_ERROR", message: "Validation failed", details: err.flatten() },
    }, 422);
  }

  // bun:sqlite uniqueness violations are the DB-level backstop for
  // check-then-write races (unique names, relation tuples). Surface them as
  // an actionable 409 instead of an opaque 500. Classify by the structured
  // error code only (HOST-M9-12) — message-text matching would also fire on
  // non-database errors that merely quote constraint text, and FK / NOT NULL
  // / CHECK violations are host data bugs, not duplicates: they fall through
  // to the logged 500. The raw message can name columns, so keep the
  // response generic.
  const code = (err as { code?: unknown }).code;
  if (code === "SQLITE_CONSTRAINT_UNIQUE" || code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
    return c.json({
      success: false,
      error: { code: "CONFLICT", message: "Resource already exists or violates a constraint" },
    }, 409);
  }

  c.get("logger").error({ err }, "unhandled error");
  return c.json({ success: false, error: { code: "INTERNAL_ERROR", message: "Internal server error" } }, 500);
}
