// HOST-M9-12 — the error handler must classify SQLite constraint failures by
// the structured error code, not message text: only uniqueness codes are a
// 409 conflict; FK / NOT NULL / CHECK violations (host data bugs) and
// non-database errors that merely quote constraint text stay logged 500s.
import type { AppEnv } from "@/shared/lib/types";
import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { errorHandler } from "@/shared/middleware/error-handler";

function makeApp() {
  const errors: unknown[] = [];
  const logger = {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: (obj: unknown) => errors.push(obj),
    fatal: () => {},
  };
  const app = new Hono<AppEnv>();
  app.use("*", (c, next) => {
    c.set("logger", logger as unknown as AppEnv["Variables"]["logger"]);
    return next();
  });
  app.get("/throw", (c) => {
    const err = new Error(c.req.query("message") ?? "boom");
    const code = c.req.query("code");
    if (code)
      (err as Error & { code?: string }).code = code;
    throw err;
  });
  app.onError(errorHandler);
  return { app, errors };
}

async function fire(code?: string, message?: string) {
  const { app, errors } = makeApp();
  const params = new URLSearchParams();
  if (code)
    params.set("code", code);
  if (message)
    params.set("message", message);
  const res = await app.fetch(new Request(`http://local/throw?${params}`));
  return { res, errors };
}

describe("errorHandler constraint classification (HOST-M9-12)", () => {
  it("maps SQLITE_CONSTRAINT_UNIQUE to 409", async () => {
    const { res, errors } = await fire("SQLITE_CONSTRAINT_UNIQUE", "UNIQUE constraint failed: t.u");
    expect(res.status).toBe(409);
    expect(errors).toHaveLength(0);
  });

  it("maps SQLITE_CONSTRAINT_PRIMARYKEY to 409", async () => {
    const { res } = await fire("SQLITE_CONSTRAINT_PRIMARYKEY", "UNIQUE constraint failed: t.id");
    expect(res.status).toBe(409);
  });

  it("lets FOREIGN KEY violations fall through to a logged 500", async () => {
    const { res, errors } = await fire("SQLITE_CONSTRAINT_FOREIGNKEY", "FOREIGN KEY constraint failed");
    expect(res.status).toBe(500);
    expect(errors).toHaveLength(1);
  });

  it("lets NOT NULL violations fall through to a logged 500", async () => {
    const { res, errors } = await fire("SQLITE_CONSTRAINT_NOTNULL", "NOT NULL constraint failed: t.n");
    expect(res.status).toBe(500);
    expect(errors).toHaveLength(1);
  });

  it("does not classify by message text alone", async () => {
    const { res, errors } = await fire(undefined, "upstream said: UNIQUE constraint failed somewhere");
    expect(res.status).toBe(500);
    expect(errors).toHaveLength(1);
  });
});
