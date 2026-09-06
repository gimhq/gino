import type { HostEnv } from "@gino/host-hono";
import type { MiddlewareHandler } from "hono";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { createHash } from "node:crypto";
import { generateId, hashToken, safeEqual, verifyToken } from "@gino/host-core/domain";
import { errorHandler, requireAuthorized } from "@gino/host-hono";
import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { adminTokenAuth, adminTokenAuthorize, channelTokenAuthorize } from "@/shared/middleware/auth";

// ─── platform crypto ───

describe("safeEqual", () => {
  it("returns true for identical strings", () => {
    expect(safeEqual("hunter2", "hunter2")).toBe(true);
    expect(safeEqual("", "")).toBe(true);
  });

  it("returns false for differing strings (incl. differing lengths)", () => {
    expect(safeEqual("hunter2", "hunter3")).toBe(false);
    expect(safeEqual("short", "a-much-longer-secret")).toBe(false);
    expect(safeEqual("secret", "")).toBe(false);
  });
});

describe("hashToken / verifyToken", () => {
  it("hashToken is the sha256 hex digest", () => {
    const expected = createHash("sha256").update("tok-abc").digest("hex");
    expect(hashToken("tok-abc")).toBe(expected);
    expect(hashToken("tok-abc")).toHaveLength(64);
  });

  it("verifyToken accepts the matching token and rejects others", () => {
    const hash = hashToken("connect-secret");
    expect(verifyToken("connect-secret", hash)).toBe(true);
    expect(verifyToken("wrong-secret", hash)).toBe(false);
    expect(verifyToken("connect-secret", "deadbeef")).toBe(false);
  });
});

describe("generateId", () => {
  it("prefixes when asked and stays opaque otherwise", () => {
    expect(generateId("agent")).toMatch(/^agent_[0-9a-z]{16}$/);
    expect(generateId()).toMatch(/^[0-9a-z]{16}$/);
  });
});

// ─── auth middleware ───

const TEST_CONFIG = {
  REGISTER_TOKEN: "reg-secret",
  HTTP_CHANNEL_TOKEN: "chan-secret",
  HTTP_ADMIN_TOKEN: "admin-secret",
} as unknown as Config;

// Minimal app: inject config into context (as app.ts does), apply the
// middleware under test, then a 200 handler. The scaffold error-handler turns
// the middleware's UnauthorizedError into the standard 401 envelope.
function appWith<E extends HostEnv>(mw: MiddlewareHandler<E>, config: Config = TEST_CONFIG) {
  const app = new Hono<AppEnv>();
  app.use("*", (c, next) => {
    c.set("config", config);
    return next();
  });
  app.use("*", mw);
  app.all("/", c => c.json({ ok: true }));
  app.onError(errorHandler);
  return app;
}

// Register-token validation moved INTO the register handler (HOST-M1M2-14) so
// failures render the protocol §3 `register_error` envelope — covered by
// tests/unit/edge-input-hardening.test.ts. The connect-token middleware lives
// in @gino/host-hono (packages/host-hono/tests/unit/auth.test.ts).

/** The channel bearer as the messages router sees it: `authorize` behind the package gate. */
const channelTokenAuth = requireAuthorized(channelTokenAuthorize(TEST_CONFIG));

describe("channelTokenAuthorize", () => {
  it("admits the channel bearer as a channel principal and nothing else", () => {
    const authorize = channelTokenAuthorize(TEST_CONFIG);
    const ctx = (authorization?: string) =>
      ({ req: { header: (name: string) => (name === "authorization" ? authorization : undefined) } }) as unknown as Parameters<typeof authorize>[0];
    expect(authorize(ctx("Bearer chan-secret"))).toEqual({ kind: "channel" });
    expect(authorize(ctx("Bearer admin-secret"))).toBeNull();
    expect(authorize(ctx())).toBeNull();
    expect(channelTokenAuthorize({ ...TEST_CONFIG, HTTP_CHANNEL_TOKEN: "" } as Config)(ctx("Bearer "))).toBeNull();
  });
});

describe("adminTokenAuthorize", () => {
  it("admits the admin bearer as an admin principal; fails closed when unconfigured", () => {
    const authorize = adminTokenAuthorize(TEST_CONFIG);
    const ctx = (authorization?: string) =>
      ({ req: { header: (name: string) => (name === "authorization" ? authorization : undefined) } }) as unknown as Parameters<typeof authorize>[0];
    expect(authorize(ctx("Bearer admin-secret"))).toEqual({ kind: "admin" });
    expect(authorize(ctx("Bearer chan-secret"))).toBeNull();
    expect(adminTokenAuthorize({ ...TEST_CONFIG, HTTP_ADMIN_TOKEN: "" } as Config)(ctx("Bearer chan-secret"))).toBeNull();
  });
});

describe("channelTokenAuth", () => {
  it("401 without an Authorization bearer", async () => {
    const app = appWith(channelTokenAuth);
    const res = await app.request("/", { method: "GET" });
    expect(res.status).toBe(401);
  });

  it("401 on a wrong bearer token", async () => {
    const app = appWith(channelTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer nope" } });
    expect(res.status).toBe(401);
  });

  it("passes with the correct channel bearer", async () => {
    const app = appWith(channelTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer chan-secret" } });
    expect(res.status).toBe(200);
  });

  it("401 on the ADMIN token (no cross-grant, HOST-XC-02)", async () => {
    const app = appWith(channelTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer admin-secret" } });
    expect(res.status).toBe(401);
  });
});

describe("adminTokenAuth (HOST-XC-02)", () => {
  it("401 without an Authorization bearer", async () => {
    const app = appWith(adminTokenAuth);
    const res = await app.request("/", { method: "GET" });
    expect(res.status).toBe(401);
  });

  it("401 on a wrong bearer token", async () => {
    const app = appWith(adminTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer nope" } });
    expect(res.status).toBe(401);
  });

  it("401 on the CHANNEL token — a channel credential never grants admin", async () => {
    const app = appWith(adminTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer chan-secret" } });
    expect(res.status).toBe(401);
  });

  it("passes with the correct admin bearer", async () => {
    const app = appWith(adminTokenAuth);
    const res = await app.request("/", { method: "GET", headers: { authorization: "Bearer admin-secret" } });
    expect(res.status).toBe(200);
  });

  it("fails CLOSED when unconfigured — no fallback to the channel token", async () => {
    const noAdmin = { ...TEST_CONFIG, HTTP_ADMIN_TOKEN: "" } as Config;
    const app = appWith(adminTokenAuth, noAdmin);
    for (const headers of [
      undefined,
      { authorization: "Bearer chan-secret" },
      { authorization: "Bearer " },
    ]) {
      const res = await app.request("/", { method: "GET", ...(headers ? { headers } : {}) });
      expect(res.status).toBe(401);
    }
  });
});
