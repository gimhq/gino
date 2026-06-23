import type { MiddlewareHandler } from "hono";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { createHash } from "node:crypto";
import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { generateId, hashToken, safeEqual, verifyToken } from "@/platform";
import { channelTokenAuth, connectTokenAuth, registerTokenAuth } from "@/shared/middleware/auth";
import { errorHandler } from "@/shared/middleware/error-handler";

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
    expect(generateId("agent")).toMatch(/^agent_[0-9a-z]{8}$/);
    expect(generateId()).toMatch(/^[0-9a-z]{8}$/);
  });
});

// ─── auth middleware ───

const TEST_CONFIG = {
  REGISTER_TOKEN: "reg-secret",
  HTTP_CHANNEL_TOKEN: "chan-secret",
} as unknown as Config;

// Minimal app: inject config into context (as app.ts does), apply the
// middleware under test, then a 200 handler. The scaffold error-handler turns
// the middleware's UnauthorizedError into the standard 401 envelope.
function appWith(mw: MiddlewareHandler<AppEnv>, config: Config = TEST_CONFIG) {
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

describe("registerTokenAuth", () => {
  it("401 when the body carries no register token", async () => {
    const app = appWith(registerTokenAuth);
    const res = await app.request("/", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
    expect(res.status).toBe(401);
  });

  it("401 on a wrong register token", async () => {
    const app = appWith(registerTokenAuth);
    const res = await app.request("/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ registerToken: "nope" }),
    });
    expect(res.status).toBe(401);
  });

  it("passes with the correct register token in the body", async () => {
    const app = appWith(registerTokenAuth);
    const res = await app.request("/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ registerToken: "reg-secret" }),
    });
    expect(res.status).toBe(200);
  });

  it("fails closed when the expected register token is unconfigured", async () => {
    const app = appWith(registerTokenAuth, { REGISTER_TOKEN: "", HTTP_CHANNEL_TOKEN: "" } as unknown as Config);
    const res = await app.request("/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ registerToken: "" }),
    });
    expect(res.status).toBe(401);
  });
});

describe("connectTokenAuth (token → agent identity)", () => {
  // A real in-memory db with one enrolled agent: the middleware resolves the
  // connect token to its OWNING agent (by connect_token_hash) and puts the id in
  // context — identity comes from the token, never a client-supplied param.
  const db = createDb(":memory:");
  const { agent, connectToken } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });

  // App wired as production is (config + db in context, then the middleware as a
  // route handler so `:agentId` params populate), echoing the resolved agentId.
  function tokenApp(path = "/") {
    const app = new Hono<AppEnv>();
    app.use("*", (c, next) => {
      c.set("config", TEST_CONFIG);
      c.set("db", db);
      return next();
    });
    app.get(path, connectTokenAuth, c => c.json({ agentId: c.get("agentId") }));
    app.onError(errorHandler);
    return app;
  }

  it("401 without x-internal-token", async () => {
    expect((await tokenApp().request("/", { method: "GET" })).status).toBe(401);
  });

  it("401 on an empty x-internal-token", async () => {
    expect((await tokenApp().request("/", { method: "GET", headers: { "x-internal-token": "   " } })).status).toBe(401);
  });

  it("401 on a token that matches no enrolled agent (fails closed)", async () => {
    expect((await tokenApp().request("/", { method: "GET", headers: { "x-internal-token": "not-a-real-token" } })).status).toBe(401);
  });

  it("resolves the agent from the token and exposes it as the context agentId", async () => {
    const res = await tokenApp().request("/", { method: "GET", headers: { "x-internal-token": connectToken } });
    expect(res.status).toBe(200);
    expect((await res.json() as { agentId: string }).agentId).toBe(agent.id);
  });

  it("passes when the :agentId path segment IS the token's agent", async () => {
    const res = await tokenApp("/:agentId").request(`/${agent.id}`, { method: "GET", headers: { "x-internal-token": connectToken } });
    expect(res.status).toBe(200);
  });

  it("403 when the :agentId path segment is a DIFFERENT agent (cross-agent)", async () => {
    const res = await tokenApp("/:agentId").request("/some-other-agent", { method: "GET", headers: { "x-internal-token": connectToken } });
    expect(res.status).toBe(403);
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
});
