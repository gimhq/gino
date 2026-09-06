import type { HostEnv } from "../../src/env";
import { createHost, ForbiddenError, memoryStorage, noopLogger } from "@gino/host-core";
import { enrollAgent } from "@gino/host-core/domain";
import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { bearerToken, connectTokenAuth, requireAuthorized } from "../../src/auth";
import { errorHandler } from "../../src/errors";

// The protocol credential middleware (connect token → agent identity) and the
// embedder-injected `authorize` gate, over the memory storage — no app config,
// no sqlite.

describe("bearerToken", () => {
  const ctx = (authorization?: string) =>
    ({ req: { header: (name: string) => (name === "authorization" ? authorization : undefined) } }) as unknown as Parameters<typeof bearerToken>[0];

  it("extracts the bearer (case-insensitive scheme, trimmed)", () => {
    expect(bearerToken(ctx("Bearer tok-1"))).toBe("tok-1");
    expect(bearerToken(ctx("  bearer   tok-2  "))).toBe("tok-2");
  });

  it("returns null for a missing header, another scheme, or an empty token", () => {
    expect(bearerToken(ctx())).toBeNull();
    expect(bearerToken(ctx("Basic abc"))).toBeNull();
    expect(bearerToken(ctx("Bearer "))).toBeNull();
  });
});

describe("connectTokenAuth (token → agent identity)", () => {
  // A memory-backed host with one enrolled agent: the middleware resolves the
  // connect token to its OWNING agent (by connect_token_hash) and puts the id in
  // context — identity comes from the token, never a client-supplied param.
  const storage = memoryStorage();
  const host = createHost({ storage, logger: noopLogger });
  const { agent, connectToken } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });

  // App wired as an embedder would (host in context, then the middleware as a
  // route handler so `:agentId` params populate), echoing the resolved agentId.
  function tokenApp(path = "/") {
    const app = new Hono<HostEnv>();
    app.use("*", (c, next) => {
      c.set("host", host);
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

describe("requireAuthorized (embedder authorize gate)", () => {
  function gated(authorize: Parameters<typeof requireAuthorized>[0]) {
    const app = new Hono<HostEnv>();
    app.use("*", requireAuthorized(authorize));
    app.all("/", c => c.json({ ok: true }));
    app.onError(errorHandler);
    return app;
  }

  it("401 with the standard envelope when authorize returns null", async () => {
    const res = await gated(() => null).request("/", { method: "GET" });
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ success: false, error: { code: "UNAUTHORIZED", message: "Unauthorized" } });
  });

  it("passes when authorize resolves a principal", async () => {
    const res = await gated(() => ({ kind: "admin" })).request("/", { method: "GET" });
    expect(res.status).toBe(200);
  });

  it("403 when authorize throws ForbiddenError", async () => {
    const res = await gated(() => {
      throw new ForbiddenError();
    }).request("/", { method: "GET" });
    expect(res.status).toBe(403);
  });
});
