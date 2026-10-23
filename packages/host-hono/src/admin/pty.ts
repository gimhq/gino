import type { Host, PtySession } from "@gino/host-core";
import type { UpgradeWebSocket, WSContext, WSEvents } from "hono/ws";
import type { Authorize, HostEnv } from "../env";
import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import { AppError, ForbiddenError, NotFoundError, UnauthorizedError } from "@gino/host-core";
import { PtyBytesData, PtyOpenData, PtyResizeData } from "@gino/host-core/domain";
import { Hono } from "hono";
import { z } from "zod";
import { ok } from "../api-response";
import { requireAuthorized } from "../auth";
import { describeRoute, errors, jsonOk, TAGS } from "../openapi";

const BUFFER_LIMIT = 256 * 1024;
const TICKET_TTL_MS = 30_000;
const ClientFrame = z.discriminatedUnion("type", [
  z.object({ type: z.literal("open"), data: PtyOpenData }),
  z.object({ type: z.literal("input"), data: PtyBytesData }),
  z.object({ type: z.literal("resize"), data: PtyResizeData }),
  z.object({ type: z.literal("close") }),
]);

/** Read the buffer size from either supported runtime's underlying socket. */
function buffered(ws: WSContext): number {
  const raw = ws.raw as { bufferedAmount?: number; getBufferedAmount?: () => number } | undefined;
  return raw?.getBufferedAmount?.() ?? raw?.bufferedAmount ?? 0;
}

function terminalEvents(host: Host, agentId: string): WSEvents {
  let pty: PtySession | undefined;
  let opening = false;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const abort = new AbortController();
  const stop = () => {
    stopped = true;
    clearTimeout(timer);
    abort.abort();
    pty?.close();
  };
  const send = (ws: WSContext, type: string, data: unknown) => {
    if (stopped || ws.readyState !== 1)
      throw new Error("Terminal client disconnected");
    const text = JSON.stringify({ type, data });
    if (buffered(ws) + Buffer.byteLength(text) > BUFFER_LIMIT)
      throw new Error("Terminal client is too slow");
    ws.send(text);
  };
  const fail = (ws: WSContext, error: unknown) => {
    if (!stopped) {
      try {
        send(ws, "exit", { error: { code: "terminal_error", message: error instanceof Error ? error.message : "Terminal failed" } });
      }
      catch { /* The socket may already be closed or full. */ }
      stop();
      ws.close(1008, "Terminal closed");
    }
  };
  const open = async (ws: WSContext, options: z.infer<typeof PtyOpenData>) => {
    try {
      pty = await host.agents.openPty(agentId, options, abort.signal);
      if (stopped) {
        pty.close();
        return;
      }
      send(ws, "ready", { ptyId: pty.id });
      const reader = pty.output.getReader();
      try {
        for (;;) {
          if (stopped)
            break;
          const { value, done } = await reader.read();
          if (done)
            break;
          send(ws, "output", { data: Buffer.from(value).toString("base64") });
        }
      }
      finally {
        reader.releaseLock();
      }
      if (!stopped) {
        send(ws, "exit", await pty.exited);
        stop();
        ws.close(1000, "Terminal exited");
      }
    }
    catch (error) {
      fail(ws, error);
    }
  };
  return {
    onOpen: (_event, ws) => {
      timer = setTimeout(() => fail(ws, new Error("Terminal open message timed out")), 5000);
      timer.unref?.();
    },
    onMessage: (event, ws) => {
      if (stopped)
        return;
      try {
        if (typeof event.data !== "string" || Buffer.byteLength(event.data) > 70_000)
          throw new Error("Invalid terminal frame");
        const frame = ClientFrame.parse(JSON.parse(event.data));
        if (frame.type === "close") {
          stop();
          ws.close(1000, "Terminal closed");
        }
        else if (frame.type === "open" && !opening) {
          opening = true;
          clearTimeout(timer);
          void open(ws, frame.data);
        }
        else if (frame.type === "input" && pty) {
          pty.write(new Uint8Array(Buffer.from(frame.data.data, "base64")));
        }
        else if (frame.type === "resize" && pty) {
          pty.resize(frame.data.cols, frame.data.rows);
        }
        else {
          throw new Error("Terminal is not ready or was already opened");
        }
      }
      catch (error) {
        fail(ws, error);
      }
    },
    onClose: stop,
    onError: stop,
  };
}

/** Terminal-only authorization; a ticket cannot authorize any other admin route. */
export function adminPtyRoutes(host: Host, authorize: Authorize, upgradeWebSocket: UpgradeWebSocket) {
  const router = new Hono<HostEnv>();
  const tickets = new Map<string, { agentId: string; expiresAt: number; origin: string }>();
  router.post("/:id/pty-ticket", describeRoute({
    tags: [TAGS.Agents],
    summary: "Issue a single-use terminal ticket (expires after 30 seconds)",
    responses: { ...jsonOk(z.object({ ticket: z.string(), expiresAt: z.number() }), "Terminal ticket"), ...errors(401, 404, 429) },
  }), requireAuthorized(authorize), (c) => {
    const agentId = c.req.param("id");
    if (!host.agents.get(agentId))
      throw new NotFoundError("agent", agentId);
    const now = Date.now();
    for (const [token, ticket] of tickets) {
      if (ticket.expiresAt <= now)
        tickets.delete(token);
    }
    if (tickets.size >= 256)
      throw new AppError("Too many pending terminal tickets", 429, "PTY_TICKET_LIMIT");
    const ticket = randomBytes(32).toString("hex");
    const expiresAt = now + TICKET_TTL_MS;
    // The authenticated browser origin survives TLS termination without trusting forwarded headers.
    tickets.set(ticket, { agentId, expiresAt, origin: c.req.header("origin") ?? new URL(c.req.url).origin });
    c.header("Cache-Control", "no-store");
    return c.json(ok({ ticket, expiresAt }));
  });
  router.get("/:id/pty", describeRoute({
    tags: [TAGS.Agents],
    summary: "Open a terminal WebSocket (single-use ticket or admin authorization)",
    responses: { 101: { description: "Terminal WebSocket; send an open message within five seconds" }, ...errors(401, 403, 404, 426) },
  }), (c, next) => {
    const origin = c.req.header("origin");
    let expectedOrigin = new URL(c.req.url).origin;
    if (c.req.header("upgrade")?.toLowerCase() !== "websocket")
      throw new AppError("WebSocket upgrade required", 426, "UPGRADE_REQUIRED");
    const token = c.req.query("ticket");
    if (token) {
      const ticket = tickets.get(token);
      tickets.delete(token);
      if (!ticket || ticket.expiresAt <= Date.now() || ticket.agentId !== c.req.param("id"))
        throw new UnauthorizedError();
      expectedOrigin = ticket.origin;
    }
    else if (!authorize(c)) {
      throw new UnauthorizedError();
    }
    if (origin && origin !== expectedOrigin)
      throw new ForbiddenError();
    if (!host.agents.get(c.req.param("id")))
      throw new NotFoundError("agent", c.req.param("id"));
    return next();
  }, upgradeWebSocket(c => terminalEvents(host, c.req.param("id")!)));
  return router;
}
