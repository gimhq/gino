// examples/embed-node — the Gino host SDK embedded in a plain Node process.
//
// What this is: `createHost` over the better-sqlite3 storage adapter, the
// `/agent/*` data plane mounted through `@gino/host-hono` with the
// `@hono/node-ws` upgrade, served by `@hono/node-server`, plus a tiny operator
// API so a shell (or scripts/roundtrip.ts) can drive one task through a real
// `gino-agent`. Nothing here is Bun-specific: `node src/server.ts` runs it
// directly (Node 24 type stripping — relative imports carry `.ts`, no enums).
//
// The env reads below are the embedder's own defaults; the SDK itself never
// reads `process.env`.
import type { Host, LogFn, Logger } from "@gino/host-core";
import type { HostEnv } from "@gino/host-hono";
import type { ServerType } from "@hono/node-server";
import { realpathSync } from "node:fs";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { createHost } from "@gino/host-core";
import { agentRouter, errorHandler } from "@gino/host-hono";
import { nodeWebSocket } from "@gino/host-hono/node";
import { openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";
import { serve } from "@hono/node-server";
import { Hono } from "hono";

export interface ServerOptions {
  /** TCP port; `0` picks a free one. Default: `PORT` env, else 3000. */
  readonly port?: number;
  /** Bind address. Default: `HOST` env, else 127.0.0.1. */
  readonly hostname?: string;
  /** Shared register token the runner presents. Default: `GINO_REGISTER_TOKEN` env. */
  readonly registerToken?: string;
  /** sqlite file. Default: `GINO_DB_PATH` env, else data/embed-node/gino.db. */
  readonly dbPath?: string;
  /** Blob store root. Default: `GINO_STORE_DIR` env, else data/embed-node/store. */
  readonly storeDir?: string;
  /** Default: a console-backed logger. */
  readonly logger?: Logger;
}

export interface RunningServer {
  readonly host: Host;
  readonly port: number;
  /** `http://<hostname>:<port>` — the runner's GINO_HOST_URL is `${url}/api`. */
  readonly url: string;
  /** Stop the host (closes agent sockets + storage) and the HTTP server. */
  readonly stop: () => Promise<void>;
}

/** Errors do not JSON.stringify; flatten them so a logged `{ err }` stays readable. */
function plain(value: unknown): unknown {
  if (value instanceof Error) {
    return { name: value.name, message: value.message, ...(value.stack ? { stack: value.stack } : {}) };
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, plain(v)]));
  }
  return value;
}

function consoleLine(level: "debug" | "info" | "warn" | "error"): LogFn {
  // eslint-disable-next-line no-console
  const write = level === "error" || level === "warn" ? console.error : console.log;
  return (objOrMsg: object | string, msg?: string): void => {
    if (typeof objOrMsg === "string") {
      write(`[host] ${level} ${objOrMsg}`);
      return;
    }
    write(`[host] ${level} ${msg ?? ""} ${JSON.stringify(plain(objOrMsg))}`);
  };
}

/** A `Logger` that prints one line per call to the console (`error`/`warn` → stderr). */
export function consoleLogger(): Logger {
  return { debug: consoleLine("debug"), info: consoleLine("info"), warn: consoleLine("warn"), error: consoleLine("error") };
}

interface CreateTaskBody {
  title?: unknown;
  ownerId?: unknown;
  agentId?: unknown;
  engine?: unknown;
  model?: unknown;
}

const asString = (v: unknown): string | undefined => (typeof v === "string" && v.length > 0 ? v : undefined);

/** Never expose the connect-token hash on the operator surface. */
function publicAgent(row: ReturnType<Host["agents"]["list"]>[number]) {
  const { connectTokenHash: _hash, ...rest } = row;
  return rest;
}

function buildApp(host: Host) {
  const app = new Hono<HostEnv>();
  const { upgradeWebSocket, injectWebSocket } = nodeWebSocket(app);

  // The runner dials `${GINO_HOST_URL}/agent/*`; with GINO_HOST_URL=…/api this is it.
  app.route("/api/agent", agentRouter(host, { upgradeWebSocket }));

  app.get("/health", c => c.json({ status: "ok" }));

  app.get("/api/agents", c => c.json({ agents: host.agents.list().map(publicAgent) }));

  app.post("/api/tasks", async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as CreateTaskBody;
    const ownerId = asString(body.ownerId);
    if (!ownerId) {
      return c.json({ error: "ownerId is required" }, 400);
    }
    const agentId = asString(body.agentId);
    const title = asString(body.title);
    const task = host.tasks.create({
      ownerId,
      engine: asString(body.engine) ?? "claude",
      model: asString(body.model) ?? "sonnet",
      ...(title !== undefined ? { name: title } : {}),
    });
    // An explicit `agentId` pins that agent: the operator is trusted here, so
    // grant the owner on it first (placement requires the owner be permitted).
    if (agentId) {
      host.agents.grant(agentId, ownerId);
    }
    const dispatch = agentId ? host.tasks.assign(task.id, agentId) : host.tasks.place(task.id);
    return c.json({ task: host.tasks.get(task.id) ?? task, dispatch }, 201);
  });

  app.post("/api/tasks/:id/messages", async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as { text?: unknown };
    const text = asString(body.text);
    if (!text) {
      return c.json({ error: "text is required" }, 400);
    }
    const sent = host.tasks.send(c.req.param("id"), { text });
    return c.json({ threadId: sent.threadId, seeded: sent.taskUpsert !== undefined, outboxId: sent.message.id });
  });

  app.get("/api/tasks/:id/threads", (c) => {
    const taskId = c.req.param("id");
    const threads = host.transcript.threads(taskId).map(threadId => ({ threadId, run: host.transcript.run(threadId) ?? null }));
    return c.json({ taskId, threads });
  });

  app.get("/api/tasks/:id/threads/:threadId", (c) => {
    const threadId = c.req.param("threadId");
    return c.json({
      taskId: c.req.param("id"),
      threadId,
      run: host.transcript.run(threadId) ?? null,
      messages: host.transcript.get(threadId),
    });
  });

  // `AppError` subclasses (404 unknown task, 409 set-once placement, 422
  // dispatch preconditions …) map to the protocol error envelope.
  app.onError(errorHandler);
  return { app, injectWebSocket };
}

/** Build the host, mount the routers and start listening. Resolves once the port is bound. */
export async function startServer(opts: ServerOptions = {}): Promise<RunningServer> {
  const logger = opts.logger ?? consoleLogger();
  const port = opts.port ?? Number(process.env.PORT ?? 3000);
  const hostname = opts.hostname ?? process.env.HOST ?? "127.0.0.1";
  const registerToken = opts.registerToken ?? process.env.GINO_REGISTER_TOKEN ?? "";
  const dbPath = opts.dbPath ?? process.env.GINO_DB_PATH ?? "data/embed-node/gino.db";
  const storeDir = opts.storeDir ?? process.env.GINO_STORE_DIR ?? "data/embed-node/store";

  const db = openDatabase({ driver: nodeSqliteDriver, path: dbPath });
  const host = createHost({
    storage: sqliteStorage(db, { storeDir }),
    logger,
    agents: { registerToken, registrationEnabled: true },
  });
  host.start();

  host.events.on("thread.result", ({ agentId, taskId, threadId, data }) => {
    logger.info({ agentId, taskId, threadId, status: data.status, result: data.result, durationMs: data.duration_ms }, "thread.result");
  });

  const { app, injectWebSocket } = buildApp(host);
  const server: ServerType = serve({ fetch: app.fetch, port, hostname });
  injectWebSocket(server);
  await new Promise<void>((resolve, reject) => {
    server.once("listening", () => resolve());
    server.once("error", reject);
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error(`unexpected server address: ${String(address)}`);
  }
  const url = `http://${hostname}:${address.port}`;
  logger.info({ url, dbPath, storeDir }, "embed-node host listening");

  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> => {
    stopping ??= new Promise<void>((resolve) => {
      host.stop();
      if ("closeAllConnections" in server) {
        server.closeAllConnections();
      }
      server.close(() => resolve());
    });
    return stopping;
  };

  return { host, port: address.port, url, stop };
}

const isMain = process.argv[1] !== undefined && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  startServer().then((running) => {
    const shutdown = (signal: string): void => {
      running.host.logger.info({ signal }, "shutting down");
      void running.stop().then(() => process.exit(0));
    };
    process.once("SIGINT", () => shutdown("SIGINT"));
    process.once("SIGTERM", () => shutdown("SIGTERM"));
  }).catch((err: unknown) => {
    console.error("[host] failed to start:", err);
    process.exit(1);
  });
}
