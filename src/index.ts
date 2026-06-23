import process from "node:process";
import { bootstrap } from "./app";
import { BUILD_INFO } from "./build-info";
import { dispatchCliSubcommand } from "./cli";

(async () => {
  const subcommandExit = await dispatchCliSubcommand(process.argv);
  if (subcommandExit !== null) {
    process.exit(subcommandExit);
  }

  const { fetch, websocket, config, logger, closeDb } = bootstrap();
  logger.info({ ...BUILD_INFO }, "build info");

  const server = Bun.serve({
    port: config.PORT,
    hostname: config.HOST,
    // `server` is threaded into the fetch env so the hono/bun WS adapter can
    // upgrade `/agent/ws/:agentId`; `websocket` handles the upgraded sockets.
    fetch: (req, srv) => fetch(req, { IP: srv.requestIP(req), server: srv }),
    websocket,
  });

  logger.info({ port: config.PORT, host: config.HOST }, "server started");

  let shuttingDown = false;

  async function safe(name: string, fn: () => unknown, silent = false): Promise<void> {
    try {
      await fn();
    }
    catch (err) {
      if (!silent)
        logger.error({ err }, `${name} failed`);
    }
  }

  // Unified teardown. fatal=true: immediate stop, silent per-step errors
  // (logger may be gone), exit 1. fatal=false: bounded drain, exit 0.
  async function closeServices(opts: { reason: string; fatal: boolean; err?: unknown }): Promise<void> {
    if (shuttingDown) {
      logger.debug({ reason: opts.reason }, "shutdown already in progress");
      return;
    }
    shuttingDown = true;

    if (opts.fatal)
      logger.fatal({ err: opts.err }, opts.reason);
    else
      logger.info({ signal: opts.reason }, "shutting down");

    const silent = opts.fatal;
    const stopServer = opts.fatal
      ? () => server.stop(true)
      : async () => {
        await Promise.race([
          server.stop(true),
          new Promise<void>(resolve => setTimeout(resolve, 25_000).unref?.()),
        ]);
        try {
          server.stop(false);
        }
        catch {}
      };

    await safe("server.stop", stopServer, silent);
    await safe("logger.flush", () => logger.flush(), true);
    await safe("closeDb", closeDb, silent);
    await safe("logger.flush", () => logger.flush(), true);

    process.exit(opts.fatal ? 1 : 0);
  }

  process.on("SIGINT", () => {
    void closeServices({ reason: "SIGINT", fatal: false });
  });
  process.on("SIGTERM", () => {
    void closeServices({ reason: "SIGTERM", fatal: false });
  });
  // External logrotate hook: reopen the log fd in place.
  process.on("SIGHUP", () => {
    logger.info("received SIGHUP — reopening log file");
    logger.reopen();
  });
  process.on("uncaughtException", (err) => {
    void closeServices({ reason: "uncaught exception", fatal: true, err });
  });
  process.on("unhandledRejection", (err) => {
    void closeServices({ reason: "unhandled rejection", fatal: true, err });
  });
})();
