import type { LodePrepareWatcher } from "./lode";
import process from "node:process";
import { sql } from "drizzle-orm";
import { bootstrap } from "./app";
import { BUILD_INFO } from "./build-info";
import { dispatchCliSubcommand } from "./cli";
import { captureLodeConfigBaseline, reportLodeServing, startLodePrepareWatcher } from "./lode";

(async () => {
  const subcommandExit = await dispatchCliSubcommand(process.argv);
  if (subcommandExit !== null) {
    process.exit(subcommandExit);
  }

  // Bootstrap failures (config, DB open, the HOST-M9-14 migration lock held by
  // a concurrent starter) surface as a clean one-line fatal, not an unhandled
  // rejection — the logger does not exist yet at this point.
  let boot: ReturnType<typeof bootstrap>;
  try {
    boot = bootstrap();
  }
  catch (err) {
    console.error("[gino] bootstrap failed:", err instanceof Error ? err.message : err);
    process.exit(1);
  }
  const { fetch, websocket, config, logger, db, host } = boot;
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
  let lodePrepare: LodePrepareWatcher | undefined;

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
          server.stop(false),
          new Promise<void>(resolve => setTimeout(resolve, 25_000).unref?.()),
        ]);
        try {
          server.stop(true);
        }
        catch {}
      };

    await safe("stopLodePrepareWatcher", () => lodePrepare?.stop(), silent);
    await safe("server.stop", stopServer, silent);
    await safe("logger.flush", () => logger.flush(), true);
    await safe("host.stop", () => host.stop(), silent);
    await safe("logger.flush", () => logger.flush(), true);

    process.exit(opts.fatal ? 1 : 0);
  }

  // Lode upgrade integration (docs/host-upgrade-design.md §6). No-op unless
  // running under the lode supervisor. Built on the official SDK (src/lode).
  try {
    // Report serving with the phase-0 token, opting into the staged-update
    // prepare handshake. Gate on the DB answering so lode's `state.ready`
    // reflects real readiness, not just a bound port.
    await reportLodeServing({
      logger,
      probe: () => {
        db.run(sql`SELECT 1`);
        return true;
      },
    });
    // Snapshot lode's config_generation now so a later lode.toml edit surfaces as
    // "config changed — restart to apply" on /system/version.
    captureLodeConfigBaseline();
    // On lode's staged-update prompt, checkpoint the WAL and flush logs before
    // acking the cut-over, so the next version starts from a consolidated DB.
    lodePrepare = startLodePrepareWatcher({
      logger,
      onPrepare: async () => {
        db.checkpoint();
        await logger.flush();
      },
    });
  }
  catch (err) {
    await closeServices({ reason: "lode readiness failed", fatal: true, err });
    return;
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
