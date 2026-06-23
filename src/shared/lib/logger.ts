import type { ConsolaInstance } from "consola";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { createConsola, LogLevels } from "consola";
import pino from "pino";

const VALID_LEVELS = new Set(["debug", "info", "warn", "error"]);
const LEVEL_PRIORITY: Record<string, number> = { debug: 1, info: 2, warn: 3, error: 4, fatal: 5 };

// Map our config LOG_LEVEL into consola's numeric level (higher = more
// verbose). Pino still records the original level into the file payload;
// this only controls what the coloured terminal reporter prints.
const CONSOLA_LEVEL_FOR: Record<string, number> = {
  debug: LogLevels.debug,
  info: LogLevels.info,
  warn: LogLevels.warn,
  error: LogLevels.error,
};

interface LoggerConfig {
  readonly LOG_LEVEL: string;
  readonly LOG_FILE: string;
  readonly LOG_TO_STDOUT: boolean;
  readonly NODE_ENV: "development" | "production" | "test";
}

interface DestState {
  failed: boolean;
}

type LogName = "debug" | "info" | "warn" | "error" | "fatal";

// Dev/test terminal tee. Pino owns the structured production output; this
// emits a readable, coloured line per call via consola so devs can see what
// is happening without piping the JSON file.
function createDevTee(threshold: number, term: ConsolaInstance) {
  return (name: LogName, msg: string, ctx?: object): void => {
    if ((LEVEL_PRIORITY[name] ?? 0) < threshold)
      return;
    if (ctx)
      term[name](msg, ctx);
    else
      term[name](msg);
  };
}

type DevTee = ReturnType<typeof createDevTee>;

/**
 * Field names that must never be logged in cleartext. Matched
 * case-insensitively against every key in the log payload at any depth,
 * so `{ outer: { user: { password } } }` is redacted the same as
 * `{ password }`. Pino's built-in `redact.paths` only matches a fixed
 * shape (`*.password`) and would silently miss nested occurrences.
 *
 * False positives (a legitimately-named UI label of `dek`, etc.) are
 * preferred over false negatives — logs are operator-facing and a leaked
 * secret can't be unredacted later.
 */
const REDACT_KEYS = new Set([
  "password",
  "token",
  "secret",
  "access_token",
  "accesstoken",
  "refresh_token",
  "refreshtoken",
  "client_secret",
  "clientsecret",
  "encrypteddek",
  "privatekey",
  "dek",
  "errorbody",
  "authorization",
  "cookie",
  "bootstraptoken",
  "bootstrap_token",
]);

const REDACT_MAX_DEPTH = 8;

function deepRedact(value: unknown, depth: number, seen: WeakSet<object>): unknown {
  if (depth > REDACT_MAX_DEPTH || value === null || typeof value !== "object")
    return value;
  if (seen.has(value))
    return "[Circular]";
  seen.add(value);
  if (Array.isArray(value)) {
    return value.map(v => deepRedact(v, depth + 1, seen));
  }
  // Preserve Error shape — pino's default serializer reads `.message`,
  // `.stack`, `.code`. Walk its own properties through the same redactor.
  if (value instanceof Error) {
    const proxy: Record<string, unknown> = {
      name: value.name,
      message: value.message,
      stack: value.stack,
    };
    for (const k of Object.getOwnPropertyNames(value)) {
      if (k in proxy)
        continue;
      proxy[k] = (value as unknown as Record<string, unknown>)[k];
    }
    return deepRedact(proxy, depth + 1, seen);
  }
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (REDACT_KEYS.has(k.toLowerCase()))
      out[k] = "[REDACTED]";
    else
      out[k] = deepRedact(v, depth + 1, seen);
  }
  return out;
}

function sanitizeLogPayload(value: unknown): unknown {
  return deepRedact(value, 0, new WeakSet<object>());
}

function createMethod(
  devTee: DevTee | null,
  file: pino.Logger,
  destState: DestState,
  fallback: ConsolaInstance,
  name: LogName,
) {
  return (objOrMsg: unknown, msg?: string) => {
    // Redact before pino or consola sees the payload — nested secrets must
    // not leak through the degraded-destination path either.
    const safeObj = typeof objOrMsg === "string" ? objOrMsg : sanitizeLogPayload(objOrMsg);
    // Tee to consola when pino's destination has errored (disk full, fd
    // closed) — otherwise async writes buffer silently to a dead stream.
    if (destState.failed) {
      if (typeof safeObj === "string")
        fallback[name](safeObj);
      else
        fallback[name](msg ?? "", safeObj);
      return;
    }
    if (typeof safeObj === "string") {
      devTee?.(name, safeObj);
      file[name](safeObj);
    }
    else {
      devTee?.(name, msg ?? "", safeObj as object);
      file[name](safeObj as object, msg ?? "");
    }
  };
}

export function createLogger(config: LoggerConfig) {
  const level = VALID_LEVELS.has(config.LOG_LEVEL) ? config.LOG_LEVEL : "info";
  const termLevel = CONSOLA_LEVEL_FOR[level] ?? LogLevels.info;

  // One coloured reporter instance is shared by the dev tee and the
  // destination-failed fallback. `withTag("app")` keeps the prefix uniform
  // and makes multi-process logs easier to grep.
  const term = createConsola({ level: termLevel }).withTag("app");

  // In production we go pino-only (file or stdout); in dev/test we also tee
  // to consola so the developer sees readable, coloured lines next to the
  // JSON file.
  const devTee: DevTee | null
    = config.NODE_ENV === "production"
      ? null
      : createDevTee(LEVEL_PRIORITY[level] ?? 2, term);

  if (!config.LOG_TO_STDOUT)
    mkdirSync(dirname(config.LOG_FILE), { recursive: true });

  const dest = pino.destination(
    config.LOG_TO_STDOUT
      ? { dest: 1, sync: false, minLength: 4096 }
      : { dest: config.LOG_FILE, sync: false, minLength: 4096 },
  );

  // If the destination errors (disk full, fd closed, etc.) flip a flag so
  // every subsequent log method tees to consola instead of buffering to a
  // dead stream.
  const destState: DestState = { failed: false };
  dest.on("error", (err: unknown) => {
    destState.failed = true;
    term.error("destination error, falling back to console-only:", err);
  });

  // Redaction is applied by deepRedact in createMethod before the payload
  // reaches pino (or the consola fallback), so pino's own redact.paths is
  // intentionally not configured — a second pass would only run on already-
  // censored data.
  const fileLogger = pino(
    {
      level,
    },
    dest,
  );

  return {
    debug: createMethod(devTee, fileLogger, destState, term, "debug"),
    info: createMethod(devTee, fileLogger, destState, term, "info"),
    warn: createMethod(devTee, fileLogger, destState, term, "warn"),
    error: createMethod(devTee, fileLogger, destState, term, "error"),
    fatal: createMethod(devTee, fileLogger, destState, term, "fatal"),
    // Backend logger flush; calls pino-destination's flushSync, unrelated to
    // React DOM's flushSync API.
    flush: () => {
      if (destState.failed)
        return;
      try {
        dest.flushSync();
      }
      catch {
        // Destination already failed/closed — nothing to flush.
      }
    },
    /**
     * Reopen the file destination — used by the SIGHUP handler in `index.ts`
     * to integrate with logrotate-style external rotators. Flushes the
     * current handle and reopens. No-op when piped to stdout (the runtime
     * owns fd 1) or already failed.
     */
    reopen: () => {
      if (config.LOG_TO_STDOUT || destState.failed)
        return;
      try {
        dest.flushSync();
      }
      catch {}
      try {
        // pino's SonicBoom destination exposes `reopen()` for this exact
        // purpose. Cast through unknown because the public typings do not
        // expose it.
        const reopenable = dest as unknown as { reopen?: () => void };
        reopenable.reopen?.();
      }
      catch (err) {
        destState.failed = true;
        term.error("reopen failed:", err);
      }
    },
  };
}

export type Logger = ReturnType<typeof createLogger>;
