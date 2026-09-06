// HOST-CORE — the logger seam (docs/host-sdk-design.md §3.1, D7).
//
// The structural subset of a pino-style logger the domain code actually calls:
// level methods taking either `(obj, msg)` or `(msg)`. The standalone app's
// pino-backed `createLogger` satisfies it; embedders pass anything with the
// same shape, or `noopLogger` to discard everything.

export interface LogFn {
  (obj: object, msg?: string): void;
  (msg: string): void;
}

export interface Logger {
  readonly debug: LogFn;
  readonly info: LogFn;
  readonly warn: LogFn;
  readonly error: LogFn;
}

const noop: LogFn = () => {};

/** A `Logger` that discards every line. */
export const noopLogger: Logger = { debug: noop, info: noop, warn: noop, error: noop };
