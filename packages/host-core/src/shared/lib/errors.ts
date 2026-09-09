// HOST-CORE — the error family every public namespace throws.
//
// Core never knows HTTP, but it does know *which class of failure* happened:
// each subclass carries a `statusCode` + a stable machine-readable `code`, and
// `toJSON()` renders the wire error envelope. An adapter maps an `AppError` to
// a response with those two fields and needs no other knowledge; anything that
// is NOT an `AppError` is a bug, not a client error, and should surface as 500.

/**
 * Base class of every error the SDK throws deliberately. `statusCode` is the
 * HTTP status an adapter should use, `code` the stable identifier a client
 * matches on. Thrown directly for cases with no dedicated subclass — the
 * lifecycle 409s (`interrupt` on an offline agent), the 410 for a removed
 * agent, `LAUNCH_UNSUPPORTED` (422), `PAYLOAD_TOO_LARGE` (413).
 * @example
 * try { host.tasks.interrupt(taskId); }
 * catch (err) { if (err instanceof AppError) return json(err.toJSON(), err.statusCode); throw err; }
 */
export class AppError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number = 500,
    public readonly code: string = "INTERNAL_ERROR",
  ) {
    super(message);
    this.name = "AppError";
  }

  toJSON(): { success: false; error: { code: string; message: string; details?: unknown } } {
    return {
      success: false,
      error: { code: this.code, message: this.message },
    };
  }
}

/** `404` / `NOT_FOUND` — the addressed agent, task, thread or artifact does not exist. */
export class NotFoundError extends AppError {
  // `id` is included in the message for debuggability. All ids passed here are
  // opaque nanoids / ULIDs (no PII), so surfacing them is safe.
  constructor(resource: string, id?: string) {
    super(id ? `${resource} ${id} not found` : `${resource} not found`, 404, "NOT_FOUND");
  }
}

/**
 * `422` / `VALIDATION_ERROR` — the input is well-formed but not acceptable
 * (an unsafe artifact name, an unknown sha, a malformed schedule). `details`
 * is opaque to core and rendered into the error envelope verbatim.
 */
export class ValidationError extends AppError {
  constructor(
    message: string,
    public readonly details: unknown,
  ) {
    super(message, 422, "VALIDATION_ERROR");
  }

  toJSON(): { success: false; error: { code: string; message: string; details?: unknown } } {
    return {
      success: false,
      error: { code: this.code, message: this.message, details: this.details },
    };
  }
}

/** `401` / `UNAUTHORIZED` — no credential, or one that does not resolve to an identity. */
export class UnauthorizedError extends AppError {
  constructor(message = "Unauthorized") {
    super(message, 401, "UNAUTHORIZED");
  }
}

/**
 * `403` / `FORBIDDEN` — the identity is known but not permitted: an agent
 * reaching for another task's bytes, an owner without a grant on an agent.
 */
export class ForbiddenError extends AppError {
  constructor(message = "Forbidden") {
    super(message, 403, "FORBIDDEN");
  }
}
