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

export class NotFoundError extends AppError {
  // `id` is included in the message for debuggability. All ids passed here are
  // opaque nanoids / ULIDs (no PII), so surfacing them is safe.
  constructor(resource: string, id?: string) {
    super(id ? `${resource} ${id} not found` : `${resource} not found`, 404, "NOT_FOUND");
  }
}

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

export class UnauthorizedError extends AppError {
  constructor(message = "Unauthorized") {
    super(message, 401, "UNAUTHORIZED");
  }
}

export class ForbiddenError extends AppError {
  constructor(message = "Forbidden") {
    super(message, 403, "FORBIDDEN");
  }
}
