import { describe, expect, it } from "bun:test";
import { AppError, NotFoundError, ValidationError } from "@/shared/lib/errors";

describe("errors", () => {
  it("AppError serialises to the standard error envelope", () => {
    const e = new AppError("boom", 500, "INTERNAL_ERROR");
    expect(e.statusCode).toBe(500);
    expect(e.toJSON()).toEqual({ success: false, error: { code: "INTERNAL_ERROR", message: "boom" } });
  });

  it("NotFoundError includes the id in the message", () => {
    const e = new NotFoundError("agent", "abc");
    expect(e.statusCode).toBe(404);
    expect(e.code).toBe("NOT_FOUND");
    expect(e.message).toBe("agent abc not found");
  });

  it("ValidationError surfaces details", () => {
    const e = new ValidationError("bad", { field: ["required"] });
    expect(e.statusCode).toBe(422);
    expect(e.toJSON()).toEqual({
      success: false,
      error: { code: "VALIDATION_ERROR", message: "bad", details: { field: ["required"] } },
    });
  });
});
