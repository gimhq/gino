import { describe, expect, it } from "bun:test";
import { err, ok, paged } from "@/shared/lib/api-response";

describe("api-response", () => {
  it("ok wraps data in a success envelope", () => {
    expect(ok({ a: 1 })).toEqual({ success: true, data: { a: 1 } });
  });

  it("paged carries meta", () => {
    expect(paged([1, 2], 5, 1, 2)).toEqual({
      success: true,
      data: [1, 2],
      meta: { total: 5, page: 1, limit: 2 },
    });
  });

  it("err carries code + message", () => {
    expect(err("NOT_FOUND", "missing")).toEqual({
      success: false,
      error: { code: "NOT_FOUND", message: "missing" },
    });
  });
});
