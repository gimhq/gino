import { describe, expect, it } from "bun:test";
import { nanoid, ulid } from "../../src/shared/lib/id";

describe("ulid", () => {
  it("produces 26-char Crockford-Base32 strings", () => {
    const id = ulid();
    expect(id).toHaveLength(26);
    expect(id).toMatch(/^[0-9a-hjkmnp-tv-z]{26}$/);
  });

  it("is monotonically increasing within the same millisecond", () => {
    const ids = Array.from({ length: 1000 }, () => ulid());
    const sorted = [...ids].sort();
    expect(ids).toEqual(sorted);
    // All unique.
    expect(new Set(ids).size).toBe(ids.length);
  });
});

describe("nanoid", () => {
  // HOST-M9-11 — 16 chars (~82.7 bits) so row-volume ids (attachments) stay
  // collision-free; the old 8-char form hit 50 % collision odds at ~1.7 M ids.
  it("produces 16-char lowercase alphanumeric ids", () => {
    const id = nanoid();
    expect(id).toHaveLength(16);
    expect(id).toMatch(/^[0-9a-z]{16}$/);
  });
});
