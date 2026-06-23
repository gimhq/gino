import { customAlphabet } from "nanoid";

// Crockford Base32 — the ULID-spec alphabet (excludes I, L, O, U).
const ENCODING = "0123456789abcdefghjkmnpqrstvwxyz";
const ENCODING_LEN = 32;
const RANDOM_LEN = 16;
const NANOID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";

const randomChars = customAlphabet(ENCODING, RANDOM_LEN);

export const nanoid = customAlphabet(NANOID_ALPHABET, 8);

function encodeTime(now: number): string {
  let time = now;
  const chars: string[] = [];
  for (let i = 0; i < 10; i++) {
    chars.unshift(ENCODING[time % ENCODING_LEN]!);
    time = Math.floor(time / ENCODING_LEN);
  }
  return chars.join("");
}

// Lexicographically increment a Crockford-Base32 string. Returns "" on
// overflow (all chars at max). Used only when the per-ms ULID call rate
// exceeds the randomness budget — the caller bumps the timestamp by 1ms.
function incrementBase32(s: string): string {
  const arr = s.split("");
  for (let i = arr.length - 1; i >= 0; i--) {
    const idx = ENCODING.indexOf(arr[i]!);
    if (idx < ENCODING_LEN - 1) {
      arr[i] = ENCODING[idx + 1]!;
      return arr.join("");
    }
    arr[i] = ENCODING[0]!;
  }
  return "";
}

// Per-process monotonic state. Two ULIDs minted in the same millisecond
// must compare in insertion order — otherwise rapid-fire inserts (cron
// log rows, batch writes) lose the "id ASC = creation order" invariant
// that callers rely on for ordering and same-ms tiebreaks. Following
// the ULID monotonic spec: same/earlier ms → reuse last ms + increment
// the random tail; clock leap → fresh random.
let lastMs = -1;
let lastRandom = "";

export function ulid(): string {
  const now = Date.now();
  let ts: number;
  let tail: string;
  if (now > lastMs) {
    ts = now;
    tail = randomChars();
  }
  else {
    // Same ms (typical) or clock went backwards (NTP step). Keep the
    // last ms so output stays monotonic regardless of wall-clock jitter.
    ts = lastMs;
    const next = incrementBase32(lastRandom);
    if (next) {
      tail = next;
    }
    else {
      // 2^80 random tail exhausted in a single ms — astronomically
      // unlikely but spec-compliant fallback: advance ts by 1ms and
      // start fresh.
      ts = lastMs + 1;
      tail = randomChars();
    }
  }
  lastMs = ts;
  lastRandom = tail;
  return encodeTime(ts) + tail;
}
