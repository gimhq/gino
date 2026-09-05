import type { DnsResolver, FetchLike, WebhookEndpoint, WebhookPayload } from "@/channels";
import type { AppDatabase } from "@/db";
import type { Channel } from "@/db/schema";
import type { Storage } from "@/host/ports/storage";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  assertSafeWebhookUrl,
  assertSafeWebhookUrlResolved,
  bindConversation,
  deliverWebhook,
  isTaskOnChannel,
  pollChannel,
  registerChannel,
  tailTask,
} from "@/channels";
import { createDb } from "@/db";
import { writeThreadMessage } from "@/msgstore";
import { AppError } from "@/shared/lib/errors";
import { sqliteStorage } from "@/store/sqlite";
import { createTask } from "@/tasks";

// D-B4s SECURITY regression — two HIGH findings in the merged M6 channels code:
//   FINDING 1 (IDOR): pollChannel/tailTask trusted a caller-supplied taskId with
//     no ownership/binding check → a channel-token holder could read ANY task's
//     transcript. The fix authorizes the explicit taskId (bound OR same-owner) and
//     surfaces 404 otherwise.
//   FINDING 2 (SSRF): deliverWebhook auto-followed redirects (a public URL could
//     3xx into private space) and assertSafeWebhookUrl checked only the hostname
//     string (DNS-rebind / CNAME bypass). The fix uses redirect:"manual" and
//     resolves DNS, rejecting any private/loopback IP. HOST-M5M6-11 tightened
//     this further: a 3xx is now a delivery FAILURE — the secret + transcript
//     are never re-sent to a Location target, public or not.

const THREAD = "thread-sec-1";

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

function channel(ownerId: string): Channel {
  return registerChannel(db, { name: `c-${ownerId}`, type: "http", ownerId });
}

function task(ownerId: string): string {
  return createTask(db, { ownerId, engine: "claude", model: "sonnet" }).id;
}

function seed(taskId: string): void {
  writeThreadMessage(storage, { taskId, threadId: THREAD, data: { seq: 1, kind: "assistant_text", content: "secret transcript" } });
}

/** Assert the thrown error is a 404 NotFound (AppError, statusCode 404). */
function expect404(fn: () => unknown): void {
  let caught: unknown;
  try {
    fn();
  }
  catch (err) {
    caught = err;
  }
  expect(caught).toBeInstanceOf(AppError);
  expect((caught as AppError).statusCode).toBe(404);
}

describe("M6 security — FINDING 1: explicit-taskId IDOR (cross-tenant transcript read)", () => {
  it("refuses (404) an explicit taskId the channel neither owns nor is bound to", () => {
    const ch = channel("owner-A");
    const victim = task("owner-B"); // a different tenant's task — NOT bound to ch
    seed(victim);

    // The IDOR: an owner-A channel asks for owner-B's task by id.
    expect404(() => pollChannel(db, ch, { taskId: victim }));
    expect404(() => tailTask(db, ch, victim)); // defense in depth: lower-level API also refuses
    expect(isTaskOnChannel(db, ch, victim)).toBe(false);

    // It must NOT leak the transcript by any path.
    expect404(() => pollChannel(db, ch, { taskId: victim, after: 0 }));
  });

  it("allows an explicit taskId that shares the channel's owner", () => {
    const ch = channel("owner-A");
    const mine = task("owner-A"); // same owner → authorized (OD16)
    seed(mine);

    expect(isTaskOnChannel(db, ch, mine)).toBe(true);
    const deliveries = pollChannel(db, ch, { taskId: mine });
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]!.messages.map(m => m.content)).toEqual(["secret transcript"]);
    expect(() => tailTask(db, ch, mine)).not.toThrow();
  });

  it("allows an explicit taskId bound to the channel even if owned elsewhere", () => {
    const ch = channel("owner-A");
    const bound = task("owner-B"); // different owner, but explicitly bound to this channel
    bindConversation(db, ch.id, "conv-x", bound);
    seed(bound);

    expect(isTaskOnChannel(db, ch, bound)).toBe(true);
    const deliveries = pollChannel(db, ch, { taskId: bound });
    expect(deliveries[0]!.messages).toHaveLength(1);
  });

  it("keeps the channel-scoped binding/fan-out paths working (no regression)", () => {
    const ch = channel("owner-A");
    const bound = task("owner-B");
    bindConversation(db, ch.id, "conv-y", bound);
    seed(bound);

    // conversationRef-resolved and all-bound fan-out are channel-scoped and
    // unchanged (reads are non-destructive, so the two polls don't couple).
    expect(pollChannel(db, ch, { conversationRef: "conv-y", after: 0 })[0]!.messages).toHaveLength(1);
    expect(pollChannel(db, ch, { after: 0 }).some(d => d.taskId === bound)).toBe(true);
  });
});

const PAYLOAD: WebhookPayload = { channel: "ch", taskId: "t", messages: [] };
const ENDPOINT: WebhookEndpoint = { id: "w1", url: "https://hooks.example.com/in" };
const PUBLIC_IP = "93.184.216.34";
const publicResolver: DnsResolver = async () => [{ address: PUBLIC_IP, family: 4 }];

/** A fetch fake that records every requested URL and replies per a scripted queue. */
function scriptedFetch(steps: Array<{ status: number; location?: string }>): { calls: string[]; fetchImpl: FetchLike } {
  const calls: string[] = [];
  const fetchImpl: FetchLike = async (url) => {
    calls.push(url);
    const step = steps[Math.min(calls.length - 1, steps.length - 1)]!;
    return {
      status: step.status,
      headers: { get: (n: string) => (n.toLowerCase() === "location" ? step.location ?? null : null) },
    };
  };
  return { calls, fetchImpl };
}

describe("M6 security — FINDING 2a: webhook redirect SSRF (no auto-follow into private)", () => {
  it("does NOT follow a 3xx redirect into a literal private host", async () => {
    const { calls, fetchImpl } = scriptedFetch([{ status: 302, location: "http://127.0.0.1/admin" }]);
    const res = await deliverWebhook(ENDPOINT, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(res.ok).toBe(false);
    expect(calls).toHaveLength(1); // only the initial request; the redirect was never issued
    expect(calls[0]).toContain("hooks.example.com");
  });

  it("does NOT follow a 3xx redirect into a host that DNS-resolves to private (rebind)", async () => {
    const { calls, fetchImpl } = scriptedFetch([{ status: 301, location: "https://rebind.evil.example/x" }]);
    const resolver: DnsResolver = async host =>
      host === "rebind.evil.example" ? [{ address: "169.254.169.254", family: 4 }] : [{ address: PUBLIC_IP, family: 4 }];
    const res = await deliverWebhook(ENDPOINT, PAYLOAD, fetchImpl, { resolver });
    expect(res.ok).toBe(false);
    expect(calls).toHaveLength(1);
  });

  it("treats EVERY 3xx as a delivery failure — even a PUBLIC Location is never followed (HOST-M5M6-11)", async () => {
    const { calls, fetchImpl } = scriptedFetch([
      { status: 302, location: "https://final.example.com/hook" },
      { status: 200 },
    ]);
    const res = await deliverWebhook(ENDPOINT, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(res.ok).toBe(false); // cursor not advanced — the batch re-delivers to the REGISTERED endpoint
    expect(res.status).toBe(302);
    expect(calls).toHaveLength(1); // the payload (and secret) are sent exactly once, to the registered URL only
    expect(calls[0]).toContain("hooks.example.com");
  });

  it("delivers normally to a public host (no regression)", async () => {
    const { calls, fetchImpl } = scriptedFetch([{ status: 200 }]);
    const res = await deliverWebhook(ENDPOINT, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(res.ok).toBe(true);
    expect(calls).toHaveLength(1);
  });
});

describe("M6 security — FINDING 2b: assertSafeWebhookUrlResolved (DNS-rebind defeat)", () => {
  it("rejects a hostname that resolves to loopback / link-local / private / unique-local", async () => {
    const cases: Array<[string, string]> = [
      ["loopback", "127.0.0.1"],
      ["link-local (cloud metadata)", "169.254.169.254"],
      ["private 10/8", "10.1.2.3"],
      ["private 192.168/16", "192.168.1.5"],
      ["ipv6 unique-local", "fd00::1"],
    ];
    for (const [, ip] of cases) {
      const resolver: DnsResolver = async () => [{ address: ip, family: ip.includes(":") ? 6 : 4 }];
      await expect(assertSafeWebhookUrlResolved("https://evil.example.com/x", resolver)).rejects.toThrow();
    }
  });

  it("rejects when ANY resolved address is private (mixed answer)", async () => {
    const resolver: DnsResolver = async () => [
      { address: PUBLIC_IP, family: 4 },
      { address: "10.0.0.9", family: 4 },
    ];
    await expect(assertSafeWebhookUrlResolved("https://mixed.example.com/x", resolver)).rejects.toThrow();
  });

  it("rejects an empty resolution (fail-closed)", async () => {
    const resolver: DnsResolver = async () => [];
    await expect(assertSafeWebhookUrlResolved("https://ghost.example.com/x", resolver)).rejects.toThrow();
  });

  it("allows a hostname that resolves only to public addresses", async () => {
    const url = await assertSafeWebhookUrlResolved("https://hooks.example.com/in", publicResolver);
    expect(url.hostname).toBe("hooks.example.com");
  });

  it("skips DNS for a literal public IP and still rejects a literal private IP", async () => {
    let consulted = false;
    const spy: DnsResolver = async () => {
      consulted = true;
      return [{ address: "10.0.0.1", family: 4 }];
    };
    const url = await assertSafeWebhookUrlResolved(`https://${PUBLIC_IP}/x`, spy);
    expect(url.hostname).toBe(PUBLIC_IP);
    expect(consulted).toBe(false); // literal IP → no DNS lookup
    await expect(assertSafeWebhookUrlResolved("http://127.0.0.1/x", spy)).rejects.toThrow();
  });
});

describe("M6 security — HOST-M5M6-02: SSRF guard normalizes IP-literal encodings", () => {
  it("rejects IPv4-mapped IPv6, unspecified, and alternate-encoded private literals at registration", () => {
    const blocked = [
      "http://[::ffff:127.0.0.1]/", // IPv4-mapped loopback
      "http://[::ffff:169.254.169.254]/", // IPv4-mapped cloud metadata
      "http://[::]/", // IPv6 unspecified
      "http://0.0.0.0:8080/", // IPv4 unspecified
      "http://100.64.0.1/", // CGNAT
      "http://2130706433/", // decimal 127.0.0.1 (canonicalized by the URL parser)
      "http://0x7f000001/", // hex 127.0.0.1
      "http://017700000001/", // octal 127.0.0.1
      "http://[64:ff9b::a9fe:a9fe]/", // NAT64 wrapping the metadata IP
    ];
    for (const url of blocked)
      expect(() => assertSafeWebhookUrl(url)).toThrow(AppError);
  });

  it("literal IPv6 skips DNS at delivery time — but only because the static check now classifies it", async () => {
    const spy: DnsResolver = async () => {
      throw new Error("must not resolve a literal IP");
    };
    await expect(assertSafeWebhookUrlResolved("http://[::ffff:10.0.0.1]/x", spy)).rejects.toThrow(AppError);
    const url = await assertSafeWebhookUrlResolved("http://[2001:4860:4860::8888]/x", spy);
    expect(url.hostname).toBe("[2001:4860:4860::8888]");
  });

  it("still allows a public mapped-IPv4 literal (no over-blocking)", () => {
    const url = assertSafeWebhookUrl("http://[::ffff:8.8.8.8]/hook");
    expect(url.protocol).toBe("http:");
  });
});
