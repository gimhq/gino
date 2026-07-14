import type { DnsResolver, FetchLike, WebhookPayload } from "@/channels";
import { describe, expect, it } from "bun:test";
import { deliverWebhook } from "@/channels";

// M6 webhook delivery hardening —
//   HOST-M5M6-05: every request carries an abort timeout, and a transport
//     failure (timeout / connection error) fails the delivery instead of
//     hanging the push loop or throwing out of it.
//   HOST-M5M6-11: a 3xx is a delivery failure — the signed payload and the
//     shared secret are never re-sent to a `Location` target.
//   HOST-M5M6-07: for plain-http targets the request is PINNED to the
//     resolved-and-validated IP (Host header preserved), so fetch's own DNS
//     lookup cannot be rebound into private space between the SSRF check and
//     the connect; https targets keep the hostname URL (TLS certificate
//     validation pins the identity).

const PAYLOAD: WebhookPayload = { channel: "ch", taskId: "t", messages: [{ seq: 1, content: "hello" }] };
const PUBLIC_IP = "93.184.216.34";
const publicResolver: DnsResolver = async () => [{ address: PUBLIC_IP, family: 4 }];

interface SeenRequest {
  url: string;
  headers: Record<string, string> | undefined;
  body: string | undefined;
  signal: AbortSignal | undefined;
}

/** A recording fetch fake that replies with a fixed status (default 200). */
function recordingFetch(status = 200, location?: string): { seen: SeenRequest[]; fetchImpl: FetchLike } {
  const seen: SeenRequest[] = [];
  const fetchImpl: FetchLike = async (url, init) => {
    seen.push({ url, headers: init?.headers, body: init?.body, signal: init?.signal });
    return {
      status,
      headers: { get: (n: string) => (n.toLowerCase() === "location" ? location ?? null : null) },
    };
  };
  return { seen, fetchImpl };
}

describe("M6 webhook delivery — timeout (HOST-M5M6-05)", () => {
  it("aborts a hung endpoint after timeoutMs and fails the delivery instead of stalling forever", async () => {
    let aborted = false;
    const hang: FetchLike = (_url, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          aborted = true;
          reject(init.signal?.reason ?? new Error("aborted"));
        });
      });
    const res = await deliverWebhook(
      { id: "w1", url: "https://hooks.example.com/in" },
      PAYLOAD,
      hang,
      { resolver: publicResolver, timeoutMs: 20 },
    );
    expect(res).toEqual({ ok: false, status: 0 });
    expect(aborted).toBe(true);
  });

  it("passes an AbortSignal to every request (the default fetch aborts on it)", async () => {
    const { seen, fetchImpl } = recordingFetch(200);
    await deliverWebhook({ id: "w1", url: "https://hooks.example.com/in" }, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(seen[0]!.signal).toBeInstanceOf(AbortSignal);
  });

  it("a network error fails the delivery (cursor stays put) instead of throwing out of the push", async () => {
    const boom: FetchLike = async () => {
      throw new Error("ECONNREFUSED");
    };
    const res = await deliverWebhook({ id: "w1", url: "https://hooks.example.com/in" }, PAYLOAD, boom, { resolver: publicResolver });
    expect(res).toEqual({ ok: false, status: 0 });
  });
});

describe("M6 webhook delivery — redirect hardening (HOST-M5M6-11)", () => {
  it("sends the secret + payload exactly once: a 3xx Location target never receives them", async () => {
    const { seen, fetchImpl } = recordingFetch(307, "https://attacker.example/steal");
    const res = await deliverWebhook(
      { id: "w1", url: "https://hooks.example.com/in", secret: "shh" },
      PAYLOAD,
      fetchImpl,
      { resolver: publicResolver },
    );
    expect(res.ok).toBe(false);
    expect(res.status).toBe(307);
    expect(seen).toHaveLength(1); // exactly one request — the redirect is never issued
    expect(seen[0]!.url).toContain("hooks.example.com");
    expect(seen[0]!.headers?.["x-webhook-secret"]).toBe("shh"); // sent only to the registered endpoint
  });

  it("requests with redirect:'manual' so the runtime fetch never auto-follows either", async () => {
    let redirectMode: string | undefined;
    const fetchImpl: FetchLike = async (_url, init) => {
      redirectMode = init?.redirect;
      return { status: 200 };
    };
    await deliverWebhook({ id: "w1", url: "https://hooks.example.com/in" }, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(redirectMode).toBe("manual");
  });
});

describe("M6 webhook delivery — DNS pinning (HOST-M5M6-07)", () => {
  it("pins the vetted resolved IP for http targets, preserving the Host header (port + path + query intact)", async () => {
    const { seen, fetchImpl } = recordingFetch(200);
    const res = await deliverWebhook(
      { id: "w1", url: "http://hooks.example.com:8080/in?x=1" },
      PAYLOAD,
      fetchImpl,
      { resolver: publicResolver },
    );
    expect(res.ok).toBe(true);
    expect(seen[0]!.url).toBe(`http://${PUBLIC_IP}:8080/in?x=1`); // connect target IS the validated address
    expect(seen[0]!.headers?.host).toBe("hooks.example.com:8080");
  });

  it("pins an IPv6 answer with brackets", async () => {
    const v6 = "2606:4700::6810:84e5";
    const resolver: DnsResolver = async () => [{ address: v6, family: 6 }];
    const { seen, fetchImpl } = recordingFetch(200);
    const res = await deliverWebhook({ id: "w1", url: "http://hooks.example.com/in" }, PAYLOAD, fetchImpl, { resolver });
    expect(res.ok).toBe(true);
    expect(seen[0]!.url).toBe(`http://[${v6}]/in`);
    expect(seen[0]!.headers?.host).toBe("hooks.example.com");
  });

  it("resolves exactly once per delivery — a rebinding resolver cannot swap the target after validation", async () => {
    // A fast-rebinding attacker answers PUBLIC on the first lookup and PRIVATE
    // afterwards. Pre-fix, fetch's own second resolution connected to the
    // private answer; pinned, the single vetted answer IS the connect target
    // and no second lookup ever happens.
    let lookups = 0;
    const rebinding: DnsResolver = async () => {
      lookups++;
      return lookups === 1 ? [{ address: PUBLIC_IP, family: 4 }] : [{ address: "169.254.169.254", family: 4 }];
    };
    const { seen, fetchImpl } = recordingFetch(200);
    const res = await deliverWebhook({ id: "w1", url: "http://rebind.evil.example/x" }, PAYLOAD, fetchImpl, { resolver: rebinding });
    expect(res.ok).toBe(true);
    expect(lookups).toBe(1);
    expect(seen[0]!.url).toBe(`http://${PUBLIC_IP}/x`); // the request can only reach the vetted address
  });

  it("keeps the hostname URL for https targets (TLS certificate validation pins the identity; SNI stays intact)", async () => {
    const { seen, fetchImpl } = recordingFetch(200);
    const res = await deliverWebhook({ id: "w1", url: "https://hooks.example.com/in" }, PAYLOAD, fetchImpl, { resolver: publicResolver });
    expect(res.ok).toBe(true);
    expect(seen[0]!.url).toBe("https://hooks.example.com/in");
    expect(seen[0]!.headers?.host).toBeUndefined(); // no override — the URL host is authoritative
  });

  it("still fails closed when the host resolves private — no request is ever issued", async () => {
    const privateResolver: DnsResolver = async () => [{ address: "169.254.169.254", family: 4 }];
    const { seen, fetchImpl } = recordingFetch(200);
    const res = await deliverWebhook({ id: "w1", url: "http://evil.example.com/x" }, PAYLOAD, fetchImpl, { resolver: privateResolver });
    expect(res).toEqual({ ok: false, status: 0 });
    expect(seen).toHaveLength(0);
  });
});
