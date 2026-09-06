import { resolve } from "node:path";
import { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "@gino/host-core";
import { describe, expect, it } from "bun:test";
import { hostOptionsFromConfig } from "@/app";
import { loadConfigStrict } from "@/config";

// HOST — HostOptions: the env schema defaults and the SDK defaults must agree,
// `hostOptionsFromConfig` is the one Config → HostOptions mapping, and the
// resolver merges the nested groups key-by-key.

describe("hostOptionsFromConfig", () => {
  it("an unconfigured env maps to DEFAULT_HOST_OPTIONS (defaults mirror the schema)", () => {
    // `loadConfigStrict` resolves STORE_DIR against cwd; the SDK default stays relative.
    const expected = { ...DEFAULT_HOST_OPTIONS, storeDir: resolve(DEFAULT_HOST_OPTIONS.storeDir) };
    expect(hostOptionsFromConfig(loadConfigStrict({ NODE_ENV: "test" }))).toEqual(expected);
  });

  it("maps every env key onto its option", () => {
    const options = hostOptionsFromConfig(loadConfigStrict({
      NODE_ENV: "test",
      GINO_STORE_DIR: "/x",
      GINO_MAX_UPLOAD_BYTES: "1",
      GINO_REGISTER_TOKEN: "t",
      GINO_REGISTER_ENABLED: "false",
      GINO_CONFIG_GC_ENABLED: "true",
      GINO_WEBHOOK_MAX_ATTEMPTS: "2",
    }));
    expect(options.storeDir).toBe("/x");
    expect(options.maxUploadBytes).toBe(1);
    expect(options.registerToken).toBe("t");
    expect(options.registrationEnabled).toBe(false);
    expect(options.configGc.enabled).toBe(true);
    expect(options.webhooks.maxAttempts).toBe(2);
  });
});

describe("resolveHostOptions", () => {
  it("merges a partial nested group over the nested defaults", () => {
    const options = resolveHostOptions({ configGc: { enabled: true } });
    expect(options.configGc).toEqual({ ...DEFAULT_HOST_OPTIONS.configGc, enabled: true });
    expect(options.webhooks).toEqual(DEFAULT_HOST_OPTIONS.webhooks);
    expect(options.storeDir).toBe(DEFAULT_HOST_OPTIONS.storeDir);
  });
});
