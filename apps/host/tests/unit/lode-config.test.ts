import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { readLodeConfig } from "@/lode/config";

// readLodeConfig parses the operator's lode.toml (via the SDK's $LODE_CONFIG
// locator) and surfaces the non-secret update/trust/runtime config for
// /system/version. Secret tables ([env], [http].headers) are never read.
const ENV = ["LODE_DIR", "LODE_CONFIG"] as const;
const saved: Record<string, string | undefined> = Object.fromEntries(ENV.map(k => [k, process.env[k]]));

let dir: string;
let configFile: string;

function setEnv(values: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(values)) {
    if (v === undefined)
      delete process.env[k];
    else
      process.env[k] = v;
  }
}

function writeToml(body: string): void {
  writeFileSync(configFile, body);
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gino-lode-cfg-"));
  configFile = join(dir, "lode.toml");
  // The SDK reads $LODE_CONFIG (lode injects it); LODE_DIR is irrelevant here.
  setEnv({ LODE_DIR: undefined, LODE_CONFIG: configFile });
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
  setEnv(saved);
});

describe("readLodeConfig", () => {
  test("not_configured without LODE_CONFIG", () => {
    setEnv({ LODE_CONFIG: undefined });
    expect(readLodeConfig().status).toBe("not_configured");
  });

  test("unreadable when the config file is absent", () => {
    expect(readLodeConfig().status).toBe("unreadable");
  });

  test("malformed on invalid TOML", () => {
    writeToml("this is = = not toml");
    expect(readLodeConfig().status).toBe("malformed");
  });

  test("parses a github source config", () => {
    writeToml(`
[global]
app = "gino"

[update]
github = "gimhq/gino"
asset = "gino-linux-x64.tar.gz"
channel = "stable"
policy = "check"
check_interval = 300
keep_versions = 3

[trust]
require_signature = "auto"

[runtime]
runtime = "bun"
version = "1.3.14"
`);
    const c = readLodeConfig();
    expect(c.status).toBe("available");
    expect(c.app).toBe("gino");
    expect(c.sourceType).toBe("github");
    expect(c.source).toBe("gimhq/gino");
    expect(c.asset).toBe("gino-linux-x64.tar.gz");
    expect(c.channel).toBe("stable");
    expect(c.policy).toBe("check");
    expect(c.checkInterval).toBe(300);
    expect(c.keepVersions).toBe(3);
    expect(c.requireSignature).toBe("auto");
    expect(c.runtime).toBe("bun");
    expect(c.runtimeVersion).toBe("1.3.14");
  });

  test("exposes only the host for a manifest source", () => {
    writeToml(`
[update]
manifest = "https://releases.example.com/gino/manifest.json?token=secret"
asset = "gino-linux-x64.tar.gz"
`);
    const c = readLodeConfig();
    expect(c.sourceType).toBe("manifest");
    expect(c.source).toBe("releases.example.com");
  });
});
