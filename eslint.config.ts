import antfu from "@antfu/eslint-config";

export default antfu({
  type: "app",
  typescript: true,
  stylistic: {
    indent: 2,
    quotes: "double",
    semi: true,
  },
  rules: {
    "no-console": "warn",
    "ts/no-explicit-any": "error",
    "ts/consistent-type-imports": ["error", { prefer: "type-imports" }],
  },
  // Host code only. The Rust runner, local host snapshot, build output,
  // runtime data and generated migrations are out of scope for the TS lint.
  ignores: [
    "**/*.json",
    "**/*.toml",
    "**/*.md",
    "**/*.yml",
    "**/*.yaml",
    "gino-agent/**",
    "backup/**",
    // Cross-implementation e2e harness + skills docs: outside the TS lint scope (as before the workspace split).
    "e2e/**",
    "skills/**",
    "**/dist/**",
    "**/node_modules/**",
    "data/**",
    "**/drizzle/**",
    "docs/**",
    // Vendored verbatim from upstream lode (re-copied to update) — not ours to lint.
    "src/lode/sdk.ts",
  ],
}, {
  // Plain-JS entry points run by `node` directly (the package Node smoke
  // tests): no type information, so the typed import rule cannot run.
  files: ["**/*.mjs"],
  rules: {
    "ts/consistent-type-imports": "off",
    "no-console": "off",
  },
}, {
  // Test files construct partial fixtures the strict project rules would
  // refuse — relax the most-friction-prone ones to `warn` / `off`.
  files: [
    "**/*.test.ts",
    "**/tests/**/*.ts",
  ],
  rules: {
    "ts/no-explicit-any": "warn",
    "no-console": "off",
  },
});
