// System module — liveness / readiness probes and build-version read.
// Schemaless (owns no tables). Mounted from `routes/public.ts` because the
// health endpoints must be callable without authentication for orchestrator
// probes.

export { systemRoutes } from "./system.routes";
