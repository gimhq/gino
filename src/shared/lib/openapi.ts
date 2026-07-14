// OpenAPI integration helpers, shared by every module's `*.routes.ts`.
//
// The host keeps plain `new Hono<AppEnv>()` route factories; OpenAPI is
// layered on via `hono-openapi` middleware:
//
//   - `describeRoute({ tags, summary, responses })` documents a route and
//     makes it appear in the generated spec (a route WITHOUT describeRoute is
//     invisible to the spec — see `modules/docs`).
//   - `validator(target, schema)` validates `query` / `json` / `param` / etc.
//     and feeds the request schema into the spec. It wraps the hono-openapi
//     validator with a hook that throws our `ValidationError`, so a bad
//     request yields the project's standard 422 envelope.
//
// Convention: every route declares `describeRoute`; routes that validate
// input with a Zod schema use `validator(...)`.
import type { ValidationTargets } from "hono";
import type { StatusCode } from "hono/utils/http-status";
import type { ZodType } from "zod";
import { validator as baseValidator, describeRoute, resolver } from "hono-openapi";
import { z } from "zod";
import { ValidationError } from "@/shared/lib/errors";

export { describeRoute, resolver };

// ─── Tags ───
// One stable tag per host module so the docs UI groups routes by domain.
// Modules beyond System land as workstreams C/D implement them.
export const TAGS = {
  System: "System",
  Agents: "Agents",
  Tasks: "Tasks",
  Messages: "Messages",
  Channels: "Channels",
  Proxy: "Proxy",
  Config: "Config",
} as const;

// ─── Security schemes ───
// Mirrors the `components.securitySchemes` registered in `modules/docs`.
// The host's auth model is bearer tokens (register / connect / channel —
// see docs/host-v2-design.md §M1), NOT session cookies.
type SecurityRequirement = Record<string, string[]>;
export const SECURITY: Record<"bearer", SecurityRequirement[]> = {
  /** `Authorization: Bearer <token>` (admin / channel / connect tokens). */
  bearer: [{ bearerToken: [] }],
};

// ─── Validation (preserves the 422 envelope) ───

type IssuePathSegment = PropertyKey | { readonly key: PropertyKey };
interface StandardIssue {
  readonly message: string;
  readonly path?: ReadonlyArray<IssuePathSegment> | undefined;
}

/**
 * Reproduce Zod's `flatten()` output (`{ formErrors, fieldErrors }`) from the
 * Standard-Schema issue list the validator hook receives, so the `details`
 * field of a 422 stays identical to a direct `schema.parse(...)` path.
 */
function flattenIssues(issues: ReadonlyArray<StandardIssue>): { formErrors: string[]; fieldErrors: Record<string, string[]> } {
  const fieldErrors: Record<string, string[]> = {};
  const formErrors: string[] = [];
  for (const issue of issues) {
    const head = issue.path?.[0];
    const key = head == null
      ? undefined
      : typeof head === "object"
        ? String(head.key)
        : String(head);
    if (key === undefined) {
      formErrors.push(issue.message);
    }
    else {
      (fieldErrors[key] ??= []).push(issue.message);
    }
  }
  return { formErrors, fieldErrors };
}

/**
 * Drop-in replacement for the hono-openapi `validator` that, on failure,
 * throws the project `ValidationError` (HTTP 422, `code: "VALIDATION_ERROR"`)
 * instead of the library default 400. Validated input is read with
 * `c.req.valid(target)` and is fully typed from `schema`.
 */
export function validator<S extends ZodType, T extends keyof ValidationTargets>(target: T, schema: S) {
  return baseValidator(target, schema, (result) => {
    if (!result.success) {
      throw new ValidationError("Validation failed", flattenIssues(result.error));
    }
  });
}

// ─── Response envelope helpers ───
// Build the `responses` map for `describeRoute` from the shared API envelope
// (`{ success, data, meta? }` / `{ success, error }`).

type ResponsesMap = Record<number, { description: string; content?: Record<string, { schema: ReturnType<typeof resolver> }> }>;

const errorEnvelope = z.object({
  success: z.literal(false),
  error: z.object({
    code: z.string(),
    message: z.string(),
    details: z.unknown().optional(),
  }),
});

function json(schema: ZodType) {
  return { "application/json": { schema: resolver(schema) } };
}

function successEnvelope(data: ZodType, withMeta = false) {
  const shape = {
    success: z.literal(true),
    data,
    ...(withMeta
      ? { meta: z.object({ total: z.number(), page: z.number(), limit: z.number() }) }
      : {}),
  };
  return z.object(shape);
}

/** `200` with `{ success: true, data }`. */
export function jsonOk(data: ZodType = z.unknown(), description = "Success"): ResponsesMap {
  return { 200: { description, content: json(successEnvelope(data)) } };
}

/** `201` with `{ success: true, data }`. */
export function jsonCreated(data: ZodType = z.unknown(), description = "Created"): ResponsesMap {
  return { 201: { description, content: json(successEnvelope(data)) } };
}

/** `200` with `{ success: true, data: T[], meta }`. */
export function jsonPaged(item: ZodType, description = "Paged list"): ResponsesMap {
  return { 200: { description, content: json(successEnvelope(z.array(item), true)) } };
}

/** `204 No Content`. */
export function noContent(description = "No content"): ResponsesMap {
  return { 204: { description } };
}

const ERROR_DESCRIPTIONS: Partial<Record<number, string>> = {
  400: "Bad request",
  401: "Unauthorized",
  403: "Forbidden",
  404: "Not found",
  409: "Conflict",
  410: "Gone",
  413: "Payload too large",
  422: "Validation failed",
  429: "Too many requests",
  500: "Internal server error",
  503: "Service unavailable",
};

/** Standard error responses keyed by status, all using the error envelope. */
export function errors(...codes: number[]): ResponsesMap {
  const out: ResponsesMap = {};
  for (const code of codes) {
    out[code] = { description: ERROR_DESCRIPTIONS[code] ?? "Error", content: json(errorEnvelope) };
  }
  return out;
}

/** A raw (non-JSON) response, e.g. Prometheus text or a binary download. */
export function raw(status: StatusCode, mediaType: string, description: string): Record<number, { description: string; content: Record<string, Record<string, never>> }> {
  return { [status as number]: { description, content: { [mediaType]: {} } } };
}
