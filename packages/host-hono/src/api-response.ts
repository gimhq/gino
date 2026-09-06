/**
 * Standard JSON envelope shared by every route. The shape is a
 * superset of the `pma-bun` /  `typescript/patterns.md` reference
 * format so the web app can rely on a single TS type across modules.
 *
 * Most call sites do not need helpers — `c.json({ success: true, data })`
 * is clear enough. The two cases that *do* benefit from a helper:
 *
 *   - paged responses, which need to keep the `meta` shape consistent
 *     across modules;
 *   - error responses, where `code` plus a human message is the
 *     contract the SPA already keys off.
 */

export interface ApiMeta {
  readonly total: number;
  readonly page: number;
  readonly limit: number;
}

export interface ApiSuccess<T> {
  readonly success: true;
  readonly data: T;
  readonly meta?: ApiMeta;
}

export interface ApiError {
  readonly success: false;
  readonly error: {
    readonly code: string;
    readonly message: string;
  };
}

export type ApiResponse<T> = ApiSuccess<T> | ApiError;

export function ok<T>(data: T): ApiSuccess<T> {
  return { success: true, data };
}

/**
 * Paged response. `page` / `limit` mirror what came in via
 * `parsePageQuery(c)`; `total` is the unpaginated `count(*)` from the
 * same query. Pass `data` as the page of rows.
 */
export function paged<T>(data: readonly T[], total: number, page: number, limit: number): ApiSuccess<readonly T[]> {
  return { success: true, data, meta: { total, page, limit } };
}

export function err(code: string, message: string): ApiError {
  return { success: false, error: { code, message } };
}
