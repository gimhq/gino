// HOST-M1M2-11 — bounded reads for the byte-ingest endpoints. The declared
// Content-Length is rejected up front, and the body is streamed with a running
// cap so a lying (or absent) Content-Length can never balloon host memory: the
// read aborts the moment the cap is crossed, before any hashing or storing.
import { AppError } from "./errors";

/** 413 — rendered by the scaffold error handler with the standard envelope. */
export class PayloadTooLargeError extends AppError {
  constructor(maxBytes: number) {
    super(`request body exceeds the ${maxBytes}-byte upload limit`, 413, "PAYLOAD_TOO_LARGE");
  }
}

/**
 * Read the full request body, throwing `PayloadTooLargeError` if either the
 * declared Content-Length or the actual streamed size exceeds `maxBytes`.
 */
export async function readBodyCapped(req: Request, maxBytes: number): Promise<Uint8Array> {
  const declared = Number(req.headers.get("content-length") ?? Number.NaN);
  if (Number.isFinite(declared) && declared > maxBytes) {
    throw new PayloadTooLargeError(maxBytes);
  }
  if (!req.body) {
    return new Uint8Array(0);
  }
  const reader = req.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      total += value.byteLength;
      if (total > maxBytes) {
        throw new PayloadTooLargeError(maxBytes);
      }
      chunks.push(value);
    }
  }
  finally {
    // Release the stream whether we finished or bailed over the cap.
    reader.cancel().catch(() => {});
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}
