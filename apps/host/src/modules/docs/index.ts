// Docs module — serves the generated OpenAPI spec at `/openapi.json` and the
// Scalar API reference UI at `/docs`. Owns no DB schema; it only reads the
// in-process route table.

export { docsCspRelax, mountDocs } from "./docs.routes";
