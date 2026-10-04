# GraphQL SDL contract (v1)

`schema.graphql` in this directory is the **canonical, CI-checked** GraphQL
schema. The **Go plane owns it**: gqlgen generates query-api's executable
schema from this file (schema-first). It is edited directly. The Python
(Strawberry) schema and its export were removed with the Python api.

## Why this exists

CHAOS-4366 (Go API epic Wave 0) requires the invariant

```
checked-in canonical SDL == gqlgen input SDL == web codegen SDL
```

to be a real CI gate, not a convention. The Go test
`TestGeneratedSchemaSourceIsTheCheckedInPin` holds the first equality in this
repo, so drift is caught here, at the source, not only downstream.

## Changing the schema

1. Edit `schema.graphql`.
2. Regenerate through `go run ./cmd/gqlgen-guard generate` (never `go generate`, which has no directive here; see internal/queryapi/server/README.md "Regeneration" for the expected-drift record) and commit the diff;
   `TestGeneratedSchemaSourceIsTheCheckedInPin` fails until you do.
3. Update `schema-digest.json` and the schema-digest history row (the
   digest is the sha256 of this file's raw bytes; see
   `docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md`,
   "When the schema digest moves"). `ci/check_go_api_routing_digest.py`
   fails until you do.

Review the diff -- this file is a contract, not a build artifact.

## Consumers

- **Web codegen** (`dev-health/web/codegen.ts`): GraphQL Code Generator
  points its `schema:` field at a copy of this file
  (`web/src/lib/graphql/schema.graphql`). Web's own CI pins that copy to
  this file; when this file changes, update and commit the web copy too, and
  run web's codegen to refresh generated TS types.
- **`query-api`** (Go, `dho query-api`, `ops/internal/queryapi`, gqlgen schema-first): gqlgen's
  code generation takes this same SDL as its input schema. See
  `ops/internal/queryapi/server/README.md` (or the equivalent docs page once wired)
  for the exact gqlgen config pointing here.
