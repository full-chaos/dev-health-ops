---
page_id: ref-schemas
summary: Versioned public schemas, canonical record kinds, enums, examples, and compatibility rules.
content_type: landing
owner: platform-api
applicability: current
lifecycle: active
---

# Schemas and contracts

- [Record kinds and enums](record-kinds-and-enums.md)

Public schemas must be generated from the same models that validate requests. Examples must be tested against that schema. Internal storage rows are not automatically public wire contracts.

## Ask Dev v1 contracts

Ask Dev's provider-neutral contracts are generated from the strict Pydantic
models in `src/dev_health_ops/api/dev/contracts.py`. The checked-in Draft
2020-12 schemas, fixture manifest, and positive and negative goldens live in
`contracts/ask-dev/v1/`. They cover capabilities, conversations, requests,
answers, claims, metrics, evidence, scope resolution, bounded tool messages,
feedback, stream events, and safe errors.

Generate and verify the artifacts from the repository root:

```bash
python -m dev_health_ops.api.dev.export_contracts write
python -m dev_health_ops.api.dev.export_contracts check
pytest tests/api/dev/test_contracts.py
```

The web repository vendors the exact generated schemas and examples, records
their source commit and SHA-256 digests, and generates TypeScript declarations
with `json-schema-to-typescript`. Its contract check fails when a copied
artifact, generated declaration, manifest digest, or fixture changes without
regeneration.

### Compatibility and version changes

Within `v1`, changes may add optional fields or add an enum value only after all
consumers tolerate it. Removing or renaming fields, making an optional field
required, narrowing a bound, changing a field's meaning, or changing stream
ordering is incompatible and requires a new schema version. A version change
must update the canonical Pydantic model, every affected positive and negative
fixture, generated schemas, web artifacts and declarations, compatibility
tests, release notes, and the PRD/TRD or decision record when product semantics
change. Old versions remain accepted for the documented compatibility window;
do not silently reinterpret an old version.

`dev_message_request.v1.question` is additionally limited to 8 KiB of UTF-8,
and feedback comments to 2 KiB of UTF-8. JSON Schema's `maxLength` counts code
points, so the schemas carry `x-max-utf8-bytes` and the authoritative Pydantic
validation enforces the byte bound.
