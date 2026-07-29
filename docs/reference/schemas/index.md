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
answers, claims, metrics, evidence references and evidence expansion, scope
resolution, bounded tool messages, feedback, stream events, and safe errors.

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

`dev_conversation_transcript.v1` is the only public persisted-history shape for
both the permanent Ask Dev window and `/dev`. It is cursor-paginated to 100
entries and exposes only the user's bounded question and scope or a validated
`dev_answer.v1`, together with safe run linkage. It never exposes rendered
storage content, tool payloads, provider messages, prompts, or credentials.
Retention-zero, deleted, expired, and non-owned conversations return the same
not-found response. `dev_message_request.v1.retry_of_run_id` is optional and
creates a new run linked to a terminal run in the same owned conversation; it
does not mutate or replace the prior question, answer, or run.

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

Evidence search returns at most 25 `dev_evidence_ref.v1` objects. Expansion
accepts at most 10 persisted references and emits at most 64 KiB of sanitized,
delimited `UNTRUSTED_DATA` excerpts. The stable evidence ID protects the
canonical source descriptor against tampering; it is not an authorization
grant. Every expansion re-resolves current organization, repository, entity,
and user access before reading the native or optional ACR source.

### Provider decision boundary

The provider SDK contract is internal and intentionally does not add
provider-specific fields to `dev_answer.v1`. A provider decision is normalized
to exactly one tool request, final structured answer, disambiguation, or
refusal. Usage, latency, provider/model fingerprints, cancellation, capability
limits, and safe error codes remain operational metadata. The public answer's
model disclosure uses only the canonical provider, model, and `platform | byo`
source fields.
