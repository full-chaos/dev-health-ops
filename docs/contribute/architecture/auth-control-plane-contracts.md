---
page_id: con-auth-cp-contracts
summary: Contributor guide to the Auth Control Plane wire contracts under contracts/auth/v1 - the seven shipped surfaces, the manifest-driven corpus, the Go, Python and ajv runners that must agree, the credential-class registry tests, and how to add or change a surface.
content_type: task-guide
owner: engineering
source_of_truth:
  - contracts/auth/v1/README.md
  - contracts/auth/v1/jsonschema
  - contracts/auth/v1/examples
  - internal/auth/contracts
  - internal/auth/contracts/
  - ci/auth-contracts-ajv/run.mjs
  - contracts/auth/v1/credential-classes.json
applicability: current
lifecycle: active
---

# Auth Control Plane contracts and SDK guide

`contracts/auth/v1/` is the language-neutral source of truth for what the Auth
Control Plane emits and accepts. The JSON Schemas, the OpenAPI document that
`$ref`s them and the golden fixtures are authoritative; the Go and Python types
are implementations of them, never the other way round. This page is the
contributor map; the rules each surface inherited live in
[`contracts/auth/v1/README.md`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/README.md)
and are not repeated here.
{: .fc-page-lede }

## What ships

| Surface | Schema | Go | Python |
| --- | --- | --- | --- |
| `principal.v1` | `jsonschema/principal.v1.schema.json` | `internal/auth/contracts/principal.go` | `src/dev_health_ops/authclient/principal.py` (removed with the Python api) |
| `error.v1` | `jsonschema/error.v1.schema.json` | `error_envelope.go` | `authclient/error_envelope.py` |
| `jwks.v1`, `discovery.v1` | `jsonschema/jwks.v1...`, `discovery.v1...` | `contracts.go` (`Validate`) | `authclient/discovery.py` |
| `provisioning-event.v1` | `jsonschema/provisioning-event.v1.schema.json` | `contracts.go` | `authclient/provisioning_event.py` |
| `audit-event.v1` | `jsonschema/audit-event.v1.schema.json` | `contracts.go` | `authclient/provisioning_event.py` (`parse_audit_event`) |
| `endpoint-auth-profile.v1` | `jsonschema/endpoint-auth-profile.v1.schema.json` | `endpoint_auth_profile.go` | `authclient/contracts.py` (schema validation) |
| `access-token.v1` | `jsonschema/access-token.v1.schema.json` | `access_token.go` (`ParseAccessToken`) | none |

`openapi/auth-v1.openapi.json` references the schemas. The TypeScript half lives in
`dev-health-web` and reads the same schema files and fixtures; it is not part of
this repository.

Distinct from these are the **inventory** contracts, which describe the
credentials that exist today rather than what the new service emits:
`credential-classes.json` (the closed credential vocabulary, with issuer,
validators, lifecycle and consumers per class) and
`endpoint-profiles.query-api.json` and `endpoint-profiles.auth-service.json`
(the authentication profile of every query-api and auth-service route). See the [credential classes](../../reference/auth/credential-classes.md)
reference.

## The corpus: one manifest per surface

Each surface has `examples/<surface>/manifest.json`, the **single inventory**
read by every runner. A runner that enumerates the directory itself is a review
defect, because three independent file lists are the drift the cross-language
tests exist to catch. A manifest has three categories:

- `accept`: must validate. It carries as much weight as `reject`; a validator
  that is too strict passes every rejection test while breaking real callers.
- `reject`: must fail at a declared `expect_instance_location` and
  `expect_keyword`; each fixture violates exactly one location.
- `reject_by_client`: valid JSON Schema, refused by the client guard (for
  example an integer written as `1.0`). File these here, not in `reject`.

Fixtures are synthetic, and a scanner test fails the build on anything that
looks like a real credential, in values and in prose.

## The runners that must agree

| Runner | Where | Runs |
| --- | --- | --- |
| Go | `internal/auth/contracts/*_goldens_test.go`, `go test ./internal/auth/contracts/` | Part of the [auth gate](auth-control-plane-go-gates.md) and the Go quality job. |
| Python | `tests/authclient/test_*_contract_goldens.py` | The Python test job. |
| ajv 2020-12, strict | `ci/auth-contracts-ajv/run.mjs`, workflow `auth-contracts-ajv.yml` | Any change under `contracts/auth/v1/`; `cd ci/auth-contracts-ajv && npm ci && node run.mjs` locally. Exit 1: a fixture disagrees. Exit 2: a schema did not compile. |

The workflow has no `paths:` trigger: the job always runs and always reports,
and only the work is conditional (`scope.sh`), because a check that is absent
is not a check that passed.

## Credential classes and routes

`credential-classes.json` is validated by `credential-classes.schema.json` and
by registry tests. Two are worth knowing before you touch a class:

- Every anchor carries a `note` naming the symbol its line holds, and a test
  (`TestEndpointProfileAnchorsStillPointAtTheirSymbols` in
  `internal/queryapi/server`) fails when a line drifts.
- `TestClassConsumersNameEveryQueryAPIRouteThatAcceptsThem` fails when a
  query-api route lists a class in its profile row but that class's `consumers`
  do not name the route. Edit the consumer description surgically; do not
  reformat the frozen file.

## Add or change a surface

1. Edit or add the schema under `jsonschema/`, keeping to the cross-language
   dialect intersection described in the contracts README. One constraint per
   property (a redundant `maxLength` beside a pattern makes Go and Python report
   different keywords).
2. Add fixtures to the manifest in all three categories as needed, each
   violating one location.
3. Add or extend the Go validator and the Python client; guard every integer on
   the raw number token in both.
4. Run Go, Python and ajv on **one tip** and paste the output, then show a
   mutation table: delete each guard, confirm exactly the predicted fixtures go
   red, restore (the README's proof obligation).

## Verified against

- `contracts/auth/v1/jsonschema/*.schema.json`, `contracts/auth/v1/openapi/auth-v1.openapi.json`.
- `internal/auth/contracts` and `src/dev_health_ops/authclient` (removed with the Python api) file names above; `tests/authclient/` (removed with the Python api).
- `ci/auth-contracts-ajv/run.mjs`, `scope.sh`; `.github/workflows/auth-contracts-ajv.yml`.
- `internal/queryapi/server/endpoint_profile_rows_test.go`: the two registry tests.
