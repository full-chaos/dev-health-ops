---
page_id: con-auth-cp-go-gates
summary: Reference for the Go build, release and security gates that protect the Auth Control Plane tree - the check_go.sh auth stages, the go-auth-service workflow and its auth-gate fan-in, how relevance and scope are decided, the fuzz registry, the strict vulnerability scan, and the image release checks.
content_type: reference
owner: engineering
source_of_truth:
  - ci/check_go.sh
  - .github/workflows/go-auth-service.yml
  - ci/auth_govulncheck.py
  - ci/auth_scope_candidates.py
  - ci/auth_gate_scope_exempt.txt
  - ci/check_go_containers.sh
  - tests/tooling/test_go_auth_service_workflow.py
applicability: current
lifecycle: active
---

# Auth Control Plane Go gates

The auth tree has its own quality and release gates so a defect in it is caught
before merge by checks that must pass, without making unrelated changes pay for them. All gate logic
is one verb of the repository's Go check script; the workflow only schedules
it.
{: .fc-page-lede }

```bash
bash ci/check_go.sh auth            # everything below, in order
bash ci/check_go.sh auth <stage>    # one stage
```

## Stages (`ci/check_go.sh auth ...`)

| Stage | What it does |
| --- | --- |
| `fmt`, `vet` | `gofmt` and `go vet` over the auth packages (`cmd/auth-service`, `cmd/auth-migrate`, `internal/auth/...`, `internal/audit/...`, `internal/storage/postgres/authschema/...`). |
| `test`, `race` | `go test` and `go test -race` over the same packages, uncached. |
| `middleware` | Runs `TestProductionMiddlewareStackIsMountedAndDriven` and **fails unless it logged `AUTH_PRODUCTION_STACK_EXECUTED`** (guardrail G-69). The test builds the API server from `apiServerOptions`, the function the service itself uses, with a probe route, and drives 404, 405, 413, a recovered panic (no panic text in the body) and the rate limit over real HTTP. A green test name without the marker is not evidence. |
| `fuzz [Target]` | One fuzz run per registered parser, each in its own invocation. Eleven targets: error envelope, revision, timestamp, edge access token, signed token, rate-limit text, forwarded-trust list, signing-key PEM, configuration, email normalization and external identity key. The budget `AUTH_FUZZTIME` (default `20s`) must be whole seconds and at least 5 s, and a target prints `auth-fuzz-proof:` only when the engine reported a non-zero execution count and passed, so a baseline-only run cannot pass. |
| `fuzz-registry` | Fails when a `Fuzz*` function exists in the auth tree without a row in `AUTH_FUZZ_TARGETS`, or a row names a missing function. |
| `vuln` | `govulncheck` (pinned version) over the auth packages, parsed by `ci/auth_govulncheck.py`. **Any reachable finding fails.** There is no ignore list and no baseline in the gate. A pull request that changes the gate itself (`ci/check_go.sh`, `ci/auth_govulncheck.py`, the workflow) is a reviewed change to files that select the gates, so it cannot do so unreviewed. An empty or config-less scan also fails. |
| `paths-selftest` | Executes the relevance rules on fixed inside and outside paths. |
| `scope-selftest` | Fails when a Go package that looks like auth (a path segment starting `auth` or `audit`) or imports the auth schema adapter is neither inside the gate nor named with a reason in `ci/auth_gate_scope_exempt.txt`; a stale or reasonless exemption fails too. Discovery uses `go list -json` for every build tag named in the repo, so a package behind a tag cannot hide. |
| `relevant BASE HEAD` | Prints `relevant=true|false` for a diff. |

## Relevance: who has to pay

A change is relevant when it touches: any file under an auth root, the import
closure of the auth packages (`go list -deps -test`, so a change to a package
the binaries import counts), a package the scope selftest governs, an exempt
package's directory, `contracts/auth/**`, `go.mod`/`go.sum`, or the gate's own
files. Renames are diffed with `--no-renames`, so a file moved **out** of the
tree still reports its old path.

## The workflow (`go-auth-service.yml`)

| Job | Runs | Content |
| --- | --- | --- |
| `auth-changes` | always | Computes `relevant`. Non-pull-request events are always relevant. |
| `auth-go-quality` | when relevant | `paths-selftest`, `scope-selftest`, `fmt`, `vet`, `fuzz-registry`, `vuln`. |
| `auth-test` | when relevant | `test`, `race`, `middleware`. |
| `auth-fuzz (<target>)` | when relevant | One matrix job per fuzz target, so each parser has its own CI-visible name. |
| `auth-gate` | **always** | Fans in. Succeeds when the change is not relevant or the pull request is a draft without `ci:full`; fails when `auth-changes` failed, or when a relevant change has any gate job that did not succeed (failed, cancelled **or skipped**). |

Relevance is decided inside the workflow, not in the trigger, so `auth-gate`
reports a conclusion on every pull request and can be a required check without
blocking unrelated ones. No job reuses a required context name (`test`, `lint`,
`go-quality`, `metrics-executed-proof`); `tests/tooling/test_go_auth_service_workflow.py`
pins the names, the fuzz matrix against the registry and the fan-in.

## Release checks for the images and Compose

These live in `go.yml` and `docker-images.yml`, not the auth workflow:

- `go-container-security` builds `auth-service` and `auth-migrate`, generates an
  SBOM and scans for critical vulnerabilities (Trivy).
- `go-container-smoke` runs both images read-only and non-root: the service must
  be live and not ready without its database and key, and `auth-migrate` must
  fail closed without configuration.
- `go-container-reproducibility` builds each image twice from scratch and
  compares image ids.
- `go-auth-compose-proof` brings up the Compose `auth` profile and probes it;
  see [deployment](auth-control-plane-deployment.md).
- `auth-contracts-ajv` validates the contract corpus; see
  [contracts](auth-control-plane-contracts.md).

## Verified against

- `ci/check_go.sh`: `AUTH_PACKAGES`, `AUTH_FUZZ_TARGETS`, `check_auth*`, `auth_relevant`, `auth_scope_candidates`.
- `.github/workflows/go-auth-service.yml`; `ci/auth_govulncheck.py`; `ci/auth_scope_candidates.py`.
- `internal/auth/authruntime/production_stack_test.go`: the marker and the driven statuses.
