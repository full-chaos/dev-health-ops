---
page_id: con-auth-pdp-contributor
summary: What a contributor changes, and which test fails when it is missed, to add or change an auth catalog action, its route mapping, a shadow class row or an enforcement cell.
content_type: task-guide
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/actions.v1.json
  - contracts/auth/v1/vocabulary/roles.v1.json
  - contracts/auth/v1/graphql-field-actions.json
  - contracts/auth/v1/golden/decision/DIGEST
  - contracts/auth/v1/README.md
  - internal/auth/policycatalog/catalog.go
  - internal/auth/policycatalog/catalog_test.go
  - internal/api/policy/shadow.go
applicability: current
lifecycle: active
---

# Auth policy decision: change guide

How to change the closed action catalog and its readers without breaking a gate. The reference pages hold the facts; this page is the order of work.
{: .fc-page-lede }

!!! warning "Status: shadow is dark; no service calls ShadowFromEnv yet, as of e590f357b896"
    Steps 4 and 5 describe code that is on `main` but not wired into a service. The pull request that wires it updates this banner.

Read first: the [policy catalog reference](../../reference/auth/policy-catalog.md), the [action map](../../reference/auth/action-map.md), the [decision and obligation wire contract](../../reference/auth/obligations.md) and the [wire contracts README](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/README.md).

## 1. Add or change an action

An action is a row in `contracts/auth/v1/vocabulary/actions.v1.json`: name, scope, resource kind, risk class, cache class and impersonation (`denied` or `allowed`). Roles that hold it are rows in `roles.v1.json`. Work in this order:

1. Write the failing test first. `internal/auth/policycatalog/catalog_test.go` pins the action count, the impersonation-denied list, the catalog-only set and the canonical bytes; the new cell states who holds the action and why.
2. Edit `actions.v1.json` and `roles.v1.json`. Both carry the same `catalog_version`: raise both by one. A high-risk action is never cached, and a role never holds every action (the [loader rules](../../reference/auth/policy-catalog.md#rules-the-loader-enforces)).
3. Raise `catalog_version` in `contracts/auth/v1/graphql-field-actions.json`. The map guard compares it with the catalog, so a stale value fails.
4. Re-pin the decision golden vectors: they move to the new `policy_revision`, and `contracts/auth/v1/golden/decision/DIGEST` is re-pinned from the directory hash (`decision_golden_shape_test.go` prints the value it wants). Do this in its own commit.
5. Add the version line to `contracts/auth/v1/README.md` and `docs/reference/auth/policy-catalog.md` in the same pull request.
6. A new action has no effect on a decision until a role holds it, and none on a route until a profile row names it. Say so in the pull request, as the version history does.

Two pull requests that both add an action collide on the version number and the DIGEST. The second one merges main and takes the next number; row and data conflicts only, never a rebase.

## 2. Find the action for a route

Add `action` to the route's row in the endpoint profile file of its service (go-api, query-api or auth-service): see [registering a new route](../../reference/auth/endpoint-profiles.md#contributor-guide-registering-a-new-route-or-credential-class). The profile gate (`CheckActionCachePolicy`) refuses a cached profile whose action is not in the catalog, or is high risk, or whose cache class is not `none` or the catalog's.

## 3. Obligations and reason codes

Decision fields, the five closed obligations and the redaction classes are in the [wire contract](../../reference/auth/obligations.md). A reason code is added in `contracts/auth/v1/vocabulary/reason-codes.json` only with the pull request of the producer that emits it. The shadow class table loads its reason column from the PDP's closed set, so an unknown reason fails the load.

## 4. Keep the shadow in step

The shadow reads the go-api profile rows that name an action (`ParseShadowRows`). It accepts only these namespaces: admin, billing, integrations, license, members, settings, teams, and only organization or platform resources. A row outside them fails the load, so a new namespace is a change to `internal/api/policy/shadow.go` and to the shadow tests, in the same pull request as the catalog row.

When the new action makes the PDP and legacy disagree, the mismatch is `unresolved` and the alert fires. That is correct. Triage it with the [shadow triage runbook](../../operate/runbooks/auth-shadow-pdp-triage.md); add a class row only after review.

## 5. Tests and gates to run

- `go test -count=1 ./internal/auth/...` and `./internal/api/policy/...`; `./internal/queryapi/graph` when a GraphQL field maps to the action.
- `go test -count=1 -tags authhistory ./internal/storage/postgres/authschema/...` when the catalog data reaches the schema.
- `go test -count=1 -tags=authhistory ./internal/auth/contracts/...` when you edit a line an endpoint profile anchors.
- `make docs:check` for the pages above.
- A mutant per rule you add: remove the role from the action, lower the risk class, leave the map version stale. Each must fail a test.

## 6. What this guide does not cover

Turning enforcement on (per namespace and service) and its outage and rollback runbook are not on `main`. They are added by their own pull requests.
