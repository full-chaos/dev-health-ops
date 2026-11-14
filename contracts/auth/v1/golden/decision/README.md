# Golden authorization decisions

One directory of decision questions and their pinned verdicts. Go reads it today;
the web client (CHAOS-10846) reads the same files and pins the same DIGEST.

## Layout

- `manifest.json`: claims exactly the files on disk. Rows are sorted by `id`; each row is `{file, id, layer}`.
- `vectors/<id>.json`: one row per file.
- `catalogs/<id>.json`: a catalog data file a row may name in `state.catalog`.
- `DIGEST`: the pin (below).

All JSON is canonical: UTF-8, LF, 2-space indent, sorted keys, trailing newline,
no floats, no repeated member. Ids are synthetic; no real credential appears.

## Layers (closed enum: `pdp`, `pep`)

Every row is run by exactly one runner. A runner refuses a row of the other layer.

- `pdp`: `state` (the GrantSnapshot input, incl. the clock) and `request` (the PDP Request fields) go through the real
  Evaluator. `expect` = `{allowed, reason, wire_reason_code, cache_class, expires_in_seconds, policy_revision, obligations}`; every field is compared.
  Runner: `internal/auth/authorization/golden_decision_test.go`. It fails when a manifest row is not run and when an
  `AllReasons()` token has no pdp vector. There is no skip list, but for `repository_allowlist_too_large` (see expect.obligations).
- `pep`: `request_raw` (the body text) goes through `authapi.DecodeAuthorizationRequest`. `expect` = `{outcome, refusal, question}`:
  `accepted` with the decoded question, or `refused` with the refusal text. A caller-asserted actor, roles, principal id or
  organization is refused. Runner: `internal/auth/authapi/authorize_golden_test.go`. Main has no authorize HTTP handler yet.

`state.delegation` is an optional member of a pdp row: the actor chain of a delegated session, `{chain: [{actor, permitted_actions}],
actor_platform_role, target_organization_id, assurance}`. An absent member means no chain, so a row without it is a plain decision. The
`state` members are otherwise exactly the list above; `delegation` is the only one a row may leave out. The runner reads it as the
`DelegationSnapshot` of the evaluator. `impersonation_denied` and `delegation_invalid` are internal reasons: no wire reason code is
defined for them here.

`state.catalog` is a closed enum with one value, `embedded`: the shipped catalog. Until catalog version 6 no shipped role held
`acr.context.read`, so the repository rows ran on a planted catalog; version 6 gives the action to viewer, member, admin and owner
and every row runs on the shipped catalog. `manifest.json` keeps an empty `catalogs` list for a catalog data file a future row may name.

## expect.reason is INTERNAL, expect.wire_reason_code is the wire value

`expect.reason` is an INTERNAL `authorization.Reason` token (the evaluator's value; today it equals the wire code for every Reason). `expect.wire_reason_code` is the decision `reason_code` the authorize
routes send for it, and every pdp row carries the member. Both come from `authapi.wireReasonCode` (CHAOS-10841): the wire code is
the Reason's own token for every Reason (`allowed_by_platform_admin` included), and the closed enum is `reason_code` in `jsonschema/authorization_decision.v1.schema.json`
(= `decision_reason_code` in `vocabulary/reason-codes.json`).

- A string: the Reason reaches the wire as a decision. The string equals `wireReasonCode(reason)` and is in the enum.
- `null`: the Reason is in `authorizeStateFailures` (`grant_read_failed`, `clock_unavailable`). The call answers 503; no decision and
  no `reason_code` reach the wire.

The member is `null` if and only if the reason is in `authorizeStateFailures`. The check is
`TestGoldenDecisionPDPRowsAssertTheirWireReason` in `internal/auth/authapi`. pep rows have no reason and no wire member.

## Not in this directory yet

- Stale-revision rows: they come with the cache readers of CHAOS-10844 (5.10) and CHAOS-10846 (5.11). No reader runs them today.
- Further impersonation rows (owner exemption, nesting, permitted-action cap): the package tests pin them, not this directory.
- Client-layer rows: same reason as stale revision.

## DIGEST

`DIGEST` is one line, `sha256:<hex>`, then LF. The digest is a sha256 over the files of this directory, excluding DIGEST,
sorted by relative path (slash form, byte order). For each file the hashed bytes are: path LF decimal-length LF bytes LF.
`internal/auth/contracts` recomputes it; a change to any file without a DIGEST update is red. Re-pin in its own commit.

## expect.obligations

`expect.obligations` is the allow's obligations in wire form, or `null`. Every pdp row carries the member, and the runner compares it exactly
(`internal/auth/authorization/golden_decision_test.go`; the router replay compares the same value on the wire).

- `null`: a deny, and the allow of a platform action (no tenant).
- An object: `tenant_scope` is the request's organization on every organization or repository allow. A repository member is present only
  for a `repository_scope: filtered` catalog action or a repository-kind action, and it is exactly one of `repository_scope` and
  `repository_allowlist`. A principal that reads every repository of the organization (no grant row of any state, or a live wildcard grant)
  has `repository_scope: organization` and no allowlist. Otherwise `repository_allowlist` is present; `[]` means none and is never left out.
  The string `*` never appears in an emitted allowlist, and a repository request for `*` is refused with `invalid_request`.
- A repository-kind allow carries `[request key]` when it comes from exact grants and `repository_scope: organization` when it is org-wide in
  fact (the membership default, a live wildcard grant); a delegated repository allow always carries `[request key]`. An organization-scope
  filtered allow carries the keys of the principal's live grants whose role holds the action, sorted and unique, at most 256; one more is the deny
  `repository_allowlist_too_large`. Catalog version 18 holds one filtered organization action, `acr.credential.issue` (device approval and consent
  take their repository scopes from it); `metrics.read`, `analytics.read`, `analytics.export`, `git.read` and `work_items.read` are org-wide for every
  active member (product rule 2). No vector reaches that deny; the runner leaves that one reason out of its coverage rule.
- The rule equals `evaluateRepository`: a key is in the allowlist exactly when the repository decision of the same principal, action and key
  allows it. One row of any state ends the membership default, for an owner too.
