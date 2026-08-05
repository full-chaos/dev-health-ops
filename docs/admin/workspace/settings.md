---
page_id: admin-workspace-settings
summary: Change supported workspace settings and verify their effect without exposing deployment secrets.
content_type: task-guide
owner: platform-product
source_of_truth:
  - current /settings and /org/admin product surfaces
applicability: current
lifecycle: active
---

# Configure workspace settings

1. Open **Admin** or **Settings** in the intended workspace.
2. Review the current value and its workspace-wide effect.
3. Change one supported setting at a time.
4. Save and verify the affected product workflow with a non-sensitive example.
5. Record the old and new value, reviewer, and rollback condition for high-impact changes.

Do not store provider secrets, private keys, or infrastructure credentials in general workspace fields. A setting that requires an environment variable or secret manager belongs under [Configure the platform](../../operate/configure/index.md).

## Ask Dev access

Ask Dev access, Ask Dev contextual entrypoints, BYO LLM configuration, and
Agent Context Runtime access are separate entitlements. An administrator must
not infer one from another. Missing or false `ask_dev` and
`ask_dev_contextual_entrypoints` decisions are denied independently. Both are
disabled by default for every tier. Future inclusion in paid plans requires
separate product change control and is not part of the current V1 entitlement.

The Ask Dev administration workflow controls one policy shared by the
persistent app-shell chat window and the full `/dev` workspace. Administrators
may select exactly 0-day ephemeral content or 30-day retained content, choose
fail-closed behavior or explicitly approve platform fallback, and use the
organization emergency disable. The emergency disable hides and blocks both
interaction surfaces and new runs; it does not delete BYO credentials or change
other LLM workflows. Existing content remains subject to its retention and
deletion policy.

The `ask_dev_contextual_entrypoints` entitlement controls only the reviewed
handoff triggers on approved product surfaces. Disabling it leaves the
persistent app-shell chat window and `/dev` workspace available when the base
`ask_dev` entitlement and runtime readiness allow them.

Administrative reads and usage summaries are content-free. Settings changes
and readiness tests are unavailable while impersonating. The supported API is
`GET /api/v1/admin/ask-dev`, `PATCH /api/v1/admin/ask-dev/settings`, and
`GET /api/v1/admin/ask-dev/usage`.

An organization administrator tests their own BYO LLM credentials with
`POST /api/v1/admin/llm-settings/readiness`. This checks the organization's
saved BYO configuration only; it never certifies or exposes information about
the platform-owned (operator-configured) provider. Certifying the
platform-owned provider is a platform-administrator action, not an
organization-administrator one, see
[Configure the platform](../../operate/configure/index.md).

Context Fabric Validation is a platform-administrator diagnostic surface. It is
not granted by `ask_dev`, `byo_llm`, or organization-administrator status.

## BYO LLM budget

An organization administrator can set a calendar-month monetary ceiling for
shared BYO LLM use. The ceiling is stored separately from the provider API key
and applies to Ask Dev and other workflows that use the organization's BYO
provider. Per-request token and concurrency limits remain active even when a
provider does not return enough information for reliable monetary enforcement.

For configured budgets, each provider network attempt first commits a
maximum-cost reservation. Provider execution occurs after that transaction
closes, and a separate transaction reconciles the reservation to reported
usage. An internal OpenAI retry therefore receives its own admission decision
and usage record before the second network request. Concurrent requests cannot
spend the same remaining balance, and replayed request identities are rejected
before another provider call is made. While a call is in flight,
`used_micro_usd` includes its committed maximum-cost reservation and then falls
to the reported actual cost after reconciliation.

A cancellation explicitly confirmed before provider dispatch is voided at zero
cost. Voided attempts do not contribute to `used_micro_usd` and do not change
the budget status reason. If a process exits after admission without proving
that dispatch did not occur or reporting terminal usage, the reservation stays
at its maximum exposure for the current window. The service does not infer zero
cost from a client timeout because the remote provider may still complete and
bill the request.

Budget values use integer micro-USD. `GET /api/v1/admin/llm-settings/budget`
returns the used, limit, remaining, UTC reset timestamp, enforcement
availability, and a safe reason code. Unknown pricing or missing token usage is
shown as unavailable; it is never displayed as zero cost. Administrators can
set `budget_limit_micro_usd` through the existing
`PUT /api/v1/admin/llm-settings` request, up to the operator-provisioned maximum.
Custom OpenAI-compatible endpoints and provider-batch calls remain unavailable
for monetary enforcement until their complete billing dimensions are certified.
They remain compatible when no monetary budget is configured. When a budget is
configured, unknown pricing is rejected before provider execution, and missing
usage on a completed attempt makes later calls fail closed until the next UTC
budget window. Budget writes are unavailable while impersonating.
