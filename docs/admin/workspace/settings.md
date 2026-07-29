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
`GET /api/v1/admin/ask-dev`, `PATCH /api/v1/admin/ask-dev/settings`,
`POST /api/v1/admin/ask-dev/readiness`, and
`GET /api/v1/admin/ask-dev/usage`.

Context Fabric Validation is a platform-administrator diagnostic surface. It is
not granted by `ask_dev`, `byo_llm`, or organization-administrator status.
