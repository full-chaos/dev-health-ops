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

1. Open **Admin** in the intended workspace, then choose **Organization** or **Settings**.
2. Review the current value and its workspace-wide effect.
3. Change one supported setting at a time.
4. Save and verify the affected product workflow with a non-sensitive example.
5. Record the old and new value, reviewer, and rollback condition for high-impact changes.

Do not store provider secrets, private keys, or infrastructure credentials in general workspace fields. A setting that requires an environment variable or secret manager belongs under [Configure the platform](../../operate/configure/index.md).

## Platform diagnostic surfaces

An organization administrator tests their own BYO LLM credentials with
`POST /api/v1/admin/llm-settings/readiness`. This checks the organization's
saved BYO configuration only, and it does so whether the own model is turned on or off; it never certifies or exposes information about
the platform-owned (operator-configured) provider. The check runs for the `openai` and
`anthropic` providers; for any other provider it answers `404` with
"BYO preflight is not available yet for provider <provider>." Certifying the
platform-owned provider is a platform-administrator action, not an
organization-administrator one, see
[Configure the platform](../../operate/configure/index.md).

Context Fabric Validation is a platform-administrator diagnostic surface. It is
not granted by `ask_dev`, `byo_llm`, or organization-administrator status.

## BYO LLM per purpose

The organization's BYO LLM row is its default. A purpose can carry its own
override, so a small explanation does not run on the model chosen for a complex
investigation. The purposes are a fixed list:

| Purpose | Used by |
| --- | --- |
| `investigate` | Ask Dev interpretation and synthesis (served to Context Fabric by the internal `llm-config` route) |
| `explain` | Investment and work unit explanations |

For each purpose the model is the override when one is stored, otherwise the
default row, otherwise the platform model. An override never
borrows the base URL of the default row. A model is required with
every override. An override for the same provider as the default row, with no
api key of its own, uses the default row's key, so one provider key can serve
several models; an override for a different provider needs its own key (a
provider that needs none, such as a local one, does not). The override lives in the same `llm` settings category as the
default row (keys `purpose:<purpose>:provider`, `model`, `api_key`,
`base_url`), and the api key is encrypted like the default one.

Admin API, all on `/api/v1/admin/llm-settings`: `PUT` with a `purpose` field
writes an override, `GET ?purpose=<purpose>` reads one (404 when none),
`DELETE ?purpose=<purpose>` removes one, and the plain `GET` lists every stored
override under `purposes`. `concurrency` and `budget_limit_micro_usd` apply to
the default row only. The plain `DELETE` removes the default row and leaves the
overrides.

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

`GET /api/v1/admin/llm-settings/models` lists the model ids a provider offers.
It needs no key: without one it returns the platform catalog, the models the
cost rate table prices for that provider (`anthropic`, `gemini`, `openai`,
`qwen`). With a saved key it also reads the saved provider's own list and marks
those ids `verified`. The optional `provider` query parameter names the provider
to list (an unknown name answers 400 `unknown_provider`); the saved key and base
URL are used only for the saved provider, never for another one. The saved model
always appears in its provider's list. Each entry carries its `id`, a `source`
(`live`, `catalog` or `saved`) and `verified`; verified ids come first, then
alphabetical. The response carries a `source` (`live`, `cache`, `catalog` or
`unavailable`) and a safe `reason` code when the provider's own list could not
be read. `unavailable` means there is nothing to list. The service keeps the
provider's list for one hour per organization and configuration; saving or
deleting the settings drops it, and a service restart reads it again. The model
field stays free text when no list is available.

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

## ACR AI features

An organization owner or administrator on a team or enterprise plan with Agent
Context Runtime can turn ACR AI features (for example MCP investigations) on or
off for the organization under **AI Setup > BYO LLM**. Off stops ACR AI
features for your org, including when you use your own key. On by default once
a BYO key is saved. The features run on the organization's saved BYO model. The
platform model is used only when a platform administrator grants the
organization platform model fallback.

Admin API: `GET /api/v1/admin/llm-settings/service-model` reads the state
(`enabled`, `eligible`, `control`, `override`); `PUT` with `{"enabled": true}`
or `{"enabled": false}` stores an explicit on or off for the organization. The change reaches investigations within about
a minute. When billing or support set this for the organization, `control` is
`managed` and the toggle answers `409`; contact support to change it. Each
change writes an audit log entry.

## Own model on or off

An organization owner or administrator can turn the organization's saved BYO
model off, and on again, without deleting it. While it is off the
organization has no usable own model: AI features other than categorization
use the platform model only when a platform administrator granted platform
model fallback, and are otherwise refused with the reason
`own_model_turned_off`. The saved provider, model, key and base URL stay as
they are, and deleting the BYO settings keeps the toggle.

Admin API: `GET /api/v1/admin/llm-settings/own-model` reads `enabled` and
`configured` (a saved BYO configuration exists); `PUT` with
`{"enabled": false}` or `{"enabled": true}` stores the state. The change
reaches investigations within about 15 seconds. Each change writes an audit
log entry with the actor and the old and new value.

The setup checks do not follow the toggle: `POST /api/v1/admin/llm-settings/readiness` and
the model list `GET /api/v1/admin/llm-settings/models` keep using the saved key while the
toggle is off, so an administrator can check a key before turning the own model back on.

## Prompt cache for an Anthropic model

An organization that brings its own Anthropic model can ask Anthropic to cache
the long, fixed instructions that Ask Dev sends at the start of each question.
It is off until an organization owner or administrator turns it on, and it
applies only while the model Ask Dev uses is on the `anthropic` provider; for
any other provider the setting has no effect.

The cache has a price. Anthropic charges about 25% more than the normal input price
to write the cached part, and about a tenth of it to read the cached part back
within five minutes. An organization that asks several questions within five
minutes of each other pays less; an organization whose questions are always
more than five minutes apart pays about 25% more on that fixed part. The question
itself and the answer are priced as before.

Admin API: `GET /api/v1/admin/llm-settings/prompt-cache` reads `enabled`,
`configured` (a saved BYO configuration exists) and `anthropic` (the saved
configuration Ask Dev uses is on the `anthropic` provider); `PUT` with
`{"enabled": true}` or `{"enabled": false}` stores the state. The change
reaches investigations within about 15 seconds. Each change writes an audit log
entry with the actor and the old and new value. Deleting the BYO settings also
clears this setting, so a configuration saved later starts with it off.
