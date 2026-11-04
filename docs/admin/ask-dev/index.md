---
page_id: admin-ask-dev
summary: Understand what an administrator must set up for Ask Dev, which flags and model settings gate it, and what limits and refusals users can meet.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/api/licensing/registry.go
  - internal/apiservice/acr/access_store.go
applicability: current
lifecycle: active
---

# Turn on Ask Dev

Ask Dev needs entitlement flags, a model for the organization, and an Ask Dev service that the web app can reach. This page lists what each one does and what a user sees when one is missing.
{: .fc-page-lede }

!!! warning "Availability"
    Ask Dev is built but is **not turned on in production today**. The production deployment keeps the streaming the web app needs turned off. This page describes the controls that exist and makes no promise about when production will have them on. To see how users use it, read [Ask a question and read the answer](../../use/ask-dev/index.md).

## Before you begin

- Required role: an operator who can set organization entitlements. Entitlements are set per organization or license by the platform operator; a self-serve switch for an organization administrator was not found.
- You know which organization you are enabling, and that it has a model (see below).
- An Ask Dev service (ACR) is deployed and the web app is configured with its address. Without that configuration the web app draws no Ask Dev composer.

## Entitlement flags

| Flag | What it does | Default |
| --- | --- | --- |
| `ask_dev` | The in-app Ask Dev layer, and admission by the Ask Dev service | Off until enabled for an organization or license |
| `agent_context_runtime` | The earlier grant for the same capability. It still admits an organization until it is retired | Off until enabled |
| `ask_dev_contextual_entrypoints` | The page-context chips on product pages | Off until enabled |
| `ask_dev_wave_3_1` | Server-owned question interpretation | Off until enabled |

See [Feature flags and availability](../../reference/configuration/feature-flags.md) for how the gates relate. No flag grants another.

### The two gates differ

The Ask Dev service admits a caller when `ask_dev` **or** `agent_context_runtime` is on. The web app shows the Ask Dev button only when `ask_dev` **and** `agent_context_runtime` are both on. From reading the code, an organization with only `ask_dev` is admitted by the service and is not shown the button; this follows from the two checks and has not been reproduced. To be sure users see Ask Dev, enable both flags.

## The model

Ask Dev answers with a language model. It uses the `investigate` purpose of the organization's saved own model (see [Settings](../workspace/settings.md)).

- An organization with a saved own model for investigations runs on it.
- An organization with no saved own model uses the platform model only when the platform has turned on both `acr_service_model` and `platform_model_fallback` for it.
- An organization whose saved keys are for other uses only is refused until a model is saved for investigations. It is not moved to the platform model.

When the model cannot be used, users meet one of the refusals in [Ask Dev model refusals](../troubleshooting/provider-connections.md). The workspace LLM settings hold the **ACR AI features** and **Use your own model** switches.

## Who can see which data

- Access is checked on every question, not once at sign-in. A user who is not a member, who is impersonating, or who has no session is refused. A flag turned off refuses new questions.
- Page hints never widen what a user may see.
- An organization with no repository is refused as `no_repository`, unless the project-only scope is turned on for it; whether it is on anywhere was not found.
- Teams are what own repositories and projects. Ask Dev does not rank people against each other.

## Limits users can meet

| Limit | Where it comes from |
| --- | --- |
| A short per-user limit on starting a session (6 per window) | the web app |
| Running questions at once, per user (default 2) and per organization (default 4) | the Ask Dev service (`ACR_ASK_DEV_MAX_RUNNING_PER_PRINCIPAL`, `ACR_ASK_DEV_MAX_RUNNING_PER_ORG`) |
| A monthly platform run allowance, default 1,000 | the Ask Dev service (`ACR_ASK_DEV_MONTHLY_RUN_ALLOWANCE`), per organization |

## Stop, history and retention depend on the deployment

- Stopping a running question needs the run lease turned on in the Ask Dev service (`ACR_CONTEXT_FABRIC_RUN_LEASE_ENABLED`, default off).
- Streaming needs `ACR_CONTEXT_FABRIC_EVENT_STREAM_ENABLED` (default off). With it off the panel says Ask Dev is not available.
- Removing expired conversations needs `ACR_ASK_DEV_RETENTION_ENABLED` (default off). See [Privacy and telemetry](../security-and-audit/privacy.md) for what is stored and for how long.
- Titles that show the first question need `ACR_THREAD_LIST_FIRST_QUESTION_ENABLED` (default off).

## Turn it off

Turn off in the reverse order of turning on: `ask_dev_contextual_entrypoints` first (chips disappear, plain questions still work), then the organization flags (the button disappears), then the Ask Dev service's streaming flag (the panel says Ask Dev is not available). Stored content is kept until the retention cleanup removes it.

## Related information

- [Ask a question and read the answer](../../use/ask-dev/index.md)
- [Feature flags and availability](../../reference/configuration/feature-flags.md)
- [Settings](../workspace/settings.md)
- [Repair stranded Ask Dev conversations](../../operate/run/ask-dev-rollout.md)
