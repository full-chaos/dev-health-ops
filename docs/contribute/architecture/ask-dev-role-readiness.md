---
page_id: con-ask-dev-role-readiness
summary: Why Ask Dev certifies a provider per role instead of with one pass/fail badge, and how a stale certification is forced to re-run.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/llm/agent/roles.py
  - src/dev_health_ops/llm/agent/role_readiness.py
  - src/dev_health_ops/llm/agent/probes/legacy_agent.py
  - src/dev_health_ops/llm/agent/readiness.py
  - src/dev_health_ops/api/dev/production_runtime.py
  - src/dev_health_ops/api/admin/routers/ask_dev.py
  - src/dev_health_ops/api/admin/routers/platform_ask_dev.py
  - tests/api/dev/test_live_openai_smoke.py
  - Amendment TRD v2 -- Ask Dev Wave 3.1 (Linear, project Ask Dev)
applicability: current
lifecycle: active
---

# Ask Dev role readiness

A provider certified against a small synthetic exchange can still exhaust its
output budget on the first real question. Ask Dev certifies a provider
**per role** it is actually asked to perform, against a request built from the
real production producers -- never a hand-authored miniature -- and treats a
structural rejection (the provider cannot handle the shape at all) as a
different, non-retryable outcome from a transient one (the network blipped).
{: .fc-page-lede }

## Why one badge was not enough

Before this work, Ask Dev certified a provider with a single exchange: a
512-token echo call against one tiny tool, followed by a second call with no
tools at all. That request is roughly 1 KB. A real Ask Dev round is the full
nine-tool registry, the fixed policy sections, and the complete decision
grammar -- roughly 12 KB at round one, more once tool results accumulate --
capped at the same 4,096-token output budget the echo probe never came close
to touching. A provider that passed certification could still exhaust its
reasoning budget on the very first real question, and the resulting failure
carried no distinct identity: it surfaced as `internal_error`, indistinguishable
from a genuine server bug.

Fixing that budget accounting is a separate, decision-gated change (the
4,096-token envelope is a ratified TRD number; widening it has its own
coupled risks -- see the budget policy module for the current state). What
this page describes is the certification model that makes such a defect
detectable and describable at all: a role-scoped, production-representative
probe with a verdict vocabulary that distinguishes "this provider cannot do
this" from "try again."

## The role model

Ask Dev asks a provider to do three structurally different things, and a
provider may be capable of one and not another:

| Role | Shape | Consumes |
| --- | --- | --- |
| `legacy_agent` | The full open-ended agent loop: nine tools, `tool_choice` negotiation, and the complete decision grammar, across multiple rounds. | The retained multi-round investigation path. |
| `intent_classification` | A strict, small typed decision with no tools and no source data. | The fallback classifier when the deterministic interpreter abstains. |
| `answer_frame_narrative` | Strict prose generation over one compact canonical frame, with no tool registry. | Optional narration over an already-computed, deterministic answer. |

`AgentRole` (`llm/agent/roles.py`) names these three. Only `legacy_agent` has a
working probe today; `intent_classification` and `answer_frame_narrative` are
valid enum members and store slots with no probe wired to them yet -- the
per-role store can hold a certification for either once their probes land,
but nothing produces one yet.

## Certification states

A per-role certification is one of:

| State | Meaning | Selectable? |
| --- | --- | --- |
| `compatible` | The probe completed with a valid typed decision inside budget. | Yes |
| `incompatible` | A deterministic, structural rejection: output exhaustion, an unsupported request parameter, a malformed decision. Re-running the identical request will not help. | Never |
| `failed` | A transient or environmental failure: timeout, rate limit, transport error, missing credentials. Worth retrying once the condition clears. | No, but recoverable |
| `stale` | A stored record whose certification key no longer matches the provider's current capability inputs. | No |
| `unchecked` | No record exists for this (provider, role) pair yet. | No |

This is a deliberate split of what used to be one boolean `FAILED`. Output
exhaustion is `incompatible`: retrying the same provider against the same
prompt, tool registry, and budget will exhaust again every time, so the
runtime must never keep selecting it hoping for a different result. A rate
limit is `failed`: the same request may well succeed a minute later.
`RoleCertificationState` and the mapping from a probe's safe error code to
one of these states live in `llm/agent/roles.py` and
`llm/agent/readiness.py::role_state_for_safe_error_code`.

## What invalidates a certification

A certification is keyed by a **certification key**, not just a provider/model
pair. `production_runtime.py::_readiness_fingerprint` folds every capability
input that can flip a verdict: the provider's source, identity, and base URL;
the adapter's wire-contract version (`READINESS_VERSION`); the composed
prompt's version (`PROMPT_VERSION`); the tool registry's contract version
(`TOOL_CONTRACT_VERSION`); the per-family token budget policy's version
(`BUDGET_POLICY_VERSION`); and the role being certified. Changing any of
these -- a prompt-section edit, a new tool, a budget policy change, asking
about a different role -- produces a different key, and a stored record under
the old key simply stops being read as current. Nothing has to notice and
manually invalidate anything.

This also gives certification an explicit migration story. The fingerprint
formula changed when this role model landed, which means every certification
recorded before that change reads as stale under the new formula and the
runtime falls back to "not yet certified" until an operator re-runs
preflight -- the same self-invalidating pattern already established when the
adapter's own wire contract last changed (`READINESS_VERSION` v2 -> v3).

## Storage and backward compatibility

The per-role profile persists under settings keys distinct from the original
single-record store: `ask_dev_role_certification_profile` for an
organization's own BYO provider, `platform_ask_dev_role_certification_profile`
for the platform-owned provider (same `org_id=""` sentinel scoping the
original platform key uses). The new store never reads the old
`ask_dev_agent_readiness` / `platform_ask_dev_agent_readiness` keys, so a
certification recorded before this model existed can never be misread as a
certification for any role -- it is simply invisible to the new code path. A
rolled-back deploy that falls back to reading the old key is unaffected by
anything the new store has written.

`RoleCertificationProfile.with_record()` updates exactly one role's record
and leaves every other role's stored record untouched: certifying
`legacy_agent` can never overwrite an already-certified
`answer_frame_narrative`, once that role has a probe of its own.

## The `legacy_agent` probe

`llm/agent/probes/legacy_agent.py::certify_legacy_agent` is built exclusively
from the real production producers: `PromptComposer` for the fixed policy
sections and the committed-subject section, `AskDevToolRegistry` for the
complete nine-tool manifest and each tool's real input schema (via
`DevOrchestrator._provider_tool_input_schema`, not a re-implementation of it),
`DevRunLimits` for the real per-call output cap, and `DevAnswer`'s own JSON
schema for the decision grammar. A production-floor assertion fails loudly if
the composed request or the tool count ever silently shrinks below what a
real round sends -- a probe that got smaller without anyone noticing would
otherwise read as coverage it no longer provides.

The probe runs two rounds that mirror the worst production shape:

1. **Round one** -- tools offered, no tool result yet. The wire sends
   `tool_choice: "required"` with no structured-output grammar.
2. **Round two** -- tools still offered, and a synthetic, schema-valid,
   non-tenant tool result is now in the conversation. The wire sends
   `tool_choice: "auto"` **and** the full `DevAnswer` grammar at the same
   time -- the combined shape every real round two and later actually sends,
   and the one the original echo probe never sent at all (its second call
   dropped tools entirely).

The synthetic tool result comes from the checked-in contract fixtures
(`contract_fixtures.positive_fixtures()`), never a live tool call and never
tenant data, satisfying the guardrail that readiness must not touch real
source data.

## Admin surfaces

Both admin readiness endpoints (`GET /admin/ask-dev`,
`GET /admin/platform/ask-dev/readiness`) gained an additive `role_readiness`
array, one entry per role, using a safe display vocabulary
(`RoleReadinessState`) that is deliberately **not** the internal
`RoleCertificationState` enum: it extends the existing `ReadinessState`
values with exactly one new state, `not_yet_certified`, so a role nothing has
certified reads as visibly distinct from `stale_readiness` (was certified,
now invalidated) and from any failure-derived state. No prompt text, tool
text, or provider payload is ever included.

The existing CHAOS-3265 boundary applies unchanged: an organization relying
on platform fallback sees the platform role's *state* (it carries no
identity) but never the platform's specific remediation text, which is
replaced with the same generic message the single-readiness field already
uses.

Platform preflight (`POST /admin/platform/ask-dev/readiness`) now also
certifies the `legacy_agent` role in the new per-role store, on the same
already-resolved provider, immediately after the existing certification call
-- so the projection reflects a real result instead of staying
`not_yet_certified` forever. That call is best-effort: a failure in it can
never regress the original, already-relied-upon certification result.

An organization's own BYO provider is not yet certified through this new
model -- that requires wiring `POST /llm-settings/readiness`
(`api/admin/routers/settings.py`), which is a separate, not-yet-scheduled
change. Until then a BYO-sourced organization's `role_readiness` honestly
reports `not_yet_certified` for every role.

## The live convergence gate

`tests/api/dev/test_live_openai_smoke.py` exercises the real adapter against
a real OpenAI-compatible endpoint when live credentials are configured. By
default, a missing credential is a `pytest.skip` -- an ordinary local run
without live credentials configured is not an error. Setting
`ASK_DEV_LIVE_GATE=1` marks the run as the actual convergence gate: under
that flag, a missing credential becomes a `pytest.fail` instead. A gate that
can silently skip its own measurement reads as passing coverage it never
took; the flag exists so that failure is loud rather than invisible. Whether
CI sets that flag for the Wave 3.1 convergence gate is a separate workflow
change, not implemented here.

## What is not built yet

- `intent_classification` and `answer_frame_narrative` have no probes. Their
  enum members and store slots exist so the store's shape does not need to
  change again when the probes land.
- The BYO `POST /llm-settings/readiness` route does not yet certify the new
  per-role store.
- The 4,096-token output envelope is unchanged. Whether it needs amending
  (and how) is a decision gated on the live per-role probe's real numbers
  against `gpt-5-nano`, not decided by this page.
