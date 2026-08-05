# `provider-scripts/` — role- and fault-aware scripted provider decisions

CHAOS-3219 Phase 1 Lane 1b. This directory feeds
`dev_health_ops.llm.agent.provider_scripts` (loaded by
`dev_health_ops.llm.agent.scripted_openai_service`, the Ask Dev acceptance
scripted OpenAI-compatible provider). It is **not** the corpus itself —
`tests/acceptance/world/ask-dev-world.v1/corpus/case-*.json` (Phase 2 Lane
2b) is the corpus; this directory is only what the *provider* does when a
corpus case's request reaches it.

## Files

- `registry-ids.v1.json` — a checked-in copy of the frozen 134-case id set
  from `scratchpad/corpus-registry-v1.md` (CHAOS-3219's accepted corpus
  case-id registry). This is the referential-integrity oracle: every case id
  a `role-*.json` file references must appear here. Do not hand-edit this
  file to add ids — if the registry is amended, regenerate this file from
  the accepted registry document.
- `role-<role>.json` — one file per **enabled** Ask Dev provider role (see
  "Enabled roles" below). Declares, per registry case id, either an explicit
  scripted decision sequence or a fault script.

## Request-fingerprint routing

The corpus runner (Phase 2) tags the question it sends with the case id it
is driving:

```text
<real question text> [[case:<case-id>]]
```

`provider_scripts.extract_case_id()` parses the tag back out. This is the
**only** channel that survives, unmodified, from an HTTP call against the
real `/api/v1/dev/**` surface all the way to this provider's wire request:
`DevMessageRequest.question` is embedded verbatim by `PromptComposer.compose()`
as `user_payload["question"]` (`api/dev/prompts/composer.py`) — no other
client-controlled request field (`conversation_id`, `client_message_id`,
`request_id`) is ever re-serialized into the provider request. This is also
the same channel the pre-existing `LIST_METRICS_QUESTION` literal match and
`_evidence_query_from_question` derivation already rely on
(`scripted_openai_service.py`).

**A question with no `[[case:...]]` tag is untouched** — it falls straight
through to the pre-CHAOS-3219 default heuristic (`_acceptance_answer`,
`_list_metrics_script`, etc.), unchanged. Every existing smoke script and
unit test that calls the scripted server directly keeps working exactly as
before. Case-tag routing is strictly additive.

### Why the tag cannot leak into a rendered answer

1. **By construction**: a scripted decision is built only from the script
   JSON's own field values; the raw tagged question is read only to decide
   which script entry applies and is never copied into any answer field.
2. **Defense in depth**: even if a future edit broke rule 1, the tag's shape
   cannot trip either backstop production already runs over every public
   copy field:
   - `no_match_terminal.INTERNAL_TOKEN_DENYLIST` /
     `contracts_v2.validators.PUBLIC_TEXT_FORBIDDEN_TOKENS` are exclusively
     underscore-joined snake_case tokens (`_underscore_members` filters to
     only `"_"`-bearing enum values; the hand-written forbidden set is the
     same shape — `"forbidden_or_not_found"`, `"not_measured"`, ...). Every
     registry case id and this tag's alphabet use `-` (kebab) inside
     dot-separated segments, never `_`. A substring match against an
     underscore token cannot fire on a hyphenated one.
   - `contracts_v2.validators._VERSIONED_ID_PATTERN` only flags a dotted
     token whose *last* segment matches `v\d+` (`status.entity.v2`-shaped).
     No registry case id's last segment is a bare version — including
     `status.single-project.positive-control-v1`, whose `-v1` is fused onto
     the qualifier word, not a standalone final segment.

See `provider_scripts.py`'s module docstring for the full argument.

## Enabled roles

Only `legacy_agent` (`AgentRole.LEGACY_AGENT`) has a working,
production-representative certification probe today:
`llm.agent.role_readiness.RoleReadinessService.certify_role` raises
`NotImplementedError` for every other `AgentRole` member
(`intent_classification`, `answer_frame_narrative`), gated on CHAOS-3285 PR4
landing their probes. `role-legacy_agent.json` is therefore the only script
file this directory needs today; the mechanism (`ROLE_ENV` /
`ASK_DEV_SCRIPTED_PROVIDER_ROLE`, default `legacy_agent`) is written
generically so a future role only needs a new `role-<role>.json` once it has
a real probe — see `provider_scripts.py`.

## `role-<role>.json` format

```jsonc
{
  "schema_version": "ask_dev_provider_script.v1",
  "role": "legacy_agent",
  "cases": {
    "<registry-case-id>": { /* one entry, see below */ }
  }
}
```

Every key under `cases` must be a member of `registry-ids.v1.json`'s id set
(enforced by `test_ask_dev_provider_roles.py`). A registry id with **no**
entry here is not an error by itself — the conformance suite does not
require full coverage — but a *request* tagged with an id that has no entry
fails loud (`case_not_scripted`), never a canned 200. Coverage grows as
Phase 2 Lane 2b authors corpus cases against ids already scripted here, or
files against this directory as ids they need scripted.

### Entry kinds

**`delegate_default`** — behave exactly like an untagged request (fall
through to the pre-existing default heuristic). Use for a case whose
questions are already correctly served by that heuristic and only needs to
be case-tag-addressable.

```json
{"kind": "delegate_default"}
```

**`decisions`** — an explicit ordered list of scripted turns, one per
request round (round = number of distinct tool results already present in
the conversation; round 0 is the first call). Requesting a round beyond the
list's length fails loud (`script_exhausted`).

```json
{
  "kind": "decisions",
  "decisions": [
    {"type": "tool_call", "tool": "query_metric_v1", "arguments": {"metric_id": "items_completed"}},
    {"type": "final_answer", "value": {"status": "degraded", "direct_summary": "..."}}
  ]
}
```

Decision `type`s: `tool_call` (`tool` is the wire-sanitized name, e.g.
`query_metric_v1`, not the dotted canonical `tool_id`), `final_answer`
(`value` is a `dev_answer.v1` draft — only `status`/`direct_summary`, per
`OpenAICompatibleAgentProvider._answer_draft_schema`), `disambiguation`
(`prompt` + `candidates`), `refusal` (`code` + `message`).

**`fault`** — one of the 6 fault types. `fires_from_round` (default `0`)
gates when the fault activates; rounds before it are served from
`pre_fault_decisions` (by round index, same rule as `decisions`).

```json
{
  "kind": "fault",
  "fault": {
    "type": "fail-before-frame" | "fail-after-frame" | "unsafe-error-text" | "oversized-output" | "slow-response" | "retry-storm-trigger",
    "fires_from_round": 0,
    "pre_fault_decisions": [ /* only meaningful when fires_from_round > 0 */ ],
    "http_error": {"status": 503, "code": "...", "message": "..."},
    "min_bytes": 262144,
    "delay_ms": 4000,
    "decision": { /* only for slow-response: the decision served after the delay */ }
  }
}
```

| Fault type | Mechanism | Required fields |
|---|---|---|
| `fail-before-frame` | Every round returns `http_error` (status/code/message) | `http_error` |
| `fail-after-frame` | Rounds `< fires_from_round` serve `pre_fault_decisions`; rounds `>= fires_from_round` return `http_error` | `fires_from_round`, `pre_fault_decisions`, `http_error` |
| `unsafe-error-text` | Every round returns `http_error` whose raw body deliberately contains a denylisted/secret-shaped token — a RED canary proving the *real* client-side path (`safe_agent_provider_error`, `errors.py`'s fixed `_SAFE_MESSAGES`) never surfaces raw provider text | `http_error` (message should embed a token matching `llm.errors._SECRET_PATTERNS` or `no_match_terminal.INTERNAL_TOKEN_DENYLIST`) |
| `oversized-output` | A normal 200 `final_answer` whose encoded `direct_summary` is `>= min_bytes` (floor: `provider_scripts.MIN_OVERSIZED_BYTES`, currently 65536) | `min_bytes >= MIN_OVERSIZED_BYTES` |
| `slow-response` | Sleeps `delay_ms` before responding with `decision` (defaults to a plain degraded final answer if omitted) | `delay_ms` |
| `retry-storm-trigger` | Every round returns a retryable `http_error` (409/429/5xx-shaped) so the caller's retry policy is exercised on every attempt | `http_error` (use a retryable status, e.g. `429`, and set `retry_after_seconds`) |

## Fail-loud contract

`provider_scripts.ScriptEngine.resolve()` never returns a canned answer for
a fingerprint it cannot resolve — it always raises `UnmappedCaseError` with
one of these `code`s, which `scripted_openai_service` renders as a
non-200 HTTP response with a JSON body shaped
`{"error": {"type": "scripted_provider_unmapped_case", "code": "...", "message": "..."}}`
(status 422; distinct `type` from the pre-existing wire-legal-tool-name
`invalid_request_error` 400, so a corpus runner can tell "your case id is
wrong" apart from "your tool payload is malformed"):

| `code` | Cause |
|---|---|
| `unknown_case_id` | Tag parsed, but the id is not in `registry-ids.v1.json` |
| `case_not_scripted` | Id is a valid registry id, but this role's file has no entry for it |
| `role_not_scripted` | No `role-<role>.json` file exists for the active role at all |
| `role_script_schema_mismatch` | The role file exists but fails schema/self-declaration checks |
| `scripts_directory_unavailable` | The provider-scripts directory itself could not be found (see `ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR` below) |
| `script_exhausted` | A `decisions` case was asked for a round past its scripted list |
| `fault_pre_decisions_exhausted` | A `fault` case's `pre_fault_decisions` ran out before `fires_from_round` |

## Environment

- `ASK_DEV_SCRIPTED_PROVIDER_ROLE` — which `role-<role>.json` to load.
  Defaults to `legacy_agent`.
- `ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR` — overrides where this directory
  is read from. Resolution is otherwise a repo-relative walk up from the
  Python package (works for any full source checkout). This directory is
  under `tests/acceptance/`, not the installed package, so a container image
  that ships only `pip install .` (the `api` Dockerfile target the
  `ask-dev-scripted-openai` Compose service also builds) will not contain it
  unless the image or a mount also provides it — cross-lane note for
  whichever lane wires Compose: either bind-mount this directory into the
  scripted-provider container, or set this env var to wherever it lands.
  Resolution is **lazy**: the untagged/default-heuristic path never touches
  this directory at all, so its absence cannot break any pre-CHAOS-3219
  behavior — only a request that actually carries a `[[case:...]]` tag can
  observe `scripts_directory_unavailable`.
