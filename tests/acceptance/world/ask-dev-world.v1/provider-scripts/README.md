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
  "Enabled roles" below). Declares, per registry case id, the case's exact
  question text plus either an explicit scripted decision sequence or a
  fault script.

## Request-fingerprint routing: a question-text hash, never a tag

**History**: an earlier revision of this file routed by a `[[case:<id>]]`
marker embedded inside the question text. Codex review found that HIGH
severity: `DevMessageRequest.question` is the literal text
`DevPersistenceService.append_user_message_and_run` persists verbatim as the
user's own conversation message — provider-side handling happens far too
late to protect that persisted row, any transcript export, or a replay.
Proving the *provider's answer* was clean (`scan_public_text`) never proved
the *user's own message* was.

**Current design**: routing keys directly off a normalized hash of the
question text itself — nothing is added to it.

```text
fingerprint = sha256( casefold( collapse_whitespace( question.strip() ) ) )
```

(`provider_scripts.normalize_question_text` / `question_fingerprint` — the
exact, pinned implementation; see that module for why the normalization
rules themselves are pinned rather than "reasonable-effort".)

Each frozen registry case already has fixed, known question text — the
corpus runner always asks a case's *exact* question, verbatim, with no
wrapper. `role-<role>.json` stores that expected `question` string alongside
each case's script; `load_role_script` builds a `fingerprint -> case` index
at load time and **hard-fails** (`ValueError`, at load time, not per-request)
if two different case ids ever produce the same fingerprint — a collision
the corpus-authoring lane must resolve by rewording one of the two
questions, never a routing ambiguity this module resolves silently.

**Result**: nothing acceptance-specific ever enters the persisted
transcript. The corpus runner's question IS the natural question a real user
would ask. There is no delimiter, wrapper, or suffix to strip, sanitize, or
backstop-scan for — so there is no leak surface for that class of concern to
exist in at all.

### What happens when a question matches nothing scripted

This is the **routine, majority-case outcome** for all non-corpus traffic —
every pre-existing smoke script, the Wave 3.1 browser oracle, and the
`legacy_agent` role-certification probe all send questions that were never
written to match this file's literal text, and none of them carry any
special marker either. A question matching no scripted case is
indistinguishable, on the wire, from an ordinary one, and is treated exactly
that way: it **falls straight through** to the pre-existing default
heuristic (`_acceptance_answer`, `_list_metrics_script`, etc.), unchanged.
This is what keeps case-tag-free routing purely additive and backward
compatible — see `scripted_openai_service.py`'s module docstring.

Fail-loud is reserved for a request this module has affirmative reason to
believe was addressed to a *specific* scripted case — its question matched
one, byte-for-byte after normalization — but which the resolved script
cannot actually serve for this round (see "Fail-loud contract" below).
Matching an exact sentence is not plausible by accident, so that is a much
stronger signal than "no match found" ever is.

### The retired `[[case:` marker is reserved, not merely dropped

If the literal substring `[[case:` appears **anywhere** in a question —
well-formed, malformed, truncated, or duplicated — the scripted provider
**always fails loud** (`legacy_case_tag_marker_present`), never falls
through to the default heuristic. This is a pure string check (no file I/O,
no script loading), so it can never be skipped due to a missing/misconfigured
scripts directory. It exists so a stray or leftover occurrence of the
retired mechanism can never silently degrade into "unmapped case
accidentally passes as a generic canned answer" — see
`provider_scripts.LEGACY_CASE_TAG_MARKER`.

### Why a missing/broken scripts directory does not fail loud

Unlike the marker check, a failure to *load* `registry-ids.v1.json` or
`role-<role>.json` (missing directory, malformed JSON, an unscripted role)
degrades to "no scripted match" — falls through to the default heuristic —
rather than 422ing. With the tag eliminated there is no per-request
syntactic signal left to distinguish "the script pipeline is broken" from
"this was never meant to be a scripted case" — both look identical (an
ordinary-shaped question, no match found) — and breaking every pre-existing
non-corpus smoke/probe/oracle the moment this directory is unavailable would
be a severe, silent regression in a shared lane other work depends on. The
infra-is-broken case is instead caught by the **static conformance suite**
(`tests/acceptance/test_ask_dev_provider_roles.py`), which loads the
registry and every enabled role's script unconditionally and asserts they
parse without error — the same "static wiring guards run in the unit tier,
before anyone trusts a green acceptance run" pattern already used elsewhere
in this lane.

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
require full coverage — it simply means no question routes to it yet.
Coverage grows as Phase 2 Lane 2b authors corpus cases and pins each one's
real question text here.

Every case entry requires a top-level `"question"` string: the **exact**
literal text a corpus runner must send to address this case. Two different
case ids whose `question` normalizes to the same fingerprint fail the whole
role script at load time (see above) — reword one.

### Entry kinds

**`delegate_default`** — behave exactly like a question that matched
nothing (fall through to the pre-existing default heuristic). Use for a case
whose questions are already correctly served by that heuristic and only
needs to be individually addressable (e.g. for the fault/conformance
matrix) by its own fixed question.

```json
{"question": "How did completed work change ...?", "kind": "delegate_default"}
```

**`decisions`** — an explicit ordered list of scripted turns, one per
request round (round = number of distinct tool results already present in
the conversation; round 0 is the first call). Requesting a round beyond the
list's length fails loud (`script_exhausted`).

```json
{
  "question": "Is meridian/web-app release-ready?",
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
  "question": "Has anything changed in meridian/web-app during the selected period?",
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

`provider_scripts.ScriptEngine.resolve()` returns `None` (never an error)
when a question matches no scripted case — see "What happens when a
question matches nothing scripted" above. It raises `UnmappedCaseError`,
which `scripted_openai_service` renders as HTTP 422 with a JSON body shaped
`{"error": {"type": "scripted_provider_unmapped_case", "code": "...", "message": "..."}}`
(distinct `type` from the pre-existing wire-legal-tool-name `invalid_request_error`
400, and from a real scripted fault's `scripted_provider_fault` type), only
for these `code`s:

| `code` | Cause |
|---|---|
| `legacy_case_tag_marker_present` | The retired `[[case:` marker appeared anywhere in the question (any shape — see above) |
| `script_exhausted` | A `decisions` case was asked for a round past its scripted list |
| `fault_pre_decisions_exhausted` | A `fault` case's `pre_fault_decisions` ran out before `fires_from_round` |
| `scripted_tool_not_offered` | A scripted `tool_call` decision named a tool the client did not offer on this round |

(`role_not_scripted` / `role_script_schema_mismatch` / `scripts_directory_unavailable`
are also `UnmappedCaseError` codes `load_role_script`/`load_registry_ids`
can raise directly, but the request path never surfaces them as a 422 — see
"Why a missing/broken scripts directory does not fail loud". They are only
observable by calling those functions directly, e.g. from the conformance
suite.)

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
  A missing directory degrades to "no scripted match" for ordinary
  requests — see above — so its absence cannot break any pre-CHAOS-3219
  behavior.
