# Phase 3 evidence summary — CHAOS-3219 armed corpus run

**Run verdict: MEASURED. Phase verdict: CLOSED (2026-08-07 ~17:10 PT).**
Filled from the grade and closure record, not from expectation.

Sources this document is filled from, in order of authority:

* the **2026-08-07 grade comment on CHAOS-3219** (`Phase 3 armed corpus run — GRADED: MEASURED`);
* the **Phase 3 closure comment on CHAOS-3219** (`Phase 3 — CLOSED (2026-08-07 ~17:10 PT)`);
* the body of **ops PR #1597** (`CHAOS-3219 Phase 3 close`), merged to `main` as `6b7517364`, which is the durable evidence record and reproduces the grade verbatim;
* the run's own **`.p3-logs/RUN-CONDITIONS.md`** and boot log, for environment facts the three narrative sources do not carry.

Anything none of these carries is left as an explicit `<PLACEHOLDER: …>` rather than
reconstructed. A reconstructed figure in an evidence document is indistinguishable
from a measured one once it is written down.

The corpus corrections described in section 4F are **on `main`** as of `6b7517364`;
they are not pending.

Graded against [`PRE-REGISTRATION.phase3.v2.md`](PRE-REGISTRATION.phase3.v2.md) **as
amended pre-run in `4f974f2c7`**. The amendment fired v2's own re-derive rule after
four PRs merged, and put the QUA shadow explicitly out of scope (Amendment 3). The
pre-registration was not touched after the run.

---

## 1. Run identity

| Field | Value |
| --- | --- |
| Graded run | Armed run #3 |
| Run timestamp | 2026-08-07 16:22 PT |
| `main` SHA under test | `3e4e24650` |
| Branch | `chaos-3219-phase3-corpus` |
| Pre-registration graded against | `PRE-REGISTRATION.phase3.v2.md` as amended in `4f974f2c7` |
| Engine identity digest | `12b961997a122ff6e7a711db0a3a5724bdb668a28c262f2fdf94c88be3675e59` |
| Provider role / cases | `legacy_agent`, 96 |
| API container | `77d8499c18a3` (pinned) |
| `WORLD_DIGEST` | `fea90782a6b462597c41173e5d5e40728c4bf5c291631b1a42a3215ef14758e5` — live matched the pinned file |
| Superseded attempts | Attempt 1 — killed unmeasured by the 09:26 machine reboot. Attempt 2 (09:47 PT) — UNMEASURED (degraded): 59/90 active cases hit HTTP 429 `cost_limit_reached`; root causes ticketed CHAOS-3573 and CHAOS-3575. An 11:04 PT run measured 122/12 and settled Phase 2 exit; **it is not this grade** — run #3 supersedes it. |

## 2. Run-validity preconditions

Every row is an observation from the run's own boot output, not an expectation
carried from the pre-registration.

| Precondition | Required | Observed | Verdict |
| --- | --- | --- | --- |
| Engine identity digest matches | `12b96199…` | `IDENTITY MATCH 12b961997a12…` role `legacy_agent`, 96 cases | PASS |
| `WORLD_DIGEST` matches pinned snapshot | match | `world restore: WORLD_DIGEST fea90782a6b46259… verified against /app/tests/acceptance/world/ask-dev-world.v1/WORLD_DIGEST`; 86 ClickHouse + 10 Postgres tables restored | PASS |
| QUA flags unset in the API container's own environment | unset | `QUA_VERIFIED=not-armed (shadow=0 commit=0)` | PASS |
| Items collected | 134 | 134 | PASS |
| Items skipped | 0 | 0 | PASS |
| Cases executed | 90 active | 90 | PASS |
| Receipt coverage `missing:` | 0 | 90 executed / 90 recorded → `missing: 0` | PASS |
| Engine controls green | 3/3 | green | PASS |
| HTTP 429 observed | none | zero 429s / `cost_limit_reached` anywhere in the run | PASS |
| Container-id pin held | no theft | `STACK STOLEN: 0` across all three stage boundaries | PASS |
| Allowance headroom | sufficient | 6 orgs × 500,000,000 microUSD, counter absent | PASS |
| Stack health | all up | 8/8 healthy | PASS |

**Verdict on validity: MEASURED.**

The pin check earns its place here. It was added during this run (`c16cd2e36`) so a
competing `compose up` is *named* at the stage boundary rather than surfacing four
stages later as `service "api" is not running`.

### Caveat recorded before any result was seen: the world was re-minted

`RUN-CONDITIONS.md` records, **pre-grade**, that the snapshot moved from the graded
11:11 run's `6dda65a695befc98…` to this run's `fea90782a6b46259…` (PR #1583's re-mint).
That is **not drift and not a MISS** — v2 requires only that the live world match the
pinned file, which it does. But this run measures against a *different world* than the
one v2's predictions were derived from, and the corpus depends on world data.

The caveat was written down in advance precisely so the re-mint could not be reached
for afterwards as a convenient explanation, with the honest statement that *"the fix
behaved differently"* and *"the world changed"* cannot be separated without a
discriminating check. **Section 4F is that discriminating check**, and it clears the
re-mint: the absence reason changed in lockstep with the span declaration and is
computed from the declared-spans flag, not from world data.

## 3. Headline result

```
134 collected · 0 skipped · 90 executed · 90 receipts · missing: 0
131 passed · 3 failed · RUN_EXIT_CODE=1
```

`RUN_EXIT_CODE=1` means the corpus went red on three cases. **That is a result, not a
degradation** — the distinction the pre-registration exists to keep.

| Measure | Predicted | Observed |
| --- | --- | --- |
| Cases red | 1 (+1 conditional, resolved to green) | 3 |
| MISSes | 0 | 2 |
| Discoveries | n/a | 0 sighted |

## 4. Grade against each pre-registered prediction

### A. The six `denied` → `refused` flips — **GREEN, all six**

CHAOS-3541's typed refusal confirmed live.

| Case | Scripted refusal code | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `adv.injection-request.sql` | `prohibited_execution` | GREEN | green | GREEN |
| `adv.injection-request.shell` | `prohibited_execution` | GREEN | green | GREEN |
| `adv.injection-request.graphql` | `prohibited_execution` | GREEN | green | GREEN |
| `adv.injection-request.mcp` | `prohibited_execution` | GREEN | green | GREEN |
| `adv.injection-request.write` | `prohibited_write` | GREEN | green | GREEN |
| `scope.prohibited-write` | `prohibited_write` | GREEN | green | GREEN |

### B. The discriminator — **HELD**

| Case | Scripted code | Predicted outcome | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `adv.injection-request.url` | `unsupported_external_fetch` | `unsupported` | stayed `unsupported` | GREEN |
| `scope.unsupported-request` | `unsupported_request` | `unsupported` | stayed `unsupported` | GREEN |

**Discriminator verdict: HELD.**

`adv.injection-request.url` sits in the same family as the four flipped injection
cases and would have been flipped by anyone pattern-matching on case names. It stayed
`unsupported`, so **the flip is demonstrably routed by refusal code** rather than
applied to a family. Had it flipped, section A's six greens would have been right by
luck and would prove nothing.

### C. Cases the other merged fixes should turn green — **GREEN, all five**

| Case | Proves | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `scope.bounded-subject-set` | CHAOS-3551 render (all three invariants) | GREEN | green | GREEN |
| `pers.clarification-persistence` | CHAOS-3577 disambiguation entry | `needs_clarification` | green | GREEN |
| `scope.ambiguous` | pairs with the above; must agree, not transpose | `needs_clarification` | green, pair agrees | GREEN |
| `portfolio.multi-project.status` | CHAOS-3578 mention spans | GREEN on both resolution-path invariants | classifies | GREEN |
| `deg.source-state.measured-zero` | newly declared `resolution_path_in` | `deterministic-exact` | passes its pinned resolution path | GREEN |

### D. Expected red — **RED as predicted**

| Case | Reason | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `deg.provider.unsupported` | CHAOS-3546 unmerged; `provider_profile_override` has no readers in `src/` | RED | red, failing exactly `public_outcome_in` | GREEN (prediction held) |

Status: **GRADED**, not void — CHAOS-3546 did not merge before the run. The case,
`readiness.capabilities.degraded`, and `readiness.capabilities.unsupported-model` all
flip DECLARED-BLOCKED on **CHAOS-3588** via PR #1595.

### E. Conditional — `adv.cross-tenant.organization-id` — **green branch, correct**

| Branch | Condition | Predicted | Applies? |
| --- | --- | --- | --- |
| CHAOS-3574 merged | classification fix present | GREEN, `not_found` | **yes** |
| CHAOS-3574 not merged | fix absent | RED on `public_outcome_in` | no |

| Check | Predicted | Observed | Verdict |
| --- | --- | --- | --- |
| `public_outcome_in` | `not_found` | green | GREEN |
| `no_unauthorized_candidate_surfaces` | PASS | pass | GREEN |
| Bystander `adv.cross-tenant.project-id` | hold | held | GREEN |
| Bystander `adv.cross-tenant.repository-id` | hold | held | GREEN |

The bystanders matter for the same reason the discriminator does: PR #1593 changed the
resolution **route**, not one case's outcome, so checking only the named case would
have read a route regression as success. Both held.

### F. Everything else — **2 MISSes**

Both are the same defect, and **both are self-inflicted by the CHAOS-3578 corpus edit
— neither is the product's.**

| Case | Invariant | Observed | Classified as |
| --- | --- | --- | --- |
| `adv.oversized.question` | `resolution_path_measured` | `empty-resolution-ledger-despite-mentions` | MISS |
| `adv.oversized.subject-set` | `resolution_path_measured` | `empty-resolution-ledger-despite-mentions` | MISS |

**Mechanism** (`resolution_path.py:498-506`): with named subject mentions declared, an
empty ledger resolves to `ABSENCE_EMPTY_LEDGER_DESPITE_MENTIONS` (a broken set);
without them it resolves to the honest `ABSENCE_EMPTY_LEDGER`, which passes. Both
cases have their oversized request **rejected before subject resolution runs**, so the
ledger is empty by construction, permanently. Declaring spans asserts that resolution
was reached, which is false for them.

The CHAOS-3578 commit added producer-derived `expected_mention_texts` to both, on the
argument that *"they are covered rather than exempted, because an exemption list is
the thing that rots."* The run falsified that argument. The same two cases, measured
both ways on the same day:

```
11:11 — no spans — absence='empty-resolution-ledger'                  PASSED
16:22 — spans    — absence='empty-resolution-ledger-despite-mentions' FAILED
```

**Not the re-minted world**, which was flagged as a candidate cause *before* results
were seen: the absence reason changed in lockstep with the declaration, and is
computed from the declared-spans flag rather than from world data.

Correction: `64111072e` removes the spans and adds the exemption as
`REJECTED_BEFORE_RESOLUTION_CASE_IDS`, **asserted rather than listed**, with both
guards mutation-proven by exit code.

## 5. Discovery signatures

| Signature | Ticket | Seen this run? |
| --- | --- | --- |
| `ask_dev.orchestrator.record_frame_programming_error` | ops #1577 | not sighted |
| Valkey allowance-path signature | ops #1575 | not sighted |

The asymmetry the pre-registration fixed held: an HTTP **429 is a MISS**, because the
allowance must not gate a measurement run — and this run had zero. A Valkey
allowance-path **signature** would have been a discovery. Neither occurred.

New signatures not anticipated by the pre-registration: **none**.

## 6. Environment

| Field | Value |
| --- | --- |
| Harness | armed corpus run, Compose acceptance stack, 8/8 services healthy |
| Boot recipe | `scripts/acceptance/armed_corpus_boot.sh` (in-tree), `BOOT_EXIT_CODE=0` |
| Compose files | `compose.yml` + `tests/acceptance/compose.ask-dev.yml` + `scripts/acceptance/acceptance_allowance_override.yml`, all in-tree and tracked |
| Compose project | `dev-health-ask-dev-acceptance` |
| Images | Built from the repo tree by `docker compose up -d --build --wait`, **not pulled tags** — so the image identity is the branch SHA, and there is no published digest to pin |
| Python | 3.14 in the API image (`requires-python >= 3.14`); the runner side uses the worktree venv at `.venv/bin/python` |
| API bind | `127.0.0.1:18099` (18080 deliberately left to the dev stack's `acr-api`) |
| API container | `77d8499c18a3`, pinned and re-checked at three stage boundaries |
| Provider | scripted (`ASK_DEV_SCRIPTED_PROVIDER_*`), no live provider |
| Allowance | 6 orgs × 500,000,000 microUSD, counter absent at boot |
| Host | `<PLACEHOLDER: not recorded — RUN-CONDITIONS.md documents that the machine was probed quiet before boot (no gate lock, no `local_validate`, no foreign agent), but does not identify the host>` |

The allowance override is worth reading as a run condition rather than a knob. It
exists because attempt 2's 429 storm was root-caused to a **false premise in the
harness**: `ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD` is the operator *maximum* an
org may be configured up to, not the effective limit, which resolves to the
`100,000,000` default when no per-org row is stored. The runner budgeted against $200
while the server enforced $100. `500,000,000` is the reachable ceiling
(`PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD` clamps both layers) and is ~1.64× the
~$304M the 90-case corpus needs. **No product constant was changed**, and the
pre-registration was not touched.

Two boot-recipe defects were found **by running the recipe, not by reading it**, and
are part of this run's record:

* `a1541dc7d` — `armed_corpus_boot.sh` could not find the web checkout **from a
  worktree**, which is the layout it is actually run from (`BOOT_EXIT_CODE=64`).
* `c16cd2e36` — container-id pinning, re-checked at three stage boundaries, verified
  in all three states.

## 7. Machine-readable artifacts

| Artifact | Path or link |
| --- | --- |
| Per-case receipts (`wave4_case_result.v1`) | `tests/acceptance/artifacts/wave4/` — 134 receipts |
| Boot log | `phase3-corpus/.p3-logs/boot4-1618.log` |
| Run log | `phase3-corpus/.p3-logs/run3-1622.log` |
| Run conditions | `phase3-corpus/.p3-logs/RUN-CONDITIONS.md` |
| Pin-move receipt | `.p3-logs/PIN-MOVE-RECEIPT.md` |
| Durable evidence record | ops PR **#1597** |
| Gate exit status | `RUN_EXIT_CODE=1` (3 red cases — a result) |
| Supporting suite | 788 passed / 134 skipped across `tests/acceptance`; ruff and mypy clean |

## 8. Intentional-failure proof

| Planted / observed defect | Case that caught it | Old state | New state | Verdict |
| --- | --- | --- | --- | --- |
| Declared `expected_mention_texts` on a case rejected before resolution (the CHAOS-3578 edit) | `adv.oversized.question`, `adv.oversized.subject-set` | 11:11 run, no spans — PASSED | 16:22 run, spans — FAILED | The checker fired on a real, unintended change; the edit was wrong, not the guard |
| Mutation of each `64111072e` guard | both correction guards | — | non-zero exit | KILLED by exit code, not by grep |
| Boot recipe run from a worktree | `armed_corpus_boot.sh` | assumed working | `BOOT_EXIT_CODE=64` | Defect was invisible on the page; only execution surfaced it |

This section is the run's own intentional-failure evidence. A separate planted-defect
pass against the corpus checkers is **not** claimed here — see section 9.

## 9. Known limitations

Residual risk, stated as such. An admitted gap is worth more than an inaccurate
coverage claim, because a reader who sees "covered" stops checking.

| Limitation | What is therefore unproven | Tracked as |
| --- | --- | --- |
| **`readiness.capabilities.degraded`'s green is VACUOUS** | It passed, and it is graded green per v2 with no deviation — **but it proves nothing about degraded-readiness handling.** It is driven by `provider_profile_override`, which nothing in `src/` reads. The same inert field is why `deg.provider.unsupported` is red: one field, two opposite-looking symptoms. **Do not cite this case as degraded-mode coverage.** | CHAOS-3588 (PR #1595) |
| `deg.provider.unsupported` and `readiness.capabilities.unsupported-model` flip DECLARED-BLOCKED | Provider-unsupported and unsupported-model readiness paths are unverified | CHAOS-3588 |
| **QUA shadow: NOT MEASURED** | Correctly disarmed throughout, and explicitly out of scope per v2 Amendment 3. This run says nothing about shadow behavior in either direction | CHAOS-3389 / CHAOS-3525 |
| 43 declared-blocked corpus cases were not executed | Mid-conversation and multi-turn behavior (CHAOS-3454), prompt injection via ingested content (CHAOS-3456), budget/plan-size exhaustion (CHAOS-3455), redacted evidence and `missing_credentials` readiness (CHAOS-3461), cross-tenant refusal for team/user/evidence ids (CHAOS-3459), conversation purge and retention (CHAOS-3547), alias/acronym two-turn resolution (CHAOS-3475), kill switches (CHAOS-3549), filtered and inherited scope (CHAOS-3543, CHAOS-3542), partially-resolved TEAM subjects (CHAOS-3429), truncated work-graph disclosure (CHAOS-3428), attributed "light on feature work" (CHAOS-3394) | as listed |
| Scripted provider, not a live one | Live-provider behavior is not exercised by this run. The QUA commit path was proven live **separately**, outside this run — CHAOS-3532 check #4, the fuzzy-mention scenario end-to-end on merged `main` | CHAOS-3532 (closed) |
| Host identity not recorded | The run cannot be reproduced on the same machine from this record alone; the machine-quiet probes are recorded, the machine is not | — |

## 10. Verdict

**Phase 3 grade: MEASURED.**

**Basis.** All run-validity preconditions held: 90/90 executed cases recorded receipts
(`missing: 0`), zero 429s, the container-id pin held with `STACK STOLEN: 0`, the
engine identity matched, and QUA was correctly disarmed. Predictions A, C, D and E
landed as written, and **the discriminator HELD** — `adv.injection-request.url` and
`scope.unsupported-request` stayed `unsupported` while their family flipped, which is
what makes the six `refused` flips demonstrated rather than asserted. The two MISSes
are both the same self-inflicted corpus-edit defect, corrected in `64111072e` with an
asserted exemption and mutation-proven guards; neither is a product defect. The one
red, `deg.provider.unsupported`, is the predicted red on the predicted clause.
`readiness.capabilities.degraded`'s green is recorded as vacuous and is not citable as
coverage.

**Exit decision.** **Phase 3 — CLOSED, 2026-08-07 ~17:10 PT**, declared on CHAOS-3219,
citing evidence PR **#1597** merged to `main` as **`6b7517364`**. The durable record is
that PR's body plus the graded-run comment, with artifacts in-tree (`.p3-logs`, 134
receipts under `tests/acceptance/artifacts/wave4/`). Phase 2 exit had been declared
separately by chris at 13:39 PT the same day.

Closing state as declared: every substantive v2 prediction landed including the
code-routing discriminator; one predicted red; two misses self-inflicted by a corpus
edit, owned with mechanism and corrected in the merged tree with an asserted exemption
and mutation-proven guards; three readiness/degraded cases flipped DECLARED-BLOCKED on
CHAOS-3588, whose underlying mechanism CHAOS-3546 retired.

**Method note carried from the closure, because it is the point.** Three of that day's
defects — the boot recipe's worktree path, the stack-collision diagnosis, and the
corpus-edit miss — were **invisible to careful review and caught by execution on the
first run**. Recipes and claims get executed, not read.
