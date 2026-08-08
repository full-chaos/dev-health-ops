# Phase 3 evidence summary — CHAOS-3219 armed corpus run

**Status: SKELETON. Every `<...>` below is an unfilled placeholder.**
This document is authored before the grade so that its structure cannot be shaped by
the result. Fill it from the run's own artifacts; do not restate a claim this document
asks for unless an artifact supports it. A section with nothing to put in it is
recorded as "none", never deleted — a missing section reads as "not applicable" when
it usually means "not checked".

Grades against [`PRE-REGISTRATION.phase3.v2.md`](PRE-REGISTRATION.phase3.v2.md) and
nothing else. If the run's conditions differ from that document's assumed merge-set,
re-derive before grading rather than folding the difference in.

---

## 1. Run identity

| Field | Value |
| --- | --- |
| Graded run | `<GRADE: pending armed run 2026-08-07>` |
| Run timestamp (PT) | `<GRADE: pending armed run 2026-08-07>` |
| `main` SHA under test | `<GRADE: pending armed run 2026-08-07>` |
| Pre-registration graded against | `PRE-REGISTRATION.phase3.v2.md` |
| Engine identity digest | `<GRADE: pending armed run 2026-08-07>` (expected `12b961997a12…`) |
| Provider role | `legacy_agent` |
| `WORLD_DIGEST` | `<GRADE: pending armed run 2026-08-07>` |
| Superseded attempts | `<GRADE: pending armed run 2026-08-07>` |

## 2. Run-validity preconditions

Every row must be a **verified observation from the run's own output**, not an
expectation carried from the pre-registration. A row that cannot be observed is
`UNMEASURED`, and an `UNMEASURED` row voids the run rather than reducing its grade.

| Precondition | Required | Observed | Verdict |
| --- | --- | --- | --- |
| Engine identity digest matches | `12b961997a12…` | `<GRADE: pending>` | `<PASS / FAIL / UNMEASURED>` |
| `WORLD_DIGEST` matches pinned snapshot | match | `<GRADE: pending>` | `<...>` |
| QUA flags unset in the API container's own environment | unset | `<GRADE: pending>` | `<...>` |
| Items collected | 134 | `<GRADE: pending>` | `<...>` |
| Items skipped | 0 | `<GRADE: pending>` | `<...>` |
| Cases executed | 90 active | `<GRADE: pending>` | `<...>` |
| Receipt coverage `missing:` | 0 | `<GRADE: pending>` | `<...>` |
| Engine controls green | 3/3 | `<GRADE: pending>` | `<...>` |
| HTTP 429 observed | none | `<GRADE: pending>` | `<...>` |

**Verdict on validity:** `<MEASURED / UNMEASURED / VOID>`

The three standing engine controls are `adv.unsafe-error-text.provider`,
`adv.abuse.retry-storm`, and `adv.oversized.result`. If any is red the scripted engine
has regressed and no other result in the run may be reported in either direction.

## 3. Headline result

```
<GRADE: pending armed run 2026-08-07>
  collected · skipped · executed · receipts · missing
  passed · failed · errored
```

| Measure | Predicted | Observed |
| --- | --- | --- |
| Cases green | everything except the predicted reds | `<GRADE: pending>` |
| Cases red | 1 (+1 conditional) | `<GRADE: pending>` |
| MISSes | 0 | `<GRADE: pending>` |
| Discoveries | n/a | `<GRADE: pending>` |

## 4. Grade against each pre-registered prediction

One row per lettered prediction. **A prediction that came true and one that was
revised after the fact are not the same evidence** — the `Verdict` column carries
`GREEN` (predicted and observed), `MISS` (predicted and not observed), or `VOID`
(its stated condition did not hold at run time, re-derived rather than graded).

### A. The six `denied` → `refused` flips

| Case | Scripted refusal code | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `adv.injection-request.sql` | `prohibited_execution` | GREEN | `<GRADE: pending>` | `<...>` |
| `adv.injection-request.shell` | `prohibited_execution` | GREEN | `<GRADE: pending>` | `<...>` |
| `adv.injection-request.graphql` | `prohibited_execution` | GREEN | `<GRADE: pending>` | `<...>` |
| `adv.injection-request.mcp` | `prohibited_execution` | GREEN | `<GRADE: pending>` | `<...>` |
| `adv.injection-request.write` | `prohibited_write` | GREEN | `<GRADE: pending>` | `<...>` |
| `scope.prohibited-write` | `prohibited_write` | GREEN | `<GRADE: pending>` | `<...>` |

### B. The discriminator — two cases that must stay `unsupported`

| Case | Scripted code | Predicted outcome | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `adv.injection-request.url` | `unsupported_external_fetch` | `unsupported` | `<GRADE: pending>` | `<...>` |
| `scope.unsupported-request` | `unsupported_request` | `unsupported` | `<GRADE: pending>` | `<...>` |

**Why this section decides whether section A means anything:** if these two come back
`refused`, the flip was applied to a case family rather than routed by refusal code,
and section A's greens are right by luck. Record the two verdicts here explicitly even
when both are green — a reader must be able to see the discriminator held, not infer it
from the absence of a complaint.

**Discriminator verdict:** `<HELD / BROKEN>`

### C. Cases the other merged fixes should turn green

| Case | Proves | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `scope.bounded-subject-set` | CHAOS-3551 render (all three invariants) | GREEN | `<GRADE: pending>` | `<...>` |
| `pers.clarification-persistence` | CHAOS-3577 disambiguation entry | `needs_clarification` | `<GRADE: pending>` | `<...>` |
| `scope.ambiguous` | pairs with the above; must agree, not transpose | `needs_clarification` | `<GRADE: pending>` | `<...>` |
| `portfolio.multi-project.status` | CHAOS-3578 mention spans | GREEN on both resolution-path invariants | `<GRADE: pending>` | `<...>` |
| `deg.source-state.measured-zero` | newly declared `resolution_path_in` | `deterministic-exact` | `<GRADE: pending>` | `<...>` |

### D. Expected red

| Case | Reason | Predicted | Observed | Verdict |
| --- | --- | --- | --- | --- |
| `deg.provider.unsupported` | CHAOS-3546 unmerged; `provider_profile_override` has no readers in `src/` | RED | `<GRADE: pending>` | `<...>` |

If CHAOS-3546 merged before the run, this entry is **VOID** — re-derive rather than
grade against it. Record which of the two happened: `<VOID / GRADED>`

### E. Conditional — `adv.cross-tenant.organization-id`

| Branch | Condition | Predicted | Applies? |
| --- | --- | --- | --- |
| CHAOS-3574 merged | classification fix present | GREEN, `not_found` | `<yes / no>` |
| CHAOS-3574 not merged | fix absent | RED on `public_outcome_in` | `<yes / no>` |

| Check | Predicted | Observed | Verdict |
| --- | --- | --- | --- |
| `public_outcome_in` | per branch above | `<GRADE: pending>` | `<...>` |
| `no_unauthorized_candidate_surfaces` | PASS | `<GRADE: pending>` | `<...>` |

A failure of `no_unauthorized_candidate_surfaces` is a **new and far more serious
finding** than any outcome-classification result in this table, and is escalated
rather than recorded.

### F. Everything else

**Any red not listed in A–E is a MISS.** List every one, with no severity triage in
this section — triage belongs in section 6, after the count is honest.

| Case | Invariant | Observed | Classified as |
| --- | --- | --- | --- |
| `<GRADE: pending>` | | | `<MISS / discovery>` |

## 5. Discovery signatures

Sightings carried forward from the pre-registration are **discoveries, not misses**:

| Signature | Ticket | Seen this run? |
| --- | --- | --- |
| `ask_dev.orchestrator.record_frame_programming_error` | ops #1577 | `<GRADE: pending>` |
| Valkey allowance-path signature | ops #1575 | `<GRADE: pending>` |

Note the asymmetry the pre-registration fixed and this document preserves: an HTTP
**429 is a MISS**, because the allowance must not gate a measurement run. A Valkey
allowance-path **signature is a discovery**. These are different observations and are
never conflated.

New signatures not anticipated by the pre-registration:

| Signature | First seen | Ticketed as |
| --- | --- | --- |
| `<GRADE: pending>` | | |

## 6. Environment

Recorded so the run is reproducible and so a later re-run can be compared rather than
merely repeated.

| Field | Value |
| --- | --- |
| Harness | `<GRADE: pending>` |
| Compose file(s) | `<GRADE: pending>` |
| Images / digests | `<GRADE: pending>` |
| Python version | `<GRADE: pending>` |
| Host | `<GRADE: pending>` |
| Provider | scripted (`ASK_DEV_SCRIPTED_PROVIDER_*`), no live provider |
| Boot recipe | `<GRADE: pending>` |
| Preconditions asserted by the boot script | `<GRADE: pending>` |

## 7. Machine-readable artifacts

The documentation gate is not satisfied by a CI link. Each row is a real committed or
archived artifact path, not a description of one.

| Artifact | Path or link |
| --- | --- |
| Per-case receipts (`wave4_case_result.v1`) | `<GRADE: pending>` |
| Receipt-coverage report | `<GRADE: pending>` |
| Run manifest | `<GRADE: pending>` |
| Raw runner log | `<GRADE: pending>` |
| Gate exit status | `<GRADE: pending>` |

## 8. Intentional-failure proof

A test suite that has never been observed failing is not evidence that it can fail.
Record the planted defect, the case that caught it, and the observation that the
**old** state passed while the new one failed.

| Planted defect | Case expected to catch it | Old state | New state | Verdict |
| --- | --- | --- | --- | --- |
| `<GRADE: pending>` | | `<passed>` | `<failed>` | `<...>` |

## 9. Known limitations

Stated as residual risk, not as reassurance. An admitted gap is worth more than an
inaccurate coverage claim, because a reader who sees "covered" stops checking.

| Limitation | What is therefore unproven | Tracked as |
| --- | --- | --- |
| 43 declared-blocked corpus cases are not executed | `<GRADE: pending — enumerate the capability each blocked group represents>` | `<CHAOS-34xx>` |
| Scripted provider, not a live one | `<GRADE: pending>` | `<...>` |
| `<GRADE: pending>` | | |

## 10. Verdict

**Phase 3 grade:** `<GRADE: pending armed run 2026-08-07>`

**Basis, in one paragraph, naming the discriminator's verdict explicitly:**
`<GRADE: pending armed run 2026-08-07>`

**Exit decision and who made it:** `<GRADE: pending>`

A grade may not be declared from a run whose section 2 verdict is `UNMEASURED` or
`VOID`, from a partial re-run, or from a cached result standing in for a fresh one.
