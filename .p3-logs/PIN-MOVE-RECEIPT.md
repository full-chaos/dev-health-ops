# Pin-move receipt — `deg.source-state.measured-zero` may now declare `resolution_path_in`

**Purpose.** The pre-registration named one live question this run had to answer:

> `deg.source-state.measured-zero` deliberately does **not** declare
> `resolution_path_in`: the pin refused it on the grounds that *no case in this
> corpus has yet been OBSERVED producing a non-null resolution_path*. Post-3533
> that should change. **If the eight class-closed cases now produce non-null
> paths, that fact is the evidence needed to add the check to measured-zero
> deliberately, with the receipt and the pin moved in one change.**

**Answer: YES.** This file is that receipt. It exists so the pin move is one
deliberate change backed by a measured run, not a plausible inference.

---

## Run identity — the measurement this evidence comes from

| field | value |
|---|---|
| branch | `chaos-3219-phase3-corpus` |
| head | `150bcb432` (base `5a20844af`) |
| run | 2026-08-07, 11:11–11:19 PT (corpus wall time 443.55s) |
| WORLD_DIGEST | `6dda65a695befc986852f15a5488a1013153201852771c1840556984c1e96e96` (identical across every receipt below) |
| engine identity | `92280f2b575990ba3489cd64643c5cfd637b3cc5133048c9028161e202555809`, role `legacy_agent`, 96 cases |
| collection | 134 collected · 0 skipped · 90 corpus cases executed |
| receipt coverage | 90 executed / 90 active receipts / **0 missing** |
| run status | **MEASURED** |
| logs | `.p3-logs/run2-1104.log`, `.p3-logs/boot2-1055.log`, grade `.p3-logs/GRADE2.txt` |

The receipt-coverage line is load-bearing. An earlier run the same day reported
`134 collected, 0 skipped` while 59 of 90 cases had died on HTTP 429 before
recording anything — every count looked clean and the evidence below would have
been drawn from a third of the corpus. Do not reuse this receipt against any run
that cannot show `missing: 0`.

---

## Evidence — 8/8 class-closed cases produced a NON-NULL resolution_path

All eight `$comment_known_red_class_closed` cases passed `resolution_path_in`,
each with a non-null path. This is the fact the pin was waiting on.

| case | status | resolution_path | `resolution_path_in` |
|---|---|---|---|
| `deg.source-state.deleted` | passed | `miss-clarification` | PASS |
| `deg.source-state.unauthorized-not-visible` | passed | `miss-clarification` | PASS |
| `scope.deleted-subject` | passed | `miss-clarification` | PASS |
| `scope.no-match` | passed | `miss-clarification` | PASS |
| `scope.outcome.unresolved` | passed | `miss-clarification` | PASS |
| `subject-label.typo-tolerance` | passed | `miss-clarification` | PASS |
| `subject-label.unauthorized-candidate-excluded` | passed | `miss-clarification` | PASS |
| `tenant.cross-tenant-identifier-refused` | passed | `miss-clarification` | PASS |

**8/8 non-null. 0 null. 0 unmeasured.** This is also the live proof of CHAOS-3533
(`orchestrator.py` persisting the whole ledger on TERMINATE).

## Direct observation of `measured-zero` itself

Stronger than the inference the pin asked for — the case produced a path of its
own, while still not asserting one:

```
deg.source-state.measured-zero: status=passed  resolution_path='deterministic-exact'
asserted checks: ran_as_declared_principal, provisioned_via_world-seeded-credentials,
                 measured_wave_3_1_preflight_path, scope_resolution_outcome_in,
                 public_outcome_in
resolution_path_in present? NO  <- the pin, still in place
```

---

## ⚠ The value to pin is `deterministic-exact`, NOT `miss-clarification`

The eight class-closed cases all resolve `miss-clarification` because they are
misses. **`measured-zero` is not a miss** — it resolves its subject exactly and
then reports a measured zero, so its observed path is **`deterministic-exact`**.

Copying the class-closed value into `measured-zero`'s check would produce a red
that looks like a product regression and is really a transcription error. The
eight cases are the evidence that paths are now *observable at all*; the value to
assert for `measured-zero` comes from `measured-zero`'s own receipt.

## Recommended change (one deliberate commit)

1. Add `resolution_path_in` to `deg.source-state.measured-zero`'s invariants with
   the allowed set **`['deterministic-exact']`**.
2. Remove/replace the `$comment` pin that refused the check, citing this receipt
   and the run identity above — the pin's stated precondition ("no case has yet
   been OBSERVED producing a non-null resolution_path") is now false, and this
   file is the observation.
3. Land both in the same change, per the pre-registration's own instruction.

## Honest limits of this receipt

* `measured-zero`'s `deterministic-exact` is **one observation from one run**. It
  is a sound basis for pinning the value the product actually produces, but it is
  not a claim about stability across future world regenerations. Re-running the
  corpus re-observes it; the receipt-coverage line is how you know a re-run
  measured it rather than skipped it.
* This receipt covers **only** the pin move. It says nothing about the four
  misses from the same run (`adv.cross-tenant.organization-id`; the transposed
  `scope.ambiguous`/`pers.clarification-persistence` pair; and
  `portfolio.multi-project.status`) — those are separately ticketed and must not
  be folded into this change.
* The run was executed with the platform cost allowance deliberately raised out
  of the way ($500, the product's reachable maximum) — harness interference only,
  documented in `.p3-logs/RUN-CONDITIONS.md`. No active corpus case asserts
  platform-allowance behaviour, so it does not affect any evidence above.
