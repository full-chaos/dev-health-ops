# semantic-leg.records.json regeneration — 2026-08-10

**This is a dated, cited explanation of why `semantic-leg.records.json` moved, not a
silent overwrite.** The file was generated once, at the original CHAOS-3647 landing
(tree `eee3d1571`), and was never regenerated again — three tickets later changed the
measured system underneath it without the artifact ever reflecting them. This note
names every one of those changes and states, for each, whether and how it moved the
eight measured rows. Matches the spirit of `trials/chaos_3619/refusal_causes
.DIVERGENCE_LEDGER` (append-only, dated, cited, directional) for a trial that has no
existing pin-vs-recompute self-check of its own to hang a machine-checked ledger off.

| | |
| --- | --- |
| Old artifact | tree `eee3d1571` (original CHAOS-3647 landing), never regenerated since |
| New artifact | produced by `python -m trials.chaos_3647.runner` at branch `embedded-surface-approval-gate`, commit `528b3fd0483bc02dbb9ce39038cf1a376cf60ade`, feature tip `f3ac1d221b6959a982a4ba803d471988d0f14ed8` at branch time |
| Store | live FalkorDB — `CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389`, `CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`, `CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1` |
| Embedder | `openai_text_embedding_3_small` (real model, both old and new) |
| Corpus / oracles | unchanged — same eight named cases, same authorization probes |

## What changed in the measured system, in landing order

1. **CHAOS-3654 (PR #1631), "give the semantic retrieval leg a refusal/clarification
   threshold."** Added `semantic_retrieval.assess_disposition`. Landed after the old
   artifact, so the old artifact's H08 row ("How is Halcyon doing?", a nonexistent
   entity) shows the semantic leg ranking 20 candidates with no refusal — the exact
   shape this ticket fixed, just not yet reflected in any regenerated artifact.
2. **CHAOS-3653 (PR #1637), "add the observation→entity hop to semantic subject
   resolution."** Widens what `retrieve_candidates` returns: an authorized
   observation's own subjects now enter the candidate pool, not only nodes whose own
   name/vector matched directly.
3. **CHAOS-3686 (PR #1656), "conversation-context resolution for pronouns and
   prior-turn references."** Added `semantic_retrieval.resolve_conversational_reference`.
   **Not exercised by this trial** — `legs.py::resolve_semantic` has no prior-turn state
   to hand it (the corpus's own `follows_case_id` chain is not threaded into
   `trials/chaos_3647` anywhere), so H04–H06 (the pronoun/prior-turn cases) show no
   benefit from this ticket in this artifact. Flagged as a separate, real gap on issue
   3666, not fixed here — it needs new state-threading through `runner.py`, a larger
   piece of work than wiring in a policy call that already had everything it needed.
4. **This change (issue 3666, harness wiring + probe fix, this PR).**
   `legs.py::resolve_semantic` previously called `retrieve_candidates` directly and
   reported every authorized candidate as the leg's answer — (1) and the disposition
   policy it built were never actually exercised by what this trial measures.
   `resolve_semantic` now calls `assess_disposition` and reports
   `disposition_result.presented` as `subjects`, so what the trial measures matches
   what a real caller would receive. Separately, `probes.py`'s `unrestricted_control`
   pass condition (`authorization_filtered_count == 0`) predates (2): a query "aimed at
   nothing restricted" can now legitimately, incidentally retrieve something restricted
   through the wider hop net and have it correctly filtered, which the old condition
   scored as a failure. Fixed to fail only when the control's own target isn't ranked.

## The three-way delta, by case

`both_correct` / `deterministic_only_correct` / `semantic_only_correct` /
`neither_correct_neither_ranked` / `neither_correct_both_ranked` /
`neither_correct_semantic_ranked_anyway` — see `records.py::Delta` for what each means.
"Old" is the tree-`eee3d1571` artifact (pre 1–4 above); "post-hop/threshold" is a
regeneration at commit `528b3fd04` **before** this PR's harness-wiring fix (captures
(1)–(3) landing but not (4)); "new" is this artifact (captures all of (1)–(4)).

| case | old (`eee3d1571`) | post-hop/threshold, pre-wiring | new (this artifact) |
| --- | --- | --- | --- |
| H01_acronym_resolution | both_correct | both_correct | both_correct |
| H02_old_and_current_name | deterministic_only_correct | deterministic_only_correct | deterministic_only_correct |
| H03_the_auth_work | neither_correct_semantic_ranked_anyway | neither_correct_both_ranked | neither_correct_both_ranked |
| H04_pronoun_follow_up | neither_correct_semantic_ranked_anyway | neither_correct_semantic_ranked_anyway | neither_correct_neither_ranked |
| H05_the_other_project_we_discussed | neither_correct_semantic_ranked_anyway | neither_correct_semantic_ranked_anyway | neither_correct_neither_ranked |
| H06_prior_attempt_reference | neither_correct_semantic_ranked_anyway | neither_correct_semantic_ranked_anyway | neither_correct_neither_ranked |
| H07_unresolved_needs_candidates | neither_correct_semantic_ranked_anyway | deterministic_only_correct | **both_correct** |
| H08_no_match_must_not_widen | deterministic_only_correct | deterministic_only_correct | **both_correct** |

**The headline: `neither_correct_semantic_ranked_anyway` — "neither leg was right, and
the semantic leg ranked something anyway" — went from 5 of 8 cases to 0.** That bucket
is named in `records.py` as "the most expensive class: a wrong answer costs more than a
refusal, and this is the one a headline 'graph reaches more answers' figure would
hide." It no longer has any members. H07 and H08 moved all the way to `both_correct`;
H03 moved out of the worst bucket into a shared-failure one (both legs wrong, neither
confidently so); H04–H06 moved from "semantic confidently wrong" to "semantic safely
refuses, matching the deterministic baseline's own refusal" — the correct interim
state pending the conversational-reference wiring named above.

**What did not move, and should not be read as this change's doing:** H03's
*deterministic* baseline itself now ranks 3 candidates where the original pin ranked
none (`baseline_ranked: 0 → 3`). This is `trials.chaos_3619`'s own already-ledgered
CHAOS-3648 (PR #1622) extractor change (`question_interpreter.extract_mentions`'s
untyped path, see `refusal_causes.DIVERGENCE_LEDGER`'s `H03_the_auth_work` entry under
`PHRASE_EXTRACTED_POST_3648`) — nothing in this PR touches extraction, and the
deterministic leg's own code (`discovery.py`) is unchanged since its original landing.
Named here only so a reader diffing this artifact against the original pin does not
attribute someone else's already-documented change to this one.

**Safety invariants, unmoved across all three artifacts:** `any_prose_disclosure:
false`, `any_unauthorized_entity_ranked: false`, zero ineffective probes. The wider hop
net changes what gets *retrieved*, never what gets *disclosed after authorization* —
that boundary held identically before and after every change on this list.

## Reproducing this

```
CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389 \
CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1 \
CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1 \
uv run python -m trials.chaos_3647.runner
```

Run it at `eee3d1571` to reproduce the original artifact; at this PR's head to
reproduce this one. `binding.commit`/`binding.feature_tip_commit`/`binding.tree_clean`
in the artifact record which tree actually produced it — check that block before
trusting any output as a measurement.
