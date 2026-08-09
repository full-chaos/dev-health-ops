# CHAOS-3617 PR2 — handoff to the relief lane

Written at commit `ef7bb6664` on branch `chaos-3617-pr2`. Worktree clean, no
partial work. PR **#1615** is open against `origin/feature/chaos-3498-context-fabric`
and is held unmerged by the team lead.

The codex adversarial review (`gpt-5.6-luna`, xhigh) returned **BLOCK** with
7 high / 2 medium / 1 low. **Three are fixed and committed. Six remain.** Full
codex output:
`/private/tmp/claude-501/-Users-chris-projects-full-chaos-dev-health-ops/a05bc824-8df7-45ac-a83b-858480359766/tasks/bi84t19cp.output`

---

## Done (do not redo)

| Fix | Commit | What |
|---|---|---|
| 1/9 — H2 | `9730118fa` | canonical orientation, not traversal order |
| 2/9 — harness | `195d2ef46` | mutation evidence restricted to the failure region |
| 3/9 — H5 | `ef7bb6664` | asserted driver's support must be its own |

**Current verification state:** 513 passed (`tests/context_fabric` +
`tests/docs`), 0 skipped with the live store up; 62 mutations all KILLED;
mypy clean over 2243 files. The gate has **not** been re-run since these three
fixes.

Run the live store with:
```bash
docker compose --profile graph-trial up -d graph-trial-store
export CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389
export CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1
```
Without those, 26 live tests **skip** and the harness's live mutation reports
SURVIVED (loudly — it will stop your run, which is correct).

---

## Remaining findings, in severity order

### H6 — driver truncation produces no valid packet

> **codex:** Reproduced with an otherwise complete isolated readout:
> `build_packet(drivers_truncated=True)` raised the contract validation error
> that no TRUNCATED_TRAVERSAL limitation was disclosed. The flag is written
> only into DriverAnalysis, omitted from the limitation condition and omitted
> from derive_outcome's gaps calculation. If a caller drops the flag to avoid
> the exception, a truncated candidate set can be treated as complete.

**My triage:** Real, and mine. I wired `cohort_truncated` into the limitation
condition and the `gaps` calculation in `packet_builder.build_packet` and
simply forgot `drivers_truncated` alongside it.

**Repro:** not run by me; codex's is source-exact and I believe it.

**Fix shape:** add `drivers_truncated` to both the `TRUNCATED_TRAVERSAL`
limitation condition and the `gaps=bool(...)` expression that feeds
`derive_outcome` — they sit within ~40 lines of each other. Then add an
end-to-end test that truncates for real via `discover_drivers(...,
max_candidates=1)` rather than by passing the flag, because passing the flag
tests the plumbing and not the bound. Cheapest of the six.

---

### H1 — trusted evidence is not bound to the asserting edge

> **codex:** Reproduced against the Helio corpus with an adversarial readout
> mutation: making proj_meridian → dep_authcore cite canonical
> wg_authcore_shared while attaching that observation only to team_atlas
> produced PRINCIPAL_DRIVER with wg_authcore_shared as support.
> `_linkage_observations` checks only relationship type, unordered endpoints,
> and global trust; it never verifies observation attachment or edge identity.

**My triage:** Real. This is the *residue* of the defect I already fixed
once. I scoped support to the asserting **edge** (correct, and the corpus
adapter only ever populates `observation_ids` from `edge.evidence_slugs`), but
I never check that the cited observation is actually **about** an endpoint of
that edge. So an edge that cites an unrelated observation inherits its trust.

**Fix shape:** in `drivers._linkage_observations`, after collecting the edge's
`observation_ids`, require each cited observation's `subject_canonical_ids` to
intersect `{near, far}`. Ones that do not are neither trusted nor untrusted —
they are *not about this linkage* and should be dropped from both sets, with
a test proving a canonical record attached elsewhere cannot vouch.

Note the interaction with **H3**: `LiveGraphReader` returns
`subject_canonical_ids=()` for every observation, so this check would reject
*all* support on the live path. Fix H3 first, or gate the check on the reader
having attachments — but do **not** make it a silent no-op when attachments
are absent, which would reproduce the original bug on the live reader only.

---

### H4 — semantic claims are authorized by the passed embedder, not provenance

> **codex:** Reproduced with the deterministic alpha readout: passing
> `CloudEmbedder()` and an EMBEDDING_SIMILARITY match emitted a packet
> labeled `graph_arm_projection.openai_text_embedding_3_small.v1`; the bare
> CloudEmbedder had api_key=None. GraphArmStore defaults to
> DeterministicEmbedder, while build_packet accepts an unrelated embedder and
> embedder_projection_suffix is only a label.

**My triage:** Real, and **the subtlest of the six**. Read this section before
touching it.

**What makes it subtle.** The guard
(`packet_builder._check_match_mechanisms`) is not wrong about what it checks —
it correctly refuses `SEMANTIC_MECHANISMS` when `embedder.semantic` is False.
The problem is that `embedder` is an **argument**, so the guard asks "does the
caller claim semantics?" when the question that matters is "were these vectors
actually produced by something semantic?".

Three distinct traps:

1. **The vectors and the embedder are never associated.** `GraphArmStore`
   embeds at write time with whatever embedder it was constructed with
   (default `DeterministicEmbedder`). `build_packet` is called later with an
   embedder argument that has no connection to the one that wrote. Nothing
   compares them.
2. **`CloudEmbedder()` with no API key still reports `semantic=True`.** It
   refuses to *embed* without a key, but the guard never asks it to embed — it
   only reads `.semantic`. So a bare, unusable CloudEmbedder unlocks semantic
   claims.
3. **`embedder_projection_suffix` is a label, not a proof.** It goes into
   `versions.projection_version`, so a packet can be *stamped* with an
   OpenAI-model projection version while the stored vectors are BLAKE2b
   hashes. That stamp is what a downstream consumer would trust.

**Fix shape (my thinking, not prescriptive):** the embedder identity has to
travel with the *projection*, not with the call. Options, roughly in order of
how much I like them:

- record the embedder id in the projection/store partition metadata at write
  time and have `build_packet` read it back from the readout, refusing when
  the caller's embedder disagrees. This is the honest fix and closes all
  three traps; it needs a new readback field or a store-level metadata read.
- failing that, at minimum make `CloudEmbedder.semantic` depend on being
  *usable* (key present), so trap 2 closes and a bare instance cannot unlock
  anything. This is a strictly smaller improvement — **do not describe it as
  closing H4.**

Whichever you pick, the test must assert the *provenance mismatch* is refused,
not merely that a non-semantic embedder refuses semantic mechanisms — that is
what the existing test already does and it passed throughout.

---

### M1 — canonical status can be overridden after readback

> **codex:** Reproduced on Helio: baseline proj_identity_rewrite produced
> principal drv_block_wu_authcore_release; passing
> entity_attributes={"wu_authcore_release":{"declared_status":"complete"}}
> removed that candidate. The implementation updates attributes for IDs
> already present in the readout despite documenting that callers cannot
> supply a different status.

**My triage:** Real, and the **docstring is simply false** — I wrote that
callers cannot supply a different status while the code does
`attributes.setdefault(id, {}).update(extra)`, which overrides. That is worse
than the behaviour: a reader trusts the docstring.

**Fix shape:** in `drivers.discover_drivers`, only accept `entity_attributes`
for ids **absent** from the readout, and raise on an attempted override rather
than silently winning. Or remove the parameter from the production path
entirely — check whether anything but tests passes it (I believe nothing
does). Either way the docstring must end up true.

---

### M2 — the no-arithmetic proof does not cover packet assembly

> **codex:** the test scans only corpus_adapter.py and drivers.py, while
> packet_builder.py computes `filtered_total =
> readout.authorization_filtered_count + cohort_authorization_filtered` and
> serializes it into the packet limitation text. Candidate ranks and
> len()-based packet/error content are also synthesized there.

**My triage:** Real, and it was the lead's own target 6. The invariant I
defended is "no number reaches a packet that a canonical service did not
mint" — and an operational count *is* a number reaching the packet.

**Fix shape:** this needs a **stated distinction**, not a widened scan. There
are two kinds of number: *canonical analytical measurements* (must be cited
verbatim, never derived) and *operational metadata about the run itself*
(counts of filtered results, truncation counts, ranks — these are the arm
describing its own behaviour and deriving them is correct and required).

Write that distinction down, then enforce it: extend the scan to
`packet_builder.py` but with an explicit, named allowlist of operational
counters, and a test that the allowlist contains no measurement-derived name.
An unexplained exception here would be worse than the current gap.

---

### L1 — an assertion that cannot fail, in my own test

> **codex:** `tests/context_fabric/test_chaos_3617_drivers.py:302-308` contains
> `any(...) or True`. That assertion cannot detect removal of the canonical
> supporting record.

**My triage:** Real and embarrassing — a vacuous assertion I wrote, inside the
test guarding the support-scoping defect, which is exactly the fault I have
been catching elsewhere all lane.

**Fix shape:** delete `or True` and assert the expected canonical evidence id
directly on the real-edge finding. Trivial; do it first as a warm-up.

---

## Declared residuals (NOT defects introduced here)

### H3 — live readback drops observation attachment

Accurate, and it is an **unmet commitment**, not a surprise. The differential
oracle's `EXCLUDED` dict in `test_chaos_3617_live_store.py` names it and says
closing it "needs observation-attachment readback (PR2)". PR2 did not.

The lead's instruction: the PR body must say **"declared for PR2, not closed
in PR2, now owed"** with his sign-off recorded — *not* a quiet re-declaration.
Draft that wording and get it signed off; do not soften it.

Consequence to keep in mind: because `LiveGraphReader` returns empty
attachments, the live path **cannot emit supported drivers at all** today, and
the live test builds a packet without drivers so it never noticed. See the
interaction note under **H1**.

### H7 — authorization is caller-declared

Accurate but **pre-existing and documented**: the frozen contract's own
validator docstring says the authorized set is producer-declared, and
`readback.derive_authorized_entity_ids` raises
`AuthorizationDerivationNotImplementedError` by design. Not introduced by this
branch. Still genuinely unclosed; treat as a declared residual with the same
honesty as H3.

---

## Verification debt

**36 of 62 mutations still declare `expect_failure="assert"`**, which any
failing assertion satisfies. The region check (fix 2/9) makes every token
*trustworthy*; it does not make a weak token *specific*. Those 36 prove the
suite went red and nothing about why.

`test_chaos_3617_guard_harness.py::test_the_bare_assert_token_count_is_pinned_and_falling`
pins the number at `<= 36` so it can only shrink. The lead's instruction: pay
them down opportunistically while working each finding's area, and whatever
remains at push time goes in the PR body as the pinned residual it is.

---

## Checklist before push

1. `git fetch origin && git rebase origin/feature/chaos-3498-context-fabric`
   over `4130dd1df`. Watch for conflicts in `mkdocs.yml` and
   `docs-data/ia/contribute.tsv` — keep **both** lines; rerere has produced
   marker-free frankenmerges on this branch before, so read the result.
2. Bump the guard count in
   `docs/contribute/architecture/graph-investigation-arm.md` if you add
   mutations — a test pins doc and harness together and it is easy to miss.
3. Full run with the live store up: `tests/context_fabric` + `tests/docs`,
   then the guard-injection harness, then `mypy src tests scripts`.
4. **`bash ci/local_validate.sh` against a COMMITTED, FROZEN tree.** Do not
   edit anything while it runs. I did once this lane; it returned three
   failures, two were mid-edit artifacts, and I then wrongly dismissed the
   third — which was a genuine long-standing failure. A dirty run does not
   just waste time, it manufactures a credible reason to dismiss a true
   positive.
5. Ask the lead for the gate slot before running it — it is host-wide
   single-flight.
6. Push: `PATH="$PWD/.venv/bin:$PATH" git push`. The pre-push hook resolves a
   bare `mypy` from PATH (global, missing stubs), so the venv prefix is
   required. **Never `--no-verify`.**
7. Never `git stash` in these worktrees — the stash stack is shared host-wide
   across every ops worktree. Use file copies.

---

## Standing constraints (unchanged)

Never merge. Never post to Linear. Never modify user-visible Ask Dev
behaviour. Nothing exposed via ACR or MCP. Both flags default off. Graphiti
stays an optional extra and is never in the default production dependency
set. Red check = fix-first, never exclude. Report full-run results only —
never green on a subset.
