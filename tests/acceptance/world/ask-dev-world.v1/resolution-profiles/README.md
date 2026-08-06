# `resolution-profiles/` — matcher-specific expected outcomes

CHAOS-3219 Wave 4 Phase 2 Lane 2a, per the CHAOS-3389 fold-in design
(`scratchpad/c3389-phase2-foldin.md`, 2026-08-05): a corpus case's
`invariants` assert the CONTRACT (a named miss never silently widens, no
`internal_error` on fuzzy labels, no unauthorized candidate ever surfaces,
etc.) — those hold regardless of which subject-resolution matcher is
running. Anything that depends on WHICH matcher is active (e.g. "an
unquoted parenthetical label resolves to `deterministic-alias` under
today's deterministic stack, but to `deterministic-exact` once QUA can
resolve it directly") lives here instead, keyed by case id, in a file named
after the profile it describes.

## Format

```jsonc
{
  "schema_version": "resolution-profile.v1",
  "profile_id": "deterministic-v1",
  "cases": {
    "<registry-or-runner-selftest-case-id>": {
      "expected_resolution_path": "deterministic-exact" | "deterministic-alias" | "miss-clarification",
      "expected_public_outcome": "answered" | "answered_with_gaps" | "needs_clarification" | "..."
      /* additional matcher-specific keys as case authoring needs them --
         this object is opaque to case_schema.py/receipt.py; only a case's
         own `invariants[].check` (e.g. `resolution_path_in`'s
         `from_profile` arg, see invariants.py) decides which keys it
         reads. */
    }
  }
}
```

Loaded by `scripts.acceptance.corpus.case_schema.load_resolution_profile`.
A case citing a `resolution_profile_ref` whose profile has no entry for
that case id fails loud at expectation-resolution time
(`resolve_case_expectations`) — never silently treated as "no
expectations".

## Today's profile: `deterministic-v1.json`

The active profile for the current deterministic (regex/substring +
CHAOS-3388 alias-matching) resolution stack — what every corpus case runs
against until CHAOS-3389 (Question Understanding Agent) ships. Lane 2b
populates this file's `cases` map as each corpus case is authored; Lane 2a
(this directory's owner) only ships the schema and the loader/validator, not
the corpus's own per-case expectation values.

## Future: a QUA profile

When CHAOS-3389 lands, a shadow (`qua-shadow-v1.json`) or committed
(`qua-v1.json`) profile is added alongside this one with the SAME case ids
but different `expected_resolution_path`/`expected_public_outcome` values
for whichever cases QUA resolves differently — the case files themselves,
this directory's loader, and the corpus runner all stay unchanged; only a
new profile file is added and the runner is pointed at it (or both, for a
shadow-vs-current diff run).
