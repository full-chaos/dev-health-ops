# Porting a provider/dataset pair to `route_ready` (Go complete-route)

This is the recipe extracted from three pairs: `launchdarkly/feature-flags`
and `github/repo-metadata` (`route_ready: true`, CUT-08 and CHAOS-3123), and
`github/prs` (CHAOS-3122, this document's own worked example — coded,
tested, and mutation-tested, but deliberately `route_ready: false` pending
`github/pr-reviews`; see defect class 9 below). All three follow the
identical eight-step shape below. If a future port doesn't fit this shape,
that is itself a signal worth writing down before proceeding.

**Ground rule:** trust `contracts/provider-matrix/v1/matrix.json` and the
code, never a ticket's status field. CHAOS-3123 was marked Done in Linear
having shipped 1 of 17 GitHub pairs; the matrix is what caught that.

**Second ground rule, added after `github/prs`'s codex adversarial pass:** a
green local gate — `ruff`/`mypy`/`go build`/`go vet`/`go test` including the
integration tier — is necessary and is NOT sufficient. `github/prs`'s gate
was fully green and its adversarial review still returned BLOCK with four
HIGH-severity findings, three of them the identical shape: *the route reports
success while silently losing data, and advances the watermark past what it
lost.* Every one of those bugs compiled cleanly, passed `go vet`, and passed
every test that existed at the time — because the tests were written by the
same reasoning that produced the bugs. Treat an adversarial pass over the
diff as a required step of this recipe, not a nice-to-have, and read the
checklist below BEFORE writing the handler, not after a review flags it.

**Tenant-isolation precondition:** every port must carry `org_id` through the
row type, production construction, a mismatch-rejecting validation, the
ClickHouse INSERT, and the readback predicate. Ship a cross-tenant test that
is proven to FAIL when `org_id` is removed from the predicate. This is not
defensive polish: a Critical cross-tenant leak was found on `github/cicd`, and
making this a precondition let the next three ports ship it first-pass.

**PR governance precondition:** when a change touches
`src/dev_health_ops/workers/provider_unit_route.py`, the PR body must contain
the `TEST-EVIDENCE` and `RISK-NOTES` markers required by the `governance`
check. These are PR-body markers, not code markers; two lanes lost a CI round
by omitting them.

**Session audit precondition:** audits found a real defect in every port
produced in this session, merged and unmerged alike, all with green CI. The
preconditions below come from actual shipped or nearly-shipped defects; they
are not style guidance.

**Further port preconditions:**

1. **A cited constructor is not proof of capability.** It must be reachable
   with only its own switch enabled. `github/security` shipped a correct case
   in `cmd/dev-health-worker/provider_sync.go:127`, but an upstream activation
   gate returned an empty worker family because that switch was missing from
   the gate condition. The registry said Go owned it; the binary could not
   construct it. Add the switch to the activation condition and extend the
   table-driven `TestBuildProviderSyncWorkerConstructsForEveryRouteReadySwitch`
   test, including `providerSyncRouteEnabled`, so only that pair's switch
   enabled constructs a worker family. Remove the switch and prove the test
   fails.
2. **The cross-tenant test must be a true collision, not a one-sided check.**
   A row under only the foreign tenant proves only that a foreign row is not
   returned. Insert the SAME natural key under TWO `org_id` values with
   distinguishable content; assert a tenant-scoped `SELECT ... FINAL` returns
   exactly one row containing the claim tenant's content; assert `EffectExact`
   in BOTH directions. Use
   `internal/providersync/github_commit_stats_effects_integration_test.go` as
   the example. This defect shipped twice on the same PR; both variants pass
   an `org_id` predicate mutation proof and read the same in a PR summary.
3. **Never both capped and successful.** If pagination reaches a cap, fail the
   unit and do not advance the watermark. Otherwise the dropped records are
   deterministic and every later run skips them. The rule is explicit at
   `internal/providersync/github_prs_route.go:203`, but
   `launchdarkly/feature-flags` and `github/cicd` currently truncate silently,
   return success, and advance state at their 5,000-flag and 1,000-run caps.
   Tracked as CHAOS-3192, CHAOS-3196, and CHAOS-3188.
4. **An empty result must be distinguishable from a broken fetch.** A producer
   that reports SUCCESS on an empty inventory passes seam tests while the
   capability is absent. Emit an explicit status distinguishing `complete`,
   `empty`, and `no_commit_at_bound`, as the `github/files` route does, and
   assert it in a test. Also persist the computed `FetchEvidence.CapReached`
   into the unit `Result`; `internal/providersync/native_rest.go:30` defines
   the evidence, but `resultWithEvidence` currently returns it only inside
   `FetchResult` at line 837. Truncation must remain queryable. CHAOS-3197.
5. **A fail-open that also advances state is permanent data loss.** Swallowing
   an error into an empty batch is survivable only if the unit remains
   retryable. Trace the downstream consequence through
   `internal/jobs/providerunit/`, `sync/sync_units.py`, and
   `sync/watermarks.py` before allowing any error to be swallowed. CHAOS-3189
   was live Python data loss found only by asking what state followed a
   "successful" empty result.

## Defect classes to check for explicitly in every pair

These are not hypothetical. Every one below was found by an adversarial
review of `github/prs`'s first draft, which had a green gate and passing
tests. Check for each of them by name while writing the handler, not by
hoping the tests you wrote yourself will catch what you didn't think to
question.

1. **A capped/paginated fetch must never report success.** If the Python
   collector's pagination is unbounded (no page cap in the production call),
   your Go pager's cap (`MaxPages`, `nativeMaxPages`, ...) is a safety rail,
   not a legitimate stopping point. Check `CapReached` after every paginated
   fetch and fail the whole unit if it's true. The alternative failure mode
   is silent and permanent: a capped fetch that still returns success still
   lets `Collect` return the claim's window as the completed watermark, so a
   later incremental run never revisits the records past the cap — they are
   gone, and nothing about the sync run's outcome says so.
2. **A client-side window filter must apply the SAME comparison Python
   applies, including when Python does NOT compare.** Read the exact
   conditional Python guards its since/until check with — often something
   like `isinstance(updated_at, datetime)` — and reproduce that guard, not
   just the comparison inside it. A record with a missing/null/unparseable
   timestamp is a case Python's `isinstance` check makes it skip the
   comparison for (so the record is included unconditionally); a Go port
   that treats "timestamp didn't parse" as "exclude this record" silently
   drops exactly those records, and if EVERY record in a window has that
   shape, the unit reports an empty, plausible-looking success. Split the
   "is the timestamp known at all" clause from the "is it inside the window"
   clauses — see defect class 4 below — so this is provable independently.
3. **A missing/blank identity input must fail closed, never fall back to a
   claim field.** If Python's identity derivation (a UUID hash, a lookup key)
   would raise or otherwise refuse to run on the input you're about to
   substitute a fallback for, match that: return an error, don't guess. A
   fallback that "looks reasonable" (e.g. the claim's own external ID) writes
   real rows under an identity nothing else in the system would derive for
   the same input — a foreign-key fork that is far harder to find later than
   a failed unit is now. Before adding a fallback, check whether the
   function you're already calling downstream (e.g. `repositoryIdentity`)
   already rejects the same bad input — duplicating its check independently
   is itself a code smell a mutation harness will flag as dead code (see
   "Use the mutation harness" below).
4. **Split every compound boolean into named clauses.** `windowKnown`,
   `before`, `after` as three separate named values (with `before || after`
   as the final combinator) instead of one `if a && b || c && d` line. This
   is not style preference: a monolithic compound condition is a single unit
   for a mutation harness (or a reviewer) to evaluate, so a broken clause
   inside it can hide behind the other clauses already being satisfied by
   whatever fixture the existing tests happen to use. `github/prs`'s window
   filter and readback comparator were both rewritten this way after
   defect class 2 shipped as one compound condition.
5. **Truncate provider timestamps to the ClickHouse column's actual
   precision AT CONSTRUCTION, not at comparison time.** `DateTime64(3)`
   loses anything finer than a millisecond on write; a Go value that still
   carries microsecond/nanosecond precision from `time.Parse` will compare
   unequal to what a readback SELECT scans back, and a process death between
   `WriteEffect`'s `Send` and the ledger's `CommitEffect` turns that into an
   unrecoverable `EffectConflict` — recovery can neither mark the row
   committed (bytes don't match) nor safely replay it (might double-write).
   Truncate once, where the value first enters the row.
6. **A readback "exact" comparison must scan and compare every column the
   row actually carries**, not the subset that was easy to wire up first.
   Every field the writer writes is a field a divergent same-version row
   could differ on; a comparison that can't see a column will report
   `EffectExact` for a row that is actually wrong, and recovery then marks
   corrupted data as committed. Query the columns straight into Go pointer
   types for anything `Nullable` in ClickHouse (see the next point) rather
   than `ifNull(col, sentinel)`-collapsing them first — that collapse can
   itself hide a real NULL-vs-empty-string divergence from the comparison.
7. **Match Python's exact string operation, not a stronger one.** If Python
   does `raw.strip().lower()`, the Go equivalent is `strings.ToLower(strings.TrimSpace(raw))`
   — leading/trailing whitespace only, including `\r`. A hand-rolled
   "strip everything, throughout the string" helper is BOTH wrong (it
   accepts input Python would reject, like `"clo sed"` matching `"closed"`)
   AND wrong the other direction (it can miss whitespace Python's `strip()`
   does remove, like a trailing `\r`, if the hand-rolled version's allowed
   character set doesn't happen to include it). Reach for the stdlib
   function with the matching name before writing a custom one.
8. **Stringify provider fields the way Python's `str()` would, not by type
   assertion.** If Python does `str(some_field)` before use, decode the
   corresponding JSON value into `any` (with `json.Decoder.UseNumber()` so a
   numeric value round-trips exactly) and stringify by Go type switch — see
   `stringValue` in `native_rest.go`, already shared across this package —
   rather than unmarshaling straight into a Go `string` field, which fails
   (and silently falls back to whatever sentinel your zero-value handling
   uses) the moment the provider sends a JSON number where Python's `str()`
   would have happily converted it.
9. **When one ClickHouse table is filled by columns that belong to MORE
   THAN ONE dataset pair, decide the column-vs-unit ownership question
   explicitly, in writing, before flipping `route_ready`.** This is the
   `github/prs` / `github/pr-reviews` situation:
   `first_review_at`/`reviews_count`/`changes_requested_count` are columns
   on `git_pull_requests`, but the data that fills them comes from Python's
   review-enrichment phase, which is a DIFFERENT dataset pair's job in the
   per-unit model this port uses. Writing the columns as fabricated zeros
   while claiming `route_ready: true` is not a "documented gap" — it is
   corrupted data with nothing distinguishing "no reviews exist" from "this
   unit never fetches reviews", and downstream analytics (review-latency,
   rework, AI-impact tiles) will read the zero as fact. `route_ready` is a
   promise the Go path produces the PRODUCT data for that pair, not merely
   that it compiles and passes its own tests. Three resolutions, pick one
   and write down why in the matrix case's comment and the PR body:
     - **The owning pair (here, `prs`) never writes the shared columns at
       all**, leaving them for the other pair to fill, and BOTH pairs stay
       not-`route_ready` until the one that fills the shared columns can run
       a full read-current-row-then-rewrite (or the two are redesigned to
       write disjoint tables). This needs the write-conflict semantics
       thought through — ReplacingMergeTree has no partial-column merge, so
       "not writing a column" and "writing it as the SQL default" are
       byte-identical on disk; the real fix is deciding which unit is the
       SOLE writer of the complete row, not just which unit skips which
       field.
     - **The two pairs land as one unit**, sharing one `CompleteRouteHandler`
       and one effect, since they are frequently one Python execution
       underneath the matrix's dataset-name split anyway (`github/prs`,
       `pr-reviews`, and `pr-comments` are all `_sync_github_prs_to_store_async`
       in Python). Slower to ship, but avoids the write race entirely.
     - **The first pair ships with `go_executor: native_go` but
       `route_ready: false`**, fully coded and tested, with the descriptor
       case documenting exactly which sibling pair it's waiting on. This is
       what `github/prs` did: the switch (`GithubPRs`) and all worker/config
       wiring are in place and inert, so flipping `RouteReady` when
       `github/pr-reviews` lands is a one-line change, not a second PR's
       worth of plumbing.
   If none of the three fits, that is itself a real design finding — say so
   explicitly rather than shipping `route_ready: true` anyway.
10. **Never assemble a "winning row" from independent per-column
    aggregates over a ReplacingMergeTree table.** `argMax(column,
    last_synced)` computed separately per column is NOT the same as reading
    the row with the maximum `last_synced`: ClickHouse's `argMax` skips a
    row whose ARGUMENT is NULL when picking the max, so a genuinely winning
    row with a NULL in one column can have that column silently backfilled
    from an OLDER, non-winning row's non-NULL value in the same column —
    verified empirically (see step 4 below) by writing two versions as
    separate physical parts (not one batch — a single-part insert did NOT
    reproduce it, which is its own trap: test this against realistic,
    separately-written parts, not a convenient single-batch fixture) and
    observing `argMax(body)` correctly pick the newer row while
    `argMax(merged_at)` incorrectly reached back to the older row's value.
    `FROM table FINAL WHERE <full ORDER BY prefix>` reads the winning
    version as one consistent row instead — cheap for a point lookup
    matching the primary key, and it doesn't need `ifNull`-collapsing NULLs
    to stay safe, so it doesn't reopen the NULL-vs-empty-string problem
    class 6's fix cares about either. A readback test that only puts NULLs
    on the OLDER version cannot see this bug — the fixture must put a NULL
    on the WINNING version specifically, with an older version that has a
    non-NULL value in that same column.
11. **A failure path that discards shared recovery state is worse than the
    failure it was handling.** If a job-runtime "release this unit for
    retry" primitive blindly overwrites a JSON result/state column instead
    of merging into it, ANY failure after an effect ledger was written
    (not only the specific one you're fixing) can delete the record of an
    in-flight, possibly-already-landed write — and the next attempt then
    has no way to classify that write as exact, absent, or conflicting, so
    it either starts a whole new unreconciled write or wedges. This is easy
    to introduce by accident: a NEW way for your handler to fail (a page
    cap, a stricter validation) that fires reliably on retry turns a
    latent, rare exposure in shared retry-path code into a deterministic
    one. Grep for the retry-release SQL/function your job runtime uses and
    confirm it merges (`COALESCE(existing, '{}') || new_fields`) rather
    than replaces; write an integration test that begins an
    `EffectReadbackRequired` effect, calls the release-for-retry path, and
    asserts the ledger is still loadable afterward.
12. **A page cap only means what it claims once the fetch is bounded the
    same way the Python authority bounds it.** If Python's collector stops
    paginating early once it crosses some recognizable boundary (e.g. an
    item older than the incremental window, for a sort=updated&desc
    listing), a Go port that fetches every page up to `MaxPages` regardless
    of content is capping on TOTAL HISTORY, not on the same thing Python
    bounds — so a repository whose total history is long but whose
    in-window page count is small will cap, and fail the unit, on every
    attempt, even though Python syncs it fine every time. Match the early
    stop, not just the final filtered set: a per-item post-hoc filter
    (defect class 2) and a pagination-level early stop are not
    interchangeable, and Python itself frequently has BOTH (a belt-and-
    suspenders structure worth mirroring exactly rather than picking one).
13. **An oracle comparison must ASSERT every field it decodes from the live
    Python call, not just decode it for inspection.** A field pulled out of
    the oracle's JSON output and left uncompared provides zero protection
    the moment the Python side changes that field — decoding without
    asserting is a more subtle version of defect class 9's original sin
    (a fixture that never exercises what it claims to prove). Also: **Go's
    test result cache does not know about your Python source files.**
    Editing only `testdata/python_*_oracle.py` and rerunning `go test`
    (without `-count=1`) can report a stale PASS from before the edit,
    because from Go's perspective no Go-tracked input changed. Always use
    `-count=1` — or better, drive the check through the shared mutation
    harness, whose proof commands you control — when iterating on or
    mutation-testing an oracle script.
14. **A stringification/type-coercion helper must cover every JSON scalar
    type the Python original's `str()`-equivalent would, and a test's name
    must claim only what it actually covers.** "Handles ANY non-null value"
    is a claim to verify against Python's actual type-dispatch (string,
    number, AND boolean at minimum — Python's `str(True)` is `"True"`,
    capitalized, not Go's lowercase `strconv.FormatBool`), not a claim to
    make and then only test with a number. If a type is deliberately out of
    scope (e.g. a JSON list/object is not a realistic shape for the
    specific field you're stringifying), say so explicitly in the
    function's doc comment rather than let the untested gap hide behind an
    overbroad test name.
15. **A precision/truncation fix needs a fixture at EVERY point that value
    is truncated, not just one.** `github/prs` truncates a provider
    timestamp to millisecond precision in one function
    (`parseGitHubPullTime`) and truncates `normalizedAt` (which becomes
    `last_synced`) at a SEPARATE call site inside `Collect`. A test proving
    the first truncation says nothing about the second, and if every OTHER
    fixture in the test file happens to use whole-second timestamps (a
    natural default when hand-writing `time.Date(...)` literals), deleting
    the second truncation is invisible to the entire existing suite. Grep
    for every `.Truncate(` call the handler makes and confirm each one has
    its own sub-precision fixture, not just the first one you wrote a test
    for.

16. **A hand-picked field list in a parity oracle is a standing invitation
    for the NEXT unpicked field to ship broken.** Defect class 13 above (an
    oracle must assert every field it decodes) and this pair's own review
    history proved the same shape of gap twice: `github/prs`'s first oracle
    hand-picked `state`/`author_name`/`created_at`; its replacement decoded
    `merged_at`/`closed_at` from the live function and simply forgot to
    assert them, for two review rounds in a row, because nothing forced the
    list of asserted fields to stay in sync with the list of fields the row
    actually carries. CHAOS-3162 replaced "assert the fields you thought to
    hand-pick" with a **generic, declarative, whole-row comparator** that
    every future pair should use instead of writing a new hand-authored
    field-by-field oracle test:
    - `internal/providersync/testdata/oracle_registry.py` — a pair
      registers itself (`PairSpec(id, build_row, excluded_fields)`) as a
      side effect of importing its own file under `testdata/oracle_pairs/`.
      Nothing in the registry, or in the CLI runner
      (`python_generic_row_oracle.py`), changes to add a pair — that is the
      whole point: a pair difference is a difference in WHAT gets compared,
      never in HOW.
    - `internal/providersync/oracle_compare_test.go` —
      `compareRowsAgainstPythonOracle`/`oracleDivergences` diff **every
      key** present on either side (the union, not the intersection) and
      fail on ANY undeclared divergence, including a key present on one
      side and absent on the other. An exclusion (a field one side
      structurally cannot have an opinion about — review-enrichment fields
      not yet built, Go-only effect bookkeeping like `org_id`/`last_synced`)
      requires a written reason at both the Python (`excluded_fields`) and
      Go (`goOnlyFields` parameter) side — mirroring
      `expected_survivor_reason` in the mutation harness: an omission must
      be declared, never silently missing.
    - The same comparator generalizes past row-CONSTRUCTION: this pair also
      used it for the readback boundary (`oracle_readback_integration_test.go`,
      comparing a written row against what a real ClickHouse `SELECT`
      returns) and a list-inclusion-DECISION boundary
      (`github_prs_window_oracle_test.go`, comparing a boolean decision
      struct rather than a row). `diffRows` doesn't know or care what shape
      of thing it's comparing — only that both sides serialize to
      `map[string]any` with the same keys meaning the same thing.
    - Proving a comparator actually catches something is itself a testable
      claim, not an assertion: for every defect class this framework was
      built to prevent, write a "rediscovers" test — the SAME comparator,
      cases, and pair id, but with a documented pre-fix/buggy builder
      substituted for the real one — and assert the divergence list is
      non-empty. **Do not try to do this by wrapping
      `compareRowsAgainstPythonOracle` in `t.Run` and checking the returned
      bool**: a subtest's `t.Errorf` marks EVERY ancestor test failed
      regardless of what the caller does with `t.Run`'s return value; call
      `oracleDivergences` directly instead and assert on its returned slice
      (see `requireOracleRediscovers` and
      `TestGenericOracleRediscoversRowConstructionDefects`).
    - **Prefer live execution over a pinned copy even when the import chain
      looks disproportionate — a monkeypatched dependency seam plus a
      sentinel exception usually gets you there.** `github_prs_window.py`
      originally shipped as a byte-for-byte PINNED copy of
      `_collect_github_pr_objects`'s two decision `if` blocks, reasoned to
      be "too much stubbing surface for a decision this small." Codex's
      third review correctly rejected that: an unordered substring-presence
      check over a multi-thousand-byte slice cannot detect a mutation that
      keeps every pinned fragment present but changes what they mean (e.g.
      deleting the SECOND `isinstance(updated_at, datetime)` guard leaves
      the string `"isinstance(updated_at, datetime)"` still present
      elsewhere in the slice — the pin reports "still matches" while being
      stale). The fix was live execution after all: `python_oracle_loader.py`
      gained `_target_github_processor()`, stubbing every module-level name
      `processors/github.py` needs to IMPORT (analytics.complexity,
      credentials.types, models.git, base_git, fetch_utils, release_ref,
      storage_protocol, testops_ingest, providers.github.client, pr_state,
      providers.usage, utils — none of them need to be FUNCTIONAL, only
      importable, since the harness's execution path never reaches them),
      then the pair itself monkeypatches the ONE dependency seam the
      function actually calls through at run time
      (`module._github_code_client_from_connector`) with a fake client
      whose `get_pull_detail` raises a distinguishing sentinel exception
      the INSTANT it's called. Since the real function's `try/finally` has
      no `except`, the sentinel propagates cleanly to the harness, which
      reads off exactly how far the loop's real `continue`/`break` control
      flow got by which of two list items (or neither) reached the fetch
      step. Verified empirically: reproducing H3's original bug in the REAL
      `processors/github.py` (not the oracle) crashed the live oracle
      immediately, which the byte-for-byte pin would only sometimes have
      caught depending on the exact edit shape. Reach for a pinned/digested
      fallback only when there is no seam at all to fake through (no I/O
      boundary, no injectable dependency) — that is a narrower case than it
      first appears.
    - **REQUIREMENT: the stub path a `load_live_module` target takes must be
      exercised LOCALLY, not just in whichever CI job happens to be more
      isolated than your workstation.** `_target_github_processor()`
      shipped, gated locally, and still broke CI's `go-storage-integration`
      job on the very next PR push: `TypeError: unsupported operand
      type(s) for |: 'NoneType' and 'NoneType'`, from
      `processors/github.py`'s own `gate: RateLimitGate | None = None`
      parameter annotation, because the stub set `CONNECTORS_AVAILABLE =
      False` (following `_target_base_git`'s precedent verbatim) which
      routes github.py into an `else:` branch that sets `RateLimitGate =
      None` at module scope — and `None | None` is not a valid type union.
      **Why the local gate did not catch this — the actual mechanism, not
      just "the environments differ":** Python 3.14 (this loader's local
      interpreter) defaults to PEP 649 deferred annotation evaluation —
      `gate: RateLimitGate | None` is not actually evaluated when the `def`
      statement runs, only the first time something reads
      `__annotations__`. `load_live_module` never did that, so
      `exec_module` returned successfully and every local test built on
      top of it passed. CI's isolated job ran an OLDER Python (pre-3.14,
      where annotations are still evaluated eagerly at `def` time) and hit
      the crash immediately on import. This is a real, general trap, not a
      one-off: a stub whose SHAPE is wrong for a name used in an
      annotation can pass on 3.14 and fail on anything older (or on
      anything, any version, that later calls `typing.get_type_hints` or
      `inspect.signature` on the loaded module — mypy does this too). The
      fix landed in the SHARED loader, not just this one target:
      `load_live_module` now calls `_force_annotation_evaluation` on every
      freshly-loaded module before returning it, which touches
      `__annotations__` on every function and class the module defines —
      forcing PEP 649's lazy evaluation to happen NOW, deterministically,
      on whichever Python is running the loader, so a broken stub fails on
      the very next line locally, on any Python version, instead of only
      on whichever CI image or future interpreter happens to evaluate it
      eagerly. Verified by re-injecting the exact broken stub
      (`CONNECTORS_AVAILABLE = False`, no `connectors.*` stubs) and
      confirming `load_live_module` now raises immediately with a message
      naming the broken annotation — and separately by running the full
      suite against a real Python 3.13 interpreter (`PYTHON=<3.13 binary>
      go test ...`), which has eager annotation evaluation and reproduces
      what CI's older Python actually sees. **Do this for every new
      `load_live_module` target**: after writing the stub, deliberately
      break one name it supplies (make it `None`, or drop a needed
      attribute) and confirm the loader itself — not just some specific
      test that happens to touch that name — reports the break; if it
      doesn't, `_force_annotation_evaluation` isn't the whole story for
      that target and something narrower needs adding.
    - **REQUIREMENT: always run `go test -count=1` (never a bare `go test`)
      when a test exercises a live oracle whose Python source lives outside
      `internal/providersync/testdata/`.** `//go:embed` cannot reach outside
      its own package directory — Go's embed patterns reject any `../`
      component (verified directly: `go vet` rejects such a pattern with
      "invalid pattern syntax") — so the `//go:embed` cache-busting fix
      above can ONLY cover files under `internal/providersync/testdata/`
      (the framework's own registry, loader, runner, and pair files). It
      structurally CANNOT also embed the production
      `src/dev_health_ops/**.py` files a live oracle executes
      (`processors/base_git.py`, `code_client.py`, `pr_state.py`,
      `processors/github.py`, and every future one). Editing any of those
      alone, with no Go file changed, can leave `go test` unable to tell
      the difference from the last run and return a stale cached PASS. This
      is the same class of trap as the mutation-harness caching bug below,
      in a place the tooling literally cannot reach — treat `-count=1` as a
      hard requirement for these tests, not a nice-to-have, and say so at
      the top of any new oracle pair file that reaches outside
      `testdata/`.
    - **A shared mutation harness proof command with no cache-bypass can
      report a false SURVIVED (or, worse, a false KILLED) depending on
      unrelated prior `go test` invocations in the same session** —
      discovered empirically while re-verifying an EXISTING (already-KILLED)
      mutation after refactoring the function it targets: the harness's
      `_run_command` does not pass `-count=1`, and a warm test cache
      returned a stale `(cached)` PASS for a proof command run against a
      manually re-applied version of the SAME mutation, even though the
      underlying `.go` file's content had genuinely changed.
      **Why this is surprising, and worth stating precisely rather than
      waving at "caching flakiness":** Go's test-result cache key is
      supposed to be content-addressed — the compiled test binary's action
      ID is a hash of its package's source, so editing a `.go` file changes
      that hash and should force a cache miss on its own, with no
      `-count=1` needed. That is true in a fully serial reproduction: a
      `go clean -testcache`, one baseline run, one mutation, one re-run —
      by itself — reliably shows the correct re-execution every time this
      was tried in isolation. The stale hit was reproduced twice, and both
      times it was in a session with SEVERAL concurrent `go build`/`go
      test`/`go vet` invocations in flight against the same package at
      once (parallel background gate scripts, plus `gopls`'s own
      continuous background compilation, all sharing one `GOCACHE`) — and
      it did NOT reproduce in later, deliberately serial retries, including
      one that added a background contention loop (`go build`/`go vet`
      looped 40 times) around the same mutate-and-test sequence without
      catching it again. That pattern — content-addressing correct in
      isolation, failure only under concurrent load against a shared cache
      directory — points at a timing-dependent interaction with a
      concurrent writer, not a flaw in the content hash itself, but this
      was NOT pinned down to an exact interleaving; treat it as the
      best-supported hypothesis from the evidence gathered, not a proven
      root cause. What IS certain, independent of whichever exact
      mechanism is responsible: `go test -count=1` disables cache lookup
      entirely by design (documented Go behavior, not a workaround), so it
      closes this hole regardless of what is ultimately causing it. Until
      `scripts/mutation_harness.py` passes `-count=1` (or an equivalent
      cache-bypass) in its own proof-command invocation, **always run `go
      clean -testcache` immediately before any `mutation_harness.py run`**
      — a full cache clear reliably reproduced the correct KILLED verdict
      every single time it was tried, unlike `-count=1` on a single proof
      command in isolation which was not separately re-verified against a
      confirmed-concurrent-load reproduction of the bug. Do not trust a
      lone SURVIVED or KILLED verdict produced against a warm cache without
      that step, and if you run a mutation plan while other `go`
      invocations are active in the same environment, clear the cache
      again afterward before trusting the result.
    - **A declared exclusion is a claim, and claims need enforcing, not just
      writing down.** `goOnlyFields[key]` asserts "the Python side
      structurally cannot have this field"; nothing checked that assertion
      against what Python actually sent until codex's third review named
      it. `oracleDivergences` now checks it on every case: if a
      `goOnlyFields` key shows up in the Python row anyway, that is a hard
      divergence in its own right (the field is not actually Go-only, and
      excluding it hides a real, comparable value) — see the "exclusion
      integrity" subtest `compareRowsAgainstPythonOracle` adds alongside
      the per-case ones. The same pass also flags a declared exclusion
      (either map) that never matched ANY key across the whole batch of
      cases as stale. Both checks are necessarily BATCH-level, not
      per-case — `compareRowsAgainstPythonOracle` now shells out to Python
      ONCE for the whole case list (it used to shell out once per case)
      specifically so this is checkable at all. Of the five defects named
      in that review round (empty `cases`, duplicate case IDs, Python `{}`
      equalling a nil Go map, exclusions silently unenforced, and
      `goOnlyFields` not verified as actually Go-only): the first two were
      already fixed the prior round; "Python `{}` equalling a nil Go map"
      turned out to already be DEAD by construction once builders return
      concrete struct types — a literal Go `nil` fails the
      `map[string]any` type assertion outright (hard `t.Fatalf`, not a
      silent pass), and two non-nil-but-genuinely-empty maps are already
      caught by `diffRows`'s existing "both rows are empty" guard; the
      other two needed the fix described above. This staleness/enforcement
      check is scoped to `oracleDivergences` callers (the row-construction
      and window-decision pairs) — the readback pair calls `diffRows`
      directly with hand-verified, static exclusions and is not yet
      covered by it.
    - This does NOT retroactively apply to the five oracles that predate
      CHAOS-3162 (`python_launchdarkly_normalization_oracle.py` and
      friends, including this pair's own OWN existing
      `python_github_prs_normalization_oracle.py`) — they keep working
      exactly as documented in step 8 below. Only NEW pairs should reach
      for the generic path first.

## The recipe

### 1. Read the Python authority for the pair, not just its shape

Find the *exact* fetch → normalize → write function chain, not an
approximation. For `github/prs` this meant tracing
`src/dev_health_ops/processors/github.py::_sync_github_prs_to_store_async`
down through `_collect_github_pr_objects` (fetch),
`providers/github/code_client.py::_pull_from_item` (parse),
`processors/base_git.py::build_git_pull_request` +
`providers/pr_state.py::normalize_pr_state` (normalize), and
`storage/clickhouse.py::insert_git_pull_requests` (write). Getting this
wrong is the single biggest risk: mirroring the *shape* of a Python function
without its exact semantics (a field default, a timestamp fallback order, a
state-mapping edge case) produces a Go row that looks plausible and is
silently wrong.

Things that are easy to miss and must be checked explicitly for every pair:

- **Identity derivation.** Does the row's primary/foreign key come from the
  claim's `SourceExternalID`, or from a value the API returns (e.g.
  `repo_id` in `git_pull_requests` comes from `get_repo_uuid_from_repo(repo_info.full_name)`
  — the API's `full_name`, not the claim's owner/repo string verbatim)?
- **The org_id write path.** `ClickHouseStore._insert_rows` auto-injects
  `org_id` from `self.org_id` when the caller's column list omits it
  (`storage/clickhouse.py:309-320`) — the row model / dataclass frequently
  has *no* `org_id` field at all even though the ClickHouse table does.
  Check `src/dev_health_ops/migrations/clickhouse/027_add_org_id_to_sorting_keys.py`
  for the table's real `ORDER BY`; don't assume `024_add_org_id.sql` covers a
  raw `git_*`/`ci_*`/`deployments` table — migration 027's
  `TABLES_NEEDING_ORG_ID_COLUMN` list is the ones 024 missed.
- **Field defaults and fallbacks.** e.g. `BaseGitProcessor.coerce_created_at`
  (`created_at or merged_at or closed_at or now()`), or repo-metadata's
  `default_branch` coercion to `"main"`. These are exactly the kind of
  one-line detail a shape-only port drops.
- **What the Python sink does *not* persist.** e.g. `repos` never persists
  `archived`; comparisons and shadow sources must only assert the shared
  contract.

### 2. Design the row type and effect destination(s)

One matrix pair may write to **one or several** ClickHouse tables — this is
not a 1:1 rule. `launchdarkly/feature-flags` writes four tables in one
`Collect` call (`feature_flag`, `feature_flag_event`, `feature_flag_link`,
`work_graph_edges`); `github/repo-metadata` and `github/prs` each write one.
The destination manifest is `CompleteRouteDescriptor.Destinations`, and
`CompleteRouteBatch.validate` enforces that `len(batch.Effects)` matches it
exactly.

Write a Go struct (`xRow`) whose JSON field names and field *order* mirror
the ClickHouse column order the Python sink actually inserts (check the
`_insert_rows(table, [...columns...], rows)` call, including any
auto-injected `org_id` — it is appended **last**, after the explicit column
list, not alphabetically sorted). This is what keeps the effect digest
(`BuildEffectBatch`) meaningful and the row byte-comparable during a future
live-parity pass.

### 3. Write the `CompleteRouteHandler`

New file: `internal/providersync/<provider>_<dataset>_route.go`.

- `Collect(ctx, claim, credential, client, normalizedAt) (CompleteRouteBatch, error)`
  is the entire contract. Validate `claim.Provider`/`claim.Dataset` and fail
  closed (`ErrInvalidConfiguration`) on anything else — this is what lets
  `cmd/dev-health-worker/provider_sync.go`'s `BuildExecutor` switch select a
  handler by claim without every handler quietly serving every claim.
- Reuse `providerfoundation.CollectGitHubLinkPages` /
  `CollectGitLabPageParamPages` / `CollectLaunchDarklyOffsetPages` for
  pagination — don't hand-roll a paging loop.
- Reuse existing helpers in the package rather than duplicating them:
  `splitGitHubRepository`, `providerRelativePath` (GHE base-path safe),
  `repositoryIdentity` (repo UUID derivation), `fetchObject` (single-object
  GET with a byte cap and single-JSON-value enforcement),
  `filterWorkItemWindow`-style client-side since/before filtering,
  `effectBatchFromValues` (canonicalizes + digests the rows into an
  `EffectBatch`).
- `normalizedAt` is a parameter, not a clock read: never call `time.Now()`
  inside `Collect`. `CompleteRouteExecutor` stabilizes this value across
  retries by loading the persisted effect ledger's `CreatedAt` on **every**
  attempt (not only expired-lease recovery) — a handler that reads its own
  clock would regenerate different rows on an ordinary River retry and get
  rejected by `PrepareEffects` with `ErrEffectLedgerConflict`, wedging the
  unit. See `contracts/provider-matrix/v1/README.md`'s "Effect timestamp
  stabilization" section.
- Watermark: for `WatermarkNone` datasets return `nil`. For
  `WatermarkIncremental` datasets, both shipped examples
  (`launchdarkly/feature-flags`, `github/prs`) simply return
  `claim.BeforeAt` — the scheduler/producer owns the window, `Collect` only
  reports it processed through that point.

### 4. Write the ClickHouse effect sink

New file: `internal/providersync/<provider>_<dataset>_effects_clickhouse.go`.

Every destination table in this codebase so far is
`ReplacingMergeTree(last_synced)`, and every sink implements both
`EffectSink.WriteEffect` and `EffectReadback.InspectEffect`:

- `WriteEffect`: decode rows (`decodeEffectRows[xRow]`), validate each
  (`row.validate(claim)`), `PrepareBatch`/`Append`/`Send` — with a
  `sink.Lease.Assert(ctx)` check both before building the batch and
  immediately before `Send`, so a lease that expired mid-build never lands a
  write.
- `InspectEffect`: **must** resolve the *winning* ReplacingMergeTree version
  via `argMax(column, last_synced)` grouped by the table's real primary key,
  never scan every physical version — pre-merge history from earlier
  occurrences is normal and must not read as a conflict. This is the
  argMax/FINAL discipline `AGENTS.md`'s ClickHouse review checklist already
  requires of every reader; the sink is a reader too.
  - **Compare EVERY column the row carries, as its own named clause** (codex
    M6 on `github/prs`'s first draft): a monolithic `a && b && c && ...`
    conjunction that only happens to reference a subset of the row's fields
    will report `EffectExact` for a persisted row that actually diverges on
    a column the conjunction never mentions — and recovery then marks
    corrupted data committed. Write `comparePullRequestVersion`-style code
    as a sequence of `if actual.X != expected.X { return EffectConflict }`
    statements, one per column, not one boolean expression. This is defect
    class 4/6 from the checklist above, applied to the readback comparator
    specifically.
  - **Scan `Nullable` ClickHouse columns straight into Go pointer types**
    (`*string`, `*time.Time`), not through `ifNull(col, sentinel)`. Besides
    being simpler, `ifNull`-collapsing a nullable string to `''` makes a true
    SQL `NULL` and an actual empty string indistinguishable to the
    comparison — a real divergence the "exact" check can no longer see. If
    you DO need a sentinel (a non-nullable timestamp column with a
    Python-side "unset" convention), the sentinel must match EXACTLY on both
    sides of the comparison: `github/prs`'s first draft used ClickHouse's
    `toDateTime64(0, 3)` (Unix epoch) in SQL but Go's zero `time.Time{}` in
    the comparison default, which read every open PR (nil `merged_at`) as a
    conflict. Write a unit test for the nullable-defaults-agree case
    explicitly (see `TestPullRequestReadbackToleratesOpenPRNullFields`).
  - **UInt32-column gotcha:** `clickhouse-go`'s `Scan` rejects `*int` for a
    `UInt32` column (`converting UInt32 to *int is unsupported`). Scan into
    a local `uint32`, then convert. This only shows up against a **real**
    ClickHouse instance — the pure Go unit tests for the comparator compile
    and pass fine with the bug present. Budget for an integration-tagged
    test (`//go:build integration`, `internal/testsupport/containers`) for
    every new sink; a fixture-only port is not proof the SQL/scan shapes
    actually work.

### 5. Wire `execution_registry.go`

Three edits, always in this order:

1. `providerExecutorRegistry["<provider>/<dataset>"] = ExecutorNativeGo`.
2. Add a field to `CompleteRouteSwitches` (e.g. `GithubPRs bool`) — one field
   per pair, never shared. Document in its comment which *other* dataset
   aliases must NOT be opened by it (see step 8).
3. Add a `case provider == "x" && dataset == "y":` arm to
   `CompleteRouteSwitches.Descriptor`, setting `Destinations`,
   `RouteReady = true`, and `RouteEnabled = switches.<TheNewField>`.
   `NativeShadow` stays `false` unless the pair also gets a dedicated
   `ShadowSource` (see `nativeShadowReady` — today only
   `github/repo-metadata` qualifies, deliberately; read its doc comment
   before adding another).

### 6. Wire the worker binary

`cmd/dev-health-worker/provider_sync.go`:

- Add a `case session.Claim.Provider == "x" && session.Claim.Dataset == "y":`
  arm inside `BuildExecutor`'s switch, constructing the sink and setting
  `routeHandler`.
- Widen `buildProviderSyncWorker`'s "construct the family when ANY route
  switch is on" condition to include the new config flag.

`cmd/dev-health-worker/dependencies.go`:

- Add the new field to `workerRouteSwitches`'s literal.
- Add a `{provider, dataset, cfg.WorkerXEnabled}` entry to
  `providerRouteSwitchesReady`'s `routes` slice.

`internal/platform/config/config.go`:

- Add `WorkerXEnabled bool` + a `WORKER_X_ENABLED` entry in the `boolEnv`
  loop + a `slog.Bool` line in `SafeAttrs`. Off by default, same as every
  existing switch — this is what keeps `route_ready: true` from moving any
  live traffic by itself.

`src/dev_health_ops/workers/provider_unit_route.py` (Python producer side —
optional but recommended for the two-key gate to actually be usable later
without a second PR): add the mirroring `x_dataset: bool = False` field +
`_flag(source, "WORKER_X_ENABLED")` wiring in `from_environment`. Without
this, `_switch_field_name` still fails closed (no field ⇒ `getattr` default
`False`), so skipping it is *safe*, just leaves the producer half of the gate
unimplemented until someone adds it.

### 7. Regenerate the matrix contract

```
PROVIDER_MATRIX_UPDATE=1 go test ./internal/providersync \
  -run TestProviderMatrixMatchesCheckedInContract -count=1
```

Then update the **hardcoded** freeze-guard literals that intentionally do
NOT auto-follow the registry (grep the constant name, don't guess):

- `internal/providersync/capability_matrix_test.go`:
  `routeReadyPairs` (add the pair), the `all := CompleteRouteSwitches{...}`
  literal + its `NumField()` count (both `TestProviderMatrixKeepsEveryRouteClosedExceptReadyPairs`),
  and the `handlers` map in `TestProviderMatrixExecutorRegistryIsHonest`.
- `cmd/dev-health-worker/provider_sync_test.go`:
  `TestProviderSyncHandlerSwitchesFollowConfiguration`'s table,
  `TestWorkerRouteSwitchesMapsEveryConfiguredRoute`'s table.
- `cmd/dev-health-worker/dependencies_test.go`:
  `TestProviderRouteSwitchesAreIndependentAndRejectIncompleteRoutes`'s table.
- `internal/platform/config/config_test.go`: the all-off assertion, the
  all-on env map, the invalid-value env map.
- `tests/workers/test_provider_unit_route.py` (if you did step 6's Python
  half): mirror the `test_github_repo_metadata_*` block for the new switch,
  plus a "does not open its sibling alias dataset" test (see step 8).

The Python side (`tests/workers/test_provider_matrix_contract.py`) needs
**no code change** — it reads the regenerated `matrix.json` and Python's
`_PROVIDER_SUPPORTED_DATASETS`/`get_dataset_spec` directly, so it fails loud
on drift without anyone having to remember to touch it.

### 8. Tests: prove parity, not just execution

**For a NEW pair, reach for the generic oracle framework (defect class 16)
first**, not the hand-authored-oracle pattern the rest of this section
walks through. The numbered steps and worked examples below (the
`python_<pair>_normalization_oracle.py` / `python_oracle_loader.py`
pattern) remain accurate and are what the five pairs that predate
CHAOS-3162 use — read them to understand the loader technique they share
with the generic framework (stubbing heavy imports so a stock interpreter
can still execute the real target function) — but a new pair's own oracle
test should be a `testdata/oracle_pairs/<pair>_<boundary>.py` registration
plus a Go file calling `compareRowsAgainstPythonOracle`, not a new
hand-picked-field comparison script. See defect class 16 for why, and
`internal/providersync/testdata/oracle_pairs/github_prs_row.py` +
`github_prs_generic_oracle_test.go` for the worked example.

**The parity oracle MUST run the real Python producer live, not compare
against a hand-authored fixture — and check in the generator, so
regeneration is reviewable.** `github/prs`'s first draft did the opposite:
it hand-wrote the "expected" Python output in a code comment (an earlier
version of this document even recommended that pattern — see the diff
history), and codex's H9 finding was exactly what that pattern risks: the
hand-authored fixture OMITTED an entire phase of the real Python function
(the review-enrichment step) and then asserted the resulting zero-valued
fields as if they were verified parity. A fixture that never exercised a
code path cannot fail when that code path is wrong; it just encodes
whatever the Go implementation already does as the "expected" result. That
is not parity evidence, however confident the surrounding comment sounds.

The fix, and the pattern every future pair should follow from the start —
see `internal/providersync/testdata/python_launchdarkly_normalization_oracle.py`
(the pre-existing template) and
`python_github_prs_normalization_oracle.py` + the `python_oracle_loader.py`
extension it added (this pair's worked example):

1. Write `internal/providersync/testdata/python_<pair>_normalization_oracle.py`.
   It imports and calls the REAL, checked-in Python functions your Go code
   mirrors — never a hand-transcribed re-implementation of what you believe
   they do.
2. The Go quality lane (`ci/check_go.sh`) runs Python oracles under a
   **stock interpreter with no project dependencies installed** — the whole
   point is these tests run without the full Python venv. If the function
   you need lives in a module with heavy imports (SQLAlchemy models, httpx
   clients, ...), do NOT try to load that module directly. Instead extend
   the shared `python_oracle_loader.py`: add the source path, and a
   `_target_<name>()` function that installs minimal stub modules (plain
   `object`/lambda placeholders, `_install_module`) satisfying only the
   *names* the target module imports at load time — it never needs the
   stubs to do anything, because the oracle never calls into them. See
   `_target_base_git()` for the worked example (stubs `analytics.complexity`,
   `metrics.schemas`, `models.git`, `processors.fetch_utils`, `utils`, all
   dead weight for `build_git_pull_request` and `coerce_created_at`
   specifically). If the target function has zero project-internal imports
   (like `providers/pr_state.py::normalize_pr_state`), skip the loader
   entirely and use `importlib.util.spec_from_file_location` directly — see
   `python_registry_oracle.py` for that simpler pattern.
3. It is fine, and often correct, for the oracle to cover LESS than the
   Go code's full input space — `github/prs`'s oracle deliberately does not
   re-execute `_pull_from_item`'s raw-JSON parsing (that module imports
   httpx, which the stock interpreter doesn't have; that layer is
   mechanical field extraction already pinned by
   `tests/providers/test_github_code_client_prs.py` in the full Python
   suite). What it must NOT do is claim to have proven something it didn't
   run — say explicitly, in the oracle's own docstring, which functions it
   calls live and which layers are covered elsewhere instead.
4. In Go, `exec.Command(pythonExecutable(t), ".../oracle.py", ...)`, decode
   its JSON stdout, and assert your Go functions produce identical output
   for the identical input — see
   `TestGitHubPRSNormalizationMatchesLivePythonFunctions`. `pythonExecutable`
   (in `capabilities_test.go`) resolves `$PYTHON` or `python3` on PATH; it
   is not pointed at `.venv` on purpose, matching the stock-interpreter
   constraint above.
5. Prove the oracle test is actually sensitive before trusting it: break the
   Go function it's checking, confirm the SPECIFIC oracle-backed test fails
   (not some other test), then revert. If it doesn't fail, the oracle isn't
   covering what you think it covers.

A skipped/unimplemented live-parity harness test (see
`TestGitHubRepositoryLiveParityHarness` pattern, if you add one — this is
about a *live, credentialed* provider call, a different and larger claim
than the oracle above) is explicitly **not** parity evidence either; say so
in its skip message.

Minimum test surface per pair (see `github_prs_route_test.go`,
`github_prs_normalization_oracle_test.go`, `github_prs_readback_test.go`,
`github_prs_effects_integration_test.go` for the worked template):

1. One "emits the bounded effect" test carrying the fixture-parity doc
   comment (hand-verified field values are still fine for the HTTP-fixture
   layer — the live oracle above is specifically for the normalization
   logic, not a replacement for an HTTP-mocked fetch/effect-shape test),
   asserting every row field, the destination manifest via
   `batch.validate(descriptor)`, the watermark, and `FetchEvidence`.
2. The live-Python-oracle test from the numbered list above, for whichever
   pure normalization functions the pair's semantics actually live in
   (state mapping, timestamp fallback chains, identity derivation, ...).
3. Fail-closed tests: wrong provider, wrong dataset, malformed payload,
   missing/blank identity input (defect class 3), a capped paginated fetch
   (defect class 1).
4. Any provider-specific edge behavior ported (state normalization, window
   filtering, GHE base-path preservation, created_at fallback chains) gets
   its own table-driven test, not folded into test 1 — and where the
   underlying logic is a compound boolean (defect class 4), the table must
   isolate each clause: known-vs-unknown timestamp, since-only, before-only,
   each bound independently of the other, not just the paired "inside the
   window" / "outside the window" cases.
5. A pure-Go readback decision-table test for `compareXVersion` (no
   ClickHouse needed) covering: absent, zero-timestamp aggregate, stale
   version, newer version (conflict), and — one dedicated sub-test PER
   COLUMN the row carries (defect class 6), not one "different content"
   catch-all — a divergence in that single column reports `EffectConflict`.
6. An `integration`-tagged test against real Postgres+ClickHouse
   (`internal/testsupport/containers`) proving: `InspectEffect` on an empty
   table, tolerating pre-merge history, a genuine conflict case, and the
   full crash-recovery path (`WriteEffect` → simulated crash before
   `CommitEffect` → recovery regenerates the identical digest → readback
   marks committed without a duplicate physical row). This is the ONLY layer
   that catches driver-level type mismatches (see step 4's UInt32 gotcha) —
   it is not optional decoration.
7. If the matrix has sibling alias datasets sharing the same Python
   `legacy_target`/processor flag (see below), a test proving the new switch
   does NOT flip them ready too.

**Use the shared mutation harness — `scripts/mutation_harness.py`, runbook
`tests/tooling/README.md` — rather than a hand-rolled mutate/test/revert
loop.** Three ad-hoc per-lane harnesses produced false results on the same
day this pair was reviewed (a leaked mutation reported as restored, `git
checkout` reverting unrelated uncommitted edits, a waiter that matched its
own process and hung forever); the shared tool exists specifically to close
those failure modes; do not re-open them by rolling your own again. Two
rules from that runbook matter most for this recipe:

- **Mutate compound predicates clause by clause, never as a unit** — this is
  defect class 4 above, restated as a harness discipline: a three-clause
  condition mutated wholesale can report `KILLED` while one clause inside it
  is both unasserted and wrong, because the OTHER clauses in whatever
  fixture the proof test uses already made the outcome differ. Write one
  mutation per named clause.
- **A `SURVIVED` result is not automatically a coverage gap.** Running
  `github/prs`'s own plan
  (`internal/providersync/testdata/mutation-plans/github_prs.json`) found
  exactly one: a mutated `if fullName == "" { ... }` guard survived because
  `repositoryIdentity` (the function called two lines later) already
  rejects an empty string with the identical error — the guard was dead
  code duplicating a check its own callee makes. The fix was deleting the
  redundant guard, not adding a test to justify keeping it; see that plan's
  `$limitation` field for the full account. Classify every survivor
  (missing test / invalid mutation / genuine redundancy) rather than
  reflexively adding assertions until everything shows `KILLED`.

Write the plan to `internal/providersync/testdata/mutation-plans/<pair>.json`
(checked in, reviewable) and run it with:

```
python3 scripts/mutation_harness.py run \
  --plan internal/providersync/testdata/mutation-plans/<pair>.json \
  --assert-all-killed
```

Report which mutation was caught by which test — that mapping is what makes
"I mutation-tested this" a checkable claim instead of an assertion.

## Difficulty tiers for the remaining GitHub pairs

Base confidence: `prs` and `repo-metadata` have proven, mutation-tested
handlers (this document's own worked recipe, plus the CHAOS-3123 precedent)
— `prs` is not yet `route_ready` (see defect class 9). Every tier below for
the other 15 pairs is from a **targeted read** of
`src/dev_health_ops/processors/github.py`'s fetch/write call sites (function
names, insert targets) — not a line-by-line trace like `prs` got. Re-verify
the exact semantics per step 1 before porting; treat the tier as a sizing
estimate, not parity evidence.

**Shared fetch/transform code already in Go.** `internal/providersync/native_rest.go`'s
`NativeRESTHandler` already implements `Fetch`-only (not `CompleteRouteHandler`)
logic for `repo-metadata`, `work-items`, `work-item-labels`,
`work-item-projects`, `work-item-history`, and `work-item-comments` — for
**both** GitHub and GitLab, sharing helpers like `filterWorkItemWindow`,
`issueStatusAndType`, `collectGitHubIssues`/`collectGitHubWorkItems`. Porting
any of these five to a real `CompleteRouteHandler` starts from adapting that
existing fetch/normalize code to build `EffectBatch` rows instead of
`NormalizedEnvelope`s, not from zero. `commits`/`commit-stats`/`files`/`blame`
share `sync_git` gating and a common `owner/repo` root but each hits a
different GitHub surface (REST list, per-commit REST, and — for
`files`/`blame` — no confirmed active Python write call was found at all;
see below).

| Pair | Tier | Why |
|---|---|---|
| `cicd` | **Low** | `_fetch_github_workflow_runs_async` → single REST list (`client.get_workflow_runs`), one destination table (`ci_pipeline_runs`), no per-item detail call, no GraphQL. Same shape as `repo-metadata`, simpler than `prs` (no N+1). |
| `deployments` | **Low** | `_fetch_github_deployments_async` → two REST list calls (deployments + releases for `release_ref`), one destination table (`deployments`). Slightly more surface than `cicd` for the release-ref join but still no per-item detail call. |
| `commits` | **Low-Medium** | `_sync_github_commits` → REST list, one destination table (`git_commits`). Straightforward, but confirm the commit-window/pagination semantics (`since`) exactly — GitHub's commits endpoint DOES support server-side `since`, unlike `/pulls`. |
| `security` | **Medium** | `_fetch_github_security_alerts_async`, one destination (`insert_security_alerts`, gated behind a `getattr` — confirm the sink actually implements it in production, not just in the fixture path). Likely several distinct GitHub alert types (Dependabot/CodeQL/secret-scanning) behind one dataset — read this one carefully before estimating further. |
| `commit-stats` | **Medium** | `_fetch_github_commit_stats_async` → REST list **plus per-commit stat fetch** (N+1, same shape as `prs`'s list+detail). One destination (`git_commit_stats`). Reuse the `prs` N+1 pagination pattern directly. |
| `work-items` | **High** | Fetch/normalize logic partially exists (`NativeRESTHandler.fetchGitHub` case `"work-items"`), BUT the Go `Descriptor` already collapses this and its four sibling aliases (see below) onto ONE canonical route with **fifteen** destination tables (`workItemRouteDestinations()` in `execution_registry.go`: `work_items`, `work_item_transitions`, `work_item_dependencies`, `work_item_reopen_events`, `work_item_interactions`, `sprints`, plus six `*_daily` rollup tables and `ai_attribution`-adjacent surfaces). Several of those are metrics computed by separate downstream jobs in Python, not by the fetch/normalize step itself — scope this pair by first determining which of the 15 declared destinations `_ingest_with_client` (provider.py) actually produces directly vs. which are computed elsewhere, and consider whether the initial port should legitimately cover a subset with the rest tracked as explicit follow-on work. This is not a single-PR-sized port. |
| `work-item-comments` | **Medium** (shares work-items complexity, narrower scope) | `fetchGitHubChildren` already fetches `/issues/{number}/comments`; write path only needs the `work_item_interactions`-style destination, not the full work-items fan-out — BUT the matrix's alias-collapse rule means this dataset can't become independently `route_ready` without either (a) the full `work-items` route landing first, or (b) a deliberate decision to decouple it, which itself needs a design call before coding. |
| `work-item-history` | **Medium**, same caveat as `work-item-comments` (alias of `work-items`). |
| `work-item-labels` | **Low**, same caveat — but the underlying fetch (`/labels`) and destination shape is close to trivial once the alias-collapse question is resolved. |
| `work-item-projects` | **Low**, same caveat (`/milestones`). |
| `pr-reviews` | **High, and load-bearing for `prs`** | Needs a **new capability**: GitHub GraphQL PR-reviews batch fetch (`GitHubWorkClient.iter_pr_reviews_batch`, Python's `_enrich_prs_with_reviews_batch`). `internal/providerfoundation` has a generic GraphQL POST helper (`CollectLinearGraphQLPages`) but no GitHub-specific query/schema handling yet — this is genuinely new Go surface, not adaptation. Shares `github/prs`'s repo-id derivation and PR-list fetch; the natural design is for `pr-reviews` to depend on/reuse `github/prs`'s handler rather than re-list PRs independently. Writes `git_pull_request_reviews` AND the `first_review_at`/`reviews_count`/`changes_requested_count` columns on `git_pull_requests` that `github/prs` deliberately leaves at zero (defect class 9) — **landing this pair is what flips BOTH `github/prs` and `github/pr-reviews` to `route_ready: true`** in `execution_registry.go`'s two Descriptor cases together, not `pr-reviews` alone. Resolve the write-conflict/column-ownership question from defect class 9 as part of scoping this pair, before writing the handler. |
| `pr-comments` | **Low, once `pr-reviews` exists** | Shares the `prs` legacy target/processor flag; Python's actual PR-comments handling folds into `comments_count` on the `git_pull_requests` row itself (there is no dedicated PR-comments raw table in the ClickHouse schema, unlike `work_item_comments` for issues) — worth confirming there is even a distinct `route_destinations` manifest to give this pair before treating it as separate work rather than a `prs` field. |
| `files` | **Unclear / needs investigation first** | No `insert_git_files` call site was found anywhere under `src/dev_health_ops/processors/github.py` in this pass — either the write path lives in a different module (a local-clone git-walk processor, not the REST GitHub client) or this capability has no active production caller today. Confirm which before estimating; per repo convention, "no caller found" is a finding to verify, not a conclusion. |
| `blame` | **Ported (CHAOS-3335 + CHAOS-3343).** The active producer is `_backfill_github_missing_data`, not the obsolete `_fetch_github_blame_sync` helper. The native handler resolves the bounded commit/tree, fetches GraphQL blame ranges, writes `git_blame`, and uses tenant-scoped persisted path coverage to select the next 500 exactly like Python's `select_unblamed_paths`. The route is independently switch-gated and default-off. |
| `tests` | **High** | TestOps report ingestion is a distinct, heavy pipeline (JUnit/report-format parsing, `_fetch_github_test_artifacts_sync` + `_sync_github_test_reports`), already flagged elsewhere as fixture-only in production (see the TestOps ingestion gap tracked separately). Do not estimate this as PRs-sized; it likely needs its own scoping pass before a Go port is even well-defined. |

## What to update when a pair lands

Follow `github/prs`'s own additions as the template:

- Add an "Activation status for `(provider, dataset)`" section to
  `contracts/provider-matrix/v1/README.md`, mirroring the existing
  `(github, repo-metadata)` / `(github, prs)` sections — same structure,
  same honesty about what's waived (canary/live-traffic parity) vs. what's
  actually proven (fixture-level field parity via the live oracle), vs.
  what's deliberately not yet true (`route_ready` staying `false` pending a
  sibling pair, if that's this pair's situation).
- If the port uncovers a new Go/Python divergence class (like the nine
  defect classes above), add it to THIS document's checklist, not just to a
  commit message — the next agent needs it before writing their sink, not
  after debugging the same failure. This checklist is the actual product of
  this recipe; keep it current more than any other section.
- Get an adversarial review (codex or equivalent) on every pair before
  calling it done, even with a green gate — see the second ground rule at
  the top of this document. `github/prs`'s own gate was green and its first
  draft still shipped four HIGH-severity silent-data-loss defects.

## Commit boundary

Each provider/dataset pair is one sync unit and is independently executable
and testable through its claim — no scheduler, no job-route activation, no
occurrence materialization required. That makes the pair the right commit
boundary: land one pair (code, tests, mutation plan, matrix/registry
wiring, this document's updates) as its own commit before starting the
next, rather than batching multiple pairs into one diff. A 17-pair diff is
unreviewable, and one defective pair in a large batch blocks every good one
alongside it.
