Adversarial review of PR #5710 at tip fb9195d8f14b8123a463091580cce30b524832eb against origin/main 306ab602210e56b6268ed7f2d1f7ec2e13c4d046.

What this change is trying to accomplish, and for whom:
The investment materialize job (internal/jobs/investment) used to allocate a work unit that had no repository evidence to the repositories owned by the teams of its member issues (a final "team ownership fallback", allocation_source team_ownership, repo_source team:<ids>). chris ruled that fallback wrong and asked to drop it: a team's repositories are never a unit's repository. After this change such a unit gets no repository share and is shown as a separate unattributed share, using the rows the writer already produced (an 'empty' row, or an 'active_hours_unassigned' row with the unit's effort, repo_id NULL). Operators and the analytics readers (investment breakdowns by repository and team, the sankey coverage) must keep working: rows stay append-only, readers take each unit's newest allocation generation, persisted per-unit identity, categorization and organization totals must not change, and units that are not recomputed keep their older generation until they are. Docs for the changed behaviour are part of the change.

Diff: git diff 306ab602210e56b6268ed7f2d1f7ec2e13c4d046...fb9195d8f14b8123a463091580cce30b524832eb -- 25 files, +160/-3467.

The PR body (## TEST-EVIDENCE, ## RISK-NOTES) is at .codex-review-context.md in this worktree. Every claim in it is unverified until you execute it.

do not read outside the repository root

Find the ways this change fails to accomplish the above, or breaks something that worked, and prove each one by running it: a test case, a command, a bring-up, a request -- executed, with its output. Use the change the way its user would, from scratch, and go wherever the change can reach; nothing reachable is out of scope because it is not named here. A claim without executed evidence, a test that cannot fail, and a guard that checks text instead of behaviour are findings too. Report each finding with path:line, severity P1-P3, and the executed reproduction. A P1 or P2 carries its own `$ ` command line and its real output, run in this round; one you could not execute is P3 OPINION (not executed), or dropped.

Question, answered verbatim before the verdict: what does this change make observable, and what regression in it would be invisible at Info?

Final line, alone, exactly one of: CLEAN | NOT CLEAN | BLOCK | REQUEST CHANGES

---

STANDING RULES FOR EVERY ROUND (appended by the wrapper; not optional):

Never run docker or compose commands, and never connect to a running service.
The shared stack (containers named `dev-health-*`, the shared compose project,
any ClickHouse/Postgres/Redis reachable on this host) belongs to other people
and to work in flight; touching it can destroy their state. Use ONLY this
review worktree and the sandbox you are given. If a check appears to need a
live service, say so in the verdict and do not attempt it.

Container-backed proofs (testcontainers, integration suites, anything needing a
container) are NOT blocked -- they run on bigboy via the oci-image recipe. Name
the proof you would need and hand it off; that is a complete, acceptable
verdict. What is never acceptable is writing down what such a command WOULD
have printed. An unrun check reported as a quoted result is the one failure
this wrapper exists to prevent, and "I could not run it here, it needs bigboy"
costs you nothing.

Architecture-sensitive checks (NaN sign bits, FMA/fused-multiply-add results,
float formatting, anything whose answer can differ per CPU) are verified in CI,
never here. Every host in this fleet is arm64. Running such a check locally
does not give you a weaker result -- it gives you a CONFIDENT WRONG one: it
passes on arm64 while the x86 case it was meant to catch is still broken
(CHAOS-4818 / #2142's NaN sign-bit reds appeared ONLY in CI). A green from the
wrong architecture is worse than no green, because it is indistinguishable
from a real one in your verdict. Say the check is CI-only and move on.


REPRODUCTION GATE (chris 2026-10-30): a finding of severity P1 or P2 MUST carry,
in its own section, at least one executed command line starting with `$ `
followed by its REAL output, run in THIS round. Argued from code but no proof is
not a finding, it is an opinion. If you cannot execute the repro, report the
finding as P3 labelled `OPINION (not executed)`, or drop it. P3 may stay argued.
The wrapper post-checks the verdict: any P1/P2 section without a `$ ` line and
output below it is relabelled `OPINION (no executed repro)` and excluded from
the EFFECTIVE verdict.

do not read outside the repository root

EXEC-MANDATORY REVIEW POLICY: this round IS opted into workspace-write; the
read-only policy does not apply. Execution is REQUIRED, not optional,
before you report any finding:

1. `go build ./... && go vet ./... && go vet -tags=integration ./...` for
   the packages your prompt names (or the whole repo, only if your prompt
   says a deletion/whole-tree-safe change makes that correct).
2. Run the changed packages' own tests WITH `-coverprofile`, then
   `go tool cover -func` on the result -- read the coverage, do not just
   run the tests and stop.
3. Callers evidence for every changed function via `rg` (paste the command
   and the hits). Inspect every OTHER site of the same shape across the
   WHOLE PACKAGE (or repo, via `rg`) -- a defect shape found once is swept
   to every other instance yourself, in this same round, not left for a
   later pass.
Never run a whole-tree test pattern (`go test ./...`, `./internal/...`)
regardless of what your prompt asks for -- name packages explicitly; if a
wider run seems genuinely necessary, say so and stop rather than running
it.

Severity is evidence-defined: P1 = you EXECUTED a repro on the live code
path and OBSERVED the defect, command and output both pasted -- no repro
attempt means it cannot be P1. P2 = a defect you EXECUTED a repro for and OBSERVED, same standard as P1 (see
REPRODUCTION GATE below). P3 = nit/style/test-strength, no repro expected.
Verdict is NOT CLEAN if at least one P1 or P2 stands (with its executed repro),
CLEAN otherwise.

If `go test`/`go run`/`go build` fails with `creating work dir: ... mkdir
...: operation not permitted`, RETRY IT EXACTLY ONCE before concluding go is
unavailable -- the first invocation in a round can hit this even with
GOTMPDIR/GOCACHE correctly pointed at a writable path, and an immediate
retry with no other change often succeeds. One retry only -- if it fails a
second time, that is a real "go test unavailable", not a hiccup, and you
say so, labelling every remaining claim EXECUTED or ARGUED so a reader can
tell a run result from a source-trace inference at a glance.

This round's own module cache -- already warmed and offline-resolve-proven
(via `GOPROXY=off go test -count=1 -run '^$' -exec <true> ./...`) before you started --
is at /private/tmp/codex-review-modcache-drop-team-ownership-repo-fallback-76d01b45-20261104T095817. If `go test` still fails on a module lookup once you are inside the sandbox, retry ONCE with GOMODCACHE=/private/tmp/codex-review-modcache-drop-team-ownership-repo-fallback-76d01b45-20261104T095817 GOPROXY=off (read-only intent -- never write there; this is the SAME cache named above, not a different one -- a first-attempt sandbox hiccup, not a missing module, is the more likely cause here) before falling back to a source-trace verdict, and say explicitly whether the retry succeeded.

THE DIFF UNDER REVIEW is written to `.codex-review.patch` in your working directory:
`git diff --stat origin/main...fb9195d8f14b8123a463091580cce30b524832eb` first, then the full unified diff. Read it (for
example `sed -n '1,200p' .codex-review.patch`): `git diff` may be denied by the
sandbox in this worktree. A round whose log shows neither a successful `git diff` nor
a read of this file is stamped VOID IN FORM, because it cannot certify a diff it did
not see.
