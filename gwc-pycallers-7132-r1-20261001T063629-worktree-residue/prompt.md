Adversarial review of PR #3468 at tip 4971aa7762af0020e038d8c4f017d9b8d40ebb4a against origin/main 4486a95ebb98cdfe48825f30173ddcf07ac2e7d5.

What this change is trying to accomplish, and for whom:
A jira sync on a customer-facing environment failed every unit because an optional Atlassian Teams step sent the organization id in the wrong form to the Atlassian gateway, and nothing in the logs, the run result or the CLI said why. This change sends the organization id in the form the gateway accepts, makes that optional step unable to fail the sync or its units (while still recording the failure as a named, value-free degraded entry that the run result carries, and failing the strict command-line verb), makes each cause of a credential-invalid refusal name its own fixed reason, logs every error the non-strict seam used to drop, and makes the command-line verb resolve the credential the integration points at. It serves the operators of the platform: working means a failure is never silent and never a clean success, no credential value reaches a log, a durable row or an error text, and no caller still passes the old form.

Diff: `git diff 3803acff1700454e55d70fa5996c6e5ac62ae5fa...4971aa7762af0020e038d8c4f017d9b8d40ebb4a` — 24 files, +1018/-29.

The PR body (## TEST-EVIDENCE, ## RISK-NOTES) is at .codex-review-context.md in this worktree. Every
claim in it is unverified until you execute it.

Find the ways this change fails to accomplish the above, or breaks something that worked, and prove each
one by running it: a test case, a command, a bring-up, a request -- executed, with its output. Use the
change the way its user would, from scratch, and go wherever the change can reach; nothing reachable is
out of scope because it is not named here. A claim without executed evidence, a test that cannot fail,
and a guard that checks text instead of behaviour are findings too. Report each finding with `path:line`,
severity P1–P3, and the executed reproduction.

Question, answered verbatim before the verdict: what does this change make observable, and what
regression in it would be invisible at Info?

Final line, alone, exactly one of: CLEAN | NOT CLEAN | BLOCK | REQUEST CHANGES

---

STANDING RULES FOR EVERY ROUND (appended by the wrapper; not optional):

Docker IS available to you in this round, via /var/run/docker.sock -- run the
`-tags=integration` Go tests and any container-backed pytest yourself. Do not
report a container-backed check as ARGUED/unrun just because it needs a
container; if a testcontainers run fails to start a container, that is a
finding about the harness -- report it verbatim, never invent what it would
have printed.

The shared compose project (`dev-health`) and any container NOT labelled
`codex-review-round=gwc-pycallers-7132-r1-20261001T063629` are OFF LIMITS: never `docker exec`/`stop`/
`rm`/`compose` against them, never connect to their ports -- they belong to
other people's work in flight and touching them can destroy their state.
Every container YOU start carries `--label codex-review-round=gwc-pycallers-7132-r1-20261001T063629`
(this exact value, also in $CODEX_REVIEW_ROUND_LABEL). Never run `docker
system prune`, `builder prune`, or `image prune` -- these are host-wide and
would destroy other lanes' state, not just yours.

Network egress is open in this round; do not fetch anything not required by
the tests.

Pass any database DSN or credential (including a throwaway per-round test
password) via an environment variable, never inline in a command string --
the same rule as never putting a secret in argv applies here too.

Architecture-sensitive checks (NaN sign bits, FMA/fused-multiply-add results,
float formatting, anything whose answer can differ per CPU) are verified in CI,
never here. Every host in this fleet is arm64. Running such a check locally
does not give you a weaker result -- it gives you a CONFIDENT WRONG one: it
passes on arm64 while the x86 case it was meant to catch is still broken
(CHAOS-4818 / #2142's NaN sign-bit reds appeared ONLY in CI). A green from the
wrong architecture is worse than no green, because it is indistinguishable
from a real one in your verdict. Say the check is CI-only and move on.

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
attempt means it cannot be P1. P2 = a plausible defect you tried to
reproduce and could not -- paste what you ran and why it didn't reproduce.
P3 = nit/style/test-strength, no repro expected. Verdict is CLEAN unless at
least one P1 is found; a P2/P3 alone never blocks.

If `go test`/`go run`/`go build` fails with `creating work dir: ... mkdir
...: operation not permitted`, RETRY IT EXACTLY ONCE before concluding go is
unavailable -- the first invocation in a round can hit this even with
GOTMPDIR/GOCACHE correctly pointed at a writable path, and an immediate
retry with no other change often succeeds. One retry only -- if it fails a
second time, that is a real "go test unavailable", not a hiccup, and you
say so, labelling every remaining claim EXECUTED or ARGUED so a reader can
tell a run result from a source-trace inference at a glance.

This round's own module cache -- already warmed and offline-resolve-proven
(via `GOPROXY=off go test -count=1 -run '^$' ./...`) before you started --
is at /var/lib/oci-cache/go-mod. If `go test` still fails on a module lookup once you are inside the sandbox, that means the persistent shared cache above is genuinely missing something -- say so explicitly (name the missing module) and fall back to a source-trace verdict. Do NOT retry against $HOME/go/pkg/mod: that path is legacy and no lane writes to it, so it is not a meaningful fallback.

THE DIFF UNDER REVIEW is written to `.codex-review.patch` in your working directory:
`git diff --stat origin/main...4971aa7762af0020e038d8c4f017d9b8d40ebb4a` first, then the full unified diff. Read it (for
example `sed -n '1,200p' .codex-review.patch`): `git diff` may be denied by the
sandbox in this worktree. A round whose log shows neither a successful `git diff` nor
a read of this file is stamped VOID IN FORM, because it cannot certify a diff it did
not see.
