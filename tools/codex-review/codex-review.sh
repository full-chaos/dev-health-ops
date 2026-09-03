#!/usr/bin/env bash
# codex-review.sh — run one adversarial codex review round the safe way.
#
# Encodes the hard-won rules of .claude/skills/codex-review/SKILL.md:
#   * the round runs in a THROWAWAY DETACHED REVIEW WORKTREE, never in the
#     lane's live worktree (codex's restore-clean cleanup reset a lane's HEAD
#     and dropped files when we pinned it into the live tree with -C)
#   * per-round timestamped verdict/log names, never a reused fixed name
#   * prompt from stdin redirect (argv prompt + stdin open = hang)
#   * push-before-round: refuses to review a tip that is not on the remote
#   * belt-and-braces: verifies the lane HEAD did not move during the round
#
# v4.3 makes Go tests EXECUTABLE inside a round.
#
# v4.8 makes Go tests EXECUTABLE ON LINUX HOSTS (bigboy). Every fix through
# v4.4 was proven only on macOS. Probed empirically on bigboy 2026-09-03
# (lane-4962-grouped-413-r1 hit this first): under `-s read-only` on Linux
# (landlock), NOTHING is writable -- not /tmp, not the review worktree passed
# via -C, not even a path granted with `--add-dir`. Every write attempt there
# returns "Read-only file system", not the "operation not permitted" macOS
# gives, so the read-only sandbox on Linux is not narrower than macOS's, it is
# absolute: zero writable paths exist under it, period. `workspace-write` was
# then probed the same way and makes both the worktree AND plain /tmp paths
# (no extra grants needed) writable on Linux, exactly as it already does on
# macOS. So the sandbox mode picked by default is now host-dependent: Linux
# defaults to workspace-write (the only mode under which Go can run there at
# all); macOS keeps read-only (still proven sufficient, see v4.3/v4.4 above).
# CODEX_REVIEW_SANDBOX, when set explicitly, still overrides this on either
# host. The GOCACHE/GOTMPDIR/TMPDIR paths themselves are UNCHANGED -- they
# already sit under /tmp, which workspace-write also permits without needing
# to move them inside the worktree.
#
# CORRECTED 2026-09-03 (CHAOS-4925). The claim below used to read "under
# read-only NOTHING is writable -- not $TMPDIR, not /tmp, ...". That is FALSE
# for /tmp, and the error cost capability rather than safety: reviewers were
# avoiding something they could do.
#
#   /tmp                     WRITABLE   (measured -- go test runs to completion)
#   $TMPDIR (/var/folders/**/T)  DENIED  (measured)
#
# Evidence is a live round, not a probe -- lane-4441-4914-r2 ran
#   GOCACHE=/tmp/... GOTMPDIR=/tmp/... go test ./internal/pythonparity
#   succeeded in 5668ms:  ok ... 0.303s
#
# HOW THE ORIGINAL CLAIM WAS WRONG, because the shape recurs: four 2x2 cells
# all pointed GOCACHE/GOTMPDIR at $TMPDIR, all four reported "operation not
# permitted", and I recorded a conclusion about writability IN GENERAL. Four
# cells agreeing was not four pieces of evidence -- it was four runs of ONE
# probe against ONE path. The probe never tried /tmp, so it could not have
# produced the positive.
#
# The 2x2 that settles the design (trivial package, one test):
#
#   read-only       + GOTMPDIR in $TMPDIR   go: creating work dir: ... not permitted
#   read-only       + GOTMPDIR in /tmp      RUNS -- measured, see above
#   workspace-write + default GOTMPDIR      FAIL [setup failed]
#   workspace-write + GOTMPDIR in ws        ok  trivial  0.244s
#
# The row that used to say read-only cannot run Go was measuring the PATH, not
# the sandbox mode. Point GOTMPDIR/GOCACHE at /tmp and read-only runs Go.
#
# The per-lane warm GOCACHE is preserved rather than moved into the per-round
# worktree (which would make it cold every round, the exact failure the GOCACHE
# comment below warns about). It stays at its stable path and is granted write
# access explicitly via `sandbox_workspace_write.writable_roots`. Verified: the
# cache is genuinely populated through the sandbox and survives outside it.
#
# The sandbox mode is OPT-IN and still defaults to read-only, because widening
# it is a policy change (the reviewer gains write access to the throwaway review
# worktree and to the lane's Go cache) and that is not this script's call.
#
# v4.2 closes SILENT-FAILURE holes in preserve_residue, the mechanism that makes
# a round recoverable when its reviewer wrote findings to the wrong filename (CF
# lost a 382k-token round that way). Each hole made a failure of that mechanism
# indistinguishable from "there was nothing to save":
#
#   1. `cp ... 2>/dev/null || true` discarded every copy's result while still
#      printing one unconditional "preserved residue" line.
#   2. the listing ran as `< <(git status ... 2>/dev/null)`; a process
#      substitution's exit status is unobservable, so a git failure meant the
#      loop never ran and the function returned in silence.
#   3. `mkdir -p "$dest" || return 0` returned silently AND leaked both temp
#      files. Worst of the three: on that path the destination does not exist,
#      so NOTHING is preserved -- and it is reached by an unwritable OUTDIR, a
#      full disk, or a stale file at that path. (Found by lane-4441.)
#
# All three are the shape of the `pgrep -fc ... 2>/dev/null || echo 0` gate that
# reported a hardcoded zero all session on this host (macOS pgrep has no -c): a
# suppressed error plus a default value is not a measurement.
#
# v4.2 also fixes `-z` rename parsing. A rename emits TWO NUL records (the
# destination, then a bare origin); the old tab-strip was the non-`-z` spelling,
# so it was dead code and the origin record was parsed as a status line.
#
# v4.8.1 (2026-09-03) fixes two false positives found on bigboy rounds 2174/2179:
#   1. HARNESS WARNING fired on lines where a non-go command (`go doc`) hit a
#      DNS/network refusal, not a write-path refusal; the check now requires
#      the matching exec block to be BOTH a go test/run/build AND itself
#      reported failed, not just any occurrence of the string in the log.
#   2. the lost-findings guard fired on a COMPLETE verdict that cited files
#      with the review worktree's absolute path in markdown links; verdict
#      text is now normalized (worktree prefix stripped, incl. /private/tmp
#      and $TMPDIR variants) before the guard runs, and the guard escalates
#      to SUSPECT only when the verdict is short (<20 lines, the existing
#      NOTE threshold) AND the residue dir holds files beyond the prompt/
#      context inputs the wrapper itself copies in.
#
# Usage: scripts/codex-review.sh [options]   (run from the lane worktree,
#        or pass -w; background the SCRIPT, not pieces of it)
#   -w DIR    lane worktree (default: $PWD)
#   -n NAME   round name prefix (default: worktree basename) -> NAME-<ts>.md/.log
#   -m MODEL  model (default: $CODEX_REVIEW_MODEL, else gpt-5.6-terra)
#   -e EFF    reasoning effort (default: $CODEX_REVIEW_EFFORT, else xhigh)
#   -p FILE   prompt file (default: prompt.md in the lane worktree)
#   -t SHA    tip to review (default: HEAD of the lane worktree)
#   -o DIR    output dir for verdict/log (default: the lane worktree)
#   -k        keep the review worktree afterwards (debugging)
#   -U        allow an unpushed tip (NOT recommended; disables the safety net)
#
# Exit: 0 = codex finished AND verdict file exists AND lane HEAD unmoved.
#       Non-zero otherwise, with the reason on stderr. The verdict file path
#       is printed as the last stdout line: VERDICT=<path>

set -euo pipefail

warn() { printf 'codex-review: %s\n' "$*" >&2; }
die()  { warn "$*"; exit 1; }

WT="$PWD" NAME="" MODEL="${CODEX_REVIEW_MODEL:-gpt-5.6-terra}" EFF="${CODEX_REVIEW_EFFORT:-xhigh}"
PROMPT="" TIP="" OUTDIR="" KEEP=0 ALLOW_UNPUSHED=0
while getopts 'w:n:m:e:p:t:o:kU' f; do
  case "$f" in
    w) WT=$OPTARG ;; n) NAME=$OPTARG ;; m) MODEL=$OPTARG ;; e) EFF=$OPTARG ;;
    p) PROMPT=$OPTARG ;; t) TIP=$OPTARG ;; o) OUTDIR=$OPTARG ;;
    k) KEEP=1 ;; U) ALLOW_UNPUSHED=1 ;;
    *) die "unknown flag" ;;
  esac
done

WT=$(cd "$WT" && pwd) || die "worktree $WT not found"
git -C "$WT" rev-parse --git-dir >/dev/null 2>&1 || die "$WT is not a git worktree"
NAME=${NAME:-$(basename "$WT")}
PROMPT=${PROMPT:-$WT/prompt.md}
OUTDIR=${OUTDIR:-$WT}
[ -s "$PROMPT" ] || die "prompt file $PROMPT missing or empty"

TIP=${TIP:-$(git -C "$WT" rev-parse HEAD)}
TIP=$(git -C "$WT" rev-parse "$TIP") || die "cannot resolve tip $TIP"
HEAD_BEFORE=$(git -C "$WT" rev-parse HEAD)

# Push-before-round: the tip must be reachable from some remote ref.
if [ "$ALLOW_UNPUSHED" -ne 1 ]; then
  git -C "$WT" branch -r --contains "$TIP" | grep -q . \
    || die "tip $TIP is not on any remote ref. Push first (this is the recovery source), or pass -U."
fi

TS=$(date +%Y%m%dT%H%M%S)
V="$OUTDIR/$NAME-$TS.md"
L="$OUTDIR/$NAME-$TS.log"
[ -e "$V" ] && die "verdict file $V already exists — refusing to reuse a name"

# Bound the Go build cache the REVIEWER's own builds use.
#
# Off the shared user cache on purpose: a review sandbox must not be able to
# thrash or clean the cache every lane shares (lane-4818 ran `go clean -cache`
# mid-flight on 09-02 and invalidated other lanes' in-progress work).
#
# KEYED ON THE LANE WORKTREE, deliberately, and NOT on $NAME. $NAME is the ROUND
# name -- this script's own rule is that it is unique per round -- so keying on
# it would give a COLD cache every round and leave a multi-GB directory under
# $TMPDIR per round. The first version of this did exactly that while carrying a
# comment claiming it was stable per lane; CF caught it on review.
#
# A lane's rounds therefore share one cache and stay warm. Lanes remove their own
# cache dir at close-out (see SKILL.md).
# /tmp, NOT $TMPDIR. lane-4441 measured this: under the read-only sandbox,
# $TMPDIR on macOS is /var/folders/**/T, which is NOT writable, while /tmp IS.
# v4.3 pointed the reviewer's Go cache at the denied path, so `go test` failed
# with "cannot create entries" / "operation not permitted" and the wrapper
# printed its go-bounds line anyway, as though it had supplied a working
# environment. Rounds that did run Go succeeded by relocating to /tmp
# THEMSELVES -- they worked around the harness, not with it. A reviewer that
# did not think to relocate would have reported "cannot execute here", which is
# the pre-v4.3 state the execution work existed to remove.
RGOCACHE="${CODEX_REVIEW_GOCACHE:-/tmp/codex-review-gocache-$(basename "$WT")}"
mkdir -p "$RGOCACHE" || die "cannot create review GOCACHE $RGOCACHE"

# Resolve the bounds ONCE, into variables, so the warn line below reports
# exactly what is applied. The first version re-evaluated the defaults inside
# the warn string, which could drift from the values actually exported.
RGOFLAGS="${GOFLAGS:+$GOFLAGS }${CODEX_REVIEW_GOFLAGS:--p=2}"
RGOMAXPROCS="${CODEX_REVIEW_GOMAXPROCS:-4}"

# Sandbox mode. read-only is the historical default and stays the default on
# macOS, where /tmp is proven writable under it (see v4.3/v4.4 above).
#
# On Linux it cannot be the default: probed on bigboy (v4.8 note above),
# read-only there grants ZERO writable paths, so every Go command dies before
# it can create its work dir or build cache -- there is no path this wrapper
# could point GOTMPDIR/GOCACHE at that would help, because none exists.
# workspace-write is therefore the Linux default; it is not a widening choice,
# it is the only mode in which Go executes there at all. CODEX_REVIEW_SANDBOX,
# when the caller sets it explicitly, always wins over this host default.
case "$(uname -s)" in
  Linux) RSANDBOX="${CODEX_REVIEW_SANDBOX:-workspace-write}" ;;
  *)     RSANDBOX="${CODEX_REVIEW_SANDBOX:-read-only}" ;;
esac
case "$RSANDBOX" in
  read-only | workspace-write) ;;
  *) die "CODEX_REVIEW_SANDBOX must be read-only or workspace-write, got '$RSANDBOX'" ;;
esac

START_EPOCH=$(date +%s)   # bounds the session-transcript recovery search
RW=$(mktemp -d "${TMPDIR:-/tmp}/codex-rw-$NAME-XXXXXX")
# Go's work dir. Deliberately a SIBLING of the review worktree, not a directory
# inside it: anything inside $RW shows up as untracked and would be swept into
# the preserved residue, burying the reviewer's actual findings under build
# droppings. Removed by cleanup() alongside the worktree.
# /tmp for the same reason as RGOCACHE above.
RGOTMPDIR=$(mktemp -d "/tmp/codex-gotmp-$NAME-XXXXXX")

# TMPDIR ITSELF, and this is broader than the Go bounds.
#
# v4.3 set GOCACHE/GOTMPDIR but left TMPDIR inherited, so every round ran with
# TMPDIR still pointing at the denied /var/folders/**/T. Measured in this
# lane's OWN round 3, five times:
#
#   zsh:1: can't create temp file for here document: operation not permitted
#
# That is not a Go problem. Any tool needing a temp file -- a heredoc, mktemp,
# sort, a python NamedTemporaryFile -- fails the same way, and the reviewer
# sees a shell that cannot run ordinary constructs. I did not notice it in my
# own round because I read the verdict and not the log.
RTMPDIR=$(mktemp -d "/tmp/codex-tmp-$NAME-XXXXXX")
rmdir "$RW"   # git worktree add wants to create it
# Everything the reviewer left in the worktree, preserved BEFORE the worktree is
# removed. CF lost a 382k-token round because the reviewer wrote its findings to
# `codex-gate.md` inside the review worktree instead of the -o path; the wrapper
# deleted the worktree and the findings with it, and the `test -s` on the -o file
# passed because that file held a one-line link to the deleted one.
#
# The reviewer choosing a different filename is not a failure mode we can
# prevent, so it is one we survive: copy first, delete second.
preserve_residue() {
  local dest="$OUTDIR/$NAME-$TS-worktree-residue"
  local had=0 line status path orig
  local kept=0 failed=0 big=0 skipped=0 rc=0
  local listing errlog
  # --ignored=matching is REQUIRED, not defensive. The review-hygiene entries in
  # `.git/info/exclude` deliberately ignore exactly these artifacts
  # (`codex-gate.*`, `/chaos-*.md`, `/lane-*.md`), so a plain
  # `git status --porcelain` does NOT list them -- it would have skipped the very
  # file CF lost.
  #
  # The listing goes to a FILE rather than a process substitution so its exit
  # status is observable. Previously this was `< <(git ... 2>/dev/null)`: when
  # git failed the loop body simply never ran, `had` stayed 0, and the function
  # returned in SILENCE -- a total loss of the forensics it exists to provide,
  # indistinguishable from "there was nothing to preserve".
  listing=$(mktemp "${TMPDIR:-/tmp}/codex-residue-list-XXXXXX") || return 0
  errlog="$listing.err"
  git -C "$RW" status --porcelain -z --untracked-files=all --ignored=matching \
      >"$listing" 2>"$errlog" || rc=$?
  if [ "$rc" -ne 0 ]; then
    warn "residue: CANNOT LIST worktree $RW (git status exit $rc) — NOTHING was preserved and review output may be unrecoverable. git said: $(head -c 400 "$errlog" | tr '\n' ' ')"
    rm -f "$listing" "$errlog"
    return 0
  fi

  # -z gives NUL-separated records, so paths with spaces or newlines survive.
  while IFS= read -r -d '' line; do
    status=${line:0:2}
    path=${line:3}
    # With -z a rename/copy is TWO records: this one carries the DESTINATION
    # path, and the ORIGIN follows as a bare record with no status field.
    # Consume the origin here.
    #
    # An earlier revision instead stripped a tab (`path=${path##*$'\t'}`). That
    # is the NON-`-z` porcelain spelling and never appears in this stream, so
    # the strip was dead code and the origin record fell through to be parsed as
    # a status line: an origin of `orig.md` became status='or', path='g.md'.
    # That was harmless only by luck -- `[ -e ]` happened to fail. A real file
    # matching the truncated name would have been copied under a false
    # provenance, e.g. `ab/findings.md` truncating to `findings.md`.
    case "$status" in
      R* | C*)
        if ! IFS= read -r -d '' orig; then
          warn "residue: truncated rename record after '$path' — the listing ended mid-entry, so the residue set may be incomplete"
        fi
        ;;
    esac
    [ -e "$RW/$path" ] || continue
    # Build residue is not findings. Skipping it keeps preservation cheap and
    # keeps the residue dir readable by whoever has to go looking in it.
    #
    # `*.o`/`*.a` are ANCHORED rather than dropped. lane-4441's argument, which
    # I was wrong about: dropping them entirely means object files accumulating
    # in a build directory get copied into the residue as findings, while the
    # thing that made the old rule harmful was only its REACH -- a bare suffix
    # with no path component matched `corpus.a` at the repository root and
    # `sub/deep/notes.o` three levels down, both of which are plausible reviewer
    # output. Anchoring keeps the cheap build-residue skip and gives back the
    # out-of-tree files. The prefix entries were always anchored to a directory;
    # the suffix entries were not, and that asymmetry was invisible in the list
    # as written.
    case "$path" in
      .venv/*|node_modules/*|target/*|dist/*|.git/*|\
      build/*.o|build/*.a|*/build/*.o|*/build/*.a|\
      obj/*.o|obj/*.a|*/obj/*.o|*/obj/*.a|\
      bin/*.o|bin/*.a|*/bin/*.o|*/bin/*.a)
        # REPORTED, not silent. The size skip named its file and summarised at
        # the end while this branch said nothing, so two identical outcomes --
        # "a file was not preserved" -- were reported completely differently,
        # inside the one function whose purpose is not losing things quietly.
        warn "residue: skipping $path (build-directory skip list)"
        skipped=$((skipped + 1))
        continue
        ;;
    esac
    # The `|| echo 0` fallback is deliberate and its DIRECTION is the point: it
    # errs toward copying rather than skipping, and it is guarded by `[ -f ]`.
    # stderr goes to the errlog rather than /dev/null so the reason survives
    # even though the value is defaulted -- a suppressed error is what made the
    # other two holes invisible.
    if [ -f "$RW/$path" ] && [ "$(wc -c < "$RW/$path" 2>>"$errlog" || echo 0)" -gt 10485760 ]; then
      warn "residue: skipping $path (>10MB)"
      big=$((big + 1))
      continue
    fi
    if [ "$had" -eq 0 ]; then
      # NOT a silent `|| return 0`. This is the path where the loss is total --
      # the destination does not exist, so nothing is preserved at all -- and it
      # is reached by exactly the conditions most likely to occur in anger: an
      # unwritable OUTDIR, a full disk, or a stale file sitting at that path.
      if ! mkdir -p "$dest" 2>>"$errlog"; then
        warn "residue: CANNOT CREATE $dest — NOTHING was preserved and review output may be unrecoverable. Check that $OUTDIR is writable and that no file occupies that path. mkdir said: $(head -c 200 "$errlog" | tr '\n' ' ')"
        rm -f "$listing" "$errlog"
        return 0
      fi
      had=1
    fi
    mkdir -p "$dest/$(dirname "$path")" 2>>"$errlog" || true
    # Counted, not swallowed. The old `|| true` printed an unconditional success
    # line over an unmeasured number of losses.
    if cp -R "$RW/$path" "$dest/$path" 2>>"$errlog"; then
      kept=$((kept + 1))
    else
      failed=$((failed + 1))
    fi
  done < "$listing"

  if [ "$had" -eq 1 ]; then
    if [ "$failed" -gt 0 ]; then
      warn "residue: preserved $kept file(s) in $dest but $failed FAILED to copy — the set is INCOMPLETE. Copy errors: $(head -c 400 "$errlog" | tr '\n' ' ')"
    else
      warn "preserved $kept file(s) of review-worktree residue in $dest — a reviewer that wrote its findings to a file other than the -o path left them here"
    fi
    # Written as an if-block, NOT `[ "$big" -gt 0 ] && warn ...`. That idiom
    # returns non-zero when the test is false; it is harmless here only because
    # `rm -f` follows it, and would abort cleanup() under `set -e` the moment it
    # became the last statement in this function.
    if [ "$big" -gt 0 ]; then
      warn "residue: $big file(s) skipped for exceeding 10MB"
    fi
    if [ "$skipped" -gt 0 ]; then
      warn "residue: $skipped file(s) skipped by the build-directory list"
    fi
  fi
  rm -f "$listing" "$errlog"
}

cleanup() {
  preserve_residue
  # if-block, NOT `[ … ] && [ … ] && rm -rf` -- same reason as preserve_residue's
  # neighbours above (lane-4441's read of 232915a7). The && chain returns
  # non-zero when either test is false. That is harmless ONLY while statements
  # follow it; the moment it becomes the last statement in cleanup() -- which a
  # future edit to the three lines below would do silently -- it becomes the
  # function's exit status, and cleanup() runs from `trap ... EXIT`, so the
  # whole wrapper would start exiting 1 on a clean round with no other change.
  if [ -n "${RGOTMPDIR:-}" ] && [ -d "$RGOTMPDIR" ]; then
    rm -rf "$RGOTMPDIR"
  fi
  # RTMPDIR too, or every round leaves a /tmp/codex-tmp-* behind. Same if-block
  # shape as its neighbours, for the reason documented above them.
  if [ -n "${RTMPDIR:-}" ] && [ -d "$RTMPDIR" ]; then
    rm -rf "$RTMPDIR"
  fi
  if [ "$KEEP" -eq 1 ]; then warn "keeping review worktree $RW (-k)"; return; fi
  git -C "$WT" worktree remove --force "$RW" 2>/dev/null \
    || warn "review worktree $RW not removed — remove it manually and check 'git worktree list'"
}
trap cleanup EXIT

git -C "$WT" worktree add --detach "$RW" "$TIP" >/dev/null || die "worktree add failed"
cp "$PROMPT" "$RW/prompt.md"
# STANDING SAFETY LINE, appended to every prompt regardless of what the lane
# wrote. A #2134 round composed and "quoted" a
# `docker exec dev-health-clickhouse-1 clickhouse-client --query ...` against
# chris's SHARED stack. It did not run (no exec block), but a reviewer that will
# write the command is one prompt away from running it, and the shared stack is
# not this round's to touch. Appended rather than merged into the lane's text so
# it cannot be edited out by a prompt author who did not think of it.
cat >> "$RW/prompt.md" <<'STANDING_RULES'

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
STANDING_RULES
for aux in .codex-review-context.md LEDGER.md; do
  [ -f "$WT/$aux" ] && cp "$WT/$aux" "$RW/$aux"
done

warn "round $NAME-$TS: model=$MODEL effort=$EFF tip=$TIP review-worktree=$RW"
warn "go bounds: GOFLAGS=$RGOFLAGS GOMAXPROCS=$RGOMAXPROCS GOCACHE=$RGOCACHE GOTMPDIR=$RGOTMPDIR TMPDIR=$RTMPDIR sandbox=$RSANDBOX"
# NO PREDICTION ABOUT WHAT THE SANDBOX CAN DO.
#
# An earlier draft printed "sandbox=read-only: NOTHING is writable, so
# go test/go build CANNOT run" and "a verdict from this round is REASONED, not
# EXECUTED". lane-4441 blocked it and was right: their #2140 r1-r3 all ran under
# `-s read-only` through this wrapper and have exec blocks showing
# `go test ... ok` (3, 7 and 2 results) and `go run .` (2 and 4 blocks), which
# compiles and writes a binary. Read-only through the wrapper's real path -- a
# worktree and GOCACHE under $TMPDIR -- demonstrably builds and tests.
#
# The second line was the damaging one. It told every reader to discount a
# read-only verdict as reasoned rather than executed, which would have devalued
# a CLEAN that was then proved executed by tracing four `go run` blocks into the
# log. A wrapper that systematically devalues correct verdicts is worse than one
# that says nothing, and it is self-reinforcing: a lane told "REASONED, not
# EXECUTED" has been given a reason not to run the check that would show the
# claim is false.
#
# What replaces it is measured after the fact rather than predicted before it.
RC=0
# BOUND REVIEWER-SPAWNED GO WORK.
#
# Prompt-level scoping does NOT hold: on 09-02 a reviewer widened a
# three-package prompt to `go test ./...` on its own and pinned the host
# (load 430 on 16 CPUs). A limit the reviewer cannot talk its way out of has to
# be in the environment, not in the prompt -- the prompt line is kept as well,
# but it is the second line of defence, not the first.
#
# GOFLAGS is APPENDED, not replaced: an inherited `-mod=readonly` or similar
# must survive. Go honours the LAST occurrence of a repeated flag, so -p=2 wins
# regardless of what precedes it.
#
# Exported into codex's environment ONLY. `env` on this one command cannot
# touch the caller's shell, which matters because lanes source this script's
# invocation from their own working shells.
# Under workspace-write the workspace ($RW) is writable by default; the per-lane
# GOCACHE and the per-round GOTMPDIR sit OUTSIDE it and must be granted
# explicitly, or `go test` still fails with "failed to initialize build cache".
# Expanded below as ${SANDBOX_ARGS[@]+"${SANDBOX_ARGS[@]}"}, NOT as
# "${SANDBOX_ARGS[@]}". Under `set -u`, bash 3.2 treats an EMPTY array's `[@]`
# as an unbound variable and aborts -- and /bin/bash on macOS is 3.2. This
# script's `#!/usr/bin/env bash` happens to resolve to Homebrew bash 5.x today,
# so the naive form works until someone runs it with a different PATH, at which
# point EVERY read-only round dies at this line. Measured both ways.
# NOTHING here touches shell_environment_policy. An earlier draft added four
# `shell_environment_policy.set.*` flags on the belief that the `env` prefix
# below did not reach the reviewer. That was WRONG and the belief came from
# measuring `codex sandbox`, which does apply this host's `inherit = "core"`,
# and generalising to `codex exec`, which does not. Sentinel-proven on
# `codex exec` (values no model could guess): GOCACHE, GOFLAGS=-p=7,
# GOMAXPROCS=11 and an invented variable all arrived intact. The `env` prefix
# works; adding the flags would have been an unreviewed change fixing nothing.
SANDBOX_ARGS=()
if [ "$RSANDBOX" = "workspace-write" ]; then
  # DEFENSIVE, and deliberately not claimed as load-bearing by default.
  #
  # Measured: `workspace-write` already makes $TMPDIR writable, and both
  # $RGOCACHE and $RGOTMPDIR default to paths under $TMPDIR -- so with the
  # default settings this grant is REDUNDANT and dropping it changes nothing
  # (mutant survived, cold cache, 1056 entries written either way).
  #
  # It becomes load-bearing the moment CODEX_REVIEW_GOCACHE points outside
  # $TMPDIR, which is exactly what that override exists for. Measured with a
  # cache at ~/.cache: grant present -> ok; grant absent -> "failed to
  # initialize build cache at /Users/chris/.cache/...: mkdir ... not permitted".
  #
  # The cache is deliberately NOT moved inside the per-round worktree: that
  # would make it cold every round, which is exactly what the GOCACHE comment
  # above warns against.
  SANDBOX_ARGS+=(-c "sandbox_workspace_write.writable_roots=[\"$RGOCACHE\",\"$RGOTMPDIR\"]")
fi
# ROUND PROVENANCE. Written BEFORE codex runs, as the first line of the log.
#
# verify-round-repros.py's MISVENUED class treats a round as CI only when this
# line carries a run id, and defaults everything else to LOCAL. Without the
# wrapper actually EMITTING the line, every future log defaults to LOCAL by
# construction and the CI branch is unreachable -- a class that can never fire
# is indistinguishable from one that always passes.
#
# arch is recorded because that is the property that actually matters: the
# whole point of MISVENUED is that an arm64 pass does not clear an x86 case.
if [ -n "${GITHUB_RUN_ID:-}" ]; then
  PROV="run-id=${GITHUB_RUN_ID} host=$(uname -n) arch=$(uname -m)"
else
  PROV="local host=$(uname -n) arch=$(uname -m)"
fi
printf 'round-provenance: %s\n' "$PROV" > "$L"
# The BOUNDS line goes into the log directly beneath provenance, not only to
# stderr (team-lead, CHAOS-4925). Both facts a reader needs about a round --
# WHERE it ran and WHAT environment it was given -- are then adjacent and
# machine-readable from the log alone.
#
# This matters because of the defect that produced v4.4: v4.3 configured GOCACHE
# on the denied path, and the only place that was visible was the wrapper's
# stderr, which nobody keeps. The log recorded the failures and not the
# configuration that caused them, so the two could not be correlated after the
# fact without the operator's terminal scrollback.
printf 'round-bounds: GOFLAGS=%s GOMAXPROCS=%s GOCACHE=%s GOTMPDIR=%s TMPDIR=%s sandbox=%s\n' \
  "$RGOFLAGS" "$RGOMAXPROCS" "$RGOCACHE" "$RGOTMPDIR" "$RTMPDIR" "$RSANDBOX" >> "$L"
warn "round-provenance: $PROV"

# NOTE THE APPEND. This redirect was `> "$L"`; it MUST stay `>>` now, or codex
# truncates the provenance line written immediately above and the log silently
# reverts to having no provenance at all -- which reads as LOCAL, the safe
# default, so nothing would ever look broken.
( cd "$RW" && env \
    GOFLAGS="$RGOFLAGS" \
    GOMAXPROCS="$RGOMAXPROCS" \
    GOCACHE="$RGOCACHE" \
    GOTMPDIR="$RGOTMPDIR" \
    TMPDIR="$RTMPDIR" \
    codex exec -m "$MODEL" -c "model_reasoning_effort=\"$EFF\"" \
    ${SANDBOX_ARGS[@]+"${SANDBOX_ARGS[@]}"} \
    -s "$RSANDBOX" -C "$RW" -o "$V" - < prompt.md ) >> "$L" 2>&1 || RC=$?

# POST-ROUND MEASUREMENT. codex logs every real command as an `exec` block, so
# this counts what the round actually did instead of guessing what it could do.
# A round with zero exec blocks executed nothing -- that one IS worth warning
# about, because any command output such a verdict quotes was produced rather
# than observed. Seen in the wild: a 45-line log, zero exec blocks, four
# confident values including an exact error string.
# NO `|| echo 0` HERE. `grep -c` PRINTS 0 and EXITS 1 when nothing matches, so
# `$(grep -c ... || echo 0)` yields the two-line string "0\n0" on a zero-exec
# round -- and `[ "0\n0" -eq 0 ]` is an "integer expression expected" error that
# takes the ELSE branch. The warning below could therefore never fire on the
# exact case it exists for. (Found by lane-4441.)
#
# Third instance of this idiom in one day: `pgrep -fc ... || echo 0` fed a
# hardcoded zero to the launch gate all session, and `wc -c ... || echo 0` was
# the Q4 discussion on v4.2. A suppressed error plus a default is not a
# measurement -- and here it was worse than a wrong number, because the wrong
# number disabled the check.
# `|| true`, and NOT a bare `$(grep -c ...)`. Three wrong forms preceded this:
#   1. `$(grep -c ... || echo 0)`  -- grep -c PRINTS 0 and EXITS 1, so this
#      yields the two-line string "0\n0"; `[ "0\n0" -eq 0 ]` errors and takes
#      the ELSE branch, so the warning could never fire on a zero-exec round.
#   2. `$(grep -c ...); X=${X:-0}`  -- correct value, but the failing command
#      substitution ABORTS under `set -euo pipefail` before the warning is
#      reached. Fixing a fail-open with a fail-hard.
#   3. this one. `|| true` keeps grep's own printed 0 and makes the substitution
#      succeed; `${X:-0}` then covers only the real absence case, grep failing
#      to run at all.
# Verified under `set -euo pipefail` on a zero-exec log AND on a 32-block log.
EXEC_BLOCKS=$(grep -c '^exec$' "$L" 2>/dev/null || true); EXEC_BLOCKS=${EXEC_BLOCKS:-0}
GO_EXECS=$(grep -A1 '^exec$' "$L" 2>/dev/null | grep -cE 'go (test|run|build)' || true); GO_EXECS=${GO_EXECS:-0}
warn "round recorded $EXEC_BLOCKS exec block(s) ($GO_EXECS go test/run/build)"

# HARNESS-BLOCKED DETECTION (lane-4441).
#
# The exec-block counter answers "did the round execute anything". It CANNOT
# answer "did the round have to route around the environment I gave it". Those
# look identical in the summary: 8 go test/run/build is 8 either way.
#
# v4.3 pointed GOCACHE at the denied $TMPDIR. Round 1 said in its own words
# "the Go command was blocked before compilation because its sandboxed build
# cache cannot create entries", then relocated to /tmp and succeeded. The
# summary line reported the successes and said nothing about the harness having
# failed first -- so a broken harness and a working one produced the same
# report, and the defect survived two rounds before a human read the blocks
# rather than counting them.
# 'Read-only file system' is v4.8's addition: Linux (landlock) denies writes
# with that string, not the 'operation not permitted' macOS gives, so a round
# that hit the pre-v4.8 Linux defect would have passed this check silently.
#
# v4.8.1: scoped to exec blocks that are BOTH a go test/run/build AND
# themselves reported failed, not any occurrence of the string anywhere in
# the log. round chaos-4757-2174-gate-r2-bigboy-20260903T182647 fired this on
# `go doc strconv.ParseUint` -- a background module lookup blocked by the
# sandbox's NETWORK policy ("dial udp ... socket: operation not permitted"),
# unrelated to workspace-write's file bounds -- inside an exec block that
# itself reported ` succeeded in 0ms`. Every exec block in that log
# succeeded; the old any-occurrence grep could not tell a benign network
# refusal a successful command shrugged off from an actual write refusal
# that broke the command. A go/test invocation that truly cannot write still
# reports ` failed in`, so gating on that keeps the real signal (v4.3/v4.8's
# denied-GOCACHE cases) while dropping this one.
BLOCKED_PAT='operation not permitted|cannot create entries|failed to initialize build cache|Read-only file system'
BLOCKED_HITS=$(awk -v pat="$BLOCKED_PAT" '
  function flush() { if (in_block && is_go && failed && blocked) hits++ }
  /^exec$/ { flush(); in_block=1; is_go=0; failed=0; blocked=0; want_cmd=1; want_status=0; next }
  in_block && want_cmd {
    if ($0 ~ /go (test|run|build)/) is_go=1
    want_cmd=0; want_status=1; next
  }
  in_block && want_status {
    if ($0 ~ /^ *failed in/) failed=1
    want_status=0; next
  }
  in_block && $0 ~ pat { blocked=1 }
  END { flush(); print hits+0 }
' "$L" 2>/dev/null || true)
BLOCKED_HITS=${BLOCKED_HITS:-0}
if [ "$BLOCKED_HITS" -gt 0 ]; then
  warn "HARNESS WARNING: $BLOCKED_HITS go test/run/build exec block(s) FAILED with the"
  warn "  sandbox refusing a path this wrapper configured. Check GOCACHE/GOTMPDIR"
  warn "  before trusting that this round had the environment it was given."
fi
if [ "$EXEC_BLOCKS" -eq 0 ]; then
  warn "NO COMMANDS WERE EXECUTED in this round. Any '\$ cmd' output the verdict quotes was produced, not observed. Verify with verify-round-repros.py before grading it."
fi

HEAD_AFTER=$(git -C "$WT" rev-parse HEAD)
[ "$HEAD_BEFORE" = "$HEAD_AFTER" ] \
  || die "LANE HEAD MOVED during the round ($HEAD_BEFORE -> $HEAD_AFTER). Recover via reflog/origin before anything else."

[ "$RC" -eq 0 ] || { warn "codex exited rc=$RC — read $L"; echo "VERDICT=$V"; exit "$RC"; }
[ -s "$V" ] || die "codex exited 0 but wrote no verdict file — treat as NO VERDICT, re-run; log: $L"

# CITATION NORMALIZATION (v4.8.1, CHAOS-4757 round 2179).
#
# A reviewer citing evidence naturally links the absolute path it read the
# file at, which under this wrapper IS the review worktree ($RW) — a path
# that is correct and complete at write time and about to be REMOVED by
# cleanup(). round chaos-4757-2179-gate-r2-bigboy-20260903T183720 wrote a
# COMPLETE, well-evidenced NOT CLEAN verdict whose citations happened to use
# that absolute prefix; the old check read the mere presence of $RW as proof
# the report was lost and exited 3 on a working round.
#
# Strip it BEFORE either check below reads the file, so a citation like
# `$RW/internal/foo.go:12` becomes the repo-relative `internal/foo.go:12` a
# reader can actually follow once $RW is gone. Three prefix spellings can
# denote the same directory and all get stripped: $RW itself, its resolved
# physical path (macOS symlinks /tmp -> /private/tmp, so `mktemp -d
# "${TMPDIR:-/tmp}/..."` can print as either), and the equivalent path under
# a $TMPDIR the caller had set (mktemp's actual base, if not plain /tmp).
RW_PREFIXES=("$RW")
RW_REAL=$(cd "$RW" 2>/dev/null && pwd -P || true)
[ -n "$RW_REAL" ] && [ "$RW_REAL" != "$RW" ] && RW_PREFIXES+=("$RW_REAL")
case "$RW" in
  /tmp/*) RW_PREFIXES+=("/private$RW") ;;
esac
if [ -n "${TMPDIR:-}" ]; then
  RW_PREFIXES+=("${TMPDIR%/}/$(basename "$RW")")
fi
V_NORM="$V.normalized.tmp"
cp "$V" "$V_NORM"
for prefix in "${RW_PREFIXES[@]}"; do
  [ -n "$prefix" ] || continue
  # Escape sed/BRE metacharacters in the path (mktemp suffixes are
  # alphanumeric, but this must not silently corrupt a verdict on a host
  # whose $TMPDIR contains one). `#` is the delimiter, not `/`, since the
  # prefix itself is full of slashes.
  esc=$(printf '%s' "$prefix" | sed -e 's/[.[\*^$()+?{}|\\#]/\\&/g')
  sed -i.bak -e "s#${esc}/##g" -e "s#${esc}##g" "$V_NORM" 2>/dev/null || true
  rm -f "$V_NORM.bak"
done
mv "$V_NORM" "$V"

# A non-empty verdict is not a verdict. The lost CF round wrote one line naming a
# file inside the review worktree, which `test -s` accepted and cleanup then
# deleted. Both shapes are now loud, and neither can exit 0 silently.
VLINES=$(wc -l < "$V" | tr -d ' ')
VSUSPECT=0
if [ "$VLINES" -lt 20 ]; then
  # WARNING ONLY -- deliberately not a gate. Line count is a proxy for effort,
  # and it produced a false positive on real evidence: a 10-line verdict on
  # CHAOS-4834 caught a false green (a path-glob bug that skipped the Go gate
  # for every root-level .go, go.mod and go.sum). Gating on it would have
  # exited 3 on the most valuable round of the day. The verdict-SHAPE rule is
  # the gate, because a clobbered report is structurally identifiable; short
  # is merely unusual.
  warn "NOTE: $V is $VLINES line(s) (<20). Short is not wrong -- a dense report is fine -- but if it reads like a summary, check $L and the residue dir. Not a gate."
fi
# The LAST non-empty line must be a verdict. Root cause (CF): `-o` is
# `--output-last-message` -- codex OVERWRITES this file with the reviewer's
# final reply at exit. A prompt that says "write your report to the file" gets
# the report written and then clobbered by a one-line sign-off, and `test -s`
# accepts the wreckage. The reviewer's final REPLY must therefore BE the report.
#
# Matched by SHAPE, not by a literal `VERDICT:` prefix. Verdict lines on this
# host are already written several ways -- CLEAN | NOT CLEAN | BLOCK | SOUND |
# NOT SOUND, with an optional `Verdict:` prefix, optional markdown bold, and an
# optional trailing parenthetical and period (`Verdict: **NOT CLEAN** (1 P1).`).
# Concretely observed: `Verdict: CLEAN`, bare
# `CLEAN`, and `Verdict: **NOT CLEAN**` -- so a strict prefix check would reject
# every round now in flight while catching nothing extra: a clobbered summary
# ("See <path> for findings") is not verdict-shaped under either rule.
# CHAOS-4925: a verdict line ANYWHERE is the test; last-line is only a warning.
#
# The old rule required the LAST non-empty line to be verdict-shaped, and
# false-alarmed on a report that led with CLEAN and closed with a caveat
# sentence. That is GOOD reviewer behaviour -- stating the verdict up front and
# qualifying it afterwards -- and the tool punished it, which pushes reviewers
# toward burying the verdict at the bottom to satisfy a checker.
#
# The failure this check actually exists to catch is a CLOBBERED summary
# ("See <path> for findings"), which contains no verdict line at all, anywhere.
# So absence-anywhere is the real signal; last-line position is a style nit and
# is now reported as one.
# The regex must match the format rounds ACTUALLY produce. lane-4441 ran the
# first version of this against their archived .md reports rather than a probe,
# and it found NO verdict in either of them -- both lead with
#   ## BLOCK -- guard false-greens remain
# so the new branch was unreachable for exactly the reports it was added for.
# Two gaps, both at the ends: a leading `##` was not allowed, and trailing prose
# after the token was not allowed.
#
# The naive loosening (allow any trailing text) is WRONG -- measured, it matches
# "Blocked on bigboy...", "Blocking issue:...", "CLEANUP: removed...",
# "soundness of the argument...". Every one of those is the verdict token as a
# PREFIX OF A LONGER WORD. So the fix is a WORD BOUNDARY, not a separator --
# requiring a separator also rejects "CLEAN with one non-blocking observation",
# which is a format both of us have used.
#
# Measured: 8 legitimate formats match, 5 prefix-of-longer-word cases rejected.
VERDICT_RE='^[[:space:]]*#{0,6}[[:space:]]*\**[[:space:]]*(verdict[[:space:]]*:)?[[:space:]]*\**[[:space:]]*((not[[:space:]]+)?(clean|sound)|block)([^[:alnum:]].*)?$'
VLAST=$(grep -v '^[[:space:]]*$' "$V" | tail -1)
if grep -Eqi "$VERDICT_RE" "$V"; then
  # A verdict exists somewhere. Only note it if it is not the closing line.
  if ! printf '%s' "$VLAST" | grep -Eqi "$VERDICT_RE"; then
    warn "note: $V carries a verdict line, but not as its last line. Not a fault --"
    warn "  leading with the verdict and closing with a caveat is fine."
  fi
elif ! printf '%s' "$VLAST" \
     | grep -Eqi "$VERDICT_RE"; then
  warn "SUSPECT VERDICT: $V contains NO verdict line anywhere:"
  warn "  ${VLAST:0:120}"
  warn "codex -o is --output-last-message: it OVERWRITES that file with the reviewer's FINAL REPLY at exit. If the prompt asked for a report written to a file, the report was clobbered by the sign-off. The reviewer's final reply must BE the report, ending with a verdict line (CLEAN | NOT CLEAN | BLOCK)."
  RESIDUE_DIR="$OUTDIR/$NAME-$TS-worktree-residue"
  if [ -d "$RESIDUE_DIR" ]; then
    warn "RECOVERY paths -- residue dir: $RESIDUE_DIR"
  else
    warn "RECOVERY paths -- residue dir: (none written; the reviewer left no files in the worktree)"
  fi
  # Best-effort: the session transcripts touched since this round began. NOT a
  # precise correlation -- concurrent rounds write concurrently -- so these are
  # CANDIDATES to search, printed because searching a named directory beats
  # discovering it exists.
  if [ -d "$HOME/.codex/sessions" ]; then
    CAND=$(find "$HOME/.codex/sessions" -name '*.jsonl' -newermt "@$START_EPOCH" 2>/dev/null | head -3)
    if [ -n "$CAND" ]; then
      warn "RECOVERY paths -- session transcript candidate(s), modified since this round started:"
      printf '%s\n' "$CAND" | while IFS= read -r c; do warn "    $c"; done
    else
      warn "RECOVERY paths -- session transcripts: $HOME/.codex/sessions/*.jsonl (none newer than this round's start; widen the search)"
    fi
  fi
  warn "RECOVERY -- the report is NOT lost, it is in the codex session: (1) raw transcripts in ~/.codex/sessions/*.jsonl, (2) \`deja\` indexes those sessions and can search them, (3) \`codex rescue\`. Recover the findings from one of those BEFORE re-running a round; a re-run costs the tokens again and returns a different review."
  VSUSPECT=1
fi
# v4.8.1: the citation normalization above already stripped every
# recognised $RW spelling, so a hit here means either an unrecognised
# spelling survived or genuine content still names the path. That alone is
# no longer proof of a lost report (round 2179 above): escalate to SUSPECT
# only when it is corroborated by BOTH of the signals that actually mean
# "the report is not here" — short (<20 lines, the existing NOTE threshold)
# AND the residue dir holds something beyond the prompt/context files this
# wrapper itself copies into every review worktree at start (those are
# inputs, not reviewer output, and their mere presence proves nothing).
RW_LEAKED=0
for prefix in "${RW_PREFIXES[@]}"; do
  [ -n "$prefix" ] && grep -qF "$prefix" "$V" 2>/dev/null && RW_LEAKED=1
done
if [ "$RW_LEAKED" -eq 1 ]; then
  RESIDUE_DIR="$OUTDIR/$NAME-$TS-worktree-residue"
  RESIDUE_HAS_FINDINGS=0
  if [ -d "$RESIDUE_DIR" ] && find "$RESIDUE_DIR" -type f \
       ! -name 'prompt.md' ! -name '.codex-review-context.md' ! -name 'LEDGER.md' \
       -print -quit 2>/dev/null | grep -q .; then
    RESIDUE_HAS_FINDINGS=1
  fi
  if [ "$VLINES" -lt 20 ] && [ "$RESIDUE_HAS_FINDINGS" -eq 1 ]; then
    warn "SUSPECT VERDICT: $V still references the review worktree path $RW after"
    warn "  normalization, is short ($VLINES lines), and residue dir $RESIDUE_DIR"
    warn "  holds files beyond prompt/context — the findings are probably in a file"
    warn "  inside it. Check the residue dir."
    VSUSPECT=1
  else
    warn "note: $V still references $RW after normalization, but is $VLINES line(s)"
    warn "  and residue holds only prompt/context files (or none) — treating as"
    warn "  citation text, not a lost report."
  fi
fi
if [ "$VSUSPECT" -eq 1 ]; then
  warn "verdict is suspect — exiting 3 rather than 0 so a wrapper cannot read this as a clean round"
  echo "VERDICT=$V"
  exit 3
fi

echo "VERDICT=$V"
