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
# v4.8.2 (2026-09-03) fixes unbounded disk accumulation (chris: "100s of gbs
# is crazy to leave around" -- ~41G of leftover cache/tmp dirs found on this
# Mac and on bigboy, none ever deleted). Two changes:
#
#   1. every cache/tmp dir this script creates is now named
#      codex-review-<kind>-<lane>-<roundid>[-...] (kind: gocache, modcache,
#      gotmp, worktree; <lane> is the lane worktree's basename, the same
#      stable key GOCACHE was already keyed on -- see the comment below --
#      falling back to "unattributed" if that basename cannot be determined;
#      <roundid> is $TS, this round's own timestamp). The verdict/out dir
#      (OUTDIR, caller-controlled, normally inside the lane's own worktree,
#      not under /tmp) is deliberately NOT part of this scheme and is never
#      touched by cleanup or either reap subcommand below.
#   2. cleanup() (trap EXIT, so this also runs on error and on SIGTERM) now
#      removes the gocache/modcache/gotmp dirs THIS RUN created, by their
#      exact variable -- never a glob -- unless CODEX_KEEP_CACHE=1. This
#      flips the old default (GOCACHE kept warm forever, relying on lanes to
#      clean it up at close-out, which is exactly what stopped happening);
#      CODEX_KEEP_CACHE=1 is the opt-in for a caller that wants the old warm
#      cache back and will police its own cleanup.
#
# v4.8.3 (2026-09-04) closes the cold-sandbox gap documented in
# .claude/skills/codex-review/SKILL.md ("Wrapper v4.8.2 cold-sandbox gap"):
# codex's sandbox blocks proxy.golang.org, so a round starting from this
# script's own COLD per-round GOMODCACHE/GOCACHE (v4.8.2 above) could not
# download modules inside the sandbox and silently fell back to a
# gofmt/diff-only, source-trace verdict instead of an executed one (bigboy,
# 2026-09-04 01:51Z, the first round after the v4.8.2 install). Two changes:
#
#   1. A WARM step now runs in the review worktree, OUTSIDE the codex
#      sandbox, right after `git worktree add` and before `codex exec`:
#      `go mod download all`, then `go build ./... && go vet ./...`, then the
#      same build+vet pair again with `-tags=integration` -- all against this
#      round's OWN RGOMODCACHE/RGOCACHE (never a shared or mounted cache, and
#      never /mnt/go-cache) and bounded by the same RGOFLAGS/-p and
#      GOMAXPROCS the reviewer's own sandboxed build already uses. Build
#      output is redirected to a scratch dir under RGOTMPDIR (`-o`) so the
#      warm build cannot leave binaries in the worktree for preserve_residue
#      to trip over. Duration and the module count (zip files under
#      "$RGOMODCACHE/cache/download") are logged to the round .log as a
#      `warm-step:` line, machine-parseable the same way `round-bounds:` is.
#      The warm build's LAST step is an offline resolve proof --
#      `GOPROXY=off go test -count=1 -run '^$' ./...` (compiles every test
#      binary the sandbox's own `go test` would need, runs none of them,
#      and cannot reach the network) -- so a warm-step OK genuinely means
#      the sandbox's `go test` will not need proxy.golang.org, not just
#      that `go build`/`go vet` succeeded. Cache sizes (`du -sh` of
#      RGOMODCACHE/RGOCACHE) are logged alongside duration and module count
#      for the same reason team-lead relayed from a live manually-warmed
#      round (lane-structure-memory r2): to size-compare a wrapper-warmed
#      round against a hand-warmed one.
#   2. If the warm step fails, the round ABORTS before codex ever starts:
#      non-zero exit, the failure plus the warm build's own output appended
#      to the round .log, and a loud message on stderr. A cold sandbox must
#      never silently degrade to a gofmt-only verdict -- this replaces the
#      manual workaround SKILL.md documented as "MANDATORY until v4.8.3
#      ships".
#
# The reviewer prompt template (the STANDING_RULES block appended to every
# round's prompt.md) gains two additions: (a) if `go test` is unavailable
# inside the sandbox, the reviewer must say so explicitly ("go test
# unavailable") and label every remaining claim EXECUTED or ARGUED, rather
# than let an unlabelled claim read as executed by default; (b) the round's
# actual RGOMODCACHE path is named in the prompt, and if `go test` still
# fails on a module lookup once inside the sandbox (the warm step proves
# the cache resolves OUTSIDE the sandbox; it cannot prove the sandbox can
# see that same cache), the reviewer is told to retry once against the
# HOST'S OWN module cache (GOMODCACHE=$HOME/go/pkg/mod, GOPROXY=off,
# read-only intent -- never granted a writable_roots entry, so a stray
# write attempt there fails the same way a read-only sandbox would deny it)
# before falling back to a source-trace verdict, and to say which GOMODCACHE
# it ended up using.
#
# Per-round cache naming and the trap-EXIT self-clean are UNCHANGED from
# v4.8.2 -- the warm step reuses the exact RGOMODCACHE/RGOCACHE this script
# already creates and already cleans up; nothing new to reap.
#
# v4.8.4 (2026-09-04) fixes four defects found by lane-s7c-outcomes running
# v4.8.3 on bigboy (Ubuntu ARM64, user ubuntu):
#
#   1. cleanup() and the --reap-mine/--reap-stale subcommands ran `rm -rf`
#      against this script's own per-round dirs without first making them
#      writable. Go's own module cache extraction marks directories
#      read-only (mode 0555) so a build cannot accidentally corrupt an
#      extracted module; `rm -rf` on such a tree fails file-by-file with
#      "Permission denied", leaves the directory behind, and buries
#      whatever the round's REAL failure was underneath that noise. Fixed
#      via a shared rm_rf_writable() helper: `chmod -R u+w` first, then
#      `rm -rf`, on the exact dir this run created or the exact reap
#      candidate the caller named -- never a shared cache, never `go env
#      GOCACHE`, never $HOME/go/pkg/mod. Same helper used by cleanup() and
#      by reap_dirs() (--reap-mine / --reap-stale).
#   2. GOCACHE/GOMODCACHE/GOTMPDIR/the review worktree were keyed on the
#      review worktree's BASENAME alone. The bigboy lane recipe's own
#      worked example clones acr into a directory literally named `acr`
#      (fixed in the recipe text, see its own note), so every lane
#      following that example got the same key and collided on the same
#      cache paths -- not a correctness bug, a silent cross-lane cache
#      collision. Fixed: the naming key is now the basename PLUS an
#      8-hex-char hash of the worktree's own resolved absolute path
#      (LANE_KEY), which is unique per checkout no matter what its
#      directory is called -- the clone basename is no longer load-bearing
#      for uniqueness. LANE itself is kept as the key's first
#      dash-delimited segment, so --reap-mine LANE's existing glob match
#      (`codex-review-*-LANE-*`) still matches every dir this run created.
#   3. The warm step's `go mod download all` needs a writable
#      $GOPATH/pkg/sumdb. On bigboy, ~/go and ~/go/pkg are root:root 755,
#      so a missing-hash lookup fails with an ENOENT under .../pkg/sumdb/...
#      that reads exactly like a network failure and is not one -- it is a
#      permission denial one directory up. Fixed: a per-round GOPATH
#      (RGOPATH) is created under this script's own /tmp scratch, named
#      and cleaned up the same way RGOTMPDIR already is, unless the caller
#      sets CODEX_REVIEW_GOPATH; exported into both the warm step's
#      environment and codex's own sandboxed environment. GOSUMDB
#      verification stays ON throughout -- this is a permission fix, never
#      a "turn off checksum verification" fix. A warm-step failure whose
#      log names a pkg/sumdb path with an unwritable parent now says so
#      explicitly: "this is a PERMISSION problem, not a network problem."
#   4. OUTDIR (and the log dir, which is the same directory) was never
#      created before use. A caller-supplied -o naming a not-yet-existing
#      directory made the warm step's own log redirect die with "No such
#      file or directory" before any verdict could be produced, or after
#      it, depending on timing -- either way, no usable output. Fixed:
#      `mkdir -p "$OUTDIR"` runs immediately after OUTDIR is resolved,
#      aborting loudly (non-zero exit, reason on stderr) if it cannot be
#      created.
#
# Also found the same pass: the bigboy recipe's manual pgrep-based codex
# launch-gate example (`pgrep -f "codex exe[c]" | wc -l`) is unsafe under
# `set -e -o pipefail` -- pgrep exits 1 on a legitimate ZERO-match count
# (an idle box, the common and DESIRED reading of that gate), and under
# pipefail that exit status survives through the `| wc -l` pipe and can
# kill a caller script that runs the gate as a bare statement, even though
# zero running rounds is exactly the "safe to launch" answer the gate
# exists to give. This wrapper does not itself run that gate (it is a
# manual pre-launch step documented in the recipe, not something
# codex-review.sh executes), so there is no in-script fix for it -- the
# recipe text itself is corrected instead (appends `|| true` to the
# pgrep|wc -l pipeline; see
# .remember/lanes/lane-oci-image/bigboy-lane-recipe.md).
#
# Two subcommands do the reaping for dirs from before this version, or from
# a round that got SIGKILLed past the trap:
#
#   --reap-mine LANE        remove every codex-review-*-LANE-* dir under
#                            $TMPDIR and /tmp with no open file (lsof/fuser),
#                            skipping and reporting anything busy.
#   --reap-stale HOURS [--dry-run]
#                            remove UNATTRIBUTED leftovers older than HOURS
#                            (mtime), with no open file: codex-go-cache-*,
#                            codex-go-modcache-*, pysum_gocache_* (pre-4.8.2
#                            names) and codex-review-*-unattributed-*.
#                            --dry-run prints what would be removed instead.
#
# Neither subcommand ever touches `go env GOCACHE` (the user's shared
# default build cache) or any path that does not match one of these exact
# prefixes -- lane-4818 already ran `go clean -cache` against the shared
# cache mid-flight on 09-02 and invalidated other lanes' in-progress work;
# these prefixes exist so a reap can never repeat that by accident.
#
# v4.8.5 (2026-09-03) fixes a silent whole-script death found by GWC's
# lane-web-681 running v4.8.4 on dev-health-web (a repo with no go.mod):
#
#   1. The warm step ran UNCONDITIONALLY, even in a repo with no Go code.
#      `go mod download all` failed as expected (no go.mod), but the count
#      line right after it -- `find "$RGOMODCACHE/cache/download" -name
#      '*.zip' | wc -l | tr -d ' '` -- runs against a path that only ever
#      gets created BY a successful `go mod download`, so it also failed;
#      under `set -euo pipefail` that pipeline failure killed the whole
#      script before the warm step's own `if [ "$WARM_RC" -ne 0 ]` block
#      ever got to run its controlled, message-printing `die`. Net effect:
#      exit 1, no round .log, no verdict, nothing but a 114-byte .log.warm
#      -- a failure indistinguishable from the wrapper never having run.
#      Fixed two ways, both needed:
#        a. the warm step now only runs when `$RW/go.mod` exists at the
#           reviewed tip. A repo with no go.mod (web, acr-frontend) skips
#           it entirely -- `warm-step: SKIPPED reason=no-go.mod` in the
#           round .log, no Go env exported to codex, straight to the
#           review. A repo WITH go.mod is unaffected: same warm step, same
#           loud abort on a real failure.
#        b. belt-and-braces, because the same crash shape can still occur
#           in a go.mod repo whose `go mod download all` fails before ever
#           creating cache/download: the WARM_MODULES count pipeline gets
#           `|| true` (same class as the `pgrep -fc ... 2>/dev/null ||
#           echo 0` idiom this file already discusses above), so a missing
#           cache/download dir can never stop the script from reaching its
#           own `if [ "$WARM_RC" -ne 0 ]` check and dying WITH a message.
#   2. The round .log ($L) was not created until the first thing wrote to
#      it -- in practice, the warm step's own failure block (or, on a
#      clean run, the round-provenance line well after the warm step).
#      Any death before that point (including the one above) left NO .log
#      at all, which is what made the failure read as "the wrapper never
#      started" instead of "the wrapper started and hit an error". Fixed:
#      `$L` is now created (empty) immediately once its path is resolved,
#      before the warm step or anything else that could die -- once the
#      wrapper has gotten this far, its own round .log is guaranteed to
#      exist no matter what happens next.
#
# v4.8.6 (2026-09-04, chris's ruling, RE-RULED same day at 07:37 PDT -- see
# below): on bigboy every Go run -- gates, integration suites, launchers,
# codex clones -- moved to the SHARED fleet caches, never per-lane/per-round
# ones, and nothing may ever `go clean` them. This wrapper's own per-round
# GOCACHE/GOMODCACHE/GOPATH scratch dirs (v4.8.2/v4.8.4 above) were the one
# place in the fleet still creating fresh throwaway caches on every round --
# on Linux ONLY, this wrapper now:
#
#   1. Honours the caller's GOCACHE/GOMODCACHE environment values (the plain
#      Go env vars, not the CODEX_REVIEW_* overrides, though those still win
#      when set -- see below), defaulting to /var/lib/oci-cache/go-build and
#      /var/lib/oci-cache/go-mod respectively -- the exact shared, ubuntu-
#      owned volume (ext4 on /dev/sdb) every other Go invocation on that
#      host now targets, SHARED WITH THE ARC POOL. Precedence, highest
#      first: CODEX_REVIEW_GOCACHE/GOMODCACHE (explicit per-call override,
#      unchanged from v4.8.2/v4.8.4) > the caller's own GOCACHE/GOMODCACHE
#      (new) > the shared-volume default (new).
#      RULING HISTORY, because it moved twice in six minutes the same day
#      and a stale copy of the first version is exactly the failure mode
#      this note exists to prevent: the 07:31 PDT ruling named
#      $HOME/.cache/go-build and $HOME/go/pkg/mod. The 07:37 PDT re-ruling
#      (chris, "Yes use it") retargeted the whole fleet at
#      /var/lib/oci-cache/{go-build,go-mod} instead and named the $HOME
#      paths LEGACY -- "leave in place, no lane writes to them". This
#      wrapper implements the 07:37 target. At the time this was written the
#      two were bind-mounted to the same inodes on bigboy (verified:
#      `stat -c '%d:%i'` on $HOME/.cache/go-build and
#      /var/lib/oci-cache/go-build both printed 2064:10485761; $HOME/go/pkg/
#      mod and /var/lib/oci-cache/go-mod both printed 2064:7340033) -- so
#      the two rulings were behaviourally identical when this shipped, but
#      the bind mount is not this wrapper's to depend on, and the LEGACY
#      label says it may not stay.
#   2. GOPATH defaults to $HOME/go (unchanged reasoning from v4.8.4) --
#      neither ruling above names a GOPATH target explicitly, and $HOME/go
#      itself (as opposed to $HOME/go/pkg/mod, which the 07:37 ruling DOES
#      name legacy) is not called out as legacy either. Same precedence
#      pattern: CODEX_REVIEW_GOPATH > the caller's own GOPATH > $HOME/go.
#   3. STOPS creating a fresh, timestamped, per-round directory for all
#      three -- `mkdir -p` only, against the resolved (shared, persistent)
#      path, never a new $LANE_KEY-$TS-suffixed one.
#   4. STOPS reaping/removing them in cleanup(): a shared, persistent
#      cache is not this run's to delete just because this run resolved
#      it. cleanup() on Linux therefore skips the RGOCACHE/RGOMODCACHE/
#      RGOPATH removal entirely (CODEX_KEEP_CACHE is meaningless there now
#      -- there is nothing per-round left to keep or discard). The
#      --reap-mine/--reap-stale subcommands are UNCHANGED: their glob
#      patterns (codex-review-*-LANE-*, codex-go-cache-*, etc.) only ever
#      matched the old per-round /tmp names, which this wrapper no longer
#      creates on Linux, so there is nothing new for them to reap and
#      nothing for them to accidentally catch in the shared paths either.
#
# The macOS (darwin) path is COMPLETELY UNCHANGED: it keeps routing
# GOCACHE/GOMODCACHE/GOPATH/GOTMPDIR/TMPDIR under literal /tmp, per-round,
# reaped by cleanup() exactly as v4.8.5 did -- that is what makes Go
# executable inside the sandbox on macOS at all (see the v4.3/v4.4 notes
# above); a $TMPDIR-rooted or $HOME-rooted cache fails there. GOSUMDB stays
# at its default (ON) on both hosts -- this is a cache-location change, never
# a checksum-verification change. Everything else in this file -- the
# 0555-safe rm_rf_writable() cleanup of review worktrees and RGOTMPDIR/
# RTMPDIR, the LANE_KEY basename+hash keying (still used for the review
# worktree and RGOTMPDIR/RTMPDIR names, which are not Go caches), the OUTDIR
# mkdir, the warm step and its go.mod gate, the WARM_MODULES `|| true` count
# pipeline, creating $L before anything else can die, and warm-step
# SKIPPED reason=no-go.mod for a repo with none -- is unchanged.
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
#
# codex-review.sh --reap-mine LANE | --reap-stale HOURS [--dry-run]
#   Standalone maintenance subcommands -- see v4.8.2 note above. Exit 0 on
#   completion (busy/skipped dirs are reported, not errors); non-zero only
#   on a usage error (missing argument).
#
# codex-review.sh --version
#   Prints "codex-review.sh vX.Y.Z" and exits 0.

set -euo pipefail

VERSION="4.8.6"

warn() { printf 'codex-review: %s\n' "$*" >&2; }
die()  { warn "$*"; exit 1; }

# ---------------------------------------------------------------------------
# v4.8.2 maintenance subcommands (--reap-mine / --reap-stale). Dispatched
# before the round-running getopts parse below, since these take a GNU-style
# long flag as $1 and never run a round.
# ---------------------------------------------------------------------------

# True (0) if anything under $1 has an open file handle; false (1) if the
# check ran clean and found nothing, or no checking tool exists at all --
# absence of lsof/fuser must not silently treat everything as busy forever,
# but IS reported once so a caller relying on the safety net notices.
#
# lsof's OWN EXIT STATUS is not that signal: measured on this host's lsof,
# `lsof +D busydir` prints the header plus every open file under it AND
# still exits 1 -- the same exit status as the genuinely-empty case, which
# prints nothing. Trusting the exit code here is the exact
# `pgrep -fc ... || echo 0` shape this file already warns about elsewhere: a
# reliably-wrong measurement that always takes the "not busy" branch. The
# real signal is OUTPUT: lsof prints nothing at all when it finds nothing,
# and at least its header line the moment it finds one open file.
reap_dir_busy() {
  local d="$1"
  if command -v lsof >/dev/null 2>&1; then
    [ -n "$(lsof +D "$d" 2>/dev/null)" ]
    return $?
  fi
  # fuser's exit status IS the documented, reliable signal (0 = accessed by
  # some process, non-zero = not) -- unlike lsof above, this one holds.
  if command -v fuser >/dev/null 2>&1; then
    fuser -s "$d" >/dev/null 2>&1
    return $?
  fi
  warn "reap: no lsof or fuser on this host -- cannot check $d for open files, treating as NOT busy"
  return 1
}

# The bases to scan. $TMPDIR and /tmp are frequently different paths (macOS:
# $TMPDIR is /var/folders/**/T, /tmp is a separate symlink target) and BOTH
# accumulate this wrapper's dirs depending on which mktemp calls used which
# base, so both are always scanned. Deduplicated by resolved physical path so
# a host where they coincide (most Linux hosts: $TMPDIR unset, defaults to
# /tmp) is not scanned twice.
reap_bases() {
  local b1="/tmp" b2="${TMPDIR:-}"
  printf '%s\n' "$b1"
  if [ -n "$b2" ] && [ -d "$b2" ]; then
    local r1 r2
    r1=$(cd "$b1" 2>/dev/null && pwd -P || printf '%s' "$b1")
    r2=$(cd "$b2" 2>/dev/null && pwd -P || printf '%s' "$b2")
    [ "$r1" != "$r2" ] && printf '%s\n' "$b2"
  fi
}

# Never let a reap pattern accidentally match the user's shared Go build
# cache, wherever it is configured -- belt-and-braces alongside the fact
# that every glob here is anchored to a codex-review/pysum/codex-go prefix,
# which the shared cache's own path (~/Library/Caches/go-build,
# ~/.cache/go-build, or whatever `go env GOCACHE` is set to) will not match.
reap_shared_gocache() {
  command -v go >/dev/null 2>&1 && go env GOCACHE 2>/dev/null || true
}

# Shared body for both subcommands: given a list of candidate dirs (one per
# line on stdin, age already filtered by the caller's find) and whether this
# is a dry run, remove the ones that pass, report the ones skipped as busy,
# and never touch $shared or anything failing the exact-path check.
reap_dirs() {
  local dry_run="$1" shared kept skipped_busy d
  shared=$(reap_shared_gocache)
  kept=0 skipped_busy=0
  while IFS= read -r d; do
    [ -n "$d" ] || continue
    [ -d "$d" ] || continue
    if [ -n "$shared" ]; then
      local d_real shared_real
      d_real=$(cd "$d" 2>/dev/null && pwd -P || printf '%s' "$d")
      shared_real=$(cd "$shared" 2>/dev/null && pwd -P || printf '%s' "$shared")
      if [ "$d_real" = "$shared_real" ]; then
        warn "reap: REFUSING to touch $d -- it is the shared \`go env GOCACHE\` path"
        continue
      fi
    fi
    if reap_dir_busy "$d"; then
      warn "reap: skipping $d (busy -- open file handle found)"
      skipped_busy=$((skipped_busy + 1))
      continue
    fi
    if [ "$dry_run" -eq 1 ]; then
      warn "reap: would remove $d"
    else
      # v4.8.4: chmod before rm -- see rm_rf_writable()'s comment below.
      # $d has already passed the exact-path shared-cache check above, so
      # this is always one of this wrapper's OWN prefixed dirs, never the
      # shared `go env GOCACHE`.
      chmod -R u+w "$d" 2>/dev/null || true
      rm -rf "$d" || warn "reap: could not fully remove $d even after chmod -R u+w"
      warn "reap: removed $d"
    fi
    kept=$((kept + 1))
  done
  warn "reap: $([ "$dry_run" -eq 1 ] && echo would-remove || echo removed)=$kept skipped-busy=$skipped_busy"
}

reap_mine() {
  local lane="$1" base
  [ -n "$lane" ] || die "--reap-mine requires a lane name"
  {
    for base in $(reap_bases); do
      # Trailing slash is REQUIRED, not cosmetic: /tmp is a symlink to
      # /private/tmp on macOS, and BSD find with -mindepth/-maxdepth given a
      # symlinked root WITHOUT a trailing slash silently descends into
      # NOTHING -- exit 0, zero output, no error. Measured on this host.
      find "$base/" -maxdepth 1 -mindepth 1 -type d -name "codex-review-*-$lane-*" 2>/dev/null
    done
  } | sort -u | reap_dirs 0
}

# Age filtering uses find's own -mmin (minutes since last modification),
# NOT a hand-rolled `stat`-based epoch comparison: BSD stat (macOS default)
# and GNU stat (Linux, and macOS when coreutils is on PATH ahead of
# /usr/bin -- measured on THIS host, where `stat -f %m` silently runs
# GNU stat's unrelated "-f" filesystem-info mode instead of erroring) use
# incompatible flags for the same value, and detecting which one you have
# is a second portability problem on top of the first. `-mmin` is the same
# flag with the same meaning in both find implementations.
reap_stale() {
  local hours="$1" dry_run="$2" base mins
  [ -n "$hours" ] || die "--reap-stale requires an hours argument"
  case "$hours" in ''|*[!0-9]*) die "--reap-stale hours must be a non-negative integer, got '$hours'" ;; esac
  mins=$((hours * 60))
  {
    for base in $(reap_bases); do
      # Trailing slash required -- see reap_mine's comment on the same trap.
      find "$base/" -maxdepth 1 -mindepth 1 -type d -mmin "+$mins" \
        \( -name 'codex-go-cache-*' -o -name 'codex-go-modcache-*' \
           -o -name 'pysum_gocache_*' -o -name 'codex-review-*-unattributed-*' \) \
        2>/dev/null
    done
  } | sort -u | reap_dirs "$dry_run"
}

case "${1:-}" in
  --version)
    printf 'codex-review.sh v%s\n' "$VERSION"
    exit 0
    ;;
  --reap-mine)
    [ $# -ge 2 ] || die "usage: codex-review.sh --reap-mine LANE"
    reap_mine "$2"
    exit 0
    ;;
  --reap-stale)
    [ $# -ge 2 ] || die "usage: codex-review.sh --reap-stale HOURS [--dry-run]"
    DRY=0
    [ "${3:-}" = "--dry-run" ] && DRY=1
    reap_stale "$2" "$DRY"
    exit 0
    ;;
esac

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
# v4.8.4: OUTDIR (and the log dir, which is the same directory -- see V/L
# below) was never created before use. A caller-supplied -o naming a
# not-yet-existing directory made the warm step's log redirect die with
# "No such file or directory" and no verdict at all. Create it now, abort
# loudly if it cannot be created.
mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"
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
# v4.8.5: create the round .log NOW, before anything else in this script can
# die (the warm step in particular -- see its changelog note above). Every
# earlier version left $L uncreated until the first thing wrote to it, so a
# death before that point (a killed pipeline under set -euo pipefail, a
# stray die() from a step that forgot to append) left NO .log at all --
# indistinguishable from the wrapper never having started. From here on,
# once the wrapper has gotten this far, its own round .log is guaranteed to
# exist no matter what happens next.
: >"$L" || die "cannot create round log $L"

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
# v4.8.2: this cache is no longer left warm by default -- see the changelog
# note near the top of the file. cleanup() below now removes it (by exact
# variable, never a glob) unless CODEX_KEEP_CACHE=1, which is the opt-in for
# a caller that wants the old warm-across-rounds behaviour back and will
# police its own cleanup (e.g. by pinning CODEX_REVIEW_GOCACHE itself).
# /tmp, NOT $TMPDIR. lane-4441 measured this: under the read-only sandbox,
# $TMPDIR on macOS is /var/folders/**/T, which is NOT writable, while /tmp IS.
# v4.3 pointed the reviewer's Go cache at the denied path, so `go test` failed
# with "cannot create entries" / "operation not permitted" and the wrapper
# printed its go-bounds line anyway, as though it had supplied a working
# environment. Rounds that did run Go succeeded by relocating to /tmp
# THEMSELVES -- they worked around the harness, not with it. A reviewer that
# did not think to relocate would have reported "cannot execute here", which is
# the pre-v4.3 state the execution work existed to remove.
#
# LANE is the stable key GOCACHE was already keyed on (this worktree's own
# basename, NOT $NAME -- see the historical comment this replaced: $NAME is
# the ROUND name and keying on it would give a cold cache every round).
# Falls back to "unattributed" only if that basename cannot be determined, so
# the dir stays reapable by --reap-stale even then.
LANE=$(basename "$WT" 2>/dev/null || true)
case "$LANE" in ''|'.'|'/') LANE=unattributed ;; esac
# v4.8.4: LANE (the worktree basename) alone is not a safe naming key --
# the bigboy lane recipe's own §10 worked example clones into a directory
# literally named `acr`, so every lane following that example got the
# SAME LANE value and collided on the same GOCACHE/GOMODCACHE path (found
# by lane-s7c-outcomes on bigboy). LANE_KEY appends an 8-hex-char hash of
# $WT's own resolved absolute path, which is unique per checkout no
# matter what its directory is called -- the clone basename stops
# mattering for uniqueness. LANE is kept as LANE_KEY's first
# dash-delimited segment (e.g. codex-review-gocache-acr-3f9a1c2b-<ts>) so
# --reap-mine LANE's existing glob (`codex-review-*-LANE-*`) still matches.
WT_REAL=$(cd "$WT" 2>/dev/null && pwd -P || printf '%s' "$WT")
if command -v shasum >/dev/null 2>&1; then
  WT_HASH=$(printf '%s' "$WT_REAL" | shasum -a 256 | cut -c1-8)
elif command -v sha256sum >/dev/null 2>&1; then
  WT_HASH=$(printf '%s' "$WT_REAL" | sha256sum | cut -c1-8)
else
  # Last resort, still disambiguates two concurrent processes even though
  # it says nothing about the path itself.
  WT_HASH=$$
fi
LANE_KEY="$LANE-$WT_HASH"

# HOST_OS resolved ONCE, here, and reused below (sandbox default, GOPATH
# default) rather than re-running `uname -s` at each site -- one source of
# truth for which branch of the v4.8.6 Linux/macOS split a given line is on.
HOST_OS="$(uname -s)"
# v4.8.6 P1 (found by codex round lane-wrapper-v486-20260904T080604, EXECUTED
# and independently reproduced): every `[ "$HOST_OS" = Linux ]` check in this
# file does an EXACT string match, and every site that checks it does so the
# SAME way -- but "the same wrong way" is still wrong. A malformed uname
# output (measured: a wrapped `uname` emitting a trailing `\r`, e.g. under an
# unusual shell/CI wrapper) fails the exact-match at EVERY site consistently,
# which sounds safe but is not: a caller that has set CODEX_REVIEW_GOCACHE/
# CODEX_REVIEW_GOMODCACHE to literal /var/lib/oci-cache paths (a SUPPORTED,
# documented override -- this file's own v4.8.2 comment describes pointing
# CODEX_REVIEW_GOCACHE at "a warm, already-writable" cache) on a host that
# genuinely IS Linux still gets misrouted into the macOS/`else` branch
# everywhere `$HOST_OS` is checked -- INCLUDING cleanup()'s removal branch,
# which then calls rm_rf_writable on the real shared bigboy cache. This is
# exactly the "go clean -cache on the shared cache" incident class this file
# already warns about elsewhere, reached through a detection bug rather than
# a cleanup bug. Fail closed HERE, immediately, rather than letting an
# unrecognised value silently pick a branch at every downstream site: only
# the two host kernels this file actually branches on are accepted.
case "$HOST_OS" in
  Linux | Darwin) ;;
  *) die "unrecognised or malformed 'uname -s' output '$HOST_OS' -- refusing to guess whether this host's Go caches are the fleet-shared bigboy volume (Linux) or a per-round /tmp cache (Darwin/other); an unexpected value here must never silently fall through to a cache-removal branch" ;;
esac

# v4.8.6 (chris's ruling, 2026-09-04, RE-RULED 07:37 PDT -- see the
# top-of-file changelog's "RULING HISTORY" note for the full story and why
# it names the fleet-shared /var/lib/oci-cache volume, not a $HOME path):
# on bigboy (Linux) every Go run -- gates, integration suites, launchers,
# codex clones -- uses the SHARED caches, never a per-lane/per-round one,
# and this wrapper's own GOCACHE/GOMODCACHE are no exception any more. On
# macOS the behaviour below this `if` is BYTE-FOR-BYTE what v4.8.2/v4.8.4
# already did: a fresh, timestamped, per-round dir under /tmp, reaped by
# cleanup() -- see the top-of-file changelog for why (macOS sandbox
# writability, proven per v4.3/v4.4).
if [ "$HOST_OS" = Linux ]; then
  # Precedence: CODEX_REVIEW_GOCACHE/GOMODCACHE (explicit per-call override,
  # unchanged since v4.8.2) > the caller's own GOCACHE/GOMODCACHE (new in
  # v4.8.6 -- a login shell that already exports these) > the shared-volume
  # default (new in v4.8.6). No $LANE_KEY/$TS suffix anywhere in this branch
  # -- that suffix is what made the old path per-round; a shared path has
  # none.
  RGOCACHE="${CODEX_REVIEW_GOCACHE:-${GOCACHE:-/var/lib/oci-cache/go-build}}"
  RGOMODCACHE="${CODEX_REVIEW_GOMODCACHE:-${GOMODCACHE:-/var/lib/oci-cache/go-mod}}"
else
  RGOCACHE="${CODEX_REVIEW_GOCACHE:-/tmp/codex-review-gocache-$LANE_KEY-$TS}"
  # GOMODCACHE, bounded for the same reason as GOCACHE: an unset GOMODCACHE
  # defaults to $HOME/go/pkg/mod, which read-only denies exactly like the
  # denied-$TMPDIR case above, and workspace-write should not be trusted to
  # widen access to the user's real mod cache just because it happens to be
  # writable there. New in v4.8.2 -- v4.8.1 and earlier left GOMODCACHE
  # unbounded.
  RGOMODCACHE="${CODEX_REVIEW_GOMODCACHE:-/tmp/codex-review-modcache-$LANE_KEY-$TS}"
fi
# `mkdir -p` either way: on macOS this CREATES the fresh per-round dir (as
# before); on Linux the shared path should already exist (chris's standing
# bigboy setup), but `-p` is a harmless no-op if it does and a one-time
# bootstrap if it somehow does not -- it is never "creating a per-round dir",
# because the path itself carries no per-round suffix on that branch.
mkdir -p "$RGOCACHE" || die "cannot create/find GOCACHE $RGOCACHE"
mkdir -p "$RGOMODCACHE" || die "cannot create/find GOMODCACHE $RGOMODCACHE"

# Resolve the bounds ONCE, into variables, so the warn line below reports
# exactly what is applied. The first version re-evaluated the defaults inside
# the warn string, which could drift from the values actually exported.
#
# A caller-supplied -p=<n> in GOFLAGS is STRIPPED before appending the
# wrapper's own bound, rather than left to sit alongside it. Go itself
# honours the LAST occurrence of a repeated flag, so `-p=4 -p=2` and `-p=2`
# behave identically today -- this is not a correctness fix. It exists
# because lane-structure-memory's round logged the literal duplicate
# (`GOFLAGS=-p=2 -p=2` when the caller had already set one), which reads as
# a bug in the wrapper's own bookkeeping even though the value was correct.
# Every other caller flag is kept, in its original order.
GOFLAGS_STRIPPED=""
if [ -n "${GOFLAGS:-}" ]; then
  for gf_tok in $GOFLAGS; do
    case "$gf_tok" in
      -p=*) ;;  # dropped -- the wrapper's own -p bound (below) replaces it
      *) GOFLAGS_STRIPPED="${GOFLAGS_STRIPPED:+$GOFLAGS_STRIPPED }$gf_tok" ;;
    esac
  done
fi
RGOFLAGS="${GOFLAGS_STRIPPED:+$GOFLAGS_STRIPPED }${CODEX_REVIEW_GOFLAGS:--p=2}"
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
case "$HOST_OS" in
  Linux) RSANDBOX="${CODEX_REVIEW_SANDBOX:-workspace-write}" ;;
  *)     RSANDBOX="${CODEX_REVIEW_SANDBOX:-read-only}" ;;
esac
case "$RSANDBOX" in
  read-only | workspace-write) ;;
  *) die "CODEX_REVIEW_SANDBOX must be read-only or workspace-write, got '$RSANDBOX'" ;;
esac

START_EPOCH=$(date +%s)   # bounds the session-transcript recovery search
# v4.8.2: renamed codex-rw-$NAME-* -> codex-review-worktree-$LANE-$TS-* (see
# changelog note near the top) so every wrapper-owned dir shares one
# reapable naming scheme. Keyed on LANE+TS rather than $NAME: two concurrent
# rounds against the same lane with the same explicit -n NAME must still get
# distinct, individually reapable dirs.
RW=$(mktemp -d "${TMPDIR:-/tmp}/codex-review-worktree-$LANE_KEY-$TS-XXXXXX")
# Go's work dir. Deliberately a SIBLING of the review worktree, not a directory
# inside it: anything inside $RW shows up as untracked and would be swept into
# the preserved residue, burying the reviewer's actual findings under build
# droppings. Removed by cleanup() alongside the worktree.
# /tmp for the same reason as RGOCACHE above.
RGOTMPDIR=$(mktemp -d "/tmp/codex-review-gotmp-$LANE_KEY-$TS-XXXXXX")

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
RTMPDIR=$(mktemp -d "/tmp/codex-review-gotmp-$LANE_KEY-$TS-shell-XXXXXX")

# v4.8.4 introduced a per-round GOPATH because bigboy's ~/go and ~/go/pkg
# used to be root:root 755, and an unset GOPATH defaulting to $HOME/go made
# `go mod download all` fail trying to create $GOPATH/pkg/sumdb/... -- an
# ENOENT that reads exactly like a network failure and is not one.
#
# v4.8.6 (chris's ruling): that per-lane workaround is retired on Linux.
# Neither the 07:31 nor the 07:37 ruling (see the top-of-file changelog's
# "RULING HISTORY" note) names a GOPATH target explicitly -- both are about
# GOCACHE/GOMODCACHE, which now live on the shared /var/lib/oci-cache volume
# (see above), not under $GOPATH at all. GOPATH itself is verified writable
# on bigboy today (measured: $HOME/go is ubuntu:ubuntu 755, not the
# root:root this comment used to warn about), so this wrapper keeps Go's own
# default rather than inventing a new one: CODEX_REVIEW_GOPATH (explicit
# override) > the caller's own GOPATH (new) > $HOME/go (Go's own default).
# No per-round dir, no $LANE_KEY/$TS suffix. If a future warm-step failure
# on Linux names an unwritable path under this, that is bigboy's setup to
# fix, not a per-round workaround for this script to reintroduce.
#
# macOS is UNCHANGED: still a fresh per-round dir under /tmp, reaped by
# cleanup() below, for the same sandbox-writability reason as RGOTMPDIR/
# RGOCACHE. GOSUMDB verification is left at its default (ON) on both hosts
# either way -- this has only ever been a permission/location fix, never a
# checksum-verification change.
if [ -n "${CODEX_REVIEW_GOPATH:-}" ]; then
  RGOPATH="$CODEX_REVIEW_GOPATH"
  mkdir -p "$RGOPATH" || die "cannot create GOPATH $RGOPATH"
elif [ "$HOST_OS" = Linux ]; then
  RGOPATH="${GOPATH:-$HOME/go}"
  mkdir -p "$RGOPATH" || die "cannot create/find GOPATH $RGOPATH"
else
  RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX") \
    || die "cannot create per-round GOPATH"
fi

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

# v4.8.4: chmod before rm. Go's own module cache extraction marks
# directories read-only (mode 0555) so a build cannot accidentally corrupt
# an extracted module; a plain `rm -rf` on such a tree fails file-by-file
# with "Permission denied", leaves the directory behind, and buries
# whatever the round's REAL failure was underneath that noise (found on
# bigboy by lane-s7c-outcomes running v4.8.3's RGOMODCACHE cleanup).
#
# Takes the dir DIRECTLY, never a glob -- every call site below passes one
# of this run's own exact variables (RGOTMPDIR/RTMPDIR/RGOCACHE/
# RGOMODCACHE/RGOPATH), so this can never reach a shared cache, `go env
# GOCACHE`, or $HOME/go/pkg/mod. Always returns 0 (a removal failure is
# WARNED, not silent, and not fatal) -- the same "never let this be the
# function's last statement under set -e" reasoning that shaped every
# if-block in preserve_residue()/cleanup() above: a non-zero return here
# would abort the REST of cleanup() (the trap handler) under `set -euo
# pipefail`, silently skipping whatever cleanup steps were still to come.
rm_rf_writable() {
  local d="$1"
  [ -n "$d" ] && [ -d "$d" ] || return 0
  chmod -R u+w "$d" 2>/dev/null || true
  rm -rf "$d" 2>/dev/null \
    || warn "cleanup: could not fully remove $d even after chmod -R u+w -- check for a still-read-only entry or an open file handle"
  return 0
}

cleanup() {
  preserve_residue
  rm_rf_writable "${RGOTMPDIR:-}"
  # RTMPDIR too, or every round leaves a /tmp/codex-review-gotmp-*-shell-*
  # behind.
  rm_rf_writable "${RTMPDIR:-}"
  # v4.8.6: on Linux, RGOPATH/RGOCACHE/RGOMODCACHE are now the SHARED,
  # PERSISTENT bigboy caches (see where they are resolved, above) -- not
  # this run's own scratch dirs any more. Removing them would be exactly
  # the "go clean -cache on the shared cache" incident class this file
  # already warns about elsewhere (lane-4818, 09-02), just via rm -rf
  # instead of `go clean`. cleanup() on Linux therefore does not touch any
  # of the three, ever, regardless of CODEX_KEEP_CACHE -- there is no
  # per-round remnant left to keep or discard. On macOS this block is
  # UNCHANGED from v4.8.4: all three are this run's own per-round dirs
  # under /tmp, removed here unless CODEX_KEEP_CACHE=1.
  if [ "$HOST_OS" = Linux ]; then
    warn "shared bigboy caches left in place (not per-round any more): GOPATH=$RGOPATH GOCACHE=$RGOCACHE GOMODCACHE=$RGOMODCACHE"
  else
    # RGOPATH (v4.8.4): the per-round GOPATH scratch dir -- see where it is
    # created, above. Same rules: this run's own dir, never $HOME/go.
    rm_rf_writable "${RGOPATH:-}"
    # v4.8.2: GOCACHE/GOMODCACHE, by exact variable and never a glob (see the
    # top-of-file changelog note). Straight rm -rf, not `go clean -cache`
    # scoped to the dir first: for a large cache `go clean -cache` walks and
    # re-verifies every entry, which is not cheap, while rm -rf on a path this
    # script itself created and owns exclusively is. CODEX_KEEP_CACHE=1 is the
    # explicit opt-out for a caller that wants the old warm-across-rounds
    # cache back (see RGOCACHE above) and will police its own cleanup.
    if [ "${CODEX_KEEP_CACHE:-0}" != 1 ]; then
      rm_rf_writable "${RGOCACHE:-}"
      rm_rf_writable "${RGOMODCACHE:-}"
    else
      warn "CODEX_KEEP_CACHE=1 -- keeping $RGOCACHE and $RGOMODCACHE"
    fi
  fi
  if [ "$KEEP" -eq 1 ]; then warn "keeping review worktree $RW (-k)"; return; fi
  git -C "$WT" worktree remove --force "$RW" 2>/dev/null \
    || warn "review worktree $RW not removed — remove it manually and check 'git worktree list'"
}
trap cleanup EXIT

git -C "$WT" worktree add --detach "$RW" "$TIP" >/dev/null || die "worktree add failed"

# ---------------------------------------------------------------------------
# v4.8.3 WARM STEP -- runs OUTSIDE the codex sandbox, in this wrapper's own
# shell, against the round's own RGOMODCACHE/RGOCACHE (created above), BEFORE
# codex starts. See the v4.8.3 changelog note near the top of this file: the
# sandbox blocks proxy.golang.org, so a round starting from a cold per-round
# module cache cannot download anything once inside it and silently falls
# back to a gofmt/diff-only verdict. This is the only point in the round
# where that download can happen at all.
#
# Bounded by the SAME RGOFLAGS/-p and GOMAXPROCS the reviewer's own sandboxed
# build is bounded by -- this is host-side work on the shared machine, not
# exempt from the fleet load rule just because it runs before the sandbox
# does. Build output goes to a scratch dir under RGOTMPDIR via `-o` so the
# warm build's binaries do not land in $RW, where preserve_residue would
# either warn about them (>10MB) or copy them as if they were reviewer
# output.
#
# A failure here ABORTS THE ROUND before codex ever starts: `set -euo
# pipefail` is already active in this script, so `die` below both prints on
# stderr and appends the warm build's own output to the round .log, then
# exits non-zero -- a cold sandbox can never silently degrade to a
# source-trace-only verdict.
#
# The LAST step of the warm build is an OFFLINE RESOLVE PROOF: `GOPROXY=off
# go test -count=1 -run '^$' ./...` compiles every test binary (matching no
# test name, so nothing actually RUNS) with the network proxy disabled --
# if any module the test build needs is not already sitting in
# RGOMODCACHE, this fails instead of reaching out to proxy.golang.org. That
# is exactly the property that matters: it proves, outside the sandbox,
# that the sandbox's `go test` will not need the network it does not have.
# Mirrors the manual proof team-lead relayed from a live round
# (lane-structure-memory r2: `go mod download` + `go build ./...` into the
# round caches, then one `go test -count=1` on a single test to prove the
# graph resolves offline).
# v4.8.5: this whole step needs Go tooling to warm anything, and only a repo
# with a go.mod at the reviewed tip has any. Gate on the ACTUAL TIP CONTENT
# ($RW/go.mod, populated by the `git worktree add` above), never the repo
# name or a flag -- a repo with no go.mod (dev-health-web, acr-frontend)
# skips the entire step and exports no Go env to codex; a repo WITH go.mod
# is unaffected and keeps the full warm step, including its loud abort.
if [ -f "$RW/go.mod" ]; then
  WARM_LOG="$L.warm"
  WARM_OUT="$RGOTMPDIR/warmbuild"
  WARM_START=$(date +%s)
  warn "warm: go mod download + build/vet (+ -tags=integration) + offline resolve proof in $RW against $RGOMODCACHE, outside the sandbox ..."
  WARM_RC=0
  ( cd "$RW" && env \
      GOFLAGS="$RGOFLAGS" \
      GOMAXPROCS="$RGOMAXPROCS" \
      GOCACHE="$RGOCACHE" \
      GOMODCACHE="$RGOMODCACHE" \
      GOTMPDIR="$RGOTMPDIR" \
      GOPATH="$RGOPATH" \
      TMPDIR="$RTMPDIR" \
      bash -c '
        set -euo pipefail
        mkdir -p "$1"
        go mod download all
        go build -o "$1/" ./...
        go vet ./...
        go build -tags=integration -o "$1/" ./...
        go vet -tags=integration ./...
        GOPROXY=off go test -count=1 -run "^\$" ./...
      ' _ "$WARM_OUT" ) >"$WARM_LOG" 2>&1 || WARM_RC=$?
  WARM_END=$(date +%s)
  WARM_DURATION=$((WARM_END - WARM_START))
  # Module count: zip files under the round's own GOMODCACHE download dir.
  # 2>/dev/null so a cache that never got far enough to create this path (e.g.
  # `go mod download` itself failed) reports 0 rather than erroring the count.
  # v4.8.5: `|| true` on the WHOLE assignment, not just the find -- under
  # `set -euo pipefail`, `find` exiting non-zero (path does not exist yet,
  # e.g. `go mod download` failed before creating it) survives the `| wc -l |
  # tr -d ' '` pipe as the pipeline's own exit status and, unguarded, killed
  # the script right here, before the `if [ "$WARM_RC" -ne 0 ]` block below
  # ever got to run its own controlled, message-printing `die` (found by
  # GWC's lane-web-681: a no-go.mod repo hit exactly this, silently, with no
  # .log at all). The captured value is unaffected -- `wc -l` on find's empty
  # output is still "0" -- only the fatal exit status is suppressed. Same
  # class as the `pgrep -fc ... 2>/dev/null || echo 0` idiom this file
  # already discusses in the v4.2/v4.8.1 notes above.
  WARM_MODULES=$(find "$RGOMODCACHE/cache/download" -name '*.zip' 2>/dev/null | wc -l | tr -d ' ') || true
  # Cache sizes (team-lead's ask, to size-compare against a manually warmed
  # round like lane-structure-memory's 432M mod / 587M build). `du -sh` on a
  # dir this script itself created and already mkdir'd, so it always exists;
  # `|| true` + a fallback field covers the (unexpected) case du itself is
  # missing or errors, rather than aborting the round over a reporting line.
  WARM_MODCACHE_SIZE=$(du -sh "$RGOMODCACHE" 2>/dev/null | cut -f1)
  WARM_MODCACHE_SIZE=${WARM_MODCACHE_SIZE:-unknown}
  WARM_GOCACHE_SIZE=$(du -sh "$RGOCACHE" 2>/dev/null | cut -f1)
  WARM_GOCACHE_SIZE=${WARM_GOCACHE_SIZE:-unknown}
  # v4.8.4: a warm-step failure whose raw `go` error names an unwritable
  # $GOPATH/pkg/sumdb path (bigboy: ~/go and ~/go/pkg are root:root 755)
  # reads exactly like a network failure -- "open .../pkg/sumdb/
  # sum.golang.org/latest: no such file or directory" -- and is not one.
  # GOPATH is already pointed at RGOPATH above, so this should not fire on
  # a correctly-configured round; if it still does (e.g. a caller-pinned
  # CODEX_REVIEW_GOPATH pointing somewhere unwritable), name the exact path
  # and say plainly that this is a permission problem, not a network one.
  if [ "$WARM_RC" -ne 0 ]; then
    SUMDB_HIT=$(grep -oE '[^ ]*pkg/sumdb/[^ ]*' "$WARM_LOG" 2>/dev/null | head -1 || true)
    if [ -n "$SUMDB_HIT" ]; then
      SUMDB_PARENT=$(dirname "$SUMDB_HIT")
      while [ -n "$SUMDB_PARENT" ] && [ "$SUMDB_PARENT" != "/" ] && [ ! -e "$SUMDB_PARENT" ]; do
        SUMDB_PARENT=$(dirname "$SUMDB_PARENT")
      done
      if [ -n "$SUMDB_PARENT" ] && [ ! -w "$SUMDB_PARENT" ]; then
        warn "WARM STEP hit $SUMDB_HIT, whose nearest existing parent ($SUMDB_PARENT) is not writable by this user -- this is a PERMISSION problem, not a network problem. GOPATH for this round was $RGOPATH; if the failing path above is NOT under that, GOPATH did not take effect for this command -- check CODEX_REVIEW_GOPATH."
      fi
    fi
    {
      printf 'warm-step: FAILED rc=%s duration=%ss modules=%s modcache=%s gocache=%s\n' \
        "$WARM_RC" "$WARM_DURATION" "$WARM_MODULES" "$WARM_MODCACHE_SIZE" "$WARM_GOCACHE_SIZE"
      printf -- '--- warm step output (%s) ---\n' "$WARM_LOG"
      cat "$WARM_LOG" 2>/dev/null || true
    } >>"$L" 2>/dev/null || true
    warn "WARM STEP FAILED after ${WARM_DURATION}s (rc=$WARM_RC, $WARM_MODULES module(s) cached in $RGOMODCACHE) -- a cold sandbox must never silently degrade to a gofmt-only verdict. See $L and $WARM_LOG. ABORTING before codex starts."
    die "warm step failed for $TIP -- round aborted, no codex round was started"
  fi
  printf 'warm-step: OK duration=%ss modules=%s modcache=%s gocache=%s resolve=ok\n' \
    "$WARM_DURATION" "$WARM_MODULES" "$WARM_MODCACHE_SIZE" "$WARM_GOCACHE_SIZE" >>"$L"
  warn "warm: OK duration=${WARM_DURATION}s modules=$WARM_MODULES modcache=$WARM_MODCACHE_SIZE gocache=$WARM_GOCACHE_SIZE cached in $RGOMODCACHE"
else
  printf 'warm-step: SKIPPED reason=no-go.mod\n' >>"$L"
  warn "warm: SKIPPED -- no go.mod at $TIP, nothing to warm, no Go env exported -- proceeding straight to codex"
fi

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

If `go test` is unavailable to you in this sandbox for any reason (module
download blocked, a denied cache path, anything else), say so explicitly and
in those words -- "go test unavailable" -- and label every remaining claim
in your verdict EXECUTED or ARGUED, so a reader can tell a run result from a
source-trace inference at a glance.
STANDING_RULES
# Second heredoc, UNQUOTED delimiter on purpose: this one interpolates the
# round's actual RGOMODCACHE/HOME paths at generation time, so the reviewer
# gets literal, copy-pasteable paths rather than shell variables it would
# have to resolve itself inside the sandbox. Kept separate from the
# single-quoted STANDING_RULES block above so that block's own `$`/backtick
# markdown-code-span characters never need escaping.
cat >> "$RW/prompt.md" <<PROMPT_MODCACHE_INFO

This round's own module cache -- already warmed and offline-resolve-proven
(via \`GOPROXY=off go test -count=1 -run '^\$' ./...\`) before you started --
is at $RGOMODCACHE. If \`go test\` still fails on a module lookup once you
are inside the sandbox, retry ONCE with GOMODCACHE=$HOME/go/pkg/mod
GOPROXY=off (read-only intent -- never write there) before falling back to
a source-trace verdict, and say explicitly which GOMODCACHE you ended up
using.
PROMPT_MODCACHE_INFO
for aux in .codex-review-context.md LEDGER.md; do
  [ -f "$WT/$aux" ] && cp "$WT/$aux" "$RW/$aux"
done

warn "round $NAME-$TS: model=$MODEL effort=$EFF tip=$TIP review-worktree=$RW"
warn "go bounds: GOFLAGS=$RGOFLAGS GOMAXPROCS=$RGOMAXPROCS GOCACHE=$RGOCACHE GOMODCACHE=$RGOMODCACHE GOTMPDIR=$RGOTMPDIR GOPATH=$RGOPATH TMPDIR=$RTMPDIR sandbox=$RSANDBOX"
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
# must survive. A caller-supplied -p=<n> is the one exception -- stripped
# above before this wrapper's own -p bound is appended, so RGOFLAGS never
# carries two -p= tokens (v4.8.2; see the comment where RGOFLAGS is built).
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
  # RGOPATH added v4.8.4, same reasoning as its neighbours: redundant under
  # the default /tmp location, load-bearing the moment CODEX_REVIEW_GOPATH
  # points somewhere else.
  SANDBOX_ARGS+=(-c "sandbox_workspace_write.writable_roots=[\"$RGOCACHE\",\"$RGOMODCACHE\",\"$RGOTMPDIR\",\"$RGOPATH\"]")
fi
# ROUND PROVENANCE. Written BEFORE codex runs.
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
# APPEND, not truncate (v4.8.3): the warm-step line above is now the actual
# first line of $L. This used to be `> "$L"` when provenance really was the
# first write to the log; changing it back to `>` here would silently erase
# the warm-step result every round.
printf 'round-provenance: %s\n' "$PROV" >> "$L"
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
printf 'round-bounds: GOFLAGS=%s GOMAXPROCS=%s GOCACHE=%s GOMODCACHE=%s GOTMPDIR=%s GOPATH=%s TMPDIR=%s sandbox=%s\n' \
  "$RGOFLAGS" "$RGOMAXPROCS" "$RGOCACHE" "$RGOMODCACHE" "$RGOTMPDIR" "$RGOPATH" "$RTMPDIR" "$RSANDBOX" >> "$L"
warn "round-provenance: $PROV"

# NOTE THE APPEND. This redirect was `> "$L"`; it MUST stay `>>` now, or codex
# truncates the provenance line written immediately above and the log silently
# reverts to having no provenance at all -- which reads as LOCAL, the safe
# default, so nothing would ever look broken.
( cd "$RW" && env \
    GOFLAGS="$RGOFLAGS" \
    GOMAXPROCS="$RGOMAXPROCS" \
    GOCACHE="$RGOCACHE" \
    GOMODCACHE="$RGOMODCACHE" \
    GOTMPDIR="$RGOTMPDIR" \
    GOPATH="$RGOPATH" \
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
