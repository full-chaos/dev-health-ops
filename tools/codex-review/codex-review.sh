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
# macOS. So the sandbox mode picked by default was, AT THE TIME, host-dependent:
# Linux defaulted to workspace-write (the only mode under which Go could run
# there at all); macOS kept read-only (still proven sufficient, see v4.3/v4.4
# above). The GOCACHE/GOTMPDIR/TMPDIR paths themselves are UNCHANGED by any of
# this -- they already sit under /tmp, which workspace-write also permits
# without needing to move them inside the worktree.
#
# SUPERSEDED v4.8.7 (chris's ruling, 09-04, round-3 confirmation-pass P3 --
# this whole paragraph was left describing the OLD default after the code
# changed, exactly the stale-doc trap this file warns against elsewhere):
# read-only is now the default on BOTH platforms, unconditionally -- a review
# round is meant to read code, not execute it, so Linux no longer needs
# workspace-write as its default. `CODEX_REVIEW_SANDBOX`, set explicitly by a
# caller, still overrides the default on either host, same as always.
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
# comment below warns about). It stays at its stable path.
#
# CORRECTED, uncounted confirmation pass on codex-review-wrapper-v487 (P3,
# source-checked): this paragraph used to claim, unconditionally, that the
# cache "is granted write access explicitly via
# `sandbox_workspace_write.writable_roots`" and "is genuinely populated
# through the sandbox" -- both false as stated. The warm step that actually
# populates this cache ALWAYS runs entirely outside codex's sandbox (the
# wrapper's own `env`+`bash -c` subshell, before `codex exec` ever starts --
# see the warm-step block further below); nothing about it is
# sandbox-mediated, in any sandbox mode. The `writable_roots` grant referenced
# here is a real mechanism, but a SEPARATE one: it exists only so the
# REVIEWER's own exec blocks (if any) can write into this same cache path
# during the round itself, and it is added ONLY when `RSANDBOX` is
# `workspace-write` -- which is no longer the default (read-only is, since
# v4.8.7). By default, no such grant is added at all.
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
#   -m MODEL  model (default: $CODEX_REVIEW_MODEL, else gpt-5.6-luna --
#             chris's ruling 09-04 15:32 PDT: reverted from gpt-5.6-terra,
#             unclear terra was giving more review accuracy)
#   -e EFF    reasoning effort (default: $CODEX_REVIEW_EFFORT, else xhigh)
#   -p FILE   prompt file (default: prompt.md in the lane worktree)
#   -t SHA    tip to review (default: HEAD of the lane worktree)
#   -b REF    base for the review patch (default: $CODEX_REVIEW_BASE, else origin/main);
#             the patch is `git diff --stat BASE...TIP` + `git diff BASE...TIP` (v4.8.22)
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

# v4.8.13 (team-lead spec, 09-06, two CF findings from a live v4.8.12 round):
#   1. UV_CACHE_DIR was never bounded or granted a writable root at all, so a
#      Python-touching round's `uv sync`/`uv run` failed inside workspace-write
#      the same way an unbounded GOCACHE used to (v4.3/v4.4 history above) --
#      RUVCACHE now follows the exact same shared-persistent-path-on-Linux,
#      per-round-under-/tmp-on-macOS pattern as RGOCACHE/RGOMODCACHE, granted
#      in the same writable_roots list, exported into the same D1 exec block.
#   2. "codex exited 0 but wrote no verdict file" turned out to be a FALSE NO
#      VERDICT on a live round: the reviewer had written a complete verdict,
#      just not at the `-o` path -- traced to `-o "$V"` being handed to codex
#      AFTER `cd "$RW"`, so a caller-relative `-o` (e.g. `-o .`) resolved
#      against $RW, not the caller's own cwd. Fixed at the root (OUTDIR is
#      now resolved to an absolute, physical path immediately after it is
#      created, so $V/$L are absolute from that point on and a `cd` anywhere
#      downstream cannot reinterpret them) AND with a fallback: before
#      declaring NO VERDICT, search the still-alive review worktree for the
#      expected verdict filename and, failing that, the newest post-launch
#      `.md` file, and copy it to the expected path rather than losing a real
#      verdict to a path mismatch.
#   3. (from #2306 r1-v4) the exec-block counter that decides VOID IN FORM
#      only matched Go verbs (`go test/run/build`), so a Python-only round
#      that genuinely executed `pytest` (52/52), `uv run`, ruff and mypy was
#      falsely declared void. The counter now also matches pytest/`uv
#      run`/`.venv/bin/pytest`/ruff/mypy exec blocks, and VOID IN FORM fires
#      only when BOTH families are zero. `\bpytest\b`/`\bruff\b`/`\bmypy\b`
#      are WORD-BOUNDED (CF nit: unbounded, "truffle" would have counted as
#      a `ruff` execution, widening the guard in the unsafe direction); `uv
#      run` and `.venv/bin/pytest` stay unanchored substrings, matching the
#      Go pattern's own tolerance, since neither is a single bare word a
#      real identifier could contain as a false-positive substring.
#   4. (from the SAME #2312 r1) VERDICT_RE falsely flagged "Verdict: request
#      changes" as SUSPECT -- "request changes" is a real, complete
#      NOT-CLEAN verdict, just not in the clean/not-clean/sound/block
#      vocabulary. Added as its own alternative in the same regex.
#   5. (CF required change on this same read) the verdict-fallback lookup's
#      *.md fallback used `find | head -1` as "the newest" file -- find's
#      output order is unspecified, so this could pick up any matching file,
#      not the newest one, including a repo doc the reviewer merely edited
#      while testing something unrelated. Now sorts by mtime (`-printf
#      '%T@ %p' | sort -rn`) for a real newest-first order, AND validates
#      every candidate's last non-blank line against VERDICT_RE (hoisted
#      earlier in the script for this purpose) before ever copying it --
#      a candidate that fails validation is never promoted to $V, only
#      logged as a rejected candidate.
# D1 exec block (the main `codex exec` invocation) is otherwise
# byte-for-byte identical to v4.8.12: same model/effort defaults, same
# GOFLAGS/GOMAXPROCS/GOCACHE/GOMODCACHE/GOTMPDIR/GOPATH/TMPDIR bounds, same
# codegraph timeout shim -- UV_CACHE_DIR is an ADDITION to that block, not a
# replacement of anything in it.
# v4.8.14 (chris's ruling, 2026-09-06, verbatim: "we already proved it
# wasn't useful to reviewers so it shouldn't be in the harness to index
# it."): codegraph removed from the review harness entirely -- the shared
# base-checkout mechanism (base-advance fetch/merge, `codegraph sync`, the
# nested round-worktree trick), the codegraph timeout shim, and the prompt
# text inviting a reviewer to run `codegraph explore` are all gone. Every
# round now takes the plain per-$WT worktree-add path unconditionally, and
# the round log prints an explicit `codegraph: disabled (chris 2026-09-06)`
# marker where the old provenance fields used to be. Everything else in
# this file -- the v4.8.13 UV_CACHE_DIR bound, verdict-fallback lookup, exec
# counters, and "request changes" verdict phrasing -- is unchanged.
# v4.8.15 (chris's ruling, 2026-09-06, verbatim: "it shouldn't be in the
# harness to index it" -- absolute, covers a reviewer invoking codegraph on
# its own initiative, not just the wrapper's own now-removed indexing
# mechanism): adds a PATH-deny stub (a `codegraph` shadow binary that refuses
# with a one-line message and exit 2) prepended to PATH in the same env
# invocation that reaches codex exec, plus a pre-exec `rm -rf "$RW/.codegraph"`
# so a stale index from an earlier round can't linger either. Everything else
# is byte-identical to v4.8.14 (the base-checkout removal, the STANDING_RULES
# rg-only wording, the UV_CACHE_DIR bound, verdict-fallback lookup, exec
# counters, "request changes" phrasing).
# v4.8.22 (CHAOS-6948): a round must be able to SEE the diff it certifies. Twice on
# 2026-09-26 (#3277 r3, #3299 r1) the reviewer could not run `git diff`: the review
# worktree's `.git` pointer targets a path the sandbox denies, so it reviewed by
# reading files and said so honestly. Both rounds stood on executed tests, but a round
# that cannot see the patch cannot certify the diff. The wrapper now (a) writes
# `git diff --stat BASE...TIP` and `git diff BASE...TIP` to `.codex-review.patch` in
# the review worktree BEFORE the sandbox starts (BASE from -b / CODEX_REVIEW_BASE,
# default origin/main), (b) names that file in the prompt, and (c) marks the round
# VOID IN FORM, in the wrapper log and inside the verdict file, when the log shows
# neither a SUCCEEDED `git diff` exec block nor a SUCCEEDED read of that file (the
# EXEC-CLASSIFY awk gained a fourth number). Everything else is byte-identical to
# v4.8.21. tools/codex-review/test-exec-classify.sh pins the new rule.
VERSION="4.8.22"

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

WT="$PWD" NAME="" MODEL="${CODEX_REVIEW_MODEL:-gpt-5.6-luna}" EFF="${CODEX_REVIEW_EFFORT:-xhigh}"
PROMPT="" TIP="" OUTDIR="" KEEP=0 ALLOW_UNPUSHED=0 BASE="${CODEX_REVIEW_BASE:-origin/main}"
while getopts 'w:n:m:e:p:t:o:b:kU' f; do
  case "$f" in
    w) WT=$OPTARG ;; n) NAME=$OPTARG ;; m) MODEL=$OPTARG ;; e) EFF=$OPTARG ;;
    p) PROMPT=$OPTARG ;; t) TIP=$OPTARG ;; o) OUTDIR=$OPTARG ;; b) BASE=$OPTARG ;;
    k) KEEP=1 ;; U) ALLOW_UNPUSHED=1 ;;
    *) die "unknown flag" ;;
  esac
done

WT=$(cd "$WT" && pwd) || die "worktree $WT not found"
git -C "$WT" rev-parse --git-dir >/dev/null 2>&1 || die "$WT is not a git worktree"
NAME=${NAME:-$(basename "$WT")}
# v4.8.6 (generalized 09-04, chris/team-lead ruling after the LANE-scratch
# confirmation-pass finding): NAME becomes a path/filename component at
# MULTIPLE sites below -- V/L (this round's own verdict+log filenames, a
# few lines down), RESIDUE_DIR (preserve_residue(), later in the file), and
# on Linux, LANE_SCRATCH_ROOT. A caller-supplied `-n` value is used
# VERBATIM and untrusted; the DEFAULT (`basename "$WT"`) is safe on its own
# merits (WT was just canonicalized to an absolute path above, and can
# never literally basename to `.`/`..`), but a `-n` value never goes
# through basename at all in the general (non-Linux-lane-scratch) path --
# `-n '../../../../tmp/evil'` reached V/L and RESIDUE_DIR completely
# unsanitized before this fix, letting a round's own "verdict" file land
# anywhere the process can write, escaping OUTDIR entirely.
#
# Fixed with ONE mechanism, validated ONCE, here -- not a sanitize-then-
# reject helper duplicated at every site NAME reaches (that duplication is
# exactly how the earlier Linux-only `basename --`-then-case-check fix
# still missed this: it protected the ONE site its own author was looking
# at while leaving V/L/RESIDUE_DIR, which never called that helper at all,
# wide open). A positive ALLOWLIST, not a blocklist: NAME must be
# non-empty, start with an alnum, and contain only alnum/`.`/`_`/`-`
# afterward -- which structurally also excludes `.`, `..`, and anything
# containing `/` (a leading `.` already fails the first-character check),
# so there is no separate "and also reject . and .." clause to keep in
# sync with the regex by hand. `LC_ALL=C` pins the character-class
# semantics so this can never accept something unexpected under a non-C
# locale. Refuse outright on anything else -- no silent rewriting, no
# best-effort basename-and-hope.
NAME_ALLOWLIST_RE='^[A-Za-z0-9][A-Za-z0-9._-]*$'
# v4.8.6, found by confirmation-pass round #3 on bigboy, EXECUTED, independently
# reproduced by the lane: `printf '%s' "$NAME" | grep -Eq "$RE"` is LINE-oriented
# -- grep -q succeeds if ANY line of its input matches, and `^`/`$` in the ERE
# anchor to LINE boundaries, not the whole string's boundaries. A NAME
# containing an embedded newline with a SAFE first line and a malicious
# second line (e.g. NAME=$'lane\n../../../escaped/owned') therefore passed
# this gate -- the first line alone satisfied the regex -- and the full
# multi-line value then reached `mkdir -p` as one argument whose later
# `/../` components are still real path separators to mkdir, escaping the
# mandated root. Fixed: bash's OWN `[[ =~ ]]` matches the ENTIRE string in
# one pass, no per-line splitting, so the same regex now actually enforces
# what it was written to enforce. This is the one place in this script
# where `[[` is used instead of the POSIX `[`/`case` idiom everywhere
# else -- deliberately: `[[ =~ ]]` is bash-builtin regex matching over the
# whole argument, and no POSIX-`[`-compatible construct offers that same
# guarantee without re-introducing this exact line-splitting hazard via an
# external tool. Run inside a subshell so `LC_ALL=C` (character-class
# determinism, same reasoning as the old grep invocation) is scoped to
# just this check, never leaking into the rest of the script's locale.
if ! (LC_ALL=C; [[ "$NAME" =~ $NAME_ALLOWLIST_RE ]]); then
  die "-n/round name '$NAME' is not a safe path/filename component -- must be non-empty, start with a letter or digit, and contain only letters, digits, '.', '_', '-' afterward, with NO embedded newline (this also rejects '.', '..', and anything containing '/' or a line break). Refusing to guess a substitute."
fi
# v4.8.7 (bigboy codex-auth incident, same day; PLACEMENT and MECHANISM both
# fixed by confirmation-pass round #1 on this branch -- P1/P2, both EXECUTED):
# the first version of this guard lived right before the `codex exec`
# launch (search RC=0 below) -- by then the review worktree was already
# created (git worktree add) and the warm step had already run, so it did
# NOT prevent the wasted work it was written to prevent; it only produced a
# clearer error message after the fact. Moved here, right after NAME
# resolution, before ANY mktemp/worktree/warm work below. The first
# version also equated "usable Codex credentials" with a non-empty
# $CODEX_HOME/auth.json -- codex also supports keyring-backed credential
# storage (cli_auth_credentials_store="keyring" in its own config, no
# auth.json involved at all), which the file check would have wrongly
# rejected, and a non-empty-but-malformed auth.json would have PASSED the
# check only to fail later inside codex anyway. Fixed: ask codex itself,
# via its own `login status` subcommand (measured: ~40ms, no network
# round-trip observed, purely local credential-store inspection) --
# whatever storage mechanism is actually configured, this is the same
# check `codex exec` itself would effectively make. Output is discarded
# (never parsed -- only the exit code matters); CODEX_HOME_EFFECTIVE is
# still resolved, for the die message only, so a caller gets an actionable
# hint about WHERE codex looked without this script re-deriving codex's
# own auth-validity logic.
CODEX_HOME_EFFECTIVE="${CODEX_HOME:-$HOME/.codex}"
# v4.8.18 (post-incident 23:41Z): this used to discard stderr entirely, so a
# config-load failure (e.g. a `[permissions.*]` table with no
# `default_permissions` in the file -- see the PERMISSIONS-PROFILE ARGS block
# further below) surfaced as a misleading "not logged in", costing 25 minutes
# to diagnose across three lanes. Capture and print codex's own stderr
# verbatim, and label the failure by what it actually says, not by assumption.
LOGIN_STATUS_OUT=$(codex login status 2>&1) || {
  # v4.8.17/team-lead's incident used the exact string "config defines
  # [permissions] profiles but does not set default_permissions" -- no
  # "error"/"Error" substring in it at all, so a pattern requiring one (an
  # earlier draft of this fix) would have MISSED it and kept mislabelling it
  # as "not logged in". Flipped the default instead: classify as auth-related
  # only when the message actually looks auth-shaped; anything else is
  # reported as a config-class failure, verbatim, unclassified rather than
  # guessed.
  case "$LOGIN_STATUS_OUT" in
    *[Nn]ot\ logged\ in*|*[Ll]og\ in*|*401*|*[Uu]nauthorized*|*auth.json*)
      die "codex reports not logged in (checked via 'codex login status', CODEX_HOME resolves to $CODEX_HOME_EFFECTIVE) -- refusing to launch a round that would fail mid-way with an HTTP 401 instead of failing loudly now. codex said verbatim: $LOGIN_STATUS_OUT. If this is bigboy, run under 'bash -lc' (so ~/.profile sets CODEX_HOME) or export CODEX_HOME=/home/ubuntu/agents/codex explicitly before retrying." ;;
    *)
      die "codex login-status check failed at CODEX_HOME=$CODEX_HOME_EFFECTIVE in a way that does NOT read as an auth problem -- do not chase login/auth.json for this one, check the config instead. codex said verbatim: $LOGIN_STATUS_OUT" ;;
  esac
}
PROMPT=${PROMPT:-$WT/prompt.md}
OUTDIR=${OUTDIR:-$WT}
# v4.8.4: OUTDIR (and the log dir, which is the same directory -- see V/L
# below) was never created before use. A caller-supplied -o naming a
# not-yet-existing directory made the warm step's log redirect die with
# "No such file or directory" and no verdict at all. Create it now, abort
# loudly if it cannot be created.
mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"
# v4.8.13 (CF finding, root cause of a live "false NO VERDICT"): a caller
# passing a RELATIVE -o (e.g. `-o .`, seen in the wild) built $V/$L below as
# relative paths. The main round subshell does `cd "$RW"` before invoking
# codex with `-o "$V"` -- so a relative $V silently resolved against the
# REVIEW WORKTREE, not the caller's cwd, and the verdict landed inside $RW
# instead of at the intended output directory. Resolving OUTDIR to its
# absolute, physical path HERE, once, immediately after it is guaranteed to
# exist, makes every path built from it ($V, $L, the residue dir) immune to
# any `cd` anywhere downstream.
OUTDIR=$(cd "$OUTDIR" && pwd -P) || die "cannot resolve the physical path of output directory $OUTDIR"
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
#
# `builtin command -p uname`, not a bare `uname` or a plain `command -p
# uname`:
#   - a bare invocation resolves `uname` through the CALLER's PATH, so a
#     caller-controlled shim earlier on PATH could make this resolve to
#     something other than the real system uname (found by codex round
#     lane-wrapper-v486-20260904T082800: an exact-but-WRONG token from a
#     PATH-shadowed uname passes the validation below just as legitimately
#     as a real one would, and could route a genuinely-Linux host into the
#     macOS cleanup branch the same way the malformed-uname P1 above did).
#     `command -p` closes this: it runs `uname` against a fixed,
#     system-defined default PATH instead of the caller's own.
#   - but `command` ITSELF is a name a caller's environment can shadow with
#     a shell FUNCTION (e.g. via `BASH_ENV`, which non-interactive bash
#     sources before this script runs) -- and a plain `command -p uname -s`
#     resolves the word `command` the normal way, so a `command() { ... }`
#     function shadow wins over the real builtin even though `uname`
#     itself is never touched (found by codex round
#     lane-wrapper-v486-20260904T084834, EXECUTED: a BASH_ENV-defined
#     `command` function made this exact line return a forged value).
#     `builtin` is the fix for THAT: it looks its argument up as a shell
#     BUILTIN specifically, skipping function (and alias) resolution for
#     that name -- `builtin command -p uname -s` cannot be redirected by
#     either a PATH shim OR a shell-function override of `command`.
#     Measured both attacks directly: a PATH-shadowed uname and a
#     BASH_ENV-shadowed `command` function are both defeated by this exact
#     form; neither `command -p uname -s` alone nor `\command -p uname -s`
#     (backslash only defeats ALIAS lookup, not a function) close the
#     second one.
#   - a hostile environment that goes one level further and shadows
#     `builtin` itself (or `bash` the interpreter, or the coreutils
#     `uname` binary on disk) is out of scope: at that point the caller
#     already has full control over this process's entire execution
#     environment, and no invocation form defends against that -- the
#     threat model here is "keep an accidental or ordinary-shim PATH/env
#     quirk from silently deleting the shared cache," not "survive an
#     adversary who already owns the shell running this script."
HOST_OS="$(builtin command -p uname -s)"
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

# v4.8.13: UV_CACHE_DIR, bounded the same way as GOCACHE/GOMODCACHE above and
# for the identical reason -- an unset UV_CACHE_DIR defaults to a path under
# $HOME (~/.cache/uv), which workspace-write does not grant, so a
# Python-touching round's `uv sync`/`uv run` failed with a cache-init error
# the same shape as the pre-v4.3 GOCACHE failures. Same Linux-shared vs.
# macOS-per-round split as its Go neighbours: a shared, persistent path on
# bigboy (never per-lane/per-round -- this is a download cache, sharing it
# across rounds is the point), a fresh per-round dir under /tmp on macOS.
#
# PATH VERIFIED EMPIRICALLY, not assumed (lane-scribe's correction stands:
# the lane-scratch bind path does NOT double as a shared cache root by
# convention the way it looks like it should). `/var/lib/oci-cache/uv-cache`
# -- the name that would match go-build/go-mod's naming pattern -- does NOT
# exist and `mkdir` on it is denied (`/var/lib/oci-cache` itself is
# root:root 755; ubuntu cannot create new top-level entries in it, only use
# ones that already exist). The real, already-live, already-populated uv
# cache on this host is `/var/lib/oci-cache/uv` (singular, no `-cache`
# suffix, confirmed ubuntu:ubuntu writable, confirmed it has uv's own
# CACHEDIR.TAG/archive-v0/builds-v0/interpreter-v4/sdists-v9 layout already
# in it) -- that is the correct default, not a guess from a naming pattern.
if [ "$HOST_OS" = Linux ]; then
  RUVCACHE="${CODEX_REVIEW_UV_CACHE_DIR:-${UV_CACHE_DIR:-/var/lib/oci-cache/uv}}"
else
  RUVCACHE="${CODEX_REVIEW_UV_CACHE_DIR:-/tmp/codex-review-uvcache-$LANE_KEY-$TS}"
fi
mkdir -p "$RUVCACHE" || die "cannot create/find UV_CACHE_DIR $RUVCACHE"

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

# Sandbox mode. read-only is the historical default on macOS, where /tmp is
# proven writable under it (see v4.3/v4.4 above).
#
# Linux USED TO default to workspace-write here: probed on bigboy (v4.8 note
# above), read-only there grants ZERO writable paths, so every Go command
# dies before it can create its work dir or build cache -- there was no path
# this wrapper could point GOTMPDIR/GOCACHE at that would help, because none
# existed. That was the right call when the goal was letting the REVIEWER
# run Go itself inside the round.
#
# CHANGED 09-04 (chris's ruling, 15:26 PDT): "codex should not be running
# tests and go lang like it's been doing" -- a review round is a code-READING
# exercise now, not a code-EXECUTING one (see the STANDING RULES read-only
# policy appended to every prompt, below). read-only is therefore the default
# on BOTH platforms: on Linux this deliberately means the reviewer's own `go
# test`/`go build` CANNOT run inside the round any more -- that is the
# intended effect, not a regression of the v4.8 finding above. The wrapper's
# OWN pre-round warm step (further below) is unaffected either way: it runs
# entirely OUTSIDE this sandbox, via the wrapper's own `env`+bash, before
# codex ever starts. CODEX_REVIEW_SANDBOX, when a caller sets it explicitly
# (e.g. a launcher that still wants workspace-write for a specific reason),
# always wins over this default -- opt-in, not opt-out.
RSANDBOX="${CODEX_REVIEW_SANDBOX:-read-only}"
case "$RSANDBOX" in
  read-only | workspace-write) ;;
  *) die "CODEX_REVIEW_SANDBOX must be read-only or workspace-write, got '$RSANDBOX'" ;;
esac

# v4.8.18 (lane-review-perms, CHAOS pending): which codex ACCESS MECHANISM
# this round uses. codex-review selects the per-round `[permissions.codex-
# review]` profile (see further below), which allowlists /var/run/docker.sock
# so the reviewer can run testcontainers-backed Go tests itself -- but that
# mode ALSO opens full network egress as an unavoidable side effect (see the
# KNOWN LIMITATION comment where it's built). legacy reproduces v4.8.17's
# behavior (the old `-s "$RSANDBOX"` + `sandbox_workspace_write.writable_roots`
# path, network stays closed) and is the DEFAULT (CF read, 2026-09-10: an
# unset knob must never open egress by accident) -- codex-review is opt-in,
# named explicitly by a caller that specifically needs docker for this round.
# Permission profiles do NOT compose with sandbox_mode/sandbox_workspace_write
# (codex's own doc) -- the two mechanisms are mutually exclusive per round,
# never both emitted below.
RPERMS="${CODEX_REVIEW_PERMS:-legacy}"
case "$RPERMS" in
  codex-review | legacy) ;;
  *) die "CODEX_REVIEW_PERMS must be codex-review or legacy, got '$RPERMS'" ;;
esac

START_EPOCH=$(date +%s)   # bounds the session-transcript recovery search
# v4.8.2: renamed codex-rw-$NAME-* -> codex-review-worktree-$LANE-$TS-* (see
# changelog note near the top) so every wrapper-owned dir shares one
# reapable naming scheme. Keyed on LANE+TS rather than $NAME: two concurrent
# rounds against the same lane with the same explicit -n NAME must still get
# distinct, individually reapable dirs.
#
# v4.8.6 (chris's ruling, same day, after a bigboy boot-drive-full incident):
# on Linux, EVERY wrapper-owned per-round scratch dir below -- the review
# worktree, Go's own work dir, and the shell TMPDIR override -- moves under
# /var/lib/oci-cache/lane-scratch/<lane>/ instead of /tmp. <lane> is $NAME,
# the same value the round's own verdict/log filenames are keyed on. This is
# a FLEET-MANDATED location, not an optional nicety: fails closed (die) if
# that root cannot be created, rather than silently falling back to /tmp and
# quietly violating the ruling on a host where the mount is missing or
# unwritable. macOS is UNCHANGED -- still /tmp, for the sandbox-writability
# reasons documented throughout this file (v4.3/v4.4).
#
# KNOWN RESIDUAL GAP, not fixed by this change: reap_bases() (top of file,
# --reap-mine/--reap-stale) still scans only /tmp and $TMPDIR. On Linux, a
# round's OWN cleanup() trap already removes RW/RGOTMPDIR/RTMPDIR
# unconditionally regardless of where they live, so normal operation is
# unaffected -- this only means a round that gets SIGKILLed before its trap
# runs now leaves an orphan under lane-scratch that --reap-stale cannot see.
# Flagged for a follow-up, not blocking this change.
if [ "$HOST_OS" = Linux ]; then
  # v4.8.6: this used to re-derive its own SAFE_LANE_NAME here via
  # `basename --` plus a local `.`/`..`/`/` rejection case -- a
  # sanitize-then-reject helper that protected ONLY this one site while
  # V/L and RESIDUE_DIR (elsewhere in the file), which never called it,
  # stayed wide open to the exact same `-n` value (see the "item 3" finding
  # this generalized). Superseded: $NAME is now validated ONCE against a
  # positive allowlist right after it's resolved (a few hundred lines up,
  # search NAME_ALLOWLIST_RE) and this file dies before reaching here if it
  # isn't safe -- so $NAME can be used directly as a path component below,
  # no local re-sanitization needed, and there's only one place left that
  # can ever get this wrong.
  LANE_SCRATCH_ROOT="/var/lib/oci-cache/lane-scratch/$NAME"
  # v4.8.7, found by confirmation-pass round #4 on bigboy (P1, mechanism
  # EXECUTED, independently reproduced by the lane in an isolated temp
  # dir -- never against the real shared /var/lib path): NAME passing
  # NAME_ALLOWLIST_RE proves the STRING is safe, not that the FILESYSTEM
  # PATH built from it is safe to use. /var/lib/oci-cache/lane-scratch/ is
  # a SHARED parent every lane on this host writes into. If anything --
  # another lane's bug, a leftover from an incident, or a hostile actor
  # with write access to that shared parent -- pre-plants
  # lane-scratch/$NAME as a SYMLINK to some other writable directory
  # before this script runs, `mkdir -p` follows it silently (standard
  # POSIX behaviour for an existing symlink-to-a-directory), and every
  # mktemp call below (RW/RGOTMPDIR/RTMPDIR) then physically writes
  # through the symlink, entirely outside the mandated lane root, with no
  # error and nothing in this script's own output that would ever reveal
  # it happened. Measured directly: `mkdir -p` on a pre-planted symlink
  # resolves and writes through it every time.
  #
  # Fixed: refuse a lane-scratch/$NAME that is ALREADY a symlink, checked
  # both BEFORE and AFTER the mkdir -p (a symlink could be planted in the
  # gap between the two) -- this is meant to be exclusively this lane's
  # own directory, reused round over round, so a symlink there is never
  # something to silently follow, regardless of where it points. Then
  # resolve the REAL physical path and verify it is actually CONTAINED
  # under the real, resolved lane-scratch PARENT (both sides resolved via
  # `pwd -P`, never compared to a hardcoded lexical string -- an ancestor
  # symlink, if this host ever legitimately had one, would move both
  # sides identically and never trip this check; only a redirection AT
  # the final `$NAME` component itself does). From here on, every
  # subsequent scratch dir is built from the RESOLVED path
  # (LANE_SCRATCH_ROOT_REAL), not the lexical one, so a symlink planted at
  # the lexical path AFTER this point can no longer redirect anything --
  # the remaining TOCTOU window (someone replacing the REAL directory
  # itself) is a materially smaller, harder-to-target attack than
  # replacing a predictable, lexically-named symlink.
  if [ -L "$LANE_SCRATCH_ROOT" ]; then
    die "the mandated Linux scratch root $LANE_SCRATCH_ROOT already exists as a SYMLINK -- refusing to follow it (this is meant to be exclusively this lane's own directory; a pre-existing symlink here is never expected and never safe to trust)"
  fi
  mkdir -p "$LANE_SCRATCH_ROOT" \
    || die "cannot create/find the mandated Linux scratch root $LANE_SCRATCH_ROOT -- refusing to silently fall back to /tmp or \$HOME"
  if [ -L "$LANE_SCRATCH_ROOT" ]; then
    die "the mandated Linux scratch root $LANE_SCRATCH_ROOT became a SYMLINK between the check above and mkdir -p -- refusing to use it (this looks like a race, not an accident)"
  fi
  LANE_SCRATCH_PARENT_REAL=$(cd /var/lib/oci-cache/lane-scratch && pwd -P) \
    || die "cannot resolve the physical path of the shared /var/lib/oci-cache/lane-scratch parent"
  LANE_SCRATCH_ROOT_REAL=$(cd "$LANE_SCRATCH_ROOT" && pwd -P) \
    || die "cannot resolve the physical path of $LANE_SCRATCH_ROOT after creating it"
  case "$LANE_SCRATCH_ROOT_REAL" in
    "$LANE_SCRATCH_PARENT_REAL"/*) : ;;
    *) die "the mandated Linux scratch root $LANE_SCRATCH_ROOT resolves to '$LANE_SCRATCH_ROOT_REAL', which is NOT contained under the real lane-scratch parent '$LANE_SCRATCH_PARENT_REAL' -- refusing to use it" ;;
  esac
  RW_BASE="$LANE_SCRATCH_ROOT_REAL"
  RGOTMPDIR_BASE="$LANE_SCRATCH_ROOT_REAL"
  RTMPDIR_BASE="$LANE_SCRATCH_ROOT_REAL"
else
  RW_BASE="${TMPDIR:-/tmp}"
  RGOTMPDIR_BASE="/tmp"
  RTMPDIR_BASE="/tmp"
fi
# v4.8.7 (confirmation-pass round #1 on this branch, P1, EXECUTED): the
# LANE_SCRATCH_ROOT containment check above only proves the path was safe
# AT CHECK TIME -- each `mktemp` call below re-resolves its base path
# fresh, so a replacement (the lane dir removed and a symlink planted in
# its place, in the gap between the check and a specific mktemp call)
# would still be followed silently, unverified. Close it the same way the
# root itself is closed: verify EVERY path this wrapper creates under the
# lane root, immediately after creating it -- not a symlink, and its real
# path is still contained under the real lane parent. A no-op on macOS
# (LANE_SCRATCH_PARENT_REAL is never set there; nothing to verify against).
# v4.8.7 (confirmation-pass round #2 on this branch, P1, EXECUTED,
# INDEPENDENTLY re-verified by the lane): the round found TWO gaps in this
# helper's original prefix-based form -- (a) a check performed once, right
# after creation, proves nothing by the time a path is actually USED much
# later (RGOTMPDIR/RTMPDIR aren't touched again until the warm step,
# ~300 lines and real wall-clock time downstream -- a swap in that gap is
# followed silently), and (b) the check's OWN two steps (the `-L` test,
# then the separate `cd`+`pwd -P`) are not atomic, and the comparison
# itself was too PERMISSIVE: "resolves to somewhere under the shared
# lane-scratch parent" is true for every lane's own directory, so a race
# landing on a SIBLING lane's real directory sailed through unnoticed
# (reproduced: split the two steps, interposed a swap to a sibling lane's
# directory in between, watched the old prefix check accept it).
#
# CHRIS'S RULING (2026-09-04, after this finding): tighten the comparison
# from prefix-containment to EXACT-PATH EQUALITY -- capture the expected
# real path ONCE, immediately after the mktemp/mkdir that creates it
# (before anything else runs), and require the LATER re-check to resolve
# to that exact same real path, not merely "somewhere under the parent".
# This closes gap (b): a race can no longer redirect to a sibling lane's
# directory and have it accepted, since a sibling's real path can never
# equal what was captured for THIS lane's own path. It does NOT close gap
# (a) -- no POSIX-shell path-based check can, since there is no atomic
# "check and use" primitive available to a bash script; a genuine attacker
# racing a specific mktemp call in real time could still, in principle,
# win a window between this check and code far downstream that uses the
# same variable again. True prevention needs the created directory pinned
# to a file descriptor (`exec {fd}<"$dir"`, then every subsequent
# operation goes through `/proc/self/fd/$fd/...` instead of the original
# path string, since a later path replacement cannot redirect an
# already-open fd) -- filed as a v4.8.8 candidate, not attempted here.
#
# ACCEPTED RESIDUAL, deliberately not closed further right now: every lane
# in this fleet runs as the same `ubuntu` user, cooperatively scheduled,
# not a hostile multi-tenant boundary. The realistic threat this whole
# symlink/containment class defends against is a BUGGY lane or an
# incident LEFTOVER -- a pre-existing bad symlink sitting at a path before
# this script ever touches it -- which a single check-right-after-creation
# already catches completely, exact-equality or not. An attacker
# deliberately racing a specific mktemp call in the few-line window before
# its own verification runs is a materially different, much narrower
# threat that this fix does not claim to close, and closing it fully
# (fd-pinning) is a real rework deferred to v4.8.8 rather than rushed here
# under round pressure. The post-`git worktree add` check a few hundred
# lines below has the identical prefix-permissiveness shape and is
# DELIBERATELY left as-is for the same reason (that path is vacated then
# recreated by git, so there is no pre-existing "expected real path" to
# capture the same way) -- also part of this accepted residual, also
# closed for real only by the same v4.8.8 fd-pinning work.
verify_scratch_containment() {
  local created="$1" label="$2" expected_real="$3"
  [ "$HOST_OS" = Linux ] || return 0
  if [ -L "$created" ]; then
    die "$label ($created) is a SYMLINK immediately after creation -- refusing to use it (this looks like a race, not an accident)"
  fi
  local real
  real=$(cd "$created" && pwd -P) \
    || die "cannot resolve the physical path of $label ($created) after creating it"
  # BOTH checks, deliberately, not either alone: parent-containment catches
  # a corruption that happened BEFORE `expected_real` was ever captured (the
  # equality check alone would see the SAME corrupted value on both sides
  # and silently agree with itself -- measured directly: a symlink planted
  # before the expected-path capture escapes entirely undetected by
  # equality alone, since there is nothing earlier to disagree with).
  # Exact equality catches a corruption AFTER the capture that still lands
  # under the parent (a swap to a sibling lane's own real directory, which
  # the parent-containment check alone accepts, since every lane's
  # directory legitimately sits under the same shared parent).
  case "$real" in
    "$LANE_SCRATCH_PARENT_REAL"/*) : ;;
    *) die "$label ($created) resolves to '$real', which is NOT contained under the real lane-scratch parent '$LANE_SCRATCH_PARENT_REAL' -- refusing to use it" ;;
  esac
  if [ "$real" != "$expected_real" ]; then
    die "$label ($created) resolves to '$real', which does NOT match the path captured immediately after creation ('$expected_real') -- refusing to use it (this looks like a race -- possibly a swap to a DIFFERENT lane's own directory, which the parent-containment check alone would not have caught)"
  fi
}
RW=$(mktemp -d "$RW_BASE/codex-review-worktree-$LANE_KEY-$TS-XXXXXX")
if [ "$HOST_OS" = Linux ]; then
  RW_EXPECTED_REAL=$(cd "$RW" && pwd -P) \
    || die "cannot resolve the physical path of review worktree scratch dir (RW) ($RW) immediately after creating it"
else
  RW_EXPECTED_REAL=""
fi
verify_scratch_containment "$RW" "review worktree scratch dir (RW)" "$RW_EXPECTED_REAL"
# Go's work dir. Deliberately a SIBLING of the review worktree, not a directory
# inside it: anything inside $RW shows up as untracked and would be swept into
# the preserved residue, burying the reviewer's actual findings under build
# droppings. Removed by cleanup() alongside the worktree.
# /tmp (macOS) / lane-scratch (Linux) for the same reason as RGOCACHE above.
RGOTMPDIR=$(mktemp -d "$RGOTMPDIR_BASE/codex-review-gotmp-$LANE_KEY-$TS-XXXXXX")
if [ "$HOST_OS" = Linux ]; then
  RGOTMPDIR_EXPECTED_REAL=$(cd "$RGOTMPDIR" && pwd -P) \
    || die "cannot resolve the physical path of Go work dir (RGOTMPDIR) ($RGOTMPDIR) immediately after creating it"
else
  RGOTMPDIR_EXPECTED_REAL=""
fi
verify_scratch_containment "$RGOTMPDIR" "Go work dir (RGOTMPDIR)" "$RGOTMPDIR_EXPECTED_REAL"

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
RTMPDIR=$(mktemp -d "$RTMPDIR_BASE/codex-review-gotmp-$LANE_KEY-$TS-shell-XXXXXX")
if [ "$HOST_OS" = Linux ]; then
  RTMPDIR_EXPECTED_REAL=$(cd "$RTMPDIR" && pwd -P) \
    || die "cannot resolve the physical path of shell TMPDIR (RTMPDIR) ($RTMPDIR) immediately after creating it"
else
  RTMPDIR_EXPECTED_REAL=""
fi
verify_scratch_containment "$RTMPDIR" "shell TMPDIR (RTMPDIR)" "$RTMPDIR_EXPECTED_REAL"

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
# v4.8.18 (team-lead, review-evidence loss): astra's mutation-sweep REPORT.md
# lived at $RW/review-evidence/ and preserve_residue()'s git-status-driven
# copy did not save it before cleanup() removed the worktree — the survivors
# list became unrecoverable for that round. This is a separate, unconditional
# copy (not gated on git status/ignore rules at all) run BEFORE the worktree
# is touched, so a reviewer writing evidence there is never depending on
# preserve_residue's git-aware logic to notice it.
copy_review_evidence() {
  local src="$RW/review-evidence"
  local dest="$OUTDIR/$NAME-$TS-review-evidence"
  if [ ! -d "$src" ]; then
    warn "review-evidence: none"
    return 0
  fi
  local n
  n=$(find "$src" -type f 2>/dev/null | wc -l | tr -d ' ')
  if mkdir -p "$dest" 2>/dev/null && cp -R "$src/." "$dest/" 2>/dev/null; then
    warn "review-evidence: copied $n files"
  else
    warn "review-evidence: FAILED to copy from $src to $dest — check $OUTDIR is writable, evidence may be lost once cleanup removes the worktree"
  fi
}

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
  copy_review_evidence
  preserve_residue
  # v4.8.18: explicit unlink, not left to the rm_rf_writable below alone --
  # a symlink to real credentials sitting in a per-round scratch dir is worth
  # removing on its own line, auditable independent of whatever else that
  # rm -rf does or does not reach.
  [ -n "${CODEX_HOME_ROUND:-}" ] && rm -f "$CODEX_HOME_ROUND/auth.json" 2>/dev/null
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
  # v4.8.14: $RW_REPO is always $WT now that codegraph's shared-base-checkout
  # path is removed (see the v4.8.14 CODEGRAPH REMOVED note below) -- this
  # $RW_REPO indirection and the OPS_BASE prune branch right after it are
  # dead code kept byte-identical to v4.8.13 rather than torn out here, since
  # neither does anything once RW_REPO can no longer differ from $WT.
  git -C "${RW_REPO:-$WT}" worktree remove --force "$RW" 2>/dev/null \
    || warn "review worktree $RW not removed — remove it manually and check 'git worktree list'"
  # CF requirement: prune $OPS_BASE's worktree registrations after remove
  # too, not only before the next round's add -- closes the same stale-
  # registration window for THIS round's own removal (e.g. a `remove
  # --force` that removed the directory but left a dangling admin entry
  # under some git version/interruption combination), rather than only
  # ever cleaning it up as a side effect of the NEXT round starting.
  if [ "${RW_REPO:-}" != "" ] && [ "$RW_REPO" != "$WT" ]; then
    git -C "$RW_REPO" worktree prune 2>/dev/null \
      || warn "cleanup: 'git worktree prune' on $RW_REPO failed (non-fatal)"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# v4.8.14 CODEGRAPH REMOVED FROM THE REVIEW HARNESS -- chris's ruling,
# 2026-09-06, verbatim: "we already proved it wasn't useful to reviewers so
# it shouldn't be in the harness to index it." This replaces the entire
# v4.8.12 CODEGRAPH SHARED BASE CHECKOUT mechanism: there is no more shared
# $OPS_BASE checkout read, fetched, or `codegraph sync`'d, and no round
# worktree is ever nested inside one. Every round now takes the plain,
# unconditional per-$WT worktree-add path below -- the same path v4.8.12
# already fell back to on macOS, high load, or a missing/mismatched base
# checkout. Because no round worktree is ever created nested inside a base
# checkout any more, none can pick up a `.codegraph/` index via the
# filesystem-nesting mechanism the old design depended on -- there is no
# separate "prevent .codegraph" step needed here, removing the nesting
# removes the only path that ever put one in a round's worktree.
# $OPS_BASE and any `.codegraph` index still sitting on disk from v4.8.12
# rounds are untouched by this wrapper going forward; this change only
# stops NEW rounds from reading them.
RW_REPO="$WT"

if [ "$RW_REPO" = "$WT" ]; then
  git -C "$WT" worktree add --detach "$RW" "$TIP" >/dev/null || die "worktree add failed"
fi
# v4.8.7 (confirmation-pass round #1 on this branch, P1, EXECUTED,
# PRE-EXISTING pattern since v4.8.2, now closed at point of use): $RW is
# `mktemp -d`'d, then `rmdir`'d a few lines above ("git worktree add wants
# to create it"), leaving a vacant, but real, path for potentially a
# while before this line runs (cache resolution, GOPATH setup, etc. all
# happen in between). An actor able to create entries in the lane
# directory in that window can plant a symlink at that exact vacant path;
# `git worktree add` follows it and writes the whole worktree outside the
# lane, unverified. This is DETECTION at point of use, not prevention of
# the race itself -- actually preventing it would need a private staging
# directory `git worktree add` never has to `rmdir` its way into (flagged
# as a v4.8.8+ redesign candidate, not attempted here). Verify immediately
# after: not a symlink, and `git rev-parse --show-toplevel` from inside it
# (git's OWN notion of where this worktree actually lives, not our own
# assumption) resolves under the real lane-scratch parent.
# v4.8.14: $RW_REPO can no longer be anything but $WT (codegraph's nested
# base-checkout path, the only thing that ever set it otherwise, is gone --
# see the v4.8.14 CODEGRAPH REMOVED note above), so this check now always
# applies. Left as an explicit condition rather than unconditional, byte-
# identical in shape to v4.8.13, to minimize the diff around dead-but-
# harmless state.
if [ "$HOST_OS" = Linux ] && [ "$RW_REPO" = "$WT" ]; then
  if [ -L "$RW" ]; then
    die "the review worktree $RW is a SYMLINK immediately after 'git worktree add' -- refusing to use it (this looks like a race, not an accident: a symlink was likely planted in the vacant slot between the earlier rmdir and this worktree add)"
  fi
  RW_TOPLEVEL=$(cd "$RW" && git rev-parse --show-toplevel 2>/dev/null) \
    || die "cannot resolve the review worktree's own toplevel via 'git rev-parse --show-toplevel' after worktree add"
  RW_TOPLEVEL_REAL=$(cd "$RW_TOPLEVEL" && pwd -P) \
    || die "cannot resolve the physical path of the review worktree toplevel ($RW_TOPLEVEL)"
  case "$RW_TOPLEVEL_REAL" in
    "$LANE_SCRATCH_PARENT_REAL"/*) : ;;
    *) die "the review worktree's own toplevel (per git rev-parse --show-toplevel) resolves to '$RW_TOPLEVEL_REAL', which is NOT contained under the real lane-scratch parent '$LANE_SCRATCH_PARENT_REAL' -- refusing to use it (git worktree add may have followed a symlink planted in the vacant slot)" ;;
  esac
fi

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
# skips the entire warm-step subshell (no `go` command runs); a repo WITH
# go.mod is unaffected and keeps the full warm step, including its loud
# abort. CORRECTED, round-3 confirmation-pass (P2, source-checked): this
# used to also claim "exports no Go env to codex" -- false, the
# GOCACHE/GOMODCACHE/GOTMPDIR/GOPATH exports further below into `codex exec`
# are unconditional regardless of this branch. Harmless in practice (an
# unused, possibly-cold cache path is inert for a non-Go repo, and the new
# read-only sandbox default denies writes there anyway), but the comment
# should not claim otherwise.
#
# v4.8.7 addendum (chris, 09-04 15:26 PDT): the warm step's own Go execution
# happens outside codex's sandbox regardless, so it was never the thing chris
# flagged ("codex should not be running tests and go lang") -- that concern is
# the REVIEWER's own exec blocks, closed above by the read-only sandbox
# default and the STANDING RULES read-only policy below. This override is a
# separate, narrower ask: an operator-controlled way to skip the warm step
# ENTIRELY (e.g. a docs-only round where warming Go serves no reviewer that
# will never run it), without waiting on the v4.8.8 no-Go-in-diff auto-skip.
# WARM_OK tracks whether the warm step actually ran and proved the module
# cache offline-resolvable -- round-3 confirmation-pass finding: the prompt
# text injected further below used to unconditionally claim "already warmed
# and offline-resolve-proven" regardless of which of these three branches
# ran (a pre-existing gap for the no-go.mod branch, now ALSO reachable via
# the new operator skip). Read below, at the point this is consumed, for
# why that mattered.
WARM_OK=0
WARM_SKIP_REASON=""
if [ "${CODEX_REVIEW_SKIP_WARM:-0}" = "1" ]; then
  printf 'warm-step: SKIPPED (operator)\n' >>"$L"
  warn "warm: SKIPPED -- CODEX_REVIEW_SKIP_WARM=1, no warm build ran (Go env is still exported to codex exec, unconditionally, further below) -- proceeding straight to codex"
  WARM_SKIP_REASON="the operator set CODEX_REVIEW_SKIP_WARM=1 for this round"
elif [ -f "$RW/go.mod" ]; then
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
        go build -o "$1/" ./... || go build ./...
        go vet ./...
        go build -tags=integration -o "$1/" ./... || go build -tags=integration ./...
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
  WARM_OK=1
else
  printf 'warm-step: SKIPPED reason=no-go.mod\n' >>"$L"
  warn "warm: SKIPPED -- no go.mod at $TIP, nothing to warm (Go env is still exported to codex exec, unconditionally, further below) -- proceeding straight to codex"
  WARM_SKIP_REASON="there is no go.mod at $TIP -- nothing to warm"
fi

cp "$PROMPT" "$RW/prompt.md"
# STANDING SAFETY LINE, appended to every prompt regardless of what the lane
# wrote. A #2134 round composed and "quoted" a
# `docker exec dev-health-clickhouse-1 clickhouse-client --query ...` against
# chris's SHARED stack. It did not run (no exec block), but a reviewer that will
# write the command is one prompt away from running it, and the shared stack is
# not this round's to touch. Appended rather than merged into the lane's text so
# it cannot be edited out by a prompt author who did not think of it.
#
# v4.8.8 (chris 09-05 13:23-13:26 PDT's exec-mandatory rewrite; CHAOS-5249 r1
# incident): this block used to append the SAME "do not run go test/build...
# regardless of whether the sandbox you are given would technically permit
# it" text to EVERY round, with no check on $RSANDBOX -- so a round launched
# under workspace-write, with a lane prompt explicitly requiring build/test/
# coverage exec, was ALSO told by this wrapper-owned text to ignore its own
# sandbox and refuse to execute anything. The reviewer followed the
# wrapper's text over the lane's prompt (CHAOS-5249 r1: zero go test/build
# exec blocks, a fabricated-sounding "standing read-only review policy"
# citation that was in fact this exact appended paragraph). The shared-stack/
# docker prohibition and the architecture-sensitivity note are UNCONDITIONAL
# (correct under either sandbox mode) and stay identical either way; only the
# go-test/build guidance now branches on the sandbox this round actually got.
cat >> "$RW/prompt.md" <<'STANDING_RULES_HEADER'

---

STANDING RULES FOR EVERY ROUND (appended by the wrapper; not optional):
STANDING_RULES_HEADER

# v4.8.18 (lane-review-perms): docker is available to the reviewer under
# codex-review+workspace-write (the permissions profile allowlists
# /var/run/docker.sock) -- the old blanket "never run docker" text is now
# FALSE in that mode and would make the reviewer refuse a container-backed
# proof it can actually run. Every other mode (legacy, or read-only under
# either RPERMS) keeps the original prohibition unchanged. Unquoted heredoc
# on purpose: the round's own label value is spliced in literally so the
# reviewer gets a copy-pasteable `--label` flag, not a shell variable it
# cannot resolve inside the sandbox.
if [ "$RPERMS" = "codex-review" ] && [ "$RSANDBOX" = "workspace-write" ]; then
  cat >> "$RW/prompt.md" <<DOCKER_RULES_AVAILABLE

Docker IS available to you in this round, via /var/run/docker.sock -- run the
\`-tags=integration\` Go tests and any container-backed pytest yourself. Do not
report a container-backed check as ARGUED/unrun just because it needs a
container; if a testcontainers run fails to start a container, that is a
finding about the harness -- report it verbatim, never invent what it would
have printed.

The shared compose project (\`dev-health\`) and any container NOT labelled
\`codex-review-round=$NAME-$TS\` are OFF LIMITS: never \`docker exec\`/\`stop\`/
\`rm\`/\`compose\` against them, never connect to their ports -- they belong to
other people's work in flight and touching them can destroy their state.
Every container YOU start carries \`--label codex-review-round=$NAME-$TS\`
(this exact value, also in \$CODEX_REVIEW_ROUND_LABEL). Never run \`docker
system prune\`, \`builder prune\`, or \`image prune\` -- these are host-wide and
would destroy other lanes' state, not just yours.

Network egress is open in this round; do not fetch anything not required by
the tests.

Pass any database DSN or credential (including a throwaway per-round test
password) via an environment variable, never inline in a command string --
the same rule as never putting a secret in argv applies here too.
DOCKER_RULES_AVAILABLE
else
  cat >> "$RW/prompt.md" <<'DOCKER_RULES_PROHIBITED'

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
DOCKER_RULES_PROHIBITED
fi

cat >> "$RW/prompt.md" <<'STANDING_RULES_SHARED'

Architecture-sensitive checks (NaN sign bits, FMA/fused-multiply-add results,
float formatting, anything whose answer can differ per CPU) are verified in CI,
never here. Every host in this fleet is arm64. Running such a check locally
does not give you a weaker result -- it gives you a CONFIDENT WRONG one: it
passes on arm64 while the x86 case it was meant to catch is still broken
(CHAOS-4818 / #2142's NaN sign-bit reds appeared ONLY in CI). A green from the
wrong architecture is worse than no green, because it is indistinguishable
from a real one in your verdict. Say the check is CI-only and move on.
STANDING_RULES_SHARED

if [ "$RSANDBOX" = "workspace-write" ]; then
  cat >> "$RW/prompt.md" <<'STANDING_RULES_EXEC'

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
STANDING_RULES_EXEC
else
  cat >> "$RW/prompt.md" <<'STANDING_RULES_READONLY'

READ-ONLY REVIEW POLICY (chris's ruling, 09-04): this round is a code-READING
exercise, not a code-EXECUTING one. Do not run `go test`, `go build`, `go
run`, `go vet`, or any other language build/test/run command, regardless of
whether the sandbox you are given would technically permit it. Your exec
blocks should be limited to inspection commands: `git`, `rg`/`grep`, `cat`,
`sed`, `awk`, `ls`, `diff`, and similar read-only tools against the files
already in this worktree. If verifying a claim genuinely requires executing
code, name the specific proof you would need and label it ARGUED/unrun in
your verdict -- do not run it yourself in this round, even if you could.
STANDING_RULES_READONLY
fi
# Second heredoc, UNQUOTED delimiter on purpose: this one interpolates the
# round's actual RGOMODCACHE/HOME paths at generation time, so the reviewer
# gets literal, copy-pasteable paths rather than shell variables it would
# have to resolve itself inside the sandbox. Kept separate from the
# single-quoted STANDING_RULES block above so that block's own `$`/backtick
# markdown-code-span characters never need escaping.
#
# v4.8.6 P2 (found by codex round lane-wrapper-v486-20260904T082800, EXECUTED):
# the retry fallback below used to unconditionally point the reviewer at
# $HOME/go/pkg/mod -- correct pre-v4.8.6, when $RGOMODCACHE was a COLD
# per-round cache and the host's real, long-lived default module cache was
# the only place with useful content to fall back to. On Linux, $RGOMODCACHE
# IS now that persistent, shared cache (or a caller-set explicit location) --
# $HOME/go/pkg/mod is the ruling's own LEGACY path, no lane writes there any
# more, so it is likely stale or empty and routing a reviewer's retry at it
# is directionless guidance at best. Linux gets no fallback path suggestion
# at all (a lookup failure against the persistent shared cache is a real gap
# worth reporting, not a location problem to route around).
#
# v4.8.6 addendum (chris via team-lead, same day): $HOME/go/pkg/mod is not a
# safe suggestion on macOS EITHER -- it names a DIFFERENT USER's cache on
# any host other than this one, and even here it is an unrelated, unwarmed
# location with no particular reason to have what the round needs. The
# macOS branch now quotes the round's OWN resolved $RGOMODCACHE (the same
# value already stated above, and the one already warmed and
# offline-resolve-proven before the round started) instead of switching
# location at all. This is not redundant: it ties into the
# "creating work dir" retry-once rule immediately below in the standing
# rules -- on macOS a `go test` module-lookup failure inside the sandbox is
# more often a transient read-only-sandbox hiccup on the FIRST invocation
# than a genuinely missing module (measured by two lanes), so retrying the
# SAME cache is the correct move, not hunting for a different one.
if [ "$HOST_OS" = Linux ]; then
  MODCACHE_FALLBACK_LINE="If \`go test\` still fails on a module lookup once you are inside the sandbox, that means the persistent shared cache above is genuinely missing something -- say so explicitly (name the missing module) and fall back to a source-trace verdict. Do NOT retry against \$HOME/go/pkg/mod: that path is legacy and no lane writes to it, so it is not a meaningful fallback."
else
  MODCACHE_FALLBACK_LINE="If \`go test\` still fails on a module lookup once you are inside the sandbox, retry ONCE with GOMODCACHE=$RGOMODCACHE GOPROXY=off (read-only intent -- never write there; this is the SAME cache named above, not a different one -- a first-attempt sandbox hiccup, not a missing module, is the more likely cause here) before falling back to a source-trace verdict, and say explicitly whether the retry succeeded."
fi
# v4.8.7, confirmation-pass round #3 (P2, mechanism EXECUTED): this text used
# to be appended UNCONDITIONALLY, regardless of whether the warm step above
# actually ran -- both the pre-existing no-go.mod branch and the new
# CODEX_REVIEW_SKIP_WARM branch would still inject "already warmed and
# offline-resolve-proven", which is simply untrue when nothing was warmed.
# WARM_OK (set above, alongside the branch that actually ran the proof) now
# gates which claim gets made -- the reviewer is told the true state either
# way, never told a proof happened when it did not.
if [ "$WARM_OK" = "1" ]; then
  cat >> "$RW/prompt.md" <<PROMPT_MODCACHE_INFO

This round's own module cache -- already warmed and offline-resolve-proven
(via \`GOPROXY=off go test -count=1 -run '^\$' ./...\`) before you started --
is at $RGOMODCACHE. $MODCACHE_FALLBACK_LINE
PROMPT_MODCACHE_INFO
elif [ "$RSANDBOX" = "workspace-write" ]; then
  # v4.8.10 (lane-5045-testops-dup peer read, EXECUTED/reproduced): this
  # branch used to say "regardless" of the READ-ONLY REVIEW POLICY unconditionally,
  # even though that policy no longer applies under workspace-write (this same
  # CHAOS-5249 shape, in a second spot the v4.8.8 split didn't touch). Under
  # workspace-write, execution is still REQUIRED (STANDING_RULES_EXEC) --
  # a cold cache is a warning about speed/flakiness, not a license to skip.
  cat >> "$RW/prompt.md" <<PROMPT_MODCACHE_INFO

No Go module cache was warmed for this round ($WARM_SKIP_REASON) -- do not
assume \`go test\`/\`go build\` will succeed, or succeed quickly, on the first
try. This does NOT excuse you from executing (see STANDING_RULES_EXEC above,
which still applies): retry once on a transient failure, and if a module is
genuinely missing offline, say so explicitly and name it.
PROMPT_MODCACHE_INFO
else
  cat >> "$RW/prompt.md" <<PROMPT_MODCACHE_INFO

No Go module cache was warmed for this round ($WARM_SKIP_REASON) -- do not
assume \`go test\`/\`go build\` will succeed, or succeed quickly, if you
attempt them (see the READ-ONLY REVIEW POLICY above: you should not be
running them at all in this round regardless).
PROMPT_MODCACHE_INFO
fi
for aux in .codex-review-context.md LEDGER.md; do
  [ -f "$WT/$aux" ] && cp "$WT/$aux" "$RW/$aux"
done

# v4.8.22 (CHAOS-6948): the review patch, written by the wrapper (outside the sandbox,
# where git works) into the review worktree, and named in the prompt. A failure to
# produce it is loud in the log and the prompt says so; the VOID IN FORM rule below
# then requires a successful `git diff` exec from the reviewer instead.
# REVIEW-PATCH-BEGIN
REVIEW_PATCH_NAME=".codex-review.patch"
REVIEW_PATCH="$RW/$REVIEW_PATCH_NAME"
REVIEW_PATCH_STATUS="unavailable"
if BASE_SHA=$(git -C "$WT" rev-parse --verify --quiet "$BASE^{commit}") \
   && { git -C "$WT" diff --no-color --stat "$BASE_SHA...$TIP"; printf '\n'; git -C "$WT" diff --no-color "$BASE_SHA...$TIP"; } > "$REVIEW_PATCH" 2>"$REVIEW_PATCH.err"; then
  REVIEW_PATCH_STATUS="written"
  rm -f "$REVIEW_PATCH.err"
  REVIEW_PATCH_BYTES=$(wc -c < "$REVIEW_PATCH" | tr -d ' ')
  warn "review patch: $REVIEW_PATCH_NAME written ($REVIEW_PATCH_BYTES bytes, base=$BASE $BASE_SHA ... tip=$TIP)"
  printf 'review-patch: file=%s bytes=%s base=%s tip=%s\n' "$REVIEW_PATCH_NAME" "$REVIEW_PATCH_BYTES" "$BASE_SHA" "$TIP" >> "$L"
  cat >> "$RW/prompt.md" <<PROMPT_REVIEW_PATCH

THE DIFF UNDER REVIEW is written to \`$REVIEW_PATCH_NAME\` in your working directory:
\`git diff --stat $BASE...$TIP\` first, then the full unified diff. Read it (for
example \`sed -n '1,200p' $REVIEW_PATCH_NAME\`): \`git diff\` may be denied by the
sandbox in this worktree. A round whose log shows neither a successful \`git diff\` nor
a read of this file is stamped VOID IN FORM, because it cannot certify a diff it did
not see.
PROMPT_REVIEW_PATCH
else
  rm -f "$REVIEW_PATCH"
  warn "review patch: NOT written (base '$BASE' unresolvable or git diff failed: $(tr '\n' ' ' < "$REVIEW_PATCH.err" 2>/dev/null | cut -c1-200)); the round must run \`git diff\` itself or be stamped VOID IN FORM"
  printf 'review-patch: unavailable base=%s tip=%s\n' "$BASE" "$TIP" >> "$L"
  rm -f "$REVIEW_PATCH.err"
  cat >> "$RW/prompt.md" <<PROMPT_REVIEW_PATCH

No review patch file could be written for this round (base $BASE). Run
\`git diff $BASE...$TIP\` yourself; a round whose log shows no successful \`git diff\`
is stamped VOID IN FORM.
PROMPT_REVIEW_PATCH
fi
# REVIEW-PATCH-END

warn "round $NAME-$TS: model=$MODEL effort=$EFF tip=$TIP review-worktree=$RW"
# v4.8.14: codegraph removed from the harness (chris's ruling, 09-06) -- the
# provenance line no longer carries a codegraph-mode/base-checkout-sha pair
# (v4.8.12's CF requirement, now moot since no base checkout is ever read),
# just the worktree and reviewed tip, plus an explicit disabled marker so a
# post-round read of this log never mistakes a pre-v4.8.14 log's absence of
# this marker for THIS round having skipped codegraph by accident.
printf 'review-target: worktree=%s tip=%s\n' "$RW" "$TIP" >> "$L"
printf 'codegraph: disabled (chris 2026-09-06)\n' >> "$L"
warn "go bounds: GOFLAGS=$RGOFLAGS GOMAXPROCS=$RGOMAXPROCS GOCACHE=$RGOCACHE GOMODCACHE=$RGOMODCACHE GOTMPDIR=$RGOTMPDIR GOPATH=$RGOPATH UV_CACHE_DIR=$RUVCACHE TMPDIR=$RTMPDIR sandbox=$RSANDBOX perms=$RPERMS"
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
if [ "$RPERMS" = "legacy" ] && [ "$RSANDBOX" = "workspace-write" ]; then
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
  # RUVCACHE added v4.8.13, same reasoning as its Go neighbours.
  SANDBOX_ARGS+=(-c "sandbox_workspace_write.writable_roots=[\"$RGOCACHE\",\"$RGOMODCACHE\",\"$RGOTMPDIR\",\"$RGOPATH\",\"$RUVCACHE\"]")
fi

# v4.8.18 PERMISSIONS-PROFILE ARGS (codex-review mode only).
#
# REVISED (team-lead, post-incident 23:41Z): the FIRST version of this block
# assumed a static `[permissions.codex-review]` table appended to the SHARED
# ~/.codex/config.toml, selected per round via `-c default_permissions=...`.
# That broke codex 0.153.4 host-wide the moment the table existed WITHOUT the
# file itself also setting `default_permissions` -- codex refuses to load
# ANY config that defines a `[permissions.*]` table unless that same file
# also names one via `default_permissions`, and a `-c` override supplied at
# invocation time does not satisfy this check (it appears to validate the
# FILE before CLI overrides are merged in). Three lanes' rounds failed
# immediately, misreported by this wrapper's own preflight as "not logged
# in" (see the login-status fix further up). Shared config.toml is rolled
# back and must never carry a `[permissions.*]` table again.
#
# Fix: a PER-ROUND, throwaway CODEX_HOME under this round's own RGOTMPDIR
# (removed by cleanup()'s unconditional `rm_rf_writable "$RGOTMPDIR"` --
# never a shared or long-lived location). Its config.toml is the real one,
# copied verbatim, PLUS `default_permissions = "codex-review"` prepended as
# the file's very first line (TOML scopes a bare `key = value` to whatever
# `[table]` header precedes it in the file -- appending it at the END would
# silently bind it to the last `[table]` in the real config instead of the
# root, exactly the class of mistake that is invisible until parsed; proven
# with `python3 -c "import tomllib; ..."` before this shipped: appended-last
# parses as `permissions.codex-review.network.unix_sockets.default_permissions`,
# prepended-first parses as the real root key), PLUS the
# `[permissions.codex-review]` table appended at the end (table headers are
# self-scoping regardless of position). auth.json is a READ-ONLY SYMLINK into
# this per-round home, never copied (never risk two writable copies of
# credentials) -- removed explicitly in cleanup() before its parent dir is
# rm -rf'd, not left to the directory removal alone. `[permissions]` never
# touches the real ~/.codex.
#
# read-only rounds under this mode need none of this: `-c
# default_permissions=":read-only"` is a codex BUILT-IN, requires no
# `[permissions]` table in any config file at all (verified against the
# clean, rolled-back shared config), so it's a plain per-invocation `-c`
# override same as before -- docker access is only ever granted alongside
# execution anyway.
#
# 🛑 KNOWN LIMITATION, chris ruling 2026-09-10 (Option A, accepted): codex
# 0.153.4's `network.unix_sockets` allowlist is INERT while
# `[permissions.codex-review.network].enabled = false` -- docker.sock is
# unreachable no matter what path/write grants are added (measured: resolved
# `/run/docker.sock`, both paths together, and a `filesystem write` grant on
# the socket path all fail identically, the last one crashing bwrap outright
# -- it tries to `mkdir` the socket path as a bind-mount target). Setting
# `enabled = true` makes the docker.sock allow work, but ALSO opens full
# network egress with NO domain filtering: an explicit `domains."x"="deny"`
# entry, and even a totally unlisted domain, both connect successfully.
# Root cause: domain allow/deny is gated on the `network_proxy` feature,
# EXPERIMENTAL and OFF by default (`codex features list`) -- turning it ON
# fixes domain deny-by-default but BREAKS the docker.sock allow entry (back
# to permission denied) -- the two mechanisms do not compose in this
# experimental state. No config was found that gets both docker access and
# domain deny-by-default at once. Chris's ruling: accept open egress for
# codex-review rounds to get docker; `network_proxy` stays OFF; re-evaluate
# once codex ships a stable proxy that composes with the socket allowlist.
#
# 🛑 CF read, accepted as part of the same ruling: docker.sock IS root on
# this host. Every filesystem `deny` entry below is therefore advisory, not
# a hard boundary -- a reviewer that runs `docker run -v /:/host ...` reads
# and writes the whole filesystem through that mount, ops/web/acr/credential
# denies included, regardless of anything in this profile. The prompt's OFF
# LIMITS line (dev-health project, other lanes' containers) is the only
# guard against that; this profile does not and cannot enforce it.
SFLAG_ARGS=()
PERMS_ARGS=()
CODEXHOME_ENV_ARGS=()
CODEX_HOME_ROUND=""
if [ "$RPERMS" = "legacy" ]; then
  # legacy mode still selects the sandbox via `-s`, byte-for-byte v4.8.17.
  SFLAG_ARGS+=(-s "$RSANDBOX")
elif [ "$RSANDBOX" != "workspace-write" ]; then
  PERMS_ARGS+=(-c 'default_permissions=":read-only"')
else
  CODEX_HOME_REAL="${CODEX_HOME:-$HOME/.codex}"
  [ -f "$CODEX_HOME_REAL/config.toml" ] || die "codex-review permissions mode needs a real config.toml at $CODEX_HOME_REAL/config.toml to build the per-round CODEX_HOME from -- none found"
  CODEX_HOME_ROUND="$RGOTMPDIR/codex-home"
  mkdir -m 700 -p "$CODEX_HOME_ROUND" || die "cannot create per-round CODEX_HOME at $CODEX_HOME_ROUND"
  {
    printf 'default_permissions = "codex-review"\n'
    cat "$CODEX_HOME_REAL/config.toml"
    # v4.8.18 (dry-run 1 attempt 1, 00:31:42Z): codex rejected a bare
    # `"**/.env"` key under `[permissions.codex-review.filesystem]` outright
    # at config-load time -- "filesystem path `**/.env` must be absolute, use
    # `~/...`, or start with `:`" -- an unanchored glob is not a valid
    # filesystem-map key on its own. `~/...` and absolute paths (the other
    # deny entries below, unaffected) are fine as direct keys; a
    # workspace-relative glob has to live under the special `:workspace_roots`
    # sub-table instead.
    #
    # v4.8.18 (dry-run 1 attempt 2, 00:36:17Z): a `**` glob on an ABSOLUTE
    # deny path is worse than invalid -- it LOADS, then dies at session start
    # inside bubblewrap: "unreadable glob expansion for /home/ubuntu/devhealth/
    # ops matched more than 8192 paths" (bwrap materialises each filesystem
    # rule as an individual bind-mount, and the ops checkout alone blows the
    # 8192-mount cap). Fix: deny the DIRECTORY itself, no `**` -- one mount,
    # denies the whole subtree ("deny wins over an equally-specific ancestor
    # rule" per the doc; a bare dir key is not "equally specific" as a
    # sibling grant, it just recurses). Measured live (`codex exec ... "run:
    # echo ok"`, real bwrap session, not just `login status`): bare dir keys
    # -> exec succeeds; `**`-suffixed absolute keys -> the 8192-path death
    # above.
    printf '\n[permissions.codex-review]\nextends = ":workspace"\n\n[permissions.codex-review.filesystem]\n'
    # glob_scan_max_depth: the ":workspace_roots" `**` globs below (small,
    # single-repo worktrees, team-lead-approved as safe) otherwise print a
    # "non-macOS sandboxing does not support unbounded ** natively" warning
    # on every round -- harmless (exec still succeeds) but noisy in every
    # log; capping the scan depth silences it. Measured: 8 clears the warning
    # against this worktree shape.
    printf 'glob_scan_max_depth = 8\n'
    # v4.8.18 (team-lead, second CF pass): asked to deny the WHOLE real
    # `~/.codex`. MEASURED and REVERTED: `~/.codex` is a symlink to
    # `~/agents/codex`, which is not just credential/session data -- it's
    # also codex's OWN INSTALLED BINARY
    # (`~/agents/codex/packages/standalone/releases/.../bin/codex`). Denying
    # the whole real directory broke codex outright ("bwrap: execvp
    # .../bin/codex: Permission denied" -- codex re-execs its own installed
    # binary building the sandboxed session, so that path must stay
    # readable+executable). A more-specific `"~/.codex/packages" = "read"`
    # override did NOT fix it either (still the same execvp denial) -- this
    # schema's `read` does not appear to imply execute. Fell back to naming
    # the actual sensitive files instead of the whole tree: `auth.json` (the
    # credential), `sessions/` and `history.jsonl`/`session_index.jsonl`
    # (past conversation content -- privacy, not an impersonation vector
    # like gh/git-creds, but denied anyway since the round has no legitimate
    # reason to read past sessions). Everything else under `~/.codex`
    # (config.toml, the sqlite state/memory/queue/log DBs, `packages/`)
    # stays readable -- the sandboxed reviewer's own CODEX_HOME is
    # CODEX_HOME_ROUND regardless, so it has no NORMAL path that reads any
    # of this; these four entries close the abnormal one.
    printf '"~/.codex/auth.json" = "deny"\n"~/.codex/sessions" = "deny"\n"~/.codex/history.jsonl" = "deny"\n"~/.codex/session_index.jsonl" = "deny"\n'
    printf '"~/.ssh" = "deny"\n"~/.gnupg" = "deny"\n'
    printf '"/home/ubuntu/devhealth/ops" = "deny"\n"/home/ubuntu/devhealth/web" = "deny"\n"/home/ubuntu/devhealth/acr" = "deny"\n'
    # CF read finding (BLOCKING, P1, caught before install): with network
    # open (this mode's own limitation, see above), any OTHER credential
    # store on this host is now reachable by the reviewer over that network
    # -- not just the codex/ssh/gnupg set the first draft denied. `gh CLI`
    # credentials specifically let a reviewer run `gh pr merge/comment/api`
    # AS CHRIS. Deny every credential store this host actually has, same
    # bare-path-no-glob shape as the entries above.
    printf '"~/.config/gh" = "deny"\n"~/.netrc" = "deny"\n"~/.git-credentials" = "deny"\n'
    printf '"~/.docker" = "deny"\n"~/.kube" = "deny"\n"~/.oci" = "deny"\n"~/.aws" = "deny"\n"~/.config/gcloud" = "deny"\n'
    # v4.8.18 (team-lead's broader ~/.config sweep, second CF pass): denied
    # by NAME after actually enumerating this host's ~/.config -- not a
    # guess. `~/.config/git`'s `[credential] helper` line points AT gh
    # (`!/usr/bin/gh auth git-credential`), so it's credential-adjacent even
    # though it holds no secret itself; `~/.config/k9s` is kube-adjacent
    # (cached contexts/exec-plugin auth); the two chrome-for-testing profile
    # dirs are e2e browser profiles that can carry cookies/saved site auth
    # from whatever a test run last logged into. Also denying this host's
    # OTHER agent credential stores, found the same way, not documented
    # anywhere else this profile would otherwise know to avoid: `~/.claude`
    # and `~/agents/claude` both carry a live `.credentials.json`
    # (Claude Code's own OAuth token).
    #
    # NOT denying `~/agents/codex` (measured, reverted): `~/.codex` is a
    # SYMLINK to it, but `~/agents/codex` is not just credential/session
    # data -- it's also codex's OWN INSTALLED BINARY
    # (`~/agents/codex/packages/standalone/releases/.../bin/codex`). Denying
    # the real directory broke codex outright: "bwrap: execvp
    # .../bin/codex: Permission denied" -- codex re-execs its own installed
    # binary as part of building the sandboxed session, so that path must
    # stay readable+executable for codex to start AT ALL under this
    # profile, regardless of anything else. The `~/.codex` symlink deny
    # above is what actually matters for credentials: the sandboxed
    # reviewer's own CODEX_HOME is CODEX_HOME_ROUND (this per-round,
    # already-composed home), never the real one -- it has no legitimate
    # reason to read `~/.codex` OR `~/agents/codex` by any path, credential
    # or binary, during normal operation.
    printf '"~/.config/git" = "deny"\n"~/.config/k9s" = "deny"\n'
    printf '"~/.config/google-chrome-for-testing" = "deny"\n"~/.config/google-chrome-for-testing-headless" = "deny"\n'
    printf '"~/.claude" = "deny"\n"~/agents/claude" = "deny"\n'
    # ABSOLUTE paths only, direct keys under [...filesystem] -- must come
    # BEFORE the ":workspace_roots" sub-table opens below, or TOML scopes
    # them into that sub-table instead of the table these keys are meant for
    # (the exact class of ordering mistake the default_permissions comment
    # above already warns about, just one level deeper).
    for _p in "$RGOCACHE" "$RGOMODCACHE" "$RGOTMPDIR" "$RGOPATH" "$RUVCACHE" "$RTMPDIR"; do
      printf '"%s" = "write"\n' "$_p"
    done
    printf '\n[permissions.codex-review.filesystem.":workspace_roots"]\n"**/.env" = "deny"\n"**/env.local" = "deny"\n'
    # See the KNOWN LIMITATION comment above where RPERMS branches:
    # enabled=true is REQUIRED for the unix_sockets allow below to take
    # effect at all (chris ruling 2026-09-10, Option A) -- it also opens
    # full network egress for this round (domain filtering needs the
    # experimental, disabled network_proxy feature, which breaks this
    # allow entry when turned on). dangerously_allow_all_unix_sockets stays
    # false -- true reaches every socket on the host, not just docker.sock.
    printf '\n[permissions.codex-review.network]\nenabled = true\ndangerously_allow_all_unix_sockets = false\n\n[permissions.codex-review.network.unix_sockets]\n"/var/run/docker.sock" = "allow"\n'
  } > "$CODEX_HOME_ROUND/config.toml" || die "cannot write $CODEX_HOME_ROUND/config.toml"
  chmod 600 "$CODEX_HOME_ROUND/config.toml"
  python3 -c "import tomllib,sys; d=tomllib.load(open(sys.argv[1],'rb')); assert d.get('default_permissions')=='codex-review', d.get('default_permissions'); assert 'codex-review' in d.get('permissions',{})" "$CODEX_HOME_ROUND/config.toml" \
    || die "per-round CODEX_HOME config.toml at $CODEX_HOME_ROUND/config.toml failed its own TOML/key sanity check -- refusing to launch codex against a config that might not mean what this wrapper intended"
  ln -s "$CODEX_HOME_REAL/auth.json" "$CODEX_HOME_ROUND/auth.json" || die "cannot symlink auth.json into per-round CODEX_HOME"
  # Same flipped classification as the base preflight above: only call it
  # "not logged in" when the text actually says so; anything else is this
  # per-round config's own fault, not the real ~/.codex credentials'.
  _login_out=$(CODEX_HOME="$CODEX_HOME_ROUND" codex login status 2>&1) || {
    case "$_login_out" in
      *[Nn]ot\ logged\ in*|*[Ll]og\ in*|*401*|*[Uu]nauthorized*|*auth.json*)
        die "per-round CODEX_HOME reports not logged in -- CODEX_HOME=$CODEX_HOME_ROUND (the auth.json symlink may be stale or the real ~/.codex/auth.json invalid). codex said verbatim: $_login_out" ;;
      *)
        die "per-round CODEX_HOME config failed to load (NOT an auth problem -- the composed config.toml at $CODEX_HOME_ROUND/config.toml is at fault, not real ~/.codex credentials). codex said verbatim: $_login_out" ;;
    esac
  }
  CODEXHOME_ENV_ARGS+=(CODEX_HOME="$CODEX_HOME_ROUND")
  # CF read ASK, REVISED (team-lead, second pass): with the `-s
  # workspace-write` cmdline flag gone in this mode (SFLAG_ARGS stays empty
  # -- see above), CF's own /proc-based round-proof reader has nothing on
  # the command line to hash any more. `profile_sha` names the ACTUAL
  # composed config.toml's own hash (not just a mode label) so a reader can
  # verify exactly which deny/write rules this specific round ran under,
  # the same way a wrapper sha already lets a reader verify which CODE ran.
  PROFILE_SHA=$(sha256sum "$CODEX_HOME_ROUND/config.toml" | cut -d' ' -f1)
  PROOF_LINE="perms: mode=codex-review codex_home=$CODEX_HOME_ROUND profile_sha=$PROFILE_SHA"
  printf '%s\n' "$PROOF_LINE" >> "$L"
  # Reviewer's testcontainers/go-test env. DOCKER_HOST left unset
  # deliberately -- the round reaches the root socket via the profile's
  # unix_sockets allowlist, not a remapped endpoint. RYUK disabled because
  # this wrapper's own pre/post label-based reap (below) is the cleanup
  # mechanism, not the ryuk sidecar container (which would itself need a
  # separate docker.sock grant this profile does not extend to it).
  #
  # KNOWN GAP (dry-run 1, 01:22:05Z): TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX
  # only redirects testcontainers-go's OWN pulls -- a reviewer typing a raw
  # `docker run <image>` still hits Docker Hub directly (dry-run 1's
  # `docker run --rm hello-world` pulled from Hub, not the oci-cache mirror).
  # Accepted as-is: the mandated proof commands are `go test -tags=integration`
  # (which DOES honor the prefix) and the prompt tells the reviewer to run
  # the actual test suite, not hand-roll `docker run` calls -- but if a
  # reviewer ever does invoke `docker run` directly, expect a Hub pull, not
  # a mirror hit.
  #
  # KNOWN GAP (dry-run 2, 01:25:12Z): the reviewer's own `bash -lc` line put
  # the testcontainer DSN password directly in argv (`DEV_HEALTH_TEST_
  # CLICKHOUSE_DSN='clickhouse://worker_test:worker_test_password@...'`).
  # Harmless in that round (throwaway per-round creds, container torn down
  # at test end) but the SAME class of leak Trap #121 exists to stop --
  # prompt-of-record guidance should tell reviewers to pass DSNs via env,
  # never inline in the command string, even for throwaway scratch creds.
  #
  # ACCEPTED (dry-run 2, same round): `uv sync --all-extras --dev
  # --no-install-project` ran inside the sandbox over the now-open network
  # (to fix a missing `sqlalchemy` before a Python-side check) -- expected
  # and accepted under the Option A open-egress ruling above (R87), not a
  # new gap, noted here for the record.
  CODEXHOME_ENV_ARGS+=(
    TESTCONTAINERS_RYUK_DISABLED=true
    TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX="${CODEX_REVIEW_HUB_PREFIX:-ghcr.io/full-chaos}"
    CODEX_REVIEW_ROUND_LABEL="$NAME-$TS"
  )
fi
DOCKER_ENABLED=0
[ "$RPERMS" = "codex-review" ] && [ "$RSANDBOX" = "workspace-write" ] && DOCKER_ENABLED=1

# v4.8.18 PRE-ROUND REAP: containers labelled codex-review-round from a
# killed/superseded round (Trap #7, CORE R76 rule #7) leak otherwise -- reap
# anything older than 2h before this round claims the label namespace. Never
# touches a container without the label; never touches `dev-health` (no
# filter on that project name here at all).
if [ "$DOCKER_ENABLED" -eq 1 ]; then
  _stale=$(docker ps -aq --filter 'label=codex-review-round' --filter 'status=exited' 2>/dev/null || true)
  if [ -n "$_stale" ]; then
    _now_epoch=$(date +%s)
    for _cid in $_stale; do
      _started=$(docker inspect -f '{{.State.StartedAt}}' "$_cid" 2>/dev/null || true)
      [ -z "$_started" ] && continue
      _started_epoch=$(date -d "$_started" +%s 2>/dev/null || echo "$_now_epoch")
      if [ $((_now_epoch - _started_epoch)) -gt 7200 ]; then
        docker rm -f "$_cid" >/dev/null 2>&1 || true
      fi
    done
  fi
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
printf 'round-bounds: GOFLAGS=%s GOMAXPROCS=%s GOCACHE=%s GOMODCACHE=%s GOTMPDIR=%s GOPATH=%s UV_CACHE_DIR=%s TMPDIR=%s sandbox=%s perms=%s\n' \
  "$RGOFLAGS" "$RGOMAXPROCS" "$RGOCACHE" "$RGOMODCACHE" "$RGOTMPDIR" "$RGOPATH" "$RUVCACHE" "$RTMPDIR" "$RSANDBOX" "$RPERMS" >> "$L"
warn "round-provenance: $PROV"

# v4.8.15 CODEGRAPH PATH DENY -- chris's ruling is absolute: "it shouldn't be
# in the harness to index it" covers a reviewer choosing to invoke codegraph
# on its own, not just the wrapper's own removed base-checkout mechanism.
# v4.8.14 stopped the WRAPPER from indexing; a reviewer running `codegraph
# orient`/`symbols`/`refs`/`callers` on its own initiative still indexes on
# bigboy -- confirmed LIVE (lane-local-stack's v4.8.14 dry-run on #2312
# produced a real .codegraph/ dir in the round worktree this way, unprompted,
# despite the prompt no longer mentioning codegraph at all). Two-part fix:
# (1) a per-round stub directory, prepended to PATH in the SAME env
# invocation that already reaches codex exec's real environment (same
# enforcement-in-the-environment pattern as the Go bounds and the old
# codegraph timeout shim this replaces), shadows the real `codegraph` binary
# with a one-line refusal; (2) any `.codegraph` dir already present in the
# round worktree is deleted before codex starts, so a stale one from an
# earlier interrupted round can't linger either.
#
# v4.8.17 (CHAOS-5374, root-caused 09-06): the v4.8.15 PATH stub above is NOT
# effective inside `codex exec`, because codex wraps every shell tool call in
# `bash -lc '...'`, a LOGIN shell that re-sources /etc/profile ->
# /etc/profile.d/oci-worker-path.sh, which unconditionally re-prepends
# $HOME/.local/bin -- where the REAL codegraph symlink lives -- ahead of
# whatever PATH the env invocation below supplied. Measured: `env
# PATH="$CODEGRAPH_DENY_DIR:$PATH" bash -lc 'codegraph --version'` prints the
# real 1.6.0 build, the stub never runs. Fixed with two independent layers,
# both carried the same way the Go bounds already are -- as extra vars in the
# SAME env invocation that reaches codex exec, no shared/system file touched:
#   1. BASH_FUNC_codegraph%%, a bash-exported SHELL FUNCTION. Bash's command
#      lookup checks shell functions before searching PATH, so this wins over
#      the real binary regardless of what any profile script does to PATH --
#      profile.d only ever touches the PATH variable, never shell functions.
#      Proven empirically to survive both a login `bash -lc` AND a bash -lc
#      spawned from within another bash -lc (mimicking codex's one-exec-block-
#      per-command-string pattern), because BASH_FUNC_* is an ordinary
#      environment variable and is therefore inherited by every descendant
#      process the same way GOCACHE etc. already are -- nothing re-imports or
#      clears it. This is the primary layer: it defends the common case, a
#      bare `codegraph ...` invocation.
#   2. BASH_ENV, pointed at a scratch file (under the same
#      CODEGRAPH_DENY_DIR) that unconditionally re-prepends
#      $CODEGRAPH_DENY_DIR onto PATH. Measured on this host's bash
#      (5.2.21): a login `bash -lc 'CMD'` sources BASH_ENV AFTER /etc/profile
#      and /etc/profile.d, so this re-prepend lands last and wins the PATH
#      race even against oci-worker-path.sh. This is belt-and-braces for the
#      one gap layer 1 cannot close: `command codegraph ...` or `\codegraph`
#      explicitly bypass shell-function lookup and fall through to a plain
#      PATH search, which layer 2 still catches.
# Neither layer defends an absolute-path invocation
# ($HOME/.codegraph/versions/*/bin/codegraph or the symlink path) -- that gap
# pre-dates this fix (v4.8.15 had it too, PATH shadowing never covers
# absolute paths) and is out of scope for CHAOS-5374, which is about the
# by-name deny being bypassable, not about closing every possible path a
# reviewer could type.
CODEGRAPH_DENY_DIR="$RGOTMPDIR/codegraph-deny"
mkdir -p "$CODEGRAPH_DENY_DIR" || die "cannot create codegraph-deny stub dir $CODEGRAPH_DENY_DIR"
cat > "$CODEGRAPH_DENY_DIR/codegraph" <<'CODEGRAPH_DENY'
#!/usr/bin/env bash
echo "codegraph is disabled in bigboy review rounds (chris 2026-09-06); use rg" >&2
exit 2
CODEGRAPH_DENY
chmod +x "$CODEGRAPH_DENY_DIR/codegraph" || die "cannot make codegraph-deny stub executable"
# v4.8.17: BASH_ENV target -- re-prepends the stub dir on every login-shell
# `bash -lc` codex spawns, AFTER /etc/profile.d has already run, closing the
# `command codegraph`/backslash-escape gap the exported function (below)
# cannot close.
CODEGRAPH_BASH_ENV="$CODEGRAPH_DENY_DIR/bash_env.sh"
# UNCONDITIONAL prepend, deliberately not guarded by a "not already present"
# check: a guard that skips when the dir already appears ANYWHERE in PATH
# (e.g. from the outer env invocation's own PATH= below) does not guarantee
# it is FIRST -- measured live: with a "skip if present" guard, the dir sat
# behind three profile.d-reasserted /home/ubuntu/.local/bin entries and the
# real binary won again. A stray duplicate entry from re-prepending every
# time is harmless; losing the race is not.
cat > "$CODEGRAPH_BASH_ENV" <<BASHENV
PATH="$CODEGRAPH_DENY_DIR:\$PATH"
export PATH
BASHENV
# v4.8.17: the exported shell FUNCTION -- wins over PATH lookup entirely for
# a bare `codegraph` invocation, so it is not exposed to the profile.d
# re-prepend race at all. Built as a plain shell variable first (not inlined
# into the env invocation below) so the function body is defined in exactly
# one place.
CODEGRAPH_DENY_FUNC='() { echo "codegraph is disabled in bigboy review rounds (chris 2026-09-06); use rg" >&2; return 2; }'
printf 'codegraph-deny: stub=%s bash_env=%s func=exported\n' "$CODEGRAPH_DENY_DIR" "$CODEGRAPH_BASH_ENV" >> "$L"
rm -rf "$RW/.codegraph" 2>/dev/null || true

# NOTE THE APPEND. This redirect was `> "$L"`; it MUST stay `>>` now, or codex
# truncates the provenance line written immediately above and the log silently
# reverts to having no provenance at all -- which reads as LOCAL, the safe
# default, so nothing would ever look broken.
#
# CF read finding: `env` without `-i` inherits the REST of this wrapper's own
# environment by default, not just the VAR=val list below -- if GH_TOKEN or
# GITHUB_TOKEN happen to be set in whatever shell launched this wrapper (none
# are on this host today), they would reach the round unfiltered, and under
# codex-review mode's open network that IS reachable by the reviewer over
# the GitHub API. `-u` strips them explicitly regardless of mode, not
# conditioned on RPERMS -- harmless when they were never set, load-bearing
# the day something upstream starts exporting one.
( cd "$RW" && env \
    -u GH_TOKEN -u GITHUB_TOKEN -u OPENAI_API_KEY -u OPENAI_BASE_URL -u OPENAI_ORG_ID \
    PATH="$CODEGRAPH_DENY_DIR:$PATH" \
    BASH_ENV="$CODEGRAPH_BASH_ENV" \
    "BASH_FUNC_codegraph%%=$CODEGRAPH_DENY_FUNC" \
    GOFLAGS="$RGOFLAGS" \
    GOMAXPROCS="$RGOMAXPROCS" \
    GOCACHE="$RGOCACHE" \
    GOMODCACHE="$RGOMODCACHE" \
    GOTMPDIR="$RGOTMPDIR" \
    GOPATH="$RGOPATH" \
    UV_CACHE_DIR="$RUVCACHE" \
    TMPDIR="$RTMPDIR" \
    ${CODEXHOME_ENV_ARGS[@]+"${CODEXHOME_ENV_ARGS[@]}"} \
    codex --no-daemon exec -m "$MODEL" -c "model_reasoning_effort=\"$EFF\"" \
    ${SANDBOX_ARGS[@]+"${SANDBOX_ARGS[@]}"} \
    ${PERMS_ARGS[@]+"${PERMS_ARGS[@]}"} \
    ${SFLAG_ARGS[@]+"${SFLAG_ARGS[@]}"} \
    -C "$RW" -o "$V" - < prompt.md ) >> "$L" 2>&1 || RC=$?

# v4.8.18 POST-ROUND DOCKER TEARDOWN: runs regardless of $RC (a failed round
# can still have started containers). Only ever touches containers carrying
# THIS round's own label value -- never `dev-health`, never any other lane's
# `codex-review-round=<other>` containers.
if [ "$DOCKER_ENABLED" -eq 1 ]; then
  _before=$(docker ps -aq --filter "label=codex-review-round=$NAME-$TS" 2>/dev/null | wc -l | tr -d ' ')
  docker ps -aq --filter "label=codex-review-round=$NAME-$TS" 2>/dev/null | xargs -r docker rm -f >/dev/null 2>&1 || true
  _after=$(docker ps -aq --filter "label=codex-review-round=$NAME-$TS" 2>/dev/null | wc -l | tr -d ' ')
  printf 'docker: profile=codex-review socket=allow containers_before=%s containers_after=%s leaked=%s\n' \
    "$_before" "$_after" "$_after" >> "$L"
fi

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
# v4.8.21 (CHAOS-6906, Trap #419): GO_EXECS, PY_EXECS and BLOCKED_HITS come from ONE
# awk pass that reads each exec block's WHOLE command body. The old counters looked at
# `grep -A1 '^exec$'`, i.e. only the first command line, so a reviewer that ran a
# multi-line `bash -lc` script (go build/vet/test on line 15) counted as 0 and the
# round was stamped VOID IN FORM although it had executed (#3276 r1: 39 blocks, 9 go
# test/run/build, 3 python, old GO_EXECS=0). The classifier below is the text between
# the EXEC-CLASSIFY markers; tools/codex-review/test-exec-classify.sh extracts and
# runs exactly that text against fixtures.
BLOCKED_PAT='operation not permitted|cannot create entries|failed to initialize build cache|Read-only file system'
EXEC_CLASSIFY_AWK=$(cat <<'AWK'
# EXEC-CLASSIFY-BEGIN
# Classifies every exec block of a codex round log by scanning its WHOLE command
# body, not only its first line. codex wraps each command in `/bin/bash -lc '...'`
# and a reviewer often runs a multi-line script, so `go test`/`pytest`/... sit on a
# later line (#3276 r1: 39 exec blocks, every go build/vet/test on a body line, the
# leading-line-only counter said 0 and stamped the round VOID IN FORM, Trap #419).
#
# A block is `exec`, the command line(s), a status line (` succeeded|failed|exited N
# in <n>ms`), then the command's output, up to the next `exec`. The first command
# line keeps the old permissive match (the verb anywhere on it, as `bash -lc 'cd x &&
# go test'` needs); later lines match only where a command can stand (line start or
# after && || ; |, past VAR=val / time / env prefixes), so a string that merely
# mentions `go test` is not a run.
#
# Prints "<go blocks> <py blocks> <go blocks that FAILED and whose output shows the
# sandbox refusing a path the wrapper configured> <diff-seen blocks> <unresolved blocks>".
#
# v4.8.22 (CHAOS-6948): the fourth number counts blocks that SUCCEEDED and either ran
# `git diff` (a command segment starting `git [-C dir] diff`) or read the review patch
# (PATCH_NAME, e.g. .codex-review.patch, named on the command line of a segment that
# starts with a reading verb). A FAILED `git diff` (the denied-worktree case that
# motivated this) and a failed read do not count. PATCH_NAME empty: only `git diff`.
#
# CHAOS-7018/D2822 (5th number, UNRESOLVED/UNKNOWN blocks): the state machine through
# v4.8.22 tracked exactly ONE open block at a time and flushed it the instant the NEXT
# `exec` marker appeared, whether or not a status line had ever resolved it. codex does
# not always print a block's status immediately after its command -- when several tool
# calls are issued before any of their results print (a real, reproduced, NOT RARE shape:
# chaos-6908-3360-r1's first attempt shows four `exec` markers back to back with zero
# status/output between them), the old machine silently scored every block caught in that
# burst as "not ok" -- including a `.codex-review.patch` read that genuinely succeeded --
# because it never got the chance to see a status line before being flushed.
#
# Fixed with a FIFO PENDING QUEUE (Q_*[] arrays, front..tail) instead of one scalar
# block: every `exec` marker PUSHES a new tail entry without flushing anything. A status
# line resolves the OLDEST still-pending entry (front), in ISSUE order -- never
# necessarily the block whose command text sits immediately above it in the log, which is
# only the same entry when nothing is mid-burst (the ordinary, non-batched case, where
# front and tail are always the same index and this reduces to the old behavior exactly).
# A block still pending at EOF -- its status never arrived -- is counted as UNKNOWN, the
# 5th number, and is NEVER folded into "diffs" (or gos/pys) as if it had been confirmed:
# the same "no repro = non-existent" discipline this whole file applies everywhere else.
# BLOCKED_PAT (sandbox-denial output scanning for go-test failures) is only attributed to
# a block resolved WHILE it is still the tail (front == tail at resolution) -- the output
# text immediately following an out-of-order resolution belongs to a LATER block, not the
# one just resolved, and attributing it would corrupt `hits` rather than merely miss it.
#
# r1 P1 fix (#3368): the status-line check below used to be gated on `phase == "cmd"`,
# which only holds for the ONE line immediately after an `exec` marker -- so a burst of
# results (three execs, THEN three status lines back to back, no exec markers between the
# statuses) resolved only the FIRST status and silently dropped the rest as bare output
# text, undercounting diffs and overcounting unknown. Reproduced by the reviewer: 3
# queued execs (go test/ls/patch-read) followed by all 3 statuses gave `1 0 0 0 2`
# instead of the correct `1 0 0 1 0`. A status line unambiguously resolves the oldest
# PENDING entry regardless of phase -- there is always at most one unambiguous "next"
# entry to resolve as long as front<=tail, so the check now fires whenever an entry is
# still pending, not only right after an `exec` marker.
function norm(s) {
  sub(/^[ \t(]+/, "", s)
  while (match(s, /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/)) s = substr(s, RLENGTH + 1)
  while (match(s, /^(time|env|exec|command|nice|sudo)[ \t]+/)) s = substr(s, RLENGTH + 1)
  return s
}
function seg_go(s) { return s ~ /^([^ \t]*\/)?go[ \t]+(test|run|build)([^A-Za-z0-9_]|$)/ }
function seg_py(s) {
  return s ~ /^([^ \t]*\/)?(pytest|ruff|mypy|python3|python|shellcheck)([^A-Za-z0-9_]|$)/ ||
         s ~ /^uv run([^A-Za-z0-9_]|$)/ ||
         s ~ /^(bash|sh)[ \t]+(-[nx][ \t]+)*[^'";|&<>]*\.sh([^A-Za-z0-9_]|$)/
}
function first_go(s) { return s ~ /go (test|run|build)/ }
function first_py(s) {
  return s ~ /(^|[^A-Za-z0-9_])(pytest|ruff|mypy|python3|python|shellcheck|py_compile)([^A-Za-z0-9_]|$)/ ||
         s ~ /uv run|\.venv\/bin\/pytest/ ||
         s ~ /(^|[^A-Za-z0-9_])(bash|sh) +(-[nx] +)*[^'";|&<>]*\.sh([^A-Za-z0-9_]|$)/
}
function first_diff(s) { return s ~ /(^|[^A-Za-z0-9_])git[ \t]+(-C[ \t]+[^ \t]+[ \t]+)?diff([^A-Za-z0-9_-]|$)/ }
function first_read(s) { return PATCH_NAME != "" && index(s, PATCH_NAME) > 0 && s ~ /(^|[^A-Za-z0-9_])(cat|sed|head|tail|less|more|rg|grep|awk|nl|bat)[ \t]/ }
function seg_gitdiff(s) { return s ~ /^([^ \t]*\/)?git[ \t]+(-C[ \t]+[^ \t]+[ \t]+)?diff([^A-Za-z0-9_-]|$)/ }
function seg_read(s) { return PATCH_NAME != "" && index(s, PATCH_NAME) > 0 && s ~ /^([^ \t]*\/)?(cat|sed|head|tail|less|more|rg|grep|awk|nl|bat)[ \t]/ }
# front=1, tail=0: an empty queue where the NEXT push (tail becomes 1) immediately makes
# front == tail == 1, the single-pending-entry state every existing (non-batched) fixture
# exercises. awk auto-inits unset numerics to 0, which would wrongly make front 0 (queue
# index 0 is never used) -- set it explicitly rather than rely on the implicit default.
BEGIN { front = 1; tail = 0 }
# finalize(i): counts gos/pys for entry i EXACTLY once, the moment its own command text is
# known complete -- whether or not it EVER gets a status line. gos/pys measure ATTEMPTED
# go/py commands (the same contract this counter has always had: a killed round's last,
# never-resolved block still counted, see the "unterminated last block" fixture below) --
# unlike diffs/hits, which require a CONFIRMED resolution and are counted in resolve() only.
function finalize(i) {
  if (i < 1 || Q_counted[i]) return
  gos += Q_is_go[i]; pys += Q_is_py[i]
  Q_counted[i] = 1
}
# resolve(): FIFO-dequeues the oldest pending entry (Q[front]) against a status line just
# read (this_ok/this_failed), finalizes it (gos/pys) if that has not already happened via a
# later `exec` marker, counts diffs on a CONFIRMED success, and decides whether output
# scanning for BLOCKED_PAT may be attributed to it (only when it was still the tail -- see
# the header comment). Always advances front by one.
function resolve(this_ok, this_failed,    attributable) {
  if (front > tail) return  # a status line with no pending entry at all: ignore, nothing to resolve
  finalize(front)
  if (this_ok && (Q_is_diff[front] || Q_is_read[front])) diffs++
  attributable = (front == tail)
  if (attributable) {
    out_target = front; out_go = Q_is_go[front]; out_failed = this_failed
  } else {
    out_target = 0  # output that follows belongs to a LATER (still-open) block, not this one
  }
  front++
}
/^exec$/ {
  finalize(tail)  # the previous tail's command text is now complete, whatever happens next
  tail++
  Q_is_go[tail] = 0; Q_is_py[tail] = 0; Q_is_diff[tail] = 0; Q_is_read[tail] = 0; Q_ncmd[tail] = 0
  phase = "cmd"
  next
}
front <= tail && $0 ~ /^ *(succeeded|failed|exited [0-9]+) in [0-9]+ms/ {
  resolve($0 ~ /^ *succeeded in/, $0 ~ /^ *failed in/)
  phase = "out"
  next
}
tail >= front && phase == "cmd" {
  Q_ncmd[tail]++
  if (Q_ncmd[tail] == 1) {
    if (first_go($0)) Q_is_go[tail] = 1
    if (first_py($0)) Q_is_py[tail] = 1
    if (first_diff($0)) Q_is_diff[tail] = 1
    if (first_read($0)) Q_is_read[tail] = 1
  }
  line = $0
  gsub(/&&|\|\||;|\||\$\(|`/, "\n", line)
  n = split(line, segs, "\n")
  for (i = 1; i <= n; i++) {
    seg = norm(segs[i])
    if (seg_go(seg)) Q_is_go[tail] = 1
    if (seg_py(seg)) Q_is_py[tail] = 1
    if (seg_gitdiff(seg)) Q_is_diff[tail] = 1
    if (seg_read(seg)) Q_is_read[tail] = 1
  }
  next
}
phase == "out" && out_target != 0 && !hit_counted[out_target] && out_go && out_failed && $0 ~ BLOCKED_PAT {
  hits++; hit_counted[out_target] = 1
}
END {
  finalize(tail)  # an unterminated last block (round killed mid-command) still attempted go/py
  unknown = (front <= tail) ? (tail - front + 1) : 0
  print gos + 0, pys + 0, hits + 0, diffs + 0, unknown + 0
}
# EXEC-CLASSIFY-END
AWK
)
EXEC_CLASSIFY=$(awk -v BLOCKED_PAT="$BLOCKED_PAT" -v PATCH_NAME="${REVIEW_PATCH_NAME:-}" "$EXEC_CLASSIFY_AWK" "$L" 2>/dev/null || true)
if [ -z "$EXEC_CLASSIFY" ]; then
  # The measurement did not happen: say so, and fall back to the old first-line
  # counters rather than reading a silent zero as "nothing executed".
  warn "EXEC CLASSIFIER PRODUCED NO OUTPUT: falling back to the first-line counters (v4.8.20 behaviour); a multi-line script can be undercounted"
  GO_EXECS=$(grep -A1 '^exec$' "$L" 2>/dev/null | grep -cE 'go (test|run|build)' || true); GO_EXECS=${GO_EXECS:-0}
  PY_EXECS=0
  BLOCKED_HITS=0
  DIFF_SEEN=1  # unmeasured: never stamp VOID on a measurement that did not happen
  DIFF_UNKNOWN=0  # same reasoning: the classifier itself did not run, nothing to report as pending
else
  read -r GO_EXECS PY_EXECS BLOCKED_HITS DIFF_SEEN DIFF_UNKNOWN <<<"$EXEC_CLASSIFY"
fi
# v4.8.13 (team-lead spec, from #2306 r1-v4): the counter above only matched
# Go verbs, so a Python-only diff's round -- reviewer ran `pytest` (52/52),
# `uv run`, ruff, mypy -- was falsely declared "VOID IN FORM: 0 go
# test/run/build exec blocks" despite genuinely having executed the evidence
# it cited. Same unanchored-substring shape as the Go pattern above (matches
# regardless of a leading backtick or path prefix, e.g. a fenced ```pytest```
# block or `.venv/bin/pytest` both contain the bare substring already).
# v4.8.13 (CF nit on the same round): bare `ruff`/`mypy`/`pytest` need word
# boundaries -- "truffle" contains `ruff`, and an unbounded match widens
# VOID IN FORM's guard in the UNSAFE direction (a false PY_EXECS hit could
# mask a round that genuinely executed nothing). `\b` is a GNU grep
# extension; confirmed on bigboy's actual grep (GNU grep 3.11) -- this
# wrapper is Linux-only, so no portability concern. `.venv/bin/pytest` still
# matches `\bpytest\b`: `/` is a non-word character, so the boundary exists
# right before `pytest` same as at a plain word start.
# v4.8.16 (CHAOS-5346, team-lead spec): same false-VOID shape as v4.8.13's
# fix, one layer down -- a round whose diff is shell-only or plain-Python
# (no pytest/uv/ruff/mypy, no go verbs at all) genuinely executed real
# commands (`bash`/`sh` SCRIPT invocations, `python3`, `python`,
# `shellcheck`, `py_compile`) but was still stamped VOID IN FORM, because
# neither counter recognised those verbs (ops #2330 confirm, acr #465).
# Added to PY_EXECS rather than a new category, since the message text
# below already reads as "the non-Go-verb bucket" and a third bucket would
# need its own wiring through EXECUTED_EXECS/warn/VOID-IN-FORM for no
# discriminating benefit.
#
# `bash`/`sh` are DELIBERATELY NOT bare `\b`-bounded verbs like the other
# four -- codex wraps EVERY exec block in `/bin/bash -lc '...'`, so a bare
# `\bbash\b` matches 100% of exec lines regardless of what runs inside the
# quotes (measured: a real Go-only round, chaos-5319-2327-r1c, went from
# PY_EXECS=0 to 48/48 exec blocks -- every single one -- with a bare
# `\bbash\b`/`\bsh\b`, which makes EXECUTED_EXECS effectively always nonzero
# whenever EXEC_BLOCKS>0 and guts VOID-IN-FORM's whole purpose). Root cause
# of #2330's own false VOID was different and narrower: the executed
# evidence was `bash scripts/battery/fetch_modules.sh` SCRIPT invocations
# (rc=17/rc=19 controls), not the wrapper. The pattern below matches only a
# script INVOCATION -- `bash`/`sh`, optional `-n`/`-x` flags, then a path
# ending `.sh` -- which the wrapper's own `/bin/bash -lc '` can never
# satisfy (no `.sh` token appears before the opening quote). Covers `bash
# x.sh`, `bash -n x.sh`, `PATH=... bash scripts/y.sh`. A `cat x.sh`/`rg
# ... x.sh` reference is NOT preceded by `bash`/`sh` + whitespace, so it
# does not match. `python -m py_compile` is covered by the bare
# `\bpy_compile\b` match (the substring appears verbatim regardless of
# invocation form); `\bpython\b` additionally matches inside `python3` at
# the boundary before the digit (word chars include digits, so `\bpython\b`
# does NOT match the "python" in "python3" -- that is covered by the
# separate `\bpython3\b` alternative).
EXECUTED_EXECS=$((GO_EXECS + PY_EXECS))
# r1 P2 fix (#3368): DIFF_UNKNOWN used to surface only inside the DIFF_SEEN=0 branch below
# -- a round with a CONFIRMED diff (DIFF_SEEN=1) but SOME blocks still unresolved (e.g. a
# batched burst where the LAST pending entry never got a status line at all) printed no
# trace of that ambiguity anywhere in its log.
# D2827: printed UNCONDITIONALLY, literally always part of the line -- even at 0 -- not
# appended only "when >0". A conditional append makes the count's ABSENCE ambiguous (did
# nothing go unresolved, or did this build of the wrapper simply not carry the fix?); a
# fixed literal position never leaves that open.
warn "round recorded $EXEC_BLOCKS exec block(s) ($GO_EXECS go test/run/build, $PY_EXECS pytest/uv run/ruff/mypy/bash-or-sh-script/python/shellcheck/py_compile, ${DIFF_UNKNOWN:-0} unresolved/unknown)"

if [ "${DIFF_SEEN:-0}" -eq 0 ]; then
  if [ "${DIFF_UNKNOWN:-0}" -gt 0 ]; then
    # CHAOS-7018/D2822: some exec block(s) never got a resolved status (a batched-tool-call
    # framing gap, not necessarily an unattempted diff read) -- say AMBIGUOUS, not a flat
    # "did not see the diff", which overclaims certainty this log does not actually support.
    warn "VOID IN FORM (AMBIGUOUS): the log shows no CONFIRMED \`git diff\` exec or read of ${REVIEW_PATCH_NAME:-the review patch} (review patch: $REVIEW_PATCH_STATUS), but $DIFF_UNKNOWN exec block(s) never resolved a status line (batched tool calls) -- this cannot be certified as 'the round never saw the diff', only as unproven; relaunch or escalate"
  else
    warn "VOID IN FORM: the log shows no successful \`git diff\` exec and no successful read of ${REVIEW_PATCH_NAME:-the review patch} (review patch: $REVIEW_PATCH_STATUS) -- the round did not see the diff it certifies; do not ledger this verdict, relaunch or escalate"
  fi
fi

# v4.8.8: under workspace-write the round was explicitly told execution is
# REQUIRED (STANDING_RULES_EXEC above) -- a zero here is not "reasoned, not
# executed" the way a read-only round's zero would be, it is the round
# disobeying its own mandatory instructions (CHAOS-5249 r1: exactly this,
# caused by a wrapper-owned paragraph that has since been fixed, not by the
# reviewer choosing to skip it -- but the check stays regardless of cause,
# since a future round could skip it for a different reason). Printed ABOVE
# the verdict so it cannot be missed by a reader who only reads the last
# line.
if [ "$RSANDBOX" = "workspace-write" ] && [ "$EXECUTED_EXECS" -eq 0 ]; then
  warn "VOID IN FORM: reviewer executed nothing (workspace-write round, 0 go test/run/build and 0 pytest/uv run/ruff/mypy/bash/sh/python/shellcheck/py_compile exec blocks) -- do not ledger this verdict as executed evidence; relaunch or escalate"
fi

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
# BLOCKED_HITS (go test/run/build blocks that FAILED with the sandbox refusing a path
# the wrapper configured) is computed by the classifier above, over the whole body.
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

# v4.8.6 (found in the field the same day: a lane keying only on "does stdout
# contain a VERDICT= line", not on the exit code, misread a FAILED round as
# a real verdict). On a non-zero codex exit, $V is not trustworthy -- it may
# not exist, or may hold a partial/stale write -- so this no longer prints
# `VERDICT=$V` at all on that path; a caller pattern-matching stdout lines
# must not be able to mistake this for a real verdict path. `NO VERDICT
# (codex rc=N)` is deliberately NOT shaped like the real `VERDICT=<path>`
# line below, so the two cannot be confused by a naive grep.
[ "$RC" -eq 0 ] || { warn "codex exited rc=$RC — read $L"; printf 'NO VERDICT (codex rc=%s)\n' "$RC"; exit "$RC"; }
# v4.8.13 verdict-fallback lookup (CF finding, live v4.8.12 round): before
# this used to `die` unconditionally the instant $V was empty. On that round
# codex had written a COMPLETE verdict, just not at $V -- root-caused above
# to a relative -o resolving against $RW after the `cd`, now fixed at the
# source (OUTDIR is absolute from the point it is set). This lookup is the
# belt to that braces: if some other path ever puts codex's actual output
# somewhere other than $V again, search the review worktree -- still alive
# here, not yet cleaned up -- for the expected filename first, then for the
# newest .md file created since this round's own launch, before declaring a
# real verdict lost to a path mismatch.
#
# VERDICT_RE is hoisted here from its historical definition point further
# below (see the big comment block down there for the regex's full
# rationale/history) SPECIFICALLY so this lookup can use it -- CF REQUIRED
# CHANGE: a candidate is copied to $V ONLY if its own last non-blank line
# validates against this same rule. Without that check, a fallback could
# promote an unrelated .md the reviewer merely touched while testing (a repo
# doc it edited, a scratch note) into the verdict path, which is worse than
# declaring NO VERDICT: a wrong file that LOOKS like a verdict is silently
# ledgered as one.
VERDICT_RE='^[[:space:]]*#{0,6}[[:space:]]*\**[[:space:]]*(verdict[[:space:]]*:)?[[:space:]]*\**[[:space:]]*((not[[:space:]]+)?(clean|sound)|block|request(s|ed)?[[:space:]]+changes?)([^[:alnum:]].*)?$'
if [ ! -s "$V" ]; then
  V_FALLBACK=""
  V_FALLBACK_REJECTED=""
  # validate_fallback_candidate: accepts only a candidate whose last
  # non-blank line matches VERDICT_RE. A candidate that fails this is NEVER
  # copied -- it is recorded (V_FALLBACK_REJECTED) so the eventual NO VERDICT
  # die below can name what was looked at and rejected, rather than looking
  # like nothing was tried at all.
  validate_fallback_candidate() {
    local candidate="$1"
    [ -n "$candidate" ] && [ -s "$candidate" ] || return 1
    local last_line
    last_line=$(grep -v '^[[:space:]]*$' "$candidate" | tail -1)
    if printf '%s' "$last_line" | grep -Eqi "$VERDICT_RE"; then
      return 0
    fi
    V_FALLBACK_REJECTED="${V_FALLBACK_REJECTED:+$V_FALLBACK_REJECTED, }$candidate"
    return 1
  }
  # Exact-filename match ANYWHERE under $RW (not just at its root) --
  # unchanged search scope from the original version, mtime-sorted newest
  # first and validated in that order, same as the *.md fallback below, in
  # the rare case more than one file shares the expected basename.
  while IFS= read -r candidate; do
    if validate_fallback_candidate "$candidate"; then
      V_FALLBACK="$candidate"
      break
    fi
  done < <(find "$RW" -type f -name "$(basename "$V")" -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | cut -d' ' -f2-)
  if [ -z "$V_FALLBACK" ]; then
    # v4.8.13 CF REQUIRED CHANGE: `find | head -1` is NOT "the newest" --
    # find's output order is unspecified, so the old form could pick up any
    # matching file, including one the reviewer merely edited while testing
    # something unrelated. Sort by mtime descending (`%T@`, GNU find, this
    # wrapper is Linux-only per the codex-review skill's Mac-freeze rule) and
    # take the true newest, THEN validate every candidate in that order
    # rather than trusting the first one found.
    while IFS= read -r candidate; do
      if validate_fallback_candidate "$candidate"; then
        V_FALLBACK="$candidate"
        break
      fi
    done < <(find "$RW" -type f -name '*.md' -newermt "@$START_EPOCH" -printf '%T@ %p\n' 2>/dev/null \
      | sort -rn | cut -d' ' -f2-)
  fi
  if [ -n "$V_FALLBACK" ]; then
    warn "verdict-fallback: nothing at the expected -o path ($V), but found $V_FALLBACK inside the review worktree with a validated verdict line -- copying it to the expected path instead of declaring NO VERDICT"
    printf 'verdict-fallback: found=%s expected=%s\n' "$V_FALLBACK" "$V" >> "$L"
    cp "$V_FALLBACK" "$V" || die "verdict-fallback found $V_FALLBACK but could not copy it to $V"
  elif [ -n "$V_FALLBACK_REJECTED" ]; then
    printf 'verdict-fallback: rejected candidate(s) with no validated verdict line: %s\n' "$V_FALLBACK_REJECTED" >> "$L"
    warn "verdict-fallback: considered but REJECTED (no validated verdict line): $V_FALLBACK_REJECTED"
  fi
fi
[ -s "$V" ] || die "codex exited 0 but wrote no verdict file (checked -o path and searched the review worktree) — treat as NO VERDICT, re-run; log: $L"

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

# v4.8.9: the summary-only VOID IN FORM warn() above (right after codex exec)
# is easy to miss -- a reader who opens only $V never sees it. Team-lead spec:
# the line must ALSO land inside the verdict file itself, immediately above
# the verdict line, not just in the wrapper's own stderr/log. Insert it here,
# after $V is finalized (post citation-normalization) so the insertion survives
# the RW-prefix rewrite above instead of being clobbered by it.
if [ "$RSANDBOX" = "workspace-write" ] && [ "$EXECUTED_EXECS" -eq 0 ]; then
  VOID_LINE="VOID IN FORM: reviewer executed nothing under workspace-write (0 go test/run/build and 0 pytest/uv run/ruff/mypy exec blocks) -- do not ledger this verdict as executed evidence; relaunch or escalate."
  LAST_NONBLANK=$(grep -vn '^[[:space:]]*$' "$V" | tail -1 | cut -d: -f1)
  if [ -n "$LAST_NONBLANK" ]; then
    awk -v n="$LAST_NONBLANK" -v line="$VOID_LINE" 'NR==n{print line} {print}' "$V" > "$V.voidfix.tmp" && mv "$V.voidfix.tmp" "$V"
  else
    printf '%s\n' "$VOID_LINE" >> "$V"
  fi
  warn "VOID IN FORM line inserted into $V, immediately above its last non-blank line"
fi

# v4.8.22: same insertion for the diff-visibility rule.
if [ "${DIFF_SEEN:-0}" -eq 0 ]; then
  if [ "${DIFF_UNKNOWN:-0}" -gt 0 ]; then
    # CHAOS-7018/D2822: name the ambiguity rather than overclaiming a confirmed absence --
    # see the matching comment on the identical branch earlier in this file.
    VOID_LINE="VOID IN FORM (AMBIGUOUS): no CONFIRMED git diff exec or read of ${REVIEW_PATCH_NAME:-the review patch} in this log, but $DIFF_UNKNOWN exec block(s) never resolved a status line (batched tool calls) -- unproven, not certified absent; relaunch or escalate."
  else
    VOID_LINE="VOID IN FORM: the round did not see the diff (no successful git diff exec and no read of ${REVIEW_PATCH_NAME:-the review patch} in its log) -- do not ledger this verdict as a review of the diff; relaunch or escalate."
  fi
  LAST_NONBLANK=$(grep -vn '^[[:space:]]*$' "$V" | tail -1 | cut -d: -f1)
  if [ -n "$LAST_NONBLANK" ]; then
    awk -v n="$LAST_NONBLANK" -v line="$VOID_LINE" 'NR==n{print line} {print}' "$V" > "$V.voiddiff.tmp" && mv "$V.voiddiff.tmp" "$V"
  else
    printf '%s\n' "$VOID_LINE" >> "$V"
  fi
  warn "VOID IN FORM (diff not seen) line inserted into $V, immediately above its last non-blank line"
fi

# CF read ASK: same PROOF_LINE written to $L above, also placed at the very
# TOP of the verdict file so a reader of $V alone (not $L) still gets it.
if [ -n "${PROOF_LINE:-}" ]; then
  { printf '%s\n\n' "$PROOF_LINE"; cat "$V"; } > "$V.prooffix.tmp" && mv "$V.prooffix.tmp" "$V"
  warn "proof line inserted at the top of $V"
fi

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
#
# v4.8.13 (team-lead spec, from #2312 r1): "Verdict: request changes" was
# flagged SUSPECT even though it is a real, complete, non-clean verdict --
# the reviewer used "request changes" (a legitimate NOT-CLEAN synonym, and a
# term of art from PR review generally) instead of the canonical
# clean/not-clean/sound/block vocabulary. Added as its own alternative,
# same word-boundary tail as every other branch.
#
# VERDICT_RE itself is now set ONCE, earlier in the script (right before the
# verdict-fallback lookup, v4.8.13 CF requirement) -- that lookup must
# validate a fallback candidate's last line against the SAME rule this check
# uses, or a rejected-by-this-check file could still have been accepted by
# the fallback moments earlier. Not reassigned here; this comment block
# documents the value's history at the point it is actually consumed.
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
  warn "codex -o is --output-last-message: it OVERWRITES that file with the reviewer's FINAL REPLY at exit. If the prompt asked for a report written to a file, the report was clobbered by the sign-off. The reviewer's final reply must BE the report, ending with a verdict line (CLEAN | NOT CLEAN | BLOCK | REQUEST CHANGES)."
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
       ! -name 'prompt.md' ! -name '.codex-review-context.md' ! -name 'LEDGER.md' ! -name '.codex-review.patch' \
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
