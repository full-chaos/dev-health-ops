"""CHAOS-4947: the main-head decision for moving tags is made ONCE, not
once per `merge`/`go-merge` matrix leg -- and it is a descendant check, not
an equals-head check.

Measured on run 33717963309: the per-leg `mainhead` step let eight legs
independently ask "is my commit still main's head", up to 34 minutes apart
in one run. A commit landing mid-window let early legs tag `latest` and
late legs decline, splitting `latest` across a combination of families that
never existed as a real commit -- and equals-head alone (even fanned in)
still strands `latest` behind a complete, newer image set whenever a build
is overtaken but still ahead of what's currently tagged.

This file asserts the STRUCTURE that closes both failure modes, not the
runtime behaviour (no registry, no git history, no Docker here) -- parsed
YAML plus a couple of text-level greps, same spirit as the other
tests/tooling files that assert about *shape* because the alternative is
asserting nothing until the next incident.
"""

# CHAOS-10177: the YAML-shape tests of this file moved to internal/cigates/workflows/workflows_fanin_test.go; the tests left here execute
# scripts and move in CHAOS-10178 (PR C), which then deletes this file.
from __future__ import annotations

import os
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "docker-images.yml"
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_TEXT)


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return list(job.get("steps") or [])

    # NOTE: the `::error::` annotation for the rc==0-yet-empty-stdout case
    # is checked BEHAVIOURALLY, not here -- see
    # test_latest_check_behaviourally_bootstraps_only_on_confirmed_absence.
    # ci-flakes review: a source-text check only proves the literal was
    # typed somewhere, not that it's emitted on the path that should emit
    # it (a guard moved to the wrong branch would still pass a
    # source-text check). The behavioural test captures actual stdout on
    # the empty-read path specifically, plus a negative control on the
    # present-read path.


def _latest_tag_step_script() -> str:
    """The exact `run:` text of the fan-in job's tag-application step, as
    PyYAML parses it (block-scalar indentation already stripped) -- this
    is what actually executes under bash, not the raw indented file text.

    ci-flakes review: the original version returned on the FIRST match
    with no exactly-one assertion -- fine today (there is exactly one),
    but if a second fan-in-shaped job ever grew a similar step, one of
    them would silently go untested while this helper kept returning
    the other and the suite reported success. Collect every match and
    assert there is exactly one, same discipline as ci-flakes' own
    `_guard_script()`."""
    matches = [
        run
        for name, job in _jobs().items()
        if "fan-in" in name or "fan_in" in name
        for step in _steps(job)
        for run in [step.get("run", "")]
        if "_inspect_classify" in run
    ]
    assert len(matches) == 1, (
        "expected exactly one fan-in step whose run: text contains "
        f"_inspect_classify, found {len(matches)} -- if this grew to more "
        "than one, every match after the first would silently go untested "
        "by this behavioural harness"
    )
    return matches[0]


# Records every `imagetools create` invocation to $RECORD_FILE and answers
# `imagetools inspect` deterministically per $SCENARIO, so a test can
# observe what the fan-in step actually DOES, not just what its source
# text mentions (ci-flakes review, delta on fa119b2d: a swapped rc==1/
# rc==2 mapping -- the exact P1 this item exists to close -- left every
# purely-source-text assertion in this file green).
_LABELLED_REVISION = "baseshabaseshabaseshabaseshabaseshabase"
_DOCKER_SHIM = r"""#!/usr/bin/env bash
if [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "inspect" ]; then
  ref="$4"
  case "$ref" in
    *:latest)
      case "$SCENARIO" in
        absent)
          echo "ERROR: ${ref}: not found" >&2
          exit 1
          ;;
        unknown)
          echo "ERROR: failed to do request: connection reset by peer" >&2
          exit 1
          ;;
        empty_success)
          # exit 0 but print nothing: the rc==0-yet-empty-stdout case.
          exit 0
          ;;
        unlabelled)
          # exit 0, valid JSON, but no revision label on either platform
          # -- falls through to the digest-walk fallback, same as
          # dev-hops-api during this PR's own migration.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{}}},"linux/arm64":{"config":{"Labels":{}}}}}'
          exit 0
          ;;
        labelled)
          # BOTH platforms carry the SAME revision label -- this is the
          # only scenario that reaches the actual descendant-vs-decline
          # `git merge-base --is-ancestor` check, the thing CHAOS-4947
          # exists to make. Whether it's a descendant is controlled by
          # the `git` shim below via $MERGE_BASE_IS_ANCESTOR, not here.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}}}}'
          exit 0
          ;;
        disagree)
          # codex round 6 (bigboy), P2: the two platforms carry DIFFERENT,
          # both non-empty revision labels -- a genuinely ambiguous read
          # (the workflow itself has no way to tell which platform is
          # authoritative), distinct from `unlabelled` (both empty,
          # legitimately falls through to the digest-walk fallback).
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"deadfeed00000000000000000000000000000000"}}}}}'
          exit 0
          ;;
        *)
          echo "unrecognized SCENARIO: ${SCENARIO}" >&2
          exit 99
          ;;
      esac
      ;;
    *)
      # This branch answers BOTH the just-published sha- source-tag probe
      # AND the digest-walk's per-candidate sha- probes (same ref shape,
      # `<image>:sha-<short>`) -- they're distinguished only by which
      # short-sha is being asked about, not by any other structural cue.
      #
      # codex round 2, P3 (my own least-sure item from round 2's prompt,
      # confirmed both ways): the harness's `git log` shim always returned
      # empty history, so the digest-walk's actual found-a-match path
      # (walk N candidates, find one whose digest equals :latest's,
      # `break`, tag from it) was NEVER exercised -- only "walked, found
      # nothing, declined". A regression in the match/break/found_base
      # logic passed every existing row. If $DIGEST_WALK_MATCH_SHORT is
      # set, the candidate whose short-sha matches it gets the SAME
      # digest as :latest's (sha256:aaaa, the `unlabelled` scenario's
      # fixed digest) -- every OTHER candidate gets the existing default
      # (sha256:bbbb, guaranteed != aaaa), so the walk must actually
      # iterate past real misses to find the real match, not just accept
      # the first candidate it's handed.
      candidate_short="${ref##*:sha-}"
      # 4752-go's peer read of #2167 (r3 order, team-lead): the source-tag
      # probe (this SAME ref shape, `<image>:sha-deadbee` for the fixed
      # $BUILT_SHA every test here uses) had no scenario control at all --
      # it always "found" the ref, so the source_rc==1/rc==2 branches at
      # the call site (SKIP vs `::error::` decline) were never exercised.
      # $SOURCE_TAG_SCENARIO, when set, answers ONLY for that exact
      # short-sha (never a digest-walk candidate's, which uses a
      # different, unrelated sha) -- everything else keeps the prior
      # unconditional-success behaviour.
      # codex round 4, P2: a MIXED run (some families UNKNOWN, others
      # CONFIRMED ABSENT, none actually found) needed its own per-family
      # override to construct -- $SOURCE_TAG_SCENARIO alone answers the
      # SAME way for every family (they share one short_sha). When
      # $SOURCE_TAG_OVERRIDE_FAMILY is set and this ref's image name
      # matches it, $SOURCE_TAG_OVERRIDE_SCENARIO wins for THIS family
      # only; every other family still gets the uniform
      # $SOURCE_TAG_SCENARIO.
      family_scenario="${SOURCE_TAG_SCENARIO:-}"
      if [ -n "${SOURCE_TAG_OVERRIDE_FAMILY:-}" ]; then
        case "${ref}" in
          *"/${SOURCE_TAG_OVERRIDE_FAMILY}:sha-"*)
            family_scenario="${SOURCE_TAG_OVERRIDE_SCENARIO:-}"
            ;;
        esac
      fi
      if [ -n "${family_scenario}" ] && [ "${candidate_short}" = "deadbee" ]; then
        case "${family_scenario}" in
          absent)
            echo "ERROR: ${ref}: not found" >&2
            exit 1
            ;;
          unknown)
            echo "ERROR: failed to do request: connection reset by peer" >&2
            exit 1
            ;;
          *)
            echo "unrecognized source-tag scenario: ${family_scenario}" >&2
            exit 99
            ;;
        esac
      fi
      if [ -n "${DIGEST_WALK_MATCH_SHORT:-}" ] && [ "${candidate_short}" = "${DIGEST_WALK_MATCH_SHORT}" ]; then
        echo '{"manifest":{"digest":"sha256:aaaa"}}'
      else
        echo '{"manifest":{"digest":"sha256:bbbb"}}'
      fi
      exit 0
      ;;
  esac
elif [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "create" ]; then
  echo "CREATE: $*" >> "${RECORD_FILE}"
  exit 0
fi
exit 0
"""

# A `git` shim with TWO SEPARATE marker files, one per subcommand this
# step can call -- not one shared "was git called at all" flag.
#
# ci-flakes review (delta on ace5d86a): a single shared marker only fails
# to conflate `git log` (digest-walk fallback) with `git merge-base
# --is-ancestor` (labelled-ancestry check) BECAUSE the three scenarios
# that assert on it today (absent/unknown/empty_success) all `continue`
# before the script can reach either call -- a property of the CODE
# UNDER TEST's current control flow, not of the marker itself. If a
# guard ever moves later, or a future scenario reaches the label code
# while still checking the old marker, the two meanings silently merge
# and nothing goes red. Two markers make that structurally impossible
# instead of merely coincidentally absent -- "coincidental correctness
# is a defect with good luck" (ci-flakes, this same review thread).
#
# For `git log` (the digest-walk fallback) this shim returns NOTHING
# (empty history) after marking it called, UNLESS $DIGEST_WALK_CANDIDATES
# is set (newline-separated full SHAs, oldest-relevant-first-parent order)
# -- in which case it prints exactly that, letting a test construct a
# specific walk history (codex round 2: needed to exercise the found-a-
# match path, which an always-empty history could never reach). Reaching
# the fallback at all (empty-history case) is itself the observable the
# empty/unknown/absent scenarios check, independent of what the (real,
# unrelated) checkout this test runs inside happens to contain for a
# nonsense BUILT_SHA. For `git merge-base --is-ancestor` (the labelled
# scenario's actual ancestry decision) it answers per
# $MERGE_BASE_IS_ANCESTOR: exit 0 (is an ancestor / descendant) or exit 1
# (is not).
_GIT_SHIM = r"""#!/usr/bin/env bash
if [ "$1" = "log" ]; then
  touch "${GIT_LOG_CALLED_FILE}"
  if [ -n "${DIGEST_WALK_CANDIDATES:-}" ]; then
    printf '%s\n' "${DIGEST_WALK_CANDIDATES}"
  fi
  exit 0
fi
if [ "$1" = "merge-base" ] && [ "$2" = "--is-ancestor" ]; then
  touch "${GIT_MERGE_BASE_CALLED_FILE}"
  if [ "${MERGE_BASE_IS_ANCESTOR}" = "true" ]; then
    exit 0
  else
    exit 1
  fi
fi
exit 0
"""


def _run_latest_tag_step(
    scenario: str,
    merge_base_is_ancestor: bool = True,
    digest_walk_candidates: list[str] | None = None,
    digest_walk_match_short: str = "",
    source_tag_scenario: str = "",
    assert_success: bool = True,
    source_tag_override_family: str = "",
    source_tag_override_scenario: str = "",
) -> tuple[str, bool, bool, str, int]:
    """Run the fan-in job's ACTUAL tag-application script under bash, with
    `docker` shimmed to answer deterministically per `scenario` and
    `git` shimmed to record `log` and `merge-base --is-ancestor`
    invocations SEPARATELY (see `_GIT_SHIM`'s comment for why one shared
    marker isn't good enough), and to answer `merge-base --is-ancestor`
    per `merge_base_is_ancestor` for the `labelled` scenario. Passing
    `digest_walk_candidates` (full SHAs, first-parent order) feeds a
    scripted `git log` history to the digest-walk fallback instead of the
    default empty one; `digest_walk_match_short` names which candidate's
    short-sha should report the SAME digest as :latest's (everything else
    gets a deliberate miss) -- both together let a test exercise the
    found-a-match path, not just "walked, found nothing". Passing
    `source_tag_scenario` (`"absent"` or `"unknown"`) scripts the
    source-tag probe itself to FAIL that way instead of always finding
    its ref -- every family shares one `short_sha`, so this affects all
    eight identically and none of them reach the :latest check at all
    (the loop `continue`s before it). Returns (recorded `imagetools
    create` invocations, whether `git log` was called, whether `git
    merge-base --is-ancestor` was called, captured stdout, the script's
    exit code). `assert_success=False` (default True) skips this
    helper's own zero-exit assertion, for a scenario -- like every family
    hitting UNKNOWN on the source tag -- that is EXPECTED to exit
    non-zero (docker-images.yml's own post-loop `source_unknown_count`
    check); the returncode is still returned either way so the caller
    can assert on it explicitly instead of this helper silently
    accepting whatever it got. `IS_MAIN_REF=true` and, when
    `source_tag_scenario` is left empty (the default), a source-tag
    probe that always "finds" its ref, so every family reaches the
    :latest check under test."""
    script = _latest_tag_step_script()
    with tempfile.TemporaryDirectory() as tmp:
        bin_dir = Path(tmp) / "bin"
        bin_dir.mkdir()
        docker_shim = bin_dir / "docker"
        docker_shim.write_text(_DOCKER_SHIM, encoding="utf-8")
        docker_shim.chmod(
            docker_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
        )
        git_shim = bin_dir / "git"
        git_shim.write_text(_GIT_SHIM, encoding="utf-8")
        git_shim.chmod(
            git_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
        )

        script_path = Path(tmp) / "fanin.sh"
        script_path.write_text(script, encoding="utf-8")

        record_file = Path(tmp) / "record.txt"
        record_file.write_text("", encoding="utf-8")
        git_log_called_file = Path(tmp) / "git_log_called.txt"
        git_merge_base_called_file = Path(tmp) / "git_merge_base_called.txt"

        env = dict(os.environ)
        env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
        env.update(
            {
                "BUILT_SHA": "deadbeef1234567890deadbeef1234567890dead0",
                "OWNER": "full-chaos",
                "IS_RELEASE": "false",
                "IS_MAIN_REF": "true",
                "REF_IS_BRANCH": "false",
                "BRANCH_TAG": "",
                "SCENARIO": scenario,
                "RECORD_FILE": str(record_file),
                "GIT_LOG_CALLED_FILE": str(git_log_called_file),
                "GIT_MERGE_BASE_CALLED_FILE": str(git_merge_base_called_file),
                "LABELLED_REVISION": _LABELLED_REVISION,
                "MERGE_BASE_IS_ANCESTOR": "true" if merge_base_is_ancestor else "false",
                "DIGEST_WALK_CANDIDATES": "\n".join(digest_walk_candidates or []),
                "DIGEST_WALK_MATCH_SHORT": digest_walk_match_short,
                "SOURCE_TAG_SCENARIO": source_tag_scenario,
                "SOURCE_TAG_OVERRIDE_FAMILY": source_tag_override_family,
                "SOURCE_TAG_OVERRIDE_SCENARIO": source_tag_override_scenario,
            }
        )
        result = subprocess.run(
            ["bash", str(script_path)],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        # codex round 3, P2: an ALL-families UNKNOWN source-tag scenario
        # is now a deliberate non-zero exit (docker-images.yml's
        # post-loop source_unknown_count/family_total check) -- the old
        # unconditional `assert returncode == 0` would itself fail any
        # test exercising that path. `assert_success=False` opts out;
        # the returncode is returned (5th element) so the caller can
        # assert on it explicitly instead of this helper silently
        # accepting any exit code.
        if assert_success:
            assert result.returncode == 0, (
                f"fan-in script exited {result.returncode} under scenario "
                f"{scenario!r} -- stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return (
            record_file.read_text(encoding="utf-8"),
            git_log_called_file.exists(),
            git_merge_base_called_file.exists(),
            result.stdout,
            result.returncode,
        )


def test_latest_check_behaviourally_bootstraps_only_on_confirmed_absence() -> None:
    """Source-text assertions cannot tell a CORRECT rc==1/rc==2 mapping
    from a SWAPPED one -- demonstrated directly (not argued): swapping
    which branch bootstraps and which declines, the exact P1 this item
    exists to close, left every purely-source-text assertion in this
    file green (verified before writing this test). Run the ACTUAL
    script under bash with a recording `docker` shim standing in for the
    registry, and observe what it DOES: CONFIRMED ABSENT must record an
    `imagetools create ... :latest` call; UNKNOWN must record none; a
    reported-success-but-empty read must also record none and must emit
    a `::error::` annotation (ci-flakes' third finding, and their
    follow-up: a source-text check for `::error::` only proves the
    literal was typed somewhere, not that it's emitted on the path that
    should emit it -- a guard moved to the wrong branch, or made
    unreachable, would still pass a source-text check. Asserting it in
    THIS behavioural row, alongside the git-shim discrimination, means
    the assertion can't pass by shadowing a neighbouring branch. Also
    asserts the negative control: a genuinely present (found) read
    emits no `::error::`)."""
    absent_record, absent_log_called, absent_mb_called, _absent_stdout, _absent_rc = (
        _run_latest_tag_step("absent")
    )
    assert "imagetools create" in absent_record and ":latest" in absent_record, (
        "CONFIRMED ABSENT did not record a bootstrap `imagetools create "
        "...:latest` call -- the classifier's rc==1 branch must "
        f"unconditionally tag latest+main, got: {absent_record!r}"
    )
    assert not absent_log_called and not absent_mb_called, (
        "CONFIRMED ABSENT reached the digest-walk fallback or the "
        "merge-base ancestry check (git was invoked) instead of "
        "short-circuiting straight to bootstrap"
    )

    # codex round 5, P2: every family shares this scenario uniformly (no
    # source_tag_scenario override), so all eight hit `:latest` UNKNOWN --
    # which is now the exact trigger for the post-loop
    # tags_moved_count==0/any_unknown_count>0 job failure (see
    # test_latest_check_all_unknown_fails_the_job below for the dedicated
    # row on that mechanism). assert_success=False because this row's own
    # point is the PER-FAMILY behaviour (no create, no fallback reached),
    # not the job-level exit code.
    (
        unknown_record,
        unknown_log_called,
        unknown_mb_called,
        _unknown_stdout,
        unknown_rc,
    ) = _run_latest_tag_step("unknown", assert_success=False)
    assert unknown_rc != 0, (
        "all eight families hitting :latest UNKNOWN should now fail the "
        f"step (post-loop tags_moved_count/any_unknown_count check), got "
        f"returncode={unknown_rc}"
    )
    assert unknown_record == "", (
        "UNKNOWN registry failure recorded an `imagetools create` call -- "
        f"it must decline and tag nothing, got: {unknown_record!r}"
    )
    assert not unknown_log_called and not unknown_mb_called, (
        "UNKNOWN registry failure reached the digest-walk fallback or the "
        "merge-base ancestry check (git was invoked) instead of declining "
        "immediately"
    )

    # ci-flakes review (delta on fa119b2d), P3: rc==0 only means the
    # inspect call itself succeeded, it doesn't guarantee non-empty
    # stdout. Before the explicit `[ -z "${latest_json}" ]` guard this
    # scenario fell through into the label-reading code and then the
    # digest-walk fallback -- which happens to decline anyway for an
    # empty label/digest, so checking only the recorded creates (as
    # above) would pass whether or not the guard exists and wouldn't be
    # a real regression test. Asserting git was never invoked is the
    # part that actually distinguishes "declined immediately" from
    # "wandered into the fallback and got lucky" -- verified by removing
    # the guard and confirming this specific assertion goes red while
    # the create-count assertion alone stays green.
    # codex round 5, P2: same uniform-scenario shape as "unknown" above --
    # all eight families hit the empty-read case, which is now ALSO an
    # any_unknown_count trigger (its own comment in the workflow already
    # says "treating as unknown"), so this scenario fails the step too.
    empty_record, empty_log_called, empty_mb_called, empty_stdout, empty_rc = (
        _run_latest_tag_step("empty_success", assert_success=False)
    )
    assert empty_rc != 0, (
        "all eight families hitting the rc==0-yet-empty :latest read should "
        f"now fail the step, got returncode={empty_rc}"
    )
    assert empty_record == "", (
        "a reported-success (exit 0) but empty :latest read recorded an "
        "`imagetools create` call -- an empty read must fail closed the "
        f"same as UNKNOWN, got: {empty_record!r}"
    )
    assert not empty_log_called and not empty_mb_called, (
        "a reported-success (exit 0) but empty :latest read reached the "
        "digest-walk fallback or the merge-base ancestry check instead of "
        "declining immediately -- this must be handled as an explicit "
        "UNKNOWN case, not left to whatever the downstream label/digest "
        "logic happens to do with empty input"
    )
    # team-lead ruling (following ci-flakes' review of the first version
    # of this test): a source-text check for `::error::` only proves the
    # literal appears somewhere in the script, not that it's emitted on
    # THIS path -- moving the annotation to the wrong branch, or making
    # it unreachable, would still pass a source-text check but must fail
    # here. Verified: moving the `::error::` echo into the neighbouring
    # rc==2 branch (so this path never reaches it) turns this specific
    # assertion red while the create-count/git-called assertions above
    # stay green, proving it observes something they don't.
    assert "::error::" in empty_stdout, (
        "a reported-success (exit 0) but empty :latest read did not emit "
        "a `::error::` GitHub Actions annotation on its own path -- this "
        f"surprising outcome must be visible as an annotation, got stdout: "
        f"{empty_stdout!r}"
    )

    # Negative control: a genuinely present but UNLABELLED read (the
    # digest-walk fallback path -- ci-flakes review: this is what "present"
    # actually exercised before this rename, not the labelled/common case
    # the old name implied) emits no PER-FAMILY `::error::` of its own --
    # the decline is a `WARNING:` line, not an inline error annotation.
    #
    # codex round 6 (bigboy), P2: every family here shares the SAME
    # $SCENARIO, so when the digest walk finds no match (the default,
    # empty git-log history -- no digest_walk_candidates passed), ALL
    # NINE families decline this way. That is no longer a silent, rc=0
    # no-op: the run-level `::error::` from the post-loop no-op guard
    # DOES now appear, from the aggregate check, not this per-family
    # line. See test_latest_check_all_families_digest_walk_exhausted_
    # fails_the_job below for that job-level assertion; this test only
    # checks the per-family WARNING text and control flow.
    (
        found_record,
        found_log_called,
        found_mb_called,
        found_stdout,
        found_rc,
    ) = _run_latest_tag_step("unlabelled", assert_success=False)
    assert found_log_called and not found_mb_called, (
        "the unlabelled scenario should reach the digest-walk fallback "
        "(git log) but never the merge-base ancestry check -- got "
        f"log_called={found_log_called}, merge_base_called={found_mb_called}"
    )
    assert found_rc == 1, (
        "codex round 6 (bigboy), P2: every family sharing this scenario "
        "with no digest-walk match means the run as a whole must now "
        f"fail via the post-loop no-op guard, got rc={found_rc}, "
        f"stdout:\n{found_stdout}"
    )
    assert (
        "WARNING: " in found_stdout
        and "has no revision label on :latest" in found_stdout
    ), (
        "expected the per-family decline to still emit its own WARNING "
        f"line (not an inline ::error::), got stdout: {found_stdout!r}"
    )


def test_latest_check_behaviourally_tags_only_true_descendants() -> None:
    """ci-flakes review: none of the other scenarios in this file reach a
    genuinely LABELLED `:latest` -- absent/unknown/empty-success all
    short-circuit before the label logic, and the unlabelled scenario
    falls straight through to the digest-walk fallback. That means the
    actual descendant-vs-decline decision this whole ticket exists to
    make (`git merge-base --is-ancestor`) was never behaviourally
    exercised: a mutation flipping that check (e.g. inverting the exit
    code test, or swapping which branch creates vs declines) would have
    survived every row in this file.

    Uses the `labelled` scenario (both platforms carry the SAME revision
    label, so `base_sha` resolves and the merge-base check is reached)
    with the `git` shim answering `merge-base --is-ancestor` both ways
    via `merge_base_is_ancestor`: True -> the built commit IS a
    descendant of currently-latest -> must create; False -> it is NOT ->
    must decline. Verified this test goes red against a mutation that
    inverts the `git merge-base --is-ancestor` exit-code check (flipped
    the if/else bodies) before trusting it."""
    descendant_record, descendant_log_called, descendant_mb_called, _stdout, _rc = (
        _run_latest_tag_step("labelled", merge_base_is_ancestor=True)
    )
    assert descendant_mb_called and not descendant_log_called, (
        "the labelled/descendant scenario should reach the merge-base "
        "ancestry check but never the digest-walk fallback -- got "
        f"log_called={descendant_log_called}, "
        f"merge_base_called={descendant_mb_called}"
    )
    assert (
        "imagetools create" in descendant_record and ":latest" in descendant_record
    ), (
        "a labelled :latest whose recorded revision IS an ancestor of the "
        "built commit did not record an `imagetools create ...:latest` "
        f"call -- a true descendant must be tagged, got: {descendant_record!r}"
    )

    non_descendant_record, non_desc_log_called, non_desc_mb_called, _stdout2, _rc2 = (
        _run_latest_tag_step("labelled", merge_base_is_ancestor=False)
    )
    assert non_desc_mb_called and not non_desc_log_called, (
        "the labelled/non-descendant scenario should reach the merge-base "
        "ancestry check but never the digest-walk fallback -- got "
        f"log_called={non_desc_log_called}, merge_base_called={non_desc_mb_called}"
    )
    assert non_descendant_record == "", (
        "a labelled :latest whose recorded revision is NOT an ancestor of "
        "the built commit still recorded an `imagetools create` call -- "
        "a non-descendant build must decline, never overwrite a newer "
        f"currently-latest, got: {non_descendant_record!r}"
    )


def test_digest_walk_finds_a_match_partway_through_history() -> None:
    """codex round 2, P3 (my own least-sure item from round 2's prompt,
    confirmed both ways): every scenario above that reaches the digest
    walk (`unlabelled`) fed it an EMPTY `git log` history -- that proves
    "walked, found nothing, declined", never "walked, found the actual
    match, tagged from it". A regression in the match/`break`/
    `found_base` logic at the workflow's `[ -n "${candidate_digest}" ] &&
    [ "${candidate_digest}" = "${latest_digest}" ]` check passed all
    prior rows in this file.

    Feeds a 3-candidate scripted history with the match at the MIDDLE
    position (not the first candidate checked) -- proving the walk
    actually iterates past a real miss rather than accepting whatever
    it's handed first. Verified this row goes red against the mutation
    codex used (`if [ -n ... ] && [ ... = ... ]` replaced with `if
    false`) before trusting it."""
    candidates = [
        "1111111111111111111111111111111111111a",  # miss (checked first)
        "2222222222222222222222222222222222222b",  # THE MATCH (checked second)
        "3333333333333333333333333333333333333c",  # never reached (walk breaks at #2)
    ]
    record, log_called, mb_called, stdout, _rc = _run_latest_tag_step(
        "unlabelled",
        digest_walk_candidates=candidates,
        digest_walk_match_short="2222222",
    )
    assert log_called and not mb_called, (
        "expected the digest-walk (git log) to be reached, not the "
        f"merge-base ancestry check -- log_called={log_called}, "
        f"merge_base_called={mb_called}"
    )
    assert "imagetools create" in record and ":latest" in record, (
        "a digest walk with a real match partway through its scripted "
        "history did not record an `imagetools create ...:latest` call "
        f"-- got record: {record!r}, stdout: {stdout!r}"
    )
    assert "2222222222222222222222222222222222222b" in stdout, (
        "the FALLBACK log line should name the matched candidate sha -- "
        f"got stdout: {stdout!r}"
    )


def test_source_tag_check_distinguishes_confirmed_absent_from_unknown() -> None:
    """4752-go's peer read of #2167, round 3 order (team-lead): runner-
    fallback's peer read of bf11cdc5 found the SOURCE-TAG classify call
    site (source_rc==1 -> silent SKIP, source_rc==2 -> `::error::`
    decline -- see the ranked-dimensions comment above the source-tag
    probe in docker-images.yml) had NO behavioural coverage at all.
    Collapsing BOTH outcomes into the same silent-SKIP branch (deleting
    the `::error::` decline entirely -- the exact P1 the :latest site's
    own rc==1/rc==2 split already exists to close, just moved one probe
    earlier) left every existing row in this file green, because none of
    them ever script the source-tag probe's own outcome -- the docker
    shim's non-:latest branch always "found" it unconditionally.

    Runs the real script with $SOURCE_TAG_SCENARIO scripting the
    source-tag probe itself (not :latest, which these two cases never
    even reach). Both must decline before the :latest check -- no
    `imagetools create` for any of the eight families, which all share one
    `short_sha` and so all hit the same scripted outcome -- and neither
    reaches the digest-walk or merge-base checks (git untouched). The
    OBSERVABLE that actually distinguishes them is stdout: rc==1 is a
    silent SKIP (no `::error::`), rc==2 MUST emit `::error::` -- proven by
    running both, asserting on each's own text, and asserting the two
    outputs differ. Verified this test goes red against the mutation
    lane-runner-fallback described (rc==2's branch replaced with the same
    silent-SKIP text as rc==1) before trusting it."""
    absent_record, absent_log, absent_mb, absent_stdout, _absent_rc = (
        _run_latest_tag_step(
            # The :latest scenario value is irrelevant here -- source_rc==1
            # `continue`s the loop long before the :latest probe is ever
            # reached for any family.
            "labelled",
            source_tag_scenario="absent",
        )
    )
    assert absent_record == "", (
        "CONFIRMED ABSENT on the source tag still recorded an "
        f"`imagetools create` call -- got: {absent_record!r}"
    )
    assert not absent_log and not absent_mb, (
        "CONFIRMED ABSENT on the source tag reached the digest-walk "
        "fallback or the merge-base ancestry check (git was invoked) "
        "instead of skipping immediately"
    )
    assert "SKIP" in absent_stdout and "confirmed absent" in absent_stdout, (
        "expected a SKIP message naming confirmed absence, got stdout: "
        f"{absent_stdout!r}"
    )
    assert "::error::" not in absent_stdout, (
        "CONFIRMED ABSENT on the source tag must be a SILENT skip -- no "
        f"`::error::` annotation, got stdout: {absent_stdout!r}"
    )

    # codex round 3, P2: every family shares one short_sha, so this
    # scenario hits ALL NINE with source_rc==2 -- which is now the exact
    # trigger for docker-images.yml's post-loop source_unknown_count ==
    # family_total check, a deliberate non-zero exit (see
    # test_source_tag_all_unknown_fails_the_job_instead_of_a_silent_noop
    # below for the dedicated row on that mechanism). assert_success=False
    # here because this row's own point is the PER-FAMILY message shape
    # (SKIP vs `::error::`), not the job-level exit code.
    unknown_record, unknown_log, unknown_mb, unknown_stdout, unknown_rc = (
        _run_latest_tag_step(
            "labelled",
            source_tag_scenario="unknown",
            assert_success=False,
        )
    )
    assert unknown_rc != 0, (
        "all eight families hitting source_rc==2 should now fail the step "
        f"(post-loop source_unknown_count==family_total check), got "
        f"returncode={unknown_rc}"
    )
    assert unknown_record == "", (
        "UNKNOWN registry failure on the source tag still recorded an "
        f"`imagetools create` call -- got: {unknown_record!r}"
    )
    assert not unknown_log and not unknown_mb, (
        "UNKNOWN registry failure on the source tag reached the "
        "digest-walk fallback or the merge-base ancestry check (git was "
        "invoked) instead of declining immediately"
    )
    assert (
        "::error::" in unknown_stdout and "could not confirm whether" in unknown_stdout
    ), (
        "UNKNOWN registry failure on the source tag must emit a "
        f"`::error::` annotation, got stdout: {unknown_stdout!r}"
    )

    # The pair, not either row alone, is what closes the P1 lane-runner-
    # fallback found: a mutation that routes rc==2 through rc==1's own
    # silent-SKIP text passes both rows in isolation (each still declines,
    # still touches no git, still records no create) but produces
    # IDENTICAL output shape on both -- this is the assertion that mutant
    # cannot survive.
    assert "::error::" not in absent_stdout and "::error::" in unknown_stdout, (
        "the confirmed-absent and unknown source-tag outcomes must be "
        "distinguishable on the wire (silent skip vs a visible `::error::` "
        f"decline) -- got absent={absent_stdout!r}, unknown={unknown_stdout!r}"
    )


def test_source_tag_all_unknown_fails_the_job_instead_of_a_silent_noop() -> None:
    """codex round 3, P2 (reproduced live): every family shares ONE
    `short_sha`, so an UNKNOWN source-tag registry error is never a
    per-family gap -- it hits all eight simultaneously. Before this row's
    fix, that produced eight `::error::` annotations, an EMPTY create
    record (no tags moved for anyone), and a ZERO-EXIT step: a full
    publish no-op that reads as a green job unless someone actually reads
    the log for the annotations. docker-images.yml now fails the step
    when `source_present_count == 0` and `source_unknown_count > 0`
    (codex round 4, P2: widened from the original "all UNKNOWN" equality
    test, which missed a MIXED all-UNKNOWN/all-ABSENT run with zero
    actual finds -- see test_source_tag_mixed_unknown_and_absent_still_
    fails_the_job below for that scenario specifically).

    Negative control: `test_source_tag_check_distinguishes_confirmed_absent_from_unknown`'s
    own "absent" call (all eight hit source_rc==1, not 2) uses the
    DEFAULT `assert_success=True` and passes -- proving this check is
    keyed on "nothing found," not "every family declined for any reason"
    (an all-CONFIRMED-ABSENT run, e.g. a fresh org with nothing published
    yet, is a legitimate steady state and must not fail the job)."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "labelled",
        source_tag_scenario="unknown",
        assert_success=False,
    )
    assert returncode == 1, (
        f"expected the all-UNKNOWN source-tag scenario to exit 1, got "
        f"returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of failing immediately after "
        "the per-family loop"
    )
    assert "0 of 7 families had a moving tag applied this run" in stdout, (
        "expected the job-level failure annotation naming the "
        f"moved/ambiguous counts, got stdout:\n{stdout}"
    )


def test_source_tag_mixed_unknown_and_absent_still_fails_the_job() -> None:
    """codex round 4, P2 (reproduced): the round-3 fix only fired when
    EVERY family's source-tag read was UNKNOWN (source_unknown_count ==
    family_total). A MIXED run -- 9 families UNKNOWN, 1 CONFIRMED ABSENT,
    zero families actually FOUND -- reached this exact silent-no-op shape
    too, because the round-3 equality test never matches when even one
    family declines via "absent" instead of "unknown": 9 != 10. Reviewer's
    own repro (matching the scenario here almost exactly, one family --
    dev-health-go-migrate -- forced to "not found" via a docker-shim
    monkeypatch) recorded rc=0, zero creates, no job-level annotation.

    Uses $SOURCE_TAG_OVERRIDE_FAMILY/$SOURCE_TAG_OVERRIDE_SCENARIO
    (source_tag_override_family/scenario) to give ONE family ("absent")
    a different outcome than the other eight ("unknown") -- constructing
    the exact mixed shape without a docker-shim monkeypatch. Verified
    this row goes RED against the round-3-only check (`source_unknown_
    count == family_total`, reverted from the round-4 `source_present_
    count == 0 and source_unknown_count > 0` form) before trusting it:
    9 != 10, so the reverted check never fires and the run exits 0."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "labelled",
        source_tag_scenario="unknown",
        source_tag_override_family="dev-health-go-migrate",
        source_tag_override_scenario="absent",
        assert_success=False,
    )
    assert returncode == 1, (
        f"expected the mixed UNKNOWN/ABSENT scenario (0 found) to exit 1, "
        f"got returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing mixed run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing mixed run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of failing immediately after "
        "the per-family loop"
    )
    assert "0 of 7 families had a moving tag applied this run" in stdout, (
        "expected the job-level failure annotation naming the "
        f"moved/ambiguous counts, got stdout:\n{stdout}"
    )
    assert "SKIP dev-health-go-migrate" in stdout, (
        "expected the one overridden-absent family to still get its own "
        f"silent SKIP line, got stdout:\n{stdout}"
    )


def test_latest_check_all_unknown_fails_the_job() -> None:
    """codex round 5, P2 (reproduced): the round-3/round-4 no-op guard
    only counted source-tag-stage ambiguity. An all-eight-source-PRESENT
    run where every `:latest` read is then UNKNOWN reaches the identical
    silent no-op shape one stage later -- zero `imagetools create` calls,
    rc=0 -- because the source-tag-only check never looks at the
    `:latest` stage at all.

    Fixed structurally this round: `tags_moved_count` (incremented at
    every `imagetools create` call site, any stage) and
    `any_unknown_count` (incremented at every self-classified-ambiguous
    branch, any stage) replace the source-tag-specific counters. This row
    proves the SAME job-level failure now fires from the `:latest` stage,
    not just the source-tag stage `test_source_tag_all_unknown_fails_the_
    job_instead_of_a_silent_noop` already covers.

    Every family shares one `short_sha` for the source-tag probe (default
    "always found"), so all eight reach `:latest`; `scenario="unknown"`
    then answers UNKNOWN for all eight uniformly. Verified this row goes
    RED against the round-4-only condition (`source_present_count==0 and
    source_unknown_count>0`, reverted from the round-5 `tags_moved_count`/
    `any_unknown_count` form) before trusting it: source_present_count=9
    there (every source tag WAS found), so that condition's own first
    clause is false and the run exits 0."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "unknown", assert_success=False
    )
    assert returncode == 1, (
        f"expected the all-:latest-UNKNOWN scenario (0 moved) to exit 1, "
        f"got returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of failing immediately after "
        "the per-family loop"
    )
    assert "0 of 7 families had a moving tag applied this run" in stdout, (
        "expected the job-level failure annotation naming the "
        f"moved/ambiguous counts, got stdout:\n{stdout}"
    )


def test_latest_check_all_families_digest_walk_exhausted_fails_the_job() -> None:
    """codex round 6 (bigboy), P2 (reproduced): an unlabelled `:latest`
    whose digest never matches any of the last 50 first-parent ancestors
    declined with only a WARNING claiming "this should resolve itself
    once ${family} gets its next label-bearing build" -- but if the
    sha- tag the walk needs is outside the 50-commit window (or was
    never published), this decline repeats identically on every future
    run, not just this one. Before this fix, an org where every family
    was still in this state (a real transitional state during this
    PR's own migration) no-op'd silently forever: zero `imagetools
    create` calls, rc=0, no job-level signal at all.

    Every family shares the same $SCENARIO ("unlabelled") and the
    default empty git-log history (no digest_walk_candidates), so the
    digest walk finds nothing for any of the eight. Verified this row
    goes RED against the pre-fix code (the digest-walk `else` branch
    with no any_unknown_count increment) before trusting it: rc=0
    there, with an unchanged record/log/merge-base shape."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "unlabelled", assert_success=False
    )
    assert returncode == 1, (
        f"expected the all-eight digest-walk-exhausted scenario to exit "
        f"1, got returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing run still recorded an `imagetools create` call: {record!r}"
    )
    assert log_called and not mb_called, (
        "expected every family to reach the digest-walk fallback (git "
        f"log) but never the merge-base ancestry check -- got "
        f"log_called={log_called}, merge_base_called={mb_called}"
    )
    assert "0 of 7 families had a moving tag applied this run" in stdout, (
        "expected the job-level failure annotation naming the "
        f"moved/ambiguous counts, got stdout:\n{stdout}"
    )


def test_latest_check_all_families_disagree_fails_the_job() -> None:
    """codex round 6 (bigboy), P2 (reproduced): a `:latest` whose two
    platforms carry DIFFERENT, both non-empty revision labels declined
    with `DECLINE ... platform labels disagree ...` but never
    incremented any terminal-failure counter. A corrupted/split
    `:latest` cannot self-heal: every future run re-reads the same
    disagreeing labels and declines again, exactly as permanent and
    silent as the all-UNKNOWN case round 5 already covers. Before this
    fix, an org where every family hit this state no-op'd silently
    forever: zero `imagetools create` calls, rc=0, no job-level signal.

    Every family shares the same $SCENARIO ("disagree"), so all eight
    decline on the platform-label mismatch and never reach the
    digest-walk fallback or the merge-base check. Verified this row
    goes RED against the pre-fix code (the `amd64_rev != arm64_rev`
    DECLINE branch with no any_unknown_count increment) before trusting
    it: rc=0 there, with an unchanged record/log/merge-base shape."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "disagree", assert_success=False
    )
    assert returncode == 1, (
        f"expected the all-eight platform-label-disagree scenario to "
        f"exit 1, got returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of declining immediately on "
        f"the platform-label mismatch -- log_called={log_called}, "
        f"merge_base_called={mb_called}"
    )
    assert "0 of 7 families had a moving tag applied this run" in stdout, (
        "expected the job-level failure annotation naming the "
        f"moved/ambiguous counts, got stdout:\n{stdout}"
    )
    assert "platform labels disagree" in stdout, (
        "expected the per-family DECLINE line to still be present, got "
        f"stdout:\n{stdout}"
    )
