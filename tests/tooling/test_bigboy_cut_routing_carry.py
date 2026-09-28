"""bigboy-cut.sh: pre-roll routing carry (CHAOS-7022) is refuse-not-skip, and post-cut
routing repoint runs unconditionally on every cut.

CHAOS-7016/D2811: go_api_routing_state rows are keyed by schema_digest. A cut that changes the
GraphQL SDL (CHAOS-6262's field deletions) computes a NEW schema digest, and bigboy-cut.sh had
no STEP that carries enabled rows forward before migrate/up recreate query-api -- every
operation silently un-routes the moment the new pod starts. A second, schema-digest-independent
gap (D2811 addendum, the rev195->rev196 prod roll): routing rows can also lag the actually-
running build after an ORDINARY roll with no schema change at all, because nothing repoints
them post-cut. Both gaps were observed failing (no `routing-carry`/`routing-repoint` STEP
existed at all) before this change.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"


def _lines() -> list[str]:
    return CUT.read_text().splitlines()


def _first(lines: list[str], needle: str, *, start: int = 0) -> int:
    for i in range(start, len(lines)):
        stripped = lines[i].lstrip()
        if needle in lines[i] and not stripped.startswith("#"):
            return i
    raise AssertionError(
        f"{needle!r} not found in bigboy-cut.sh after line {start + 1}"
    )


def test_carry_runs_after_repin_and_before_migrate_up() -> None:
    """The digest-changed path: carry must run while query-api is still pre-roll."""
    lines = _lines()
    repin_at = _first(lines, "bigboy-repin.sh")
    carry_at = _first(lines, "dho goapi routing carry", start=repin_at)
    migrate_at = _first(lines, "--no-deps migrate", start=carry_at)
    up_at = _first(lines, "--no-build api query-api go-api", start=carry_at)
    assert repin_at < carry_at < migrate_at, (
        f"routing carry (line {carry_at + 1}) must run after repin (line {repin_at + 1}) "
        f"and before migrate (line {migrate_at + 1}) -- query-api must still be the "
        f"pre-roll process when carry reads it"
    )
    assert carry_at < up_at, (
        f"routing carry (line {carry_at + 1}) must run before the api/query-api/go-api "
        f"recreate (line {up_at + 1})"
    )


def test_carry_never_hand_types_the_target_digest() -> None:
    """CHAOS-7022 item 3: the target digest is read from the image's own binary, never typed."""
    lines = _lines()
    carry_at = _first(lines, "dho goapi routing carry")
    carry_line = lines[carry_at]
    assert "sha256:" not in carry_line, (
        "the carry invocation must not carry a hand-typed schema digest -- "
        "it is computed by the binary's own embedded SDL"
    )
    args_at = _first(lines, "CARRY_ARGS=")
    args_line = lines[args_at]
    assert "CARRY_ARGS" in carry_line, (
        "the carry call must reuse the shared CARRY_ARGS (registry/buildinfo endpoints), "
        "not repeat or diverge from it per call"
    )
    assert "-registry-url" in args_line and "-buildinfo-url" in args_line, (
        "carry must read the live digest from the deployed process's /registry, "
        "not from a flag"
    )


def test_digest_unchanged_refusal_is_treated_as_pass() -> None:
    """The digest-UNCHANGED path: carry's own 'digests already agree' refusal is a no-op, not
    a failure.

    D2828/D2829: branching on carry's raw error TEXT was replaced with `-json`'s `reason`
    field (a small, closed vocabulary set at the Go call site that knows why -- never
    guessed from prose, see carryResult's doc comment in internal/goapicli/routing/carry.go).
    This test now asserts the SHELL branches on `$CARRY_REASON = "digest_unchanged"`, not on
    any particular string carry happens to print -- test_bigboy_cut_carry_refusal_text_
    behavior.py proves the REAL dho binary actually reports that reason for this case, end
    to end, against a real fake registry server.
    """
    lines = _lines()
    carry_at = _first(lines, "dho goapi routing carry")
    block = "\n".join(lines[carry_at : carry_at + 15])
    assert 'CARRY_REASON" = "digest_unchanged"' in block, (
        "the digest-unchanged branch must key off -json's reason field, not off any "
        "particular error text"
    )
    assert "st routing-carry 0" in block and "no schema-digest change" in block


def test_carry_refusal_for_any_other_reason_aborts_before_the_recreate() -> None:
    """Refuse-not-skip: any OTHER carry refusal must abort the cut, not just log rc=1."""
    lines = _lines()
    carry_at = _first(lines, "dho goapi routing carry")
    migrate_at = _first(lines, "--no-deps migrate", start=carry_at)
    block = "\n".join(lines[carry_at:migrate_at])
    assert "st routing-carry 1" in block
    fail_at = block.index("st routing-carry 1")
    tail = block[fail_at:]
    assert "exit 1" in tail, (
        "a carry refusal other than the named digest-unchanged case must abort the cut "
        "(exit 1) before migrate/up ever runs -- refuse-not-skip, CHAOS-7022"
    )


def test_carry_has_the_repoint_then_retry_fallback() -> None:
    """The documented, real exception (rev196, both bigboy's re-cut and the actual prod
    roll): a refusal naming the stale-build reason triggers ONE repoint-then-retry before
    anything is treated as fatal.

    D2828/D2829: keyed off -json's `reason="stale_build"`, not off carry's raw error text
    (which was ALSO wrong at one point -- see this file's own git history -- text was never
    a stable contract to begin with). test_bigboy_cut_carry_refusal_text_behavior.py proves
    this end to end: a real seeded row, a real dho binary, the stale-build refusal, the
    repoint, and a real successful retry.
    """
    lines = _lines()
    carry_at = _first(lines, "dho goapi routing carry")
    migrate_at = _first(lines, "--no-deps migrate", start=carry_at)
    block = "\n".join(lines[carry_at:migrate_at])
    assert 'CARRY_REASON" = "stale_build"' in block, (
        "the stale-build repoint-then-retry branch must key off -json's reason field, not "
        "off any particular error text"
    )
    assert block.count("dho goapi routing repoint") == 1, (
        "exactly one repoint call in the pre-roll carry block's fallback"
    )
    assert block.count("dho goapi routing carry") == 2, (
        "exactly two carry attempts: the first, and the one retry after repoint"
    )


def test_carry_retry_failure_after_repoint_still_aborts() -> None:
    """Only a SECOND refusal (after the repoint-then-retry) is fatal -- but it must still
    be fatal, not silently accepted."""
    lines = _lines()
    carry_at = _first(lines, "dho goapi routing carry")
    migrate_at = _first(lines, "--no-deps migrate", start=carry_at)
    block = "\n".join(lines[carry_at:migrate_at])
    assert "still refused after repoint-then-retry" in block
    assert block.count("exit 1") >= 2, (
        "both the repoint-before-retry failure path and the carry-still-refused-after-retry "
        "path must abort (exit 1), not just the final catch-all"
    )


def test_repoint_runs_after_routing_parity_every_cut_unconditionally() -> None:
    """D2811 addendum: repoint runs after every cut, schema-change or not -- no digest gate."""
    lines = _lines()
    parity_at = _first(lines, "st routing-parity")
    repoint_at = _first(lines, "dho goapi routing repoint", start=parity_at)
    assert parity_at < repoint_at, (
        f"routing repoint (line {repoint_at + 1}) must run after routing-parity "
        f"(line {parity_at + 1}), against the newly-running build"
    )
    # Unconditional: the repoint invocation line itself carries no digest/schema comparison,
    # unlike carry -- it is provenance-only and safe every cut, schema-change or not.
    repoint_line = lines[repoint_at]
    assert "sha256:" not in repoint_line
    st_at = _first(lines, "st routing-repoint", start=repoint_at)
    between = "\n".join(lines[repoint_at:st_at])
    assert "if " not in between and "elif" not in between, (
        "repoint must run unconditionally every cut, not gated behind a digest-changed check"
    )
