"""The weights generator ci/venue_oracle_weights.py reads a run log (its own test; goes with the script)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_the_weights_generator_reads_a_run_log(tmp_path: Path) -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "venue_oracle_weights", ROOT / "ci" / "venue_oracle_weights.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    prefix = "venue-oracles (shard 1/7)\tRun the shard\t2026-09-26T08:02:13.4333960Z "
    other = "venue-oracles (shard 2/7)\tRun the shard\t2026-09-26T08:02:14.0000000Z "
    lines = [
        prefix + "    --- PASS: TestNotTop/sub (9.00s)",  # a subtest: not a row
        prefix + "--- PASS: TestOne (10.20s)",
        other + "--- PASS: TestTwo (30.00s)",  # another leg's test, interleaved
        prefix + "--- PASS: TestThree (5.01s)",
        prefix + "ok  \tgithub.com/full-chaos/dev-health-ops/internal/a\t15.3s",
        other + "ok  \tgithub.com/full-chaos/dev-health-ops/internal/b\t31.0s",
        "--- PASS: TestOne (99.00s)",  # a plain (unprefixed) log line, other leg id ""
        "FAIL\tgithub.com/full-chaos/dev-health-ops/internal/a\t1.0s",
    ]
    seen: dict[tuple[str, str], float] = {}
    module.parse_log(lines, seen)
    assert seen == {
        ("internal/a", "TestOne"): 99.0,  # the largest across the logs
        ("internal/a", "TestThree"): 5.01,
        ("internal/b", "TestTwo"): 30.0,
    }
    text, unmeasured = module.build(
        [
            ("internal/a", "TestOne"),
            ("internal/a", "TestThree"),
            ("internal/b", "TestTwo"),
            ("internal/c", "TestKept"),
            ("internal/c", "TestNew"),
            ("internal/d", "TestStill"),
        ],
        seen,
        {
            ("internal/c", "TestKept"): (77, False),
            ("internal/d", "TestStill"): (5, True),
            ("internal/gone", "TestGone"): (5, False),
        },
    )
    body = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert body == [
        "internal/a\tTestOne\t99",
        "internal/a\tTestThree\t6",  # rounded up
        "internal/b\tTestTwo\t30",
        "internal/c\tTestKept\t77",  # no log measured it: the existing weight stays
        "internal/c\tTestNew\t600\tunmeasured",  # a new row: planned pessimistically
        "internal/d\tTestStill\t5\tunmeasured",  # still unmeasured: flag kept
    ]
    assert unmeasured == [("internal/c", "TestNew"), ("internal/d", "TestStill")]
    # a measurement clears the flag
    text, unmeasured = module.build(
        [("internal/d", "TestStill")],
        {("internal/d", "TestStill"): 41.2},
        {("internal/d", "TestStill"): (5, True)},
    )
    assert "internal/d\tTestStill\t42\n" in text and unmeasured == []
    assert module.PROVISIONAL_WEIGHT >= 600
    assert text.startswith("# Measured seconds of one venue-oracles registry `run` row")
