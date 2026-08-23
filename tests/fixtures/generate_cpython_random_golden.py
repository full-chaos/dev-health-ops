"""Generate golden vectors for the CPython RNG port (CUT-20 R2, milestone 0).

WHY THIS EXISTS
---------------
capacity's product rows ARE Monte Carlo output: p50_days, p85_days, p50_items
and p85_items all come from monte_carlo_forecast_days/_items
(compute_capacity.py:137,191), which draw via `random.choice` after
`random.seed(seed)`. Go's math/rand produces a completely different stream from
the same seed, so an exact port of CPython's Mersenne Twister is the only way a
Go capacity executor can be compared row-for-row against Python.

These vectors come FROM the producer -- the live CPython `random` module -- not
from a reimplementation and not from a specification. A golden hand-derived
from the MT19937 paper would agree with the paper and disagree with CPython
wherever CPython's seeding or rejection sampling differs from the textbook, and
that disagreement is exactly the bug class this file exists to catch.

WHAT IS COVERED, AND WHY EACH CASE IS HERE
------------------------------------------
* getrandbits over many k -- k <= 32 is one tempered word shifted right, k > 32
  is assembled from several. The boundary cases 31/32/33 and 63/64 are where a
  port's word-assembly goes wrong.
* choice over NON-POWER-OF-TWO lengths -- `_randbelow(n)` masks to n.bit_length()
  bits and REJECTS any draw >= n. When n is a power of two nothing is ever
  rejected, so a port with a broken rejection loop matches perfectly. Lengths 3,
  5, 6, 7, 10, 100 and 1000 all reject; 2, 4, 8 and 1024 never do and are kept
  only as contrast.
* NEGATIVE seeds -- CPython seeds from abs(seed). A port that skips the
  absolute value produces a different stream for every negative seed and an
  identical one for every positive seed, so positive-only vectors would not
  notice.
* Seeds above 2**32 -- the seed is split into 32-bit little-endian chunks and
  fed through init_by_array. A single-word port agrees on small seeds and
  diverges on large ones.
* A LONG stream -- MT19937 regenerates its state block every 624 draws. A port
  that mishandles the twist agrees for 623 draws and then diverges, which no
  short vector would reveal.
"""

from __future__ import annotations

import json
import random
import sys
from typing import Any

# Chosen so the set exercises: zero, one, a small ordinary value, negatives
# (abs() path), exactly 2**32 (first two-chunk value), and a multi-chunk value.
SEEDS = [0, 1, 42, -1, -12345, 2**32, 2**64 + 12345, 6364136223846793005]

# k <= 32 is a single shifted word; above that CPython assembles words.
BIT_WIDTHS = [1, 2, 7, 8, 15, 16, 31, 32, 33, 53, 63, 64]

# Lengths that DO reject (not powers of two) and lengths that never do.
CHOICE_LENGTHS = [2, 3, 4, 5, 6, 7, 8, 10, 100, 1000, 1024]

DRAWS = 64
LONG_STREAM_DRAWS = 10_000


def getrandbits_case(seed: int, k: int) -> dict[str, Any]:
    random.seed(seed)
    return {
        "kind": "getrandbits",
        "seed": str(seed),
        "k": k,
        "draws": [str(random.getrandbits(k)) for _ in range(DRAWS)],
    }


def choice_case(seed: int, n: int) -> dict[str, Any]:
    """Record the INDEX choice() lands on, not the element.

    Choosing from range(n) makes the element equal its own index, which isolates
    _randbelow -- the part that can actually be wrong -- from list indexing.
    """
    random.seed(seed)
    population = list(range(n))
    return {
        "kind": "choice_index",
        "seed": str(seed),
        "n": n,
        "rejects": n & (n - 1) != 0,
        "draws": [random.choice(population) for _ in range(DRAWS)],
    }


def long_stream_case(seed: int, n: int) -> dict[str, Any]:
    """A stream long enough to cross many MT19937 state regenerations.

    624 draws exhaust one state block. Ten thousand crosses it sixteen times,
    so a port that gets the twist wrong cannot stay in agreement.
    """
    random.seed(seed)
    population = list(range(n))
    return {
        "kind": "choice_index_long",
        "seed": str(seed),
        "n": n,
        "rejects": n & (n - 1) != 0,
        "draws": [random.choice(population) for _ in range(LONG_STREAM_DRAWS)],
    }


def interleaved_case(seed: int) -> dict[str, Any]:
    """One seeding, then a MIXED call sequence.

    Every other case reseeds before it runs, so each only ever inspects the
    stream's opening. This one seeds once and interleaves different call shapes,
    which is what catches a port that advances its state by the wrong amount for
    one call type -- an error invisible while each shape is measured alone.
    """
    random.seed(seed)
    operations: list[dict[str, Any]] = []
    population_7 = list(range(7))
    population_100 = list(range(100))
    for index in range(200):
        if index % 3 == 0:
            operations.append(
                {"op": "getrandbits", "k": 17, "value": str(random.getrandbits(17))}
            )
        elif index % 3 == 1:
            operations.append(
                {"op": "choice", "n": 7, "value": str(random.choice(population_7))}
            )
        else:
            operations.append(
                {"op": "choice", "n": 100, "value": str(random.choice(population_100))}
            )
    return {"kind": "interleaved", "seed": str(seed), "operations": operations}


def build() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for seed in SEEDS:
        for k in BIT_WIDTHS:
            cases.append(getrandbits_case(seed, k))
        for n in CHOICE_LENGTHS:
            cases.append(choice_case(seed, n))
        cases.append(interleaved_case(seed))
    # Long streams over a rejecting and a non-rejecting length, so a twist bug
    # is caught whether or not the rejection loop is involved.
    cases.append(long_stream_case(12345, 7))
    cases.append(long_stream_case(12345, 8))
    return {
        "generator": "tests/fixtures/generate_cpython_random_golden.py",
        "python_version": sys.version.split()[0],
        "note": (
            "Vectors captured from the live CPython random module. Regenerate "
            "only with a deliberate reason -- a changed golden means either "
            "CPython changed its stream or someone edited the producer."
        ),
        "cases": cases,
    }


def main(argv: list[str]) -> int:
    document = build()
    if len(argv) > 1 and argv[1] == "--check":
        with open(argv[2], encoding="utf-8") as handle:
            existing = json.load(handle)
        if existing.get("cases") != document["cases"]:
            sys.stderr.write(
                "cpython random golden is STALE: the live interpreter no longer "
                "produces the recorded stream\n"
            )
            return 1
        sys.stdout.write("CPYTHON_RANDOM_GOLDEN_CURRENT\n")
        return 0
    sys.stdout.write(json.dumps(document, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
