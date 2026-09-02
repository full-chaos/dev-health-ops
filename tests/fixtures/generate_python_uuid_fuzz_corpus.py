"""Brute-force `uuid.UUID`'s accept set over a seeded random alphabet.

# Why this exists alongside the hand-picked corpus

`generate_python_uuid_accept_set.py` holds cases a human chose, and a
hand-picked corpus can only contain the axes its author already knew about.
That limit was not hypothetical here: the curated corpus was ASCII-only and
hex-digit-only for four review rounds, so it could not see that the last step
of the normalisation is `int(hex, 16)` rather than a hex decode. A review round
found that; the corpus did not.

This generator removes the author from the loop. It samples strings from an
alphabet of every character CLASS that the normalisation and the `int()`
grammar treat specially, and records CPython's verdict for each. It does not
know what the interesting cases are, which is the point -- it finds
combinations nobody thought to write down, including interactions between
phases (a fullwidth digit that becomes the `0` of an `0x` prefix only after the
Unicode fold, say).

The two corpora have different jobs and neither replaces the other: the curated
one documents WHY each case matters and stays readable, and this one covers the
combinations. Both are regression floors -- do not prune either.

Deterministic: the seed is recorded in the output and the row order is stable,
so a regenerated corpus that differs signals a CHANGED INTERPRETER, not noise.

Regenerate:
    python tests/fixtures/generate_python_uuid_fuzz_corpus.py \
        --out internal/pythonparity/testdata/uuid_fuzz_corpus.json
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import sys
import unicodedata
import uuid

# One representative per class the normalisation or the int() grammar reacts
# to. Weights bias toward the characters that make a value PLAUSIBLE (hex
# digits and hyphens), so a useful share of samples survives the length gate
# and reaches the grammar behind it -- an alphabet of pure exotica would spend
# every sample proving the gate rejects junk.
ALPHABET: list[tuple[str, int]] = [
    # Hex digits, both cases: the bulk of any value that gets past the gate.
    *[(c, 40) for c in "0123456789"],
    *[(c, 20) for c in "abcdef"],
    *[(c, 8) for c in "ABCDEF"],
    # Structural characters the normalisation removes or strips.
    ("-", 30),
    ("{", 10),
    ("}", 10),
    (":", 6),
    ("u", 6),
    ("r", 4),
    ("n", 4),
    ("i", 4),
    ("d", 4),
    # int() grammar: sign, prefix, separator.
    ("+", 8),
    ("_", 8),
    ("x", 8),
    ("X", 4),
    # int()'s space set, and two near-misses that are NOT in it.
    (" ", 8),
    ("\t", 3),
    ("\n", 3),
    ("\r", 2),
    ("\x0b", 1),
    ("\x0c", 1),
    ("\x85", 2),
    ("\xa0", 2),
    (" ", 1),
    (" ", 1),
    (" ", 1),
    (" ", 1),
    ("　", 2),
    ("\x1c", 2),  # str.isspace() says yes, int() says no
    ("​", 2),  # looks blank, is not a space to either
    # Unicode decimal digits from several scripts: these fold to ASCII digits.
    ("１", 6),  # FULLWIDTH ONE
    ("０", 4),  # FULLWIDTH ZERO
    ("٠", 4),  # ARABIC-INDIC ZERO
    ("٩", 3),  # ARABIC-INDIC NINE
    ("༡", 3),  # TIBETAN ONE
    ("᱐", 2),  # OL CHIKI ZERO
    ("\U0001d7ce", 2),  # MATHEMATICAL BOLD ZERO (non-BMP)
    # Non-Nd lookalikes that must NOT fold.
    ("ａ", 2),  # FULLWIDTH LATIN A
    ("①", 1),  # CIRCLED ONE (No, not Nd)
    ("½", 1),  # VULGAR FRACTION (No, not Nd)
]

CHARS = [c for c, _ in ALPHABET]
WEIGHTS = [w for _, w in ALPHABET]

# Digit characters that FOLD to an ASCII hex digit, for substitution into a body
# that would otherwise be plain ASCII.
FOLDING_DIGITS = ["１", "０", "٠", "٩", "༡", "᱐", "\U0001d7ce"]

SEED = 20260902
SAMPLES = 4000

# Sampling is GRAMMAR-DRIVEN, not uniform over the alphabet.
#
# The first version of this generator drew 32 characters uniformly and had SIX
# accepted rows out of four thousand: a random draw essentially never satisfies
# the length gate, so every sample tested the gate and none tested the grammar
# behind it. The yield guard at the bottom of main() caught that, which is why
# it is there.
#
# So a sample is BUILT the way a real value is built -- a body of the right
# order of length, then mutations layered on -- and the exotic characters enter
# through the mutations. That keeps samples near the accept/reject boundary,
# which is the only region where a divergence can hide.


def _body(rng: random.Random) -> str:
    length = rng.choice([32] * 12 + [31, 33, 30, 34])
    body = "".join(rng.choices("0123456789abcdefABCDEF", k=length))
    for _ in range(rng.choice([0, 0, 0, 1, 1, 2, 3])):
        position = rng.randrange(len(body))
        body = body[:position] + rng.choice(FOLDING_DIGITS) + body[position + 1 :]
    return body


def _mutate(rng: random.Random, value: str) -> str:
    """Apply one mutation drawn from the classes the normalisation reacts to."""
    kind = rng.choice(
        [
            "hyphens",
            "hyphens",
            "brace",
            "urn",
            "space",
            "sign",
            "prefix",
            "underscore",
            "insert",
            "insert",
            "truncate",
        ]
    )
    if kind == "hyphens":
        for _ in range(rng.randint(1, 5)):
            position = rng.randrange(len(value) + 1)
            value = value[:position] + "-" + value[position:]
        return value
    if kind == "brace":
        return (
            rng.choice(["{", "{{", "}", "", "}}"])
            + value
            + rng.choice(["}", "}}", "{", "", "{{"])
        )
    if kind == "urn":
        return (
            rng.choice(["urn:", "uuid:", "urn:uuid:", "URN:UUID:", "urn:urn:"]) + value
        )
    if kind == "space":
        space = rng.choice(
            [" ", "\t", "\n", "\r", "\x0b", "\x85", "\xa0", " ", "　", "\x1c", "​"]
        )
        if rng.random() < 0.3:  # interior, which never parses
            position = rng.randrange(1, len(value))
            return value[:position] + space + value[position:]
        return space * rng.randint(1, 2) + value + space * rng.randint(0, 2)
    if kind == "sign":
        return rng.choice(["+", "-", " +", "+ "]) + value
    if kind == "prefix":
        return rng.choice(["0x", "0X", "٠x", "0o", "0b", "0x0x"]) + value
    if kind == "underscore":
        for _ in range(rng.randint(1, 3)):
            position = rng.randrange(len(value) + 1)
            value = value[:position] + "_" + value[position:]
        return value
    if kind == "truncate":
        return value[: rng.randrange(max(1, len(value) - 3), len(value) + 1)]
    position = rng.randrange(len(value) + 1)
    return (
        value[:position]
        + rng.choices(CHARS, weights=WEIGHTS, k=1)[0]
        + value[position:]
    )


def sample(rng: random.Random) -> str:
    value = _body(rng)
    for _ in range(rng.choice([0, 1, 1, 1, 2, 2, 3])):
        value = _mutate(rng, value)
    return value


def seeded_corpus() -> list[str]:
    """Sample, then de-duplicate while preserving first-seen order."""
    rng = random.Random(SEED)
    seen: dict[str, None] = {}
    for _ in range(SAMPLES):
        seen.setdefault(sample(rng), None)
    return list(seen)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out")
    args = parser.parse_args()

    rows = []
    for case in seeded_corpus():
        try:
            rows.append(
                {"input": case, "verdict": "ACCEPT", "uuid": str(uuid.UUID(case))}
            )
        except Exception as error:  # noqa: BLE001 - the verdict IS the exception
            rows.append(
                {"input": case, "verdict": "REJECT", "error": type(error).__name__}
            )

    accepted = sum(1 for row in rows if row["verdict"] == "ACCEPT")
    if accepted < 100:
        # A corpus that rejects nearly everything tests the length gate over and
        # over and the grammar behind it never. Fail the GENERATOR rather than
        # ship a corpus that looks broad and is not.
        raise SystemExit(
            f"only {accepted} of {len(rows)} samples were accepted; "
            "the alphabet or length distribution needs rebalancing"
        )

    payload = (
        json.dumps(
            {
                "schema": "python_uuid_fuzz_corpus.v1",
                "seed": SEED,
                "samples": SAMPLES,
                "measured_on": platform.python_version(),
                "unicodedata": unicodedata.unidata_version,
                "accepted": accepted,
                "cases": rows,
            },
            indent=1,
            sort_keys=True,
        )
        + "\n"
    )
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(
            f"written {args.out}: {len(rows)} rows, {accepted} accepted",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
