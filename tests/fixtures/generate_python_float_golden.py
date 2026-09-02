"""Generate the Python float() coercion golden for CHAOS-4441.

WHY A GRAMMAR AND NOT strconv.ParseFloat
----------------------------------------
`membership._float_value` coerces a weight with a bare `float(value)` inside a
try/except ValueError. Go's `strconv.ParseFloat` is the obvious substitute and it
is wrong in BOTH directions -- measured, not assumed:

    "0x1p-2"    Python -> ValueError -> 0.0     Go -> 0.25   (Go too permissive)
    "1e309"     Python -> inf                   Go -> +Inf WITH ErrRange, so a
                                                port mapping any error to 0.0
                                                yields 0.0        (too strict)
    "1_000.5"   Python -> 1000.5                Go -> error
    " 1.5 "     Python -> 1.5                   Go -> error
    "１２３"      Python -> 123.0                 Go -> error

Both directions change a persisted `weight`, and weight decides both threshold
inclusion in `membership_categories` and the argmax that sets `is_dominant`.

WHAT CPython ACTUALLY DOES, from the source rather than from the observations
----------------------------------------------------------------------------
`float(str)` is `float_new_impl` -> `PyFloat_FromString`, which:

  1. Calls `_PyUnicode_TransformDecimalAndSpaceToASCII`. Every Unicode DECIMAL
     digit (category Nd) becomes its ASCII counterpart and every Unicode space
     becomes ' '. This is why full-width digits parse, and it is the same Nd set
     `int()` accepts -- see generate_python_decimal_digits_golden.py, which
     already emits that table.
  2. Calls `_Py_string_to_number_with_underscores`, which strips underscores but
     REJECTS any placement that is not strictly between two digits.
  3. Parses the remaining ASCII with `PyOS_string_to_double`, whose grammar has
     decimal, `inf`/`infinity`, and `nan` branches -- and NO hex branch. That
     absence is why "0x1p-2" raises. `float.fromhex` is the hex entry point and
     is a different function.

The corpus below drives the real builtin, so it records behaviour rather than my
reading of the source. The source note is here to explain WHY the cases are
grouped as they are, not to stand in for the measurement.

Usage:
    uv run python tests/fixtures/generate_python_float_golden.py [--stdout]
"""

from __future__ import annotations

import json
import struct
import sys
import unicodedata
from pathlib import Path

from dev_health_ops.work_graph.investment.membership import _float_value

OUTPUT_PATH = Path(__file__).parent / "python_float_python_golden.json"


# NOTE ON _float_value: it is IMPORTED above, not copied. An earlier revision of
# this file inlined the body, justified as needing to cover NON-string inputs.
# That reasoning was wrong -- the reference takes `object` and handles those
# itself, so importing loses nothing. A copied body makes the corpus measure MY
# TRANSCRIPTION of the reference rather than the reference, which is exactly the
# failure CHAOS-4803 is about. Caught by a codex round.


def _bits(value: float) -> str:
    """IEEE-754 bits as hex.

    The corpus compares BITS, not values. -0.0 == 0.0 is True in both languages,
    so a value comparison silently accepts a sign-of-zero divergence -- exactly
    the hole that got through in the pythonparity.Sum comparator until a codex
    round caught it. NaN payloads are pinned for the same reason.
    """
    return format(struct.unpack("<Q", struct.pack("<d", value))[0], "016x")


def _string_cases() -> list[str]:
    cases: list[str] = [
        # Plain decimals, including the forms with an implicit side.
        "0",
        "1",
        "-1",
        "1.5",
        "-1.5",
        ".5",
        "-.5",
        "5.",
        "-5.",
        "0.0",
        "-0.0",
        # Exponents, including overflow and underflow to zero.
        "1e3",
        "1E3",
        "1e+3",
        "1e-3",
        "1.5e10",
        "1e309",
        "-1e309",
        "1e-400",
        "-1e-400",
        "1e",
        "e5",
        "1e+",
        "1e-",
        # Specials. Case-insensitive, sign accepted, "infinity" spelled out.
        "inf",
        "INF",
        "Inf",
        "-inf",
        "+inf",
        "infinity",
        "INFINITY",
        "-infinity",
        "nan",
        "NAN",
        "NaN",
        "-nan",
        "+nan",
        "infi",
        "infinit",
        "nano",
        # Underscores: legal only strictly between digits.
        "1_000",
        "1_000.5",
        "1_0e1_0",
        "1__0",
        "_1",
        "1_",
        "1._5",
        "1_.5",
        "-1_000",
        "1_000_000",
        # Whitespace. float() strips a specific set; the corpus pins which.
        " 1.5",
        "1.5 ",
        " 1.5 ",
        "\t1.5",
        "1.5\n",
        "\r1.5",
        "\f1.5",
        "\v1.5",
        "\x1c1.5",
        "\x1d1.5",
        "\x1e1.5",
        "\x1f1.5",
        "\x851.5",
        " 1.5",
        " 1.5",
        " 1.5",
        "​1.5",
        "1 . 5",
        " ",
        "",
        # Unicode decimal digits: Nd parses, other numeric classes do not.
        "１２３",  # full-width 123
        "١٢٣",  # Arabic-Indic 123
        "१२३",  # Devanagari 123
        "１.５",  # full-width 1.5
        "²",  # superscript two: isdigit, NOT isdecimal
        "①",  # circled one: isdigit, NOT isdecimal
        "Ⅻ",  # Roman numeral twelve: isnumeric only
        # CASE FOLDING OF THE inf/nan WORDS MUST BE ASCII-ONLY.
        #
        # CPython matches these with a byte-wise tolower (PyOS_strnicmp). Go's
        # strings.ToLower is full Unicode and maps U+0130 to 'i', so "\u0130NF"
        # folds to "inf" and matches -- accepting a value Python rejects. That
        # defect was live in floatcoerce.go until this corpus grew these cases;
        # it is the exact class the file exists to prevent, reintroduced by the
        # case-folding step inside it.
        #
        # The fullwidth spellings are the control: they must ALSO be rejected,
        # and they are rejected by both foldings, so they cannot detect the bug
        # on their own. Only the U+0130 cases discriminate.
        "\u0130NF",
        "\u0130nf",
        "iNF\u0130N\u0130TY",
        "\u0130NFINITY",
        "n\u0130n",
        "NA\u0130",
        "\uff29\uff2e\uff26",  # fullwidth INF, control
        "\uff2e\uff21\uff2e",  # fullwidth NAN, control
        "\u212ain",  # KELVIN SIGN, lowercases to 'k' in Go
        "in\u0066",  # plain 'f', a same-shape control
        # Hex: rejected by float(), accepted by Go's ParseFloat. The whole point.
        "0x1p-2",
        "0X1P-2",
        "0x10",
        "0xff",
        "0b101",
        "0o17",
        # Sign and separator noise.
        "+1.5",
        "++1.5",
        "--1.5",
        "1.5.5",
        "1,5",
        "1 5",
        "--",
        "+",
        "-",
        # Things that look like numbers but are not.
        "None",
        "True",
        "null",
        "1.5f",
        "1.5d",
        "L1",
        "1L",
        # THE CROSS PRODUCT of character class and magnitude.
        #
        # This corpus varied character class thoroughly at SHORT lengths and
        # magnitude thoroughly in ASCII, and never both -- 3 long ASCII cases,
        # 21 short non-ASCII cases, 0 that are both. Each axis covered, the
        # intersection empty. Raised by lane-4752-go, whose corpus had the same
        # shape for the same reason and whose discriminating input was 2151
        # full-width digits (2151 runes, 6453 bytes).
        #
        # "Vary every axis" reads as satisfied by a corpus like that. The
        # discriminating input lives in the product, and a rune-vs-byte or
        # buffer-sizing defect in the Nd transform only shows up when a
        # multi-byte character appears at length.
        "\uff11" * 100,  # 100 full-width ones
        "\uff10" * 400 + "\uff11",  # 401 full-width, leading zeros
        "\uff19" * 400,  # 400 full-width nines
        "\u0661" * 300,  # 300 Arabic-Indic ones
        "\uff11" * 50 + "." + "\uff15" * 50,  # full-width with a fraction point
        "\uff11" * 30 + "e" + "\uff11" * 3,  # full-width mantissa AND exponent
        " " * 40 + "\uff11" * 40,  # leading run of spaces then digits
        "\uff11" * 40 + "_" + "\uff11" * 40,  # underscore between full-width digits
        # Long inputs: digit-limit behaviour differs from int(), which has one.
        "1" * 100,
        "0." + "0" * 400 + "1",
        "9" * 400,
    ]
    return cases


def _typed_cases() -> list[tuple[str, object]]:
    """Non-string inputs, to pin the isinstance ladder including bool-before-int."""
    return [
        ("bool_true", True),
        ("bool_false", False),
        ("int_zero", 0),
        ("int_positive", 42),
        ("int_negative", -42),
        ("int_huge", 2**70),
        ("float_value", 1.5),
        ("float_negative_zero", -0.0),
        ("float_inf", float("inf")),
        ("float_nan", float("nan")),
        ("none", None),
        ("list", [1.0]),
        ("dict", {"a": 1.0}),
        ("bytes", b"1.5"),
    ]


def main() -> None:
    string_cases = []
    for raw in _string_cases():
        try:
            float(raw)
            raises = False
        except ValueError:
            raises = True
        value = _float_value(raw)
        string_cases.append(
            {
                # Carried as escaped codepoints so the fixture survives any
                # editor or transport that would normalise the exotic ones.
                "input_codepoints": [ord(character) for character in raw],
                "float_raises_value_error": raises,
                "result_bits": _bits(value),
                "result_repr": repr(value),
            }
        )

    typed_cases = []
    # `typed_value`, not `value`: the string loop above binds `value` to a float,
    # and rebinding the same name to an `object` here is a type error mypy is
    # right to reject. Worth a comment because the two loops read as parallel and
    # the shadowing is invisible at a glance.
    for label, typed_value in _typed_cases():
        result = _float_value(typed_value)
        typed_cases.append(
            {
                "label": label,
                "python_type": type(typed_value).__name__,
                "result_bits": _bits(result),
                "result_repr": repr(result),
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_python_float_golden.py. "
            "Do not hand-edit."
        ),
        "_policy": (
            "float(str) transforms Unicode Nd digits and Unicode spaces to ASCII, "
            "strips underscores only when strictly between digits, then parses a "
            "grammar with decimal/inf/infinity/nan branches and NO hex branch. "
            "strconv.ParseFloat differs in both directions: it accepts hex floats "
            "Python rejects, and it returns ErrRange alongside +Inf for 1e309 "
            "where Python simply returns inf."
        ),
        "unidata_version": unicodedata.unidata_version,
        "python_version": sys.version.split()[0],
        "string_cases": string_cases,
        "typed_cases": typed_cases,
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    raised = sum(1 for c in string_cases if c["float_raises_value_error"])
    print(f"wrote {OUTPUT_PATH}")
    print(f"  string cases: {len(string_cases)}  ({raised} raise ValueError -> 0.0)")
    print(f"  typed cases:  {len(typed_cases)}")


if __name__ == "__main__":
    main()
