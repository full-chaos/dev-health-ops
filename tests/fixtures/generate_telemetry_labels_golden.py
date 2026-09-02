"""Generate the LLM-telemetry label golden for CHAOS-4441.

IMPORTS the reference bucketing functions and the frozensets; it never restates
them. A transcribed allow-list would pass the day it was written and silently
diverge the first time someone adds a provider or an error family.

THREE THINGS THAT LOOK SAFE AND ARE NOT
---------------------------------------
1. `.strip()` IS `str.isspace()`, NOT float()'s set.

   Measured: U+001C, U+001D, U+001E and U+001F are all stripped here, and so are
   U+0085, U+00A0 and U+2028; U+200B is not. That is the 29-code-point
   `str.isspace()` set, so the Go port must use pythonparity.Strip.

   This is the OPPOSITE of floatcoerce.go, where float()'s narrower 25-point set
   is correct and pythonparity.Strip would be wrong. Same repo, same lane, two
   adjacent functions, two different whitespace classes. Picking by familiarity
   picks wrong roughly half the time, which is why both sides are pinned.

2. `str.lower()` is NOT a rune-wise lowercase, so it is NOT Go's
   strings.ToLower. Two measured divergences:

     'ΟΔΟΣ'.lower() -> 'οδος'   final sigma, context-sensitive
                       Go gives 'οδοσ' -- no such rule
     'İ'.lower()    -> 'i' + U+0307, TWO code points
                       Go's unicode.ToLower gives one, dropping the dot

   A provider or model string containing either produces a different bucket in
   the two planes. `bounded` then maps the mismatch to "other" on one side only,
   so the divergence surfaces as quietly wrong telemetry rather than an error.

3. model_bucket's prefix chain is ORDER-DEPENDENT. "gpt-5-nano" also starts with
   "gpt-5", so checking the shorter prefix first would collapse nano and mini
   into "openai-reasoning-other". Measured: 'gpt-5-turbo' -> reasoning-other
   while 'gpt-5-nano-2025' -> gpt-5-nano, which only holds in the reference's
   order.

classify_llm_exception_family is deliberately NOT covered here: it dispatches on
the LLM error class hierarchy, which belongs with the client port, not the
compute plane.

Usage:
    uv run python tests/fixtures/generate_telemetry_labels_golden.py [--stdout]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.llm_telemetry_labels import (
    CATEGORIZATION_STATUSES,
    PARSE_STATUSES,
    PROMPT_KINDS,
    PROMPT_VERSIONS,
    PROVIDERS,
    STAGES,
    VALIDATION_ERROR_FAMILIES,
    bounded,
    model_bucket,
    provider_bucket,
    validation_error_family,
)

OUTPUT_PATH = Path(__file__).parent / "telemetry_labels_python_golden.json"

# Every code point str.isspace() accepts, plus near-misses that it does not.
# U+001C-U+001F are the discriminator against float()'s set; U+200B and U+180E
# are the discriminator against "anything that looks blank".
WHITESPACE_PROBES = [
    0x20,
    0x09,
    0x0A,
    0x0B,
    0x0C,
    0x0D,
    0x1C,
    0x1D,
    0x1E,
    0x1F,
    0x85,
    0xA0,
    0x1680,
    0x2000,
    0x2028,
    0x2029,
    0x202F,
    0x205F,
    0x3000,
    0x200B,
    0x180E,
    0xFEFF,
]


def _lower_cases() -> list[str]:
    """Strings whose str.lower() is compared DIRECTLY against the Go port.

    Position is an axis in its own right here. Final_Sigma depends on WHERE the
    sigma sits, not on which rune it is, so a corpus can vary the character class
    exhaustively and still hold position constant -- which is how lane-4752-go's
    derived rot guard missed it while enumerating every multi-rune mapping the
    interpreter reports.
    """
    return [
        # --- sigma by position ---
        "\u039f\u0394\u039f\u03a3",  # final
        "A\u03a3B",  # medial, cased letter follows
        "\u03a3A",  # initial
        "A\u03a3",  # final
        "A\u03a3.B",  # case-ignorable then a cased letter
        "A\u03a3.",  # case-ignorable, nothing cased after
        "\u03a3",  # alone: no preceding cased letter
        "\u0391\u03a3",  # final after a Greek capital
        # --- multi-rune and reaching-ASCII mappings ---
        "\u0130",
        "\u0130stanbul",
        "A\u0130B",  # U+0130 -> two code points
        "\u212a",
        "moc\u212a",
        "\u212aelvin",  # KELVIN -> ascii 'k'
        "\u017f",
        "fal\u017fe",  # LONG S: already lowercase
        # --- controls that must be unchanged or plain ---
        "abc",
        "ABC",
        "",
        " ",
        "123",
        "\u00df",
        "\u1e9e",
    ]


def _provider_cases() -> list[str]:
    cases = sorted(PROVIDERS)
    cases += [
        "",
        "OpenAI",
        "  openai  ",
        "OPENAI",
        "openai\t",
        "\nopenai\n",
        "open ai",
        "openai2",
        "azure",
        "bedrock",
        "unknown-provider",
        # U+212A KELVIN SIGN is the ONLY non-ASCII letter that str.lower()
        # maps INTO an ASCII letter this allow-list uses -- it lowercases to
        # "k", and both "mock" and "unknown" contain one. So these reach a
        # REAL bucket rather than "other", in both planes.
        #
        # Pinned because the corpus held this axis constant, and because the
        # axis is asymmetric in a way that is easy to get backwards: U+017F
        # (LONG S) does NOT reach "s" under lower() -- it is already
        # lowercase, so "fal\u017fe".lower() is unchanged and Python rejects
        # it. It DOES reach "s" under Unicode simple folding, which is why
        # strings.EqualFold is unsafe for s/k keywords while lower() is unsafe
        # only for k. Raised by lane-pathb-go; verified against the
        # interpreter.
        "moc\u212a",
        "MOC\u212a",
        "un\u212anown",
        "UN\u212aNOWN",
        "fal\u017fe",
        "\u017ftatus",
        # SIGMA POSITION is its own axis, distinct from character class.
        # Final_Sigma depends on WHERE the sigma sits, not on which rune it
        # is: capital sigma lowercases to the final form only when no cased
        # letter follows (skipping case-ignorables). So a corpus can vary the
        # rune exhaustively and still hold position constant.
        #
        # This corpus previously covered both forms only by accident --
        # "\u039f\u0394\u039f\u03a3" happens to be final and
        # "\u039f\u0394\u039f\u03a3-model" happens to be medial because a
        # suffix was appended for unrelated reasons. Made deliberate here.
        # Raised by lane-4752-go, whose derived rot guard enumerated every
        # multi-rune lowercase mapping from the live interpreter and STILL
        # missed final sigma, because it is a single-rune mapping that
        # depends on position. Deriving a corpus from the reference removes
        # the transcription risk, not the axis risk.
        "A\u03a3B",  # medial: a cased letter follows -> medial sigma
        "\u03a3A",  # initial
        "A\u03a3",  # final: nothing follows -> final sigma
        "A\u03a3.B",  # case-ignorable between sigma and a cased letter
        "A\u03a3.",  # case-ignorable, nothing cased after -> still final
        # The lower() divergences, as provider strings.
        "ΟΔΟΣ",
        "İ",
        "İstanbul",
        "ΑΣ",
        "SS",
        "ẞ",
    ]
    cases += [chr(cp) + "openai" + chr(cp) for cp in WHITESPACE_PROBES]
    return cases


def _model_cases() -> list[str | None]:
    return [
        None,
        "",
        "   ",
        "\t\n",
        # The prefix chain, longest-first, and the strings that discriminate it.
        "gpt-5-nano",
        "gpt-5-nano-2025-08-07",
        "gpt-5-mini",
        "gpt-5-mini-x",
        "gpt-5",
        "gpt-5-turbo",
        "gpt-6",
        "gpt-6-preview",
        "openai/gpt-oss",
        "openai/gpt-oss-20b",
        "gpt-4",
        "gpt-4o",
        "gpt-4-turbo",
        "claude",
        "claude-opus-4",
        "Claude-3-5-Sonnet",
        "gemini",
        "gemini-2.5-pro",
        "qwen",
        "qwen2.5-coder",
        "Qwen/Qwen3-8B",
        "llama",
        "llama-3.1-70b",
        "local-model",
        "local",
        # "local" is NOT in the ("llama", "local-") tuple -- it needs the
        # hyphen. Pinned because the near-miss is easy to "fix" into a bug.
        "mistral",
        "deepseek",
        "unknown-model",
        "  GPT-5-NANO  ",
        "GPT-4O",
        # lower() divergences as model strings.
        "ΟΔΟΣ-model",
        "İ-model",
    ]


def _validation_error_cases() -> list[str]:
    cases = sorted(VALIDATION_ERROR_FAMILIES)
    cases += [
        f"{family}: detail here" for family in sorted(VALIDATION_ERROR_FAMILIES)[:5]
    ]
    cases += [
        "",
        ":",
        "::",
        "no_colon_at_all",
        "  invalid_json  : detail",
        "invalid_json:detail:more",
        "unknown_family: detail",
        "INVALID_JSON: detail",
        # split(":", 1)[0] then .strip() -- leading whitespace before the family
        # is stripped, so these are the same family.
        "\tinvalid_json: x",
        "\x1cinvalid_json: x",
    ]
    return cases


def main() -> None:
    payload: dict[str, Any] = {
        "_comment": (
            "Generated by tests/fixtures/generate_telemetry_labels_golden.py. "
            "Do not hand-edit."
        ),
        "_policy": (
            "strip() is str.isspace() (29 code points, includes U+001C-U+001F) -- "
            "NOT float()'s 25-point set. lower() is NOT rune-wise: final sigma and "
            "U+0130 both diverge from Go's strings.ToLower. model_bucket's prefix "
            "chain is order-dependent because gpt-5-nano also starts with gpt-5."
        ),
        # Allow-lists, sorted. Sorted because these are frozensets, whose
        # iteration order varies with PYTHONHASHSEED -- the same hazard as the
        # taxonomy sets.
        "prompt_kinds": sorted(PROMPT_KINDS),
        "stages": sorted(STAGES),
        "prompt_versions": sorted(PROMPT_VERSIONS),
        "providers": sorted(PROVIDERS),
        "categorization_statuses": sorted(CATEGORIZATION_STATUSES),
        "parse_statuses": sorted(PARSE_STATUSES),
        "validation_error_families": sorted(VALIDATION_ERROR_FAMILIES),
        # str.lower() results DIRECTLY, not filtered through a bucket.
        #
        # The provider/model cases cannot test pythonLower's sigma handling at
        # all: every sigma-bearing string is outside the ASCII allow-list under
        # BOTH spellings, so it buckets to "other" either way. Verified by
        # swapping the Go implementation to strings.ToLower -- zero provider
        # subtests failed.
        #
        # That is this package's own gates-vs-transformations lesson landing on
        # its test design. pythonLower is a TRANSFORMATION; ProviderBucket is a
        # GATE that bounds its output to a fixed ASCII set. Testing the
        # transformation only through the gate discards exactly the distinctions
        # the transformation exists to preserve, and the containment argument
        # that makes the x/text lookahead divergence acceptable is the same
        # property that blinds the corpus to it.
        "lower_cases": [
            {
                "input_codepoints": [ord(c) for c in case],
                "lowered_codepoints": [ord(c) for c in case.lower()],
            }
            for case in _lower_cases()
        ],
        "provider_cases": [
            {
                "input_codepoints": [ord(c) for c in case],
                "bucket": provider_bucket(case),
            }
            for case in _provider_cases()
        ],
        "model_cases": [
            {
                "input_codepoints": None if case is None else [ord(c) for c in case],
                "is_none": case is None,
                "bucket": model_bucket(case),
            }
            for case in _model_cases()
        ],
        "validation_error_cases": [
            {
                "input_codepoints": [ord(c) for c in case],
                "family": validation_error_family(case),
            }
            for case in _validation_error_cases()
        ],
        # bounded() itself, including that the default is overridable.
        "bounded_cases": [
            {
                "value": "openai",
                "in_allowed": True,
                "result": bounded("openai", PROVIDERS),
            },
            {
                "value": "nope",
                "in_allowed": False,
                "result": bounded("nope", PROVIDERS),
            },
            {
                "value": "nope",
                "in_allowed": False,
                "custom_default": "fallback",
                "result": bounded("nope", PROVIDERS, "fallback"),
            },
            # bounded does NOT strip or lower -- its callers do. An empty string
            # is not in any allow-list, so it becomes the default rather than
            # being treated as absent.
            {"value": "", "in_allowed": False, "result": bounded("", PROVIDERS)},
            {
                "value": "OpenAI",
                "in_allowed": False,
                "result": bounded("OpenAI", PROVIDERS),
            },
        ],
        "whitespace_probes": [
            {"code_point": cp, "python_isspace": chr(cp).isspace()}
            for cp in WHITESPACE_PROBES
        ],
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    print(f"wrote {OUTPUT_PATH}")
    print(f"  provider cases:         {len(payload['provider_cases'])}")
    print(f"  model cases:            {len(payload['model_cases'])}")
    print(f"  validation error cases: {len(payload['validation_error_cases'])}")
    print(f"  whitespace probes:      {len(payload['whitespace_probes'])}")


if __name__ == "__main__":
    main()
