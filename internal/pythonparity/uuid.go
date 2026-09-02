// Package pythonparity holds Go primitives that must accept exactly what a
// specific CPython construct accepts, no more and no less.
//
// These exist because a Go port that reaches for the idiomatic Go equivalent of
// a Python operation gets a DIFFERENT accept set, and the difference is
// invisible until something measures it. A general-purpose parser is not a
// faithful stand-in for the reference's own normalisation, and where a ported
// producer runs BEFORE the reference (writing rows the reference's own
// validation would have prevented), the asymmetry is a correctness bug rather
// than a cosmetic one.
//
// Every function here states its measured accept set and is covered by a
// differential against the live interpreter. Add to it rather than
// re-implementing a fourth variant in a third package.
//
// # There is no "safe direction" for a shared parity helper
//
// A validator can usually name one direction as the dangerous one and be
// deliberately conservative in the other. A helper SHARED between call sites
// cannot, because the direction that hurts is a property of the CALLER, not of
// the helper. The two call sites of ParseUUID prove it by having opposite
// polarity:
//
//   - As a VALIDATOR (the work-graph build's scope gate, CHAOS-4757): the
//     adapter runs BEFORE the reference validates and it WRITES. Accepting a
//     value the reference rejects means rows are written and the bridge then
//     fails the request, leaving them behind for a build that never ran.
//     Accepting-too-much is the defect; refusing-too-much merely fails closed.
//
//   - As a CLASSIFIER (the investment materializer's `_parse_repo_id`,
//     CHAOS-4441): the reference swallows a parse failure into `None`, and
//     `None` is a REAL BUCKET -- the unattributed repo -- not an error path. So
//     refusing a value the reference accepts does not fail anything. It moves
//     effort from an attributed repo into the unattributed one, silently, with
//     every total still balancing. Refusing-too-much is the defect there, and a
//     conservation check cannot see it.
//
// So this function is held to EXACT equivalence in both directions, and the
// differential asserts both. A future contributor tempted to widen or narrow it
// "to be safe" should be clear that safe is not defined here -- only exact is.
//
// # Two kinds of function live here, and they fail differently
//
// The polarity axis above is orthogonal to this one: a GATE can have either
// polarity, and a TRANSFORMATION has neither.
//
// Some members are GATES. They partition inputs into accepted and rejected --
// ParseUUID, parsePythonIntBase16. A gate has a direction to reason about, even
// when (as above) which direction is dangerous depends on the caller.
//
// The rest are TRANSFORMATIONS. Sum and transformDecimalAndSpaceToASCII map
// every input to some output, and there is no accept set to be conservative
// about. "Close" has no meaning: a float differing in the last bit, or a digit
// transformed to the wrong ASCII character, is simply a different answer, and the
// direction of the difference carries no safety.
//
// This distinction earns its space because it predicts two things.
//
// It predicts the REVIEW QUESTION. For a gate the question is "which direction
// does the divergence go?". For a transformation that question is malformed, and
// asking it wastes a round. A comparator for Sum was once written as value
// equality under a bitwise name, and the instinct that let it through was
// gate-shaped: it was checked for accepting too much, when the only thing that
// mattered was whether it distinguished -0.0 from 0.0. The corpus already
// contained the case.
//
// It predicts the TEST STRATEGY, which is the load-bearing half. A hand-written
// matrix is defensible for a gate: the boundary is the thing being tested and you
// can enumerate near it. For a transformation the same matrix silently encodes
// the AUTHOR'S MODEL of the reference. A curated UUID corpus stayed ASCII-only
// and 32-hex-digits-only for four review rounds; an invalid-UTF-8 decode policy
// was settled by listing three plausible behaviours, and all three were wrong.
// Both are the same error: AN ENUMERATION OF HYPOTHESES IS NOT A MEASUREMENT OF
// BEHAVIOUR. The only available claim for a transformation is exact agreement
// over a corpus that varies the axes that matter, and the only honest way to get
// it is to run both sides.
//
// And a transformation's failure is not merely different, it is INVISIBLE. A
// gate's wrong answer eventually surfaces as a refused request or a written row.
// Sum one ULP out surfaces as nothing at all, until a categorisation is re-billed
// months later. That is why transformations need MORE measurement discipline than
// gates rather than less.
//
// # The obvious Go primitive is wrong in a direction that depends on the site
//
// Whitespace and case are the two families where this bites hardest, because in
// both the natural Go choice is wrong for one Python operation and exact for
// another. Name which Python function a site ports before choosing.
//
// Whitespace -- Python has THREE classes:
//
//	str.isspace()      29 code points, includes U+001C-U+001F   str.strip()
//	numeric parsers    25 code points, == Go's unicode.IsSpace  int(), float()
//	str.splitlines()   10 boundaries, U+001C-U+001E but NOT U+001F
//
// So strings.TrimSpace is too NARROW for str.strip() and EXACT for int()/float().
//
// Case -- three relations, and Go's strings.ToLower is neither Python one:
//
//	str.lower()          full Unicode + SpecialCasing; final sigma is
//	                     context-sensitive and U+0130 lowercases to TWO code
//	                     points. Port with x/text cases.Lower(language.Und) --
//	                     correct BY DESIGN, since CPython's str.lower() is
//	                     locale-independent and never applies the Turkish or
//	                     Lithuanian tailorings.
//	C tolower on bytes   ASCII only. Used by CPython's own keyword matching
//	                     (PyOS_strnicmp). Port with a hand-rolled A-Z fold.
//	strings.ToLower      rune-wise Unicode; matches NEITHER. It maps U+0130 to
//	                     'i', so it invents keyword matches CPython never makes.
//
// strings.EqualFold is a fourth relation (Unicode simple folding) and is unsafe
// for any keyword containing 's' or 'k' -- exactly two ASCII letters are
// reachable by a non-ASCII simple fold, 's' from U+017F and 'k' from U+212A.
// Note the asymmetry: U+017F is already lowercase, so str.lower() reaches only
// 'k' while folding reaches both.
package pythonparity

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ParseUUID accepts exactly what CPython's `uuid.UUID(hex=...)` accepts.
//
// CPython normalises before validating:
//
//	hex = hex.replace('urn:', '').replace('uuid:', '').strip('{}').replace('-', '')
//	if len(hex) != 32: raise ValueError
//	int(hex, 16)
//
// Reproduced verbatim, because every shortcut changes the set:
//
//   - `replace` removes EVERY occurrence anywhere in the string, not a prefix.
//     So `urn:`, `uuid:`, `urn:urn:uuid:` and `urn:uuid:urn:uuid:` all parse.
//
//   - `strip('{}')` removes ANY NUMBER of leading and trailing braces, and does
//     not require them to balance. So `{{X}}`, `{X` and `X}` all parse.
//
//   - Both are CASE-SENSITIVE, which is the divergence that matters:
//     `urn:uuid:X` parses and `URN:UUID:X` raises, because the uppercase prefix
//     is never removed and the leftover colons fail the length check.
//
//   - `len()` counts CHARACTERS, not bytes, so the gate is 32 codepoints and a
//     multi-byte value can pass it.
//
//   - The final step is `int(hex, 16)`, NOT a hex decode. Its grammar is much
//     wider than "32 hex digits": it folds Unicode decimal digits to ASCII,
//     accepts surrounding whitespace, a leading `+`, an `0x` prefix and
//     underscores between digits. All of those fit in 32 characters, so all of
//     them are reachable here:
//
//     uuid.UUID("１" * 32)                        # fullwidth digits: accepted
//     uuid.UUID(" " + "1" * 30 + " ")             # padded: accepted
//     uuid.UUID("0x" + "1" * 30)                  # prefixed: accepted
//     uuid.UUID("1_1_1_1_1_1_1_1_1_1_1_1_1_1_1_11")  # underscored: accepted
//
//     Describing this step as a hex decode was the defect a review round found:
//     it refused all four, which for the CLASSIFIER caller below silently moves
//     work into the unattributed bucket with every total still balancing.
//
// # Why not github.com/google/uuid.Parse
//
// Beyond being a hex decode rather than `int()`, it dispatches on LENGTH: at 38
// characters it assumes the braced form and
// strips the first and last character WITHOUT checking they are braces, so
// `X<uuid>X`, `[<uuid>]`, `!<uuid>?` and a space-padded value all parse there
// and all raise in CPython. It is also case-insensitive about the URN prefix,
// so it accepts `URN:UUID:X` — which CPython rejects.
//
// That direction is the dangerous one for a producer that runs ahead of the
// reference: it accepts a value the reference will reject, does its writes, and
// the reference then fails the request, leaving rows behind for a build that
// never legitimately ran.
//
// Measured, not described: `internal/pythonparity/testdata/uuid_accept_set.json`
// is generated from the live interpreter and the differential asserts both
// directions.
func ParseUUID(value string) (uuid.UUID, error) {
	hex := strings.ReplaceAll(value, "urn:", "")
	hex = strings.ReplaceAll(hex, "uuid:", "")
	hex = strings.Trim(hex, "{}")
	hex = strings.ReplaceAll(hex, "-", "")
	// `len()` on a Python str counts CHARACTERS. Counting bytes here refuses
	// every non-ASCII value that reaches this point, including ones the
	// reference accepts.
	if length := utf8.RuneCountInString(hex); length != 32 {
		return uuid.Nil, fmt.Errorf("uuid %q: python normalisation yields %d characters, want 32", value, length)
	}

	parsed, err := parsePythonIntBase16(hex)
	if err != nil {
		return uuid.Nil, fmt.Errorf("uuid %q: %w", value, err)
	}
	// CPython's own range check, verbatim: `if not 0 <= int < 1<<128`.
	//
	// UNREACHABLE BY CONSTRUCTION, and kept anyway. Both halves are dead given
	// the steps above, for reasons that belong to those steps rather than to
	// this one:
	//
	//   - Negative: `int()` never sees a sign, because `replace('-', '')` above
	//     removes every hyphen BEFORE the length gate. "-" + 31 digits is
	//     rejected as 31 characters, not as a negative.
	//   - Overflow: 32 characters cannot encode more than 128 bits, and a sign,
	//     an "0x" prefix, whitespace or an underscore all consume characters
	//     that would otherwise be digits, so they only ever shrink the value.
	//
	// It stays because it is a line of the reference, and because the first
	// reason is a property of a DIFFERENT step: anything that later relaxes the
	// hyphen removal or the gate makes it live again. A planted-defect round
	// confirmed no input reaches it — treat that as the measured reason it is
	// untested, not as a gap in the corpus.
	if parsed.Sign() < 0 || parsed.BitLen() > 128 {
		return uuid.Nil, fmt.Errorf("uuid %q: int is out of range (need a 128-bit value)", value)
	}

	var out uuid.UUID
	parsed.FillBytes(out[:])
	return out, nil
}
