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
package pythonparity

import (
	"fmt"
	"strings"

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
//   - `strip('{}')` removes ANY NUMBER of leading and trailing braces, and does
//     not require them to balance. So `{{X}}`, `{X` and `X}` all parse.
//   - Both are CASE-SENSITIVE, which is the divergence that matters:
//     `urn:uuid:X` parses and `URN:UUID:X` raises, because the uppercase prefix
//     is never removed and the leftover colons fail the length check.
//   - There is NO whitespace stripping. A padded value raises.
//
// # Why not github.com/google/uuid.Parse
//
// It dispatches on LENGTH: at 38 characters it assumes the braced form and
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
	if len(hex) != 32 {
		return uuid.Nil, fmt.Errorf("uuid %q: python normalisation yields %d hex digits, want 32", value, len(hex))
	}
	// The remaining 32 characters must be hex. uuid.Parse's 32-character branch
	// is an exact-length hex decode with no stripping, so it is safe here.
	parsed, err := uuid.Parse(hex)
	if err != nil {
		return uuid.Nil, fmt.Errorf("uuid %q: %w", value, err)
	}
	return parsed, nil
}
