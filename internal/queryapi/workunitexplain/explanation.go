package workunitexplain

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"encoding/json"
	"fmt"
	"io"
)

// Explanation ports api/models/schemas.py's WorkUnitExplanation
// (schemas.py:344-353) field for field, name for name and IN ORDER --
// Pydantic declares no alias generator on that model, so the JSON tags
// below are the Python attribute names, and FastAPI serialises a model's
// members in declaration order.
//
// CategoryRationale keeps insertion order: Python fills the dict in the
// work unit's theme order, and pydantic writes it in that order.
type Explanation struct {
	WorkUnitID            string                    `json:"work_unit_id"`
	AIGenerated           bool                      `json:"ai_generated"`
	Summary               string                    `json:"summary"`
	CategoryRationale     pyjson.OrderedMap[string] `json:"category_rationale"`
	EvidenceHighlights    []string                  `json:"evidence_highlights"`
	UncertaintyDisclosure string                    `json:"uncertainty_disclosure"`
	EvidenceQualityLimits string                    `json:"evidence_quality_limits"`
}

// nonAIExplanation is explain_work_unit's early return for a provider that
// resolves to "none" (work_unit_explain.py:76-89): ai_generated false and
// every text field empty. category_rationale and evidence_highlights are
// empty CONTAINERS rather than nil, because Python's own literal passes
// `{}` and `[]` and Go's encoding/json renders a nil map or slice as
// `null` instead.
func nonAIExplanation(workUnitID string) Explanation {
	return Explanation{
		WorkUnitID:            workUnitID,
		AIGenerated:           false,
		Summary:               "",
		CategoryRationale:     pyjson.NewOrderedMap[string](),
		EvidenceHighlights:    []string{},
		UncertaintyDisclosure: "",
		EvidenceQualityLimits: "",
	}
}

// WriteJSON encodes value to w as one of this route's response bodies.
//
// This is the ONE encoding path both bodies take, and it writes straight
// to the ResponseWriter rather than marshalling to a buffer the caller then
// hands to Write: every route in this binary answers through the JSON
// encoder, and a raw write of pre-marshalled bytes is refused by the
// repository's own scanners for that reason.
//
// HTML escaping is DISABLED because the reference plane's JSONResponse
// escapes none of `<`, `>` or `&`, and both bodies here carry text somebody
// else supplied -- model prose, and a reflected work-unit id.
//
// U+2028 and U+2029 are NOT carried raw, and cannot be: SetEscapeHTML
// governs only EscapeForHTML (`&`, `<`, `>`); the two separator code points
// are governed by a separate EscapeForJS option that this encoder type
// exposes no setter for, and a pre-encoded value is re-escaped for JS on
// the way out regardless. So the reference writes their raw UTF-8 bytes
// and this route writes \u2028 / \u2029. Both spellings decode to the same
// string, every client of this route parses the body as JSON, and one
// encoding path for every route outranks byte equality on two code points.
//
// The encoder appends one trailing newline, which is insignificant JSON
// whitespace and is the same byte every sibling route's body carries.
func WriteJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode response body: %w", err)
	}
	return nil
}
