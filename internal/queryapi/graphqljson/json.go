// Package graphqljson implements query-api's custom GraphQL `JSON` scalar
// marshaler (CHAOS-4506, Wave 4). gqlgen.yml previously bound `JSON` to
// `github.com/99designs/gqlgen/graphql.Map` (`map[string]interface{}`) as a
// Wave-0 placeholder, documented there as KNOWN INCORRECT: the canonical
// schema declares `scalar JSON @specifiedBy(url:
// "https://ecma-international.org/wp-content/uploads/ECMA-404_2nd_edition_december_2017.pdf")`
// (contracts/graphql/v1/schema.graphql:1408) -- i.e. ANY ECMA-404 value
// (object, array, string, number, bool, or null) -- but graphql.Map can only
// ever hold a JSON *object*; it structurally cannot represent an array,
// string, number, or bool at all, not merely reject unusual input.
//
// The Python producer confirms the wider contract, not just the schema
// text: `AnalyticsResult.evidenceQualityDistribution` and
// `EvidenceQualityStats.bandCounts` (ops/src/dev_health_ops/api/graphql/
// models/outputs.py:123,134) are both typed `strawberry.scalars.JSON`,
// Strawberry's built-in scalar with pass-through serialize/parse_value --
// it round-trips whatever Python value it is handed, the full ECMA-404
// range, not just dicts.
//
// This port's OWN two fields only ever emit two of those forms in
// practice -- `null`, or a `dict[str, int]` with exactly 5 fixed keys
// (band_counts's keys: high/moderate/low/very_low/unknown) -- see
// json_test.go for the enumerated, pinned forms. But the `JSON` scalar
// binding in gqlgen.yml is GLOBAL: nine other schema fields share it
// (parameterOverrides, reportPlan x3, parameters x3, provenanceRecords --
// contracts/graphql/v1/schema.graphql, grep `: JSON`), all of which
// resolve today through SavedReport/SavedReports/ReportRuns resolvers that
// are still `panic("not implemented")` stubs in schema.resolvers.go. So a
// narrow "always assume object" fix (repeating graphql.Map's own mistake,
// just for the one shape this ticket happens to need) would leave a
// structurally-wrong scalar for the next resolver written against it. Now
// -- before any of those three stubs is implemented -- is the cheapest
// point to fix it generically instead, so this package implements the
// FULL ECMA-404 range.
//
// Go type choice: `json.RawMessage` ([]byte) rather than `any`/`interface{}`
// -- gqlgen's method-based scalar binding requires the bound type to carry
// MarshalGQL/UnmarshalGQL methods, and Go forbids attaching methods to a
// type whose underlying type is an interface (`type JSON any; func (JSON)
// M()` does not compile: "invalid receiver type"). A defined type over
// []byte is concrete, so methods attach cleanly, and json.RawMessage's
// bytes are already valid encoded JSON -- MarshalGQL can write them
// through untouched instead of re-encoding a decoded Go value (which would
// also lose key order and risk float64 precision round-tripping through
// interface{}, e.g. a 64-bit integer id).
package graphqljson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// JSON is query-api's GraphQL `JSON` scalar Go representation. The zero
// value (nil/empty) marshals as the JSON literal `null`, matching
// Python's `None` -> Strawberry JSON scalar -> GraphQL `null` for
// `evidenceQualityDistribution`'s nullable case.
type JSON json.RawMessage

// Null is the canonical empty/absent JSON value -- marshals to `null`.
var Null = JSON(nil)

// FromValue encodes an arbitrary Go value (map, slice, string, number,
// bool, or nil) into a JSON scalar. Resolvers construct field values this
// way rather than hand-building json.RawMessage, e.g.
// `graphqljson.FromValue(bandCounts)` where bandCounts is a
// `map[string]int`.
func FromValue(v any) (JSON, error) {
	if v == nil {
		return Null, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("graphqljson: encode: %w", err)
	}
	return JSON(b), nil
}

// IsNull reports whether j represents the JSON `null` value (including the
// Go zero value / empty byte slice, which MarshalGQL also treats as
// `null`).
func (j JSON) IsNull() bool {
	trimmed := bytes.TrimSpace(j)
	return len(trimmed) == 0 || string(trimmed) == "null"
}

// MarshalGQL implements graphql.Marshaler. It writes the underlying bytes
// verbatim -- j is always valid JSON by construction (FromValue/
// UnmarshalGQL both go through encoding/json), so no re-validation or
// re-encoding is needed, and nothing is lost (key order, number
// formatting) that a decode-then-encode round trip through `any` would
// risk.
func (j JSON) MarshalGQL(w io.Writer) {
	if j.IsNull() {
		_, _ = io.WriteString(w, "null")
		return
	}
	_, _ = w.Write(j)
}

// UnmarshalGQL implements graphql.Unmarshaler. gqlgen has already decoded
// the client's GraphQL variable/literal into a Go value (map[string]any,
// []any, string, float64/int64, bool, or nil) by the time this is called;
// re-marshaling it through encoding/json produces the canonical JSON
// bytes this type stores. This accepts every ECMA-404 shape, matching
// Strawberry's pass-through `parse_value` on the Python side -- unlike
// graphql.Map's UnmarshalGQL, which rejects anything that is not already
// a map.
func (j *JSON) UnmarshalGQL(v any) error {
	if v == nil {
		*j = Null
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("graphqljson: JSON scalar: %w", err)
	}
	*j = JSON(b)
	return nil
}
