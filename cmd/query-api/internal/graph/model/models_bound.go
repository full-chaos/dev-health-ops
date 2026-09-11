package model

import "github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"

// The four GraphQL types whose Go shape is a decision rather than a
// transcription of the SDL, bound by name in gqlgen.yml so gqlgen uses these
// declarations instead of generating its own.
//
// What the binding carries: `json:"value"` WITHOUT `omitempty` on every
// nullable value field. gqlgen's own model generator writes `,omitempty` for a
// nullable field, which makes a nil value DISAPPEAR from an encoding/json
// rendering instead of appearing as null -- and "the measure is absent" and
// "the measure is null" are the same sentence only until something reads them.
// Each of these fields exists because an all-NULL aggregate must reach a client
// as an explicit null rather than as a manufactured zero; letting it become an
// absent key instead would give back the ambiguity the pointer types removed.
//
// The GraphQL wire does not go through these tags today -- gqlgen marshals a
// response through its own per-field marshalers -- so this is a decision about
// what the type MEANS, kept true for any other encoder that meets it. An
// executed cell in models_bound_test.go shows the difference the tag makes.
//
// Before this file, these declarations lived in models_gen.go as hand-edits
// that every regeneration reverted. That is the whole reason they are here:
// gqlgen does not overwrite a type it is told to bind.

// BreakdownItem is one row of a breakdown result. Value is a pointer because an
// all-NULL aggregate is a real answer; see analytics/breakdown.go's
// breakdownRow doc comment for why the database can produce one.
type BreakdownItem struct {
	Key   string   `json:"key"`
	Value *float64 `json:"value"`
	Label *string  `json:"label,omitempty"`
}

// SankeyEdge is one edge of a flow matrix. See analytics/flowmatrix.go's
// queryNodes doc comment for why Value is nullable.
type SankeyEdge struct {
	Source string   `json:"source"`
	Target string   `json:"target"`
	Value  *float64 `json:"value"`
}

// SankeyNode is one node of a flow matrix, nullable for the same reason as
// SankeyEdge.
type SankeyNode struct {
	ID        string   `json:"id"`
	Label     string   `json:"label"`
	Dimension string   `json:"dimension"`
	Value     *float64 `json:"value"`
}

// TimeseriesBucket is one bucket of a timeseries. See
// analytics/timeseries.go's ExecuteTimeseries doc comment for why Value is
// nullable.
type TimeseriesBucket struct {
	Date  graphqldate.Date `json:"date"`
	Value *float64         `json:"value"`
}

// SankeyCoverage splits the repository coverage headline. The last three fields
// are nullable because neither plane can always measure them, and a
// non-nullable Float would force a 0 into the cases where the measurement is
// simply absent -- "0% of this org's coverage is team-fallback" is a confident
// false claim where null is an honest absent one. The share fields are guarded
// on the same denominator the headline uses, so direct plus fallback reads back
// as exactly the number the cards already show; the fanout is a width, not a
// share, and is passed through unscaled. analytics/sankeycoverage.go, above the
// repoTotal guard, is where that reasoning lives.
type SankeyCoverage struct {
	TeamCoverage             float64  `json:"teamCoverage"`
	RepoCoverage             float64  `json:"repoCoverage"`
	DirectRepoCoverage       *float64 `json:"directRepoCoverage"`
	TeamFallbackRepoCoverage *float64 `json:"teamFallbackRepoCoverage"`
	RepoFanoutReposPerUnit   *float64 `json:"repoFanoutReposPerUnit"`
}
