package goapiproof

import (
	"net/url"
	"time"
)

// This file holds GET+POST /api/v1/work-units' own corpus content --
// requests, numeric-leaf declarations and the two declared, structurally
// uncoverable Python-plane divergences this route carries. restcorpus.go
// registers the two vars below into restEndpointSpecs with a two-line
// addition; restRunOrder carries two further lines. Nothing else in that
// shared file changes here.
//
// TWO KNOWN DIVERGENCES are deliberately declared with NO Shape and no
// BaselineDefect entry at all -- not an oversight, a property this
// comparator cannot express for either mechanism:
//
//  1. The work_unit_supersessions exclusion. GET/POST /api/v1/work-units
//     returns a flat LIST of independent per-unit records, not an
//     aggregate. The shared latest-investments source this route
//     composes excludes a work unit a later regrouping run retired; the
//     reference plane's own query has no knowledge of that exclusion and
//     can still return that unit as an EXTRA element of its own list.
//     Unlike drilldown/prs' page-limit-bounded RMT dedup
//     (WorkGraphEdgeDedupShape), THIS route's query carries no LIMIT-
//     driven equal-length guarantee that turns a missing element into a
//     positional value-shift: when the org's total row count is under
//     the request's own limit (the common case, and every corpus fixture
//     here), the two lists are simply different LENGTHS. compare.go's
//     own dispatch (classifyBaselineDefects) offers a finding to any
//     Shape's admits() only when leafDifference(finding.Shape) holds --
//     ShapeValue/ShapeNull/ShapeScalarType -- and a length/presence
//     difference is ShapeLength/ShapePresence, categorically excluded
//     before any Shape is ever consulted (confirmed by this file's own
//     existing precedent: explainParity's own status-filter BaselineDefect
//     entry (restcorpus.go) documents the identical wall for its own
//     list-length manifestation of an unrelated mechanism). No
//     BaselineDefect entry is added here: one that names Paths under a
//     mechanism that can only ever produce an uncoverable shape would
//     report zero admissions on every comparison and fail
//     StaleBaselineDefects the first time this route is actually run. A
//     live occurrence stays a visible, unadmitted finding -- expected,
//     not a regression, and not silenced.
//  2. The quotes table backing each unit's textual evidence carries no
//     LIMIT either (FetchWorkUnitInvestmentQuotes/
//     fetch_work_unit_investment_quotes), so an unmerged duplicate
//     physical row makes the reference plane's own evidence.textual list
//     one element LONGER, byte-identical duplicate included -- the exact
//     same length-only shape as (1), for the exact same reason
//     uncoverable by any Shape in this package. This port's own read
//     dedups (GROUP BY + argMax over the table's own key); the reference
//     plane's does not.
var workUnitsFloats = map[string]string{
	"data.evidence.contextual.span_days": "toTS.Sub(fromTS).Hours()/24 (workunitassembly.go) vs Python's (to_ts-from_ts).total_seconds()/86400.0 -- the same quantity via a different float op order, agreeing to ~15 significant digits but not bit-for-bit",
	// The four leaves below are float-DECLARED (Python: float; wire: a
	// JSON number) but each is a plain per-row argMax pass-through, never
	// a cross-row sum/avg/stddev the merge order could perturb --
	// workUnitsFloatExact opts every one of them back to Tier A. They are
	// named here too because FloatTierB is this entry's own registry of
	// every KNOWN float leaf (validateNumericLeaves requires a
	// FloatExactLeaves key to also appear here), not only the tolerant
	// ones.
	"data.effort.value":             "(argMax(tuple(effort_value), computed_at)).1 (LatestWorkUnitInvestmentsSource) -- one row's own stored Float64, opted back to exact below",
	"data.investment.themes":        "argMax(theme_distribution_json, computed_at)'s own Map(String,Float64) value, read via mapKeys/mapValues -- one row's stored value, opted back to exact below",
	"data.investment.subcategories": "the same argMax(subcategory_distribution_json, computed_at) pass-through as data.investment.themes, opted back to exact below",
	"data.evidence_quality.value":   "(argMax(tuple(evidence_quality), computed_at)).1 -- one row's own stored Float64, opted back to exact below",
}

// workUnitsFloatExact opts the four argMax-pass-through leaves back to
// Tier A: each is ONE physical row's own stored value (never summed,
// averaged or otherwise merged across rows), so two planes reading the
// SAME latest version must agree bit for bit -- unlike
// data.evidence.contextual.span_days above, there is no floating-point
// operation-order difference here to tolerate.
var workUnitsFloatExact = map[string]string{
	"data.effort.value":             "argMax selects one row's own stored Float64 verbatim; no cross-row arithmetic on either plane",
	"data.investment.themes":        "argMax selects one row's own stored Map(String,Float64) verbatim; no cross-row arithmetic on either plane",
	"data.investment.subcategories": "the same argMax pass-through as data.investment.themes",
	"data.evidence_quality.value":   "argMax selects one row's own stored Float64 verbatim; no cross-row arithmetic on either plane",
}

// workUnitsParity is GET and POST /api/v1/work-units' own shared Parity.
var workUnitsParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            workUnitsFloats,
	FloatExactLeaves:      workUnitsFloatExact,
}

var workUnitsGetEndpointSpec = RESTEndpointSpec{
	Method: "GET",
	Path:   "/api/v1/work-units",
	Requests: []RESTRequest{
		{
			Name:                "default_window",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			Name:                "range_days_90",
			Query:               url.Values{"range_days": {"90"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// team scope, scope_id bound at run time to filters/
			// options' own live team_id -- resolve_repo_filter_ids'
			// team branch (ResolveRepoFilterIDs' own
			// resolveRepoIDsForTeams), the same live id every other
			// team-scoped GET request in this corpus binds to. Timeout
			// raised above the run default: the baseline leg's own
			// team-to-repo resolution is an extra read the org-scope
			// default entry never makes and can outrun the default budget.
			Name:                "team_scoped",
			Query:               url.Values{"scope_type": {"team"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitsParity,
			IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
			Timeout:    180 * time.Second,
		},
		{
			// repo scope, scope_id bound to filters/options' own live
			// repo_id.
			Name:                "repo_scoped",
			Query:               url.Values{"scope_type": {"repo"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitsParity,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
		},
		{
			// include_textual=false skips the quotes fetch entirely
			// (build_work_unit_investments' own `if include_text:`
			// branch, work_units.py:289) -- exercised separately from
			// the default (textual quotes included) request above.
			Name:                "include_textual_false",
			Query:               url.Values{"include_textual": {"false"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// explicit limit -- _bounded_limit_param's own clamp
			// (api/main.py:211-214), the GET-query-param twin of
			// POST's own "explicit_limit" sibling below.
			Name:                "explicit_limit",
			Query:               url.Values{"limit": {"5"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// Same shared int_parsing validator drilldown/prs's own GET
			// already exercises (both routes share
			// pydantic_validation_error.go unchanged).
			Name:                "invalid_range_days",
			Query:               url.Values{"range_days": {"not-a-number"}},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
	},
}

var workUnitsPostEndpointSpec = RESTEndpointSpec{
	Method: "POST",
	Path:   "/api/v1/work-units",
	Requests: []RESTRequest{
		{
			Name:                "default_filters",
			Body:                map[string]any{"filters": map[string]any{}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// team scope bound to filters/options' own live team_id
			// (restidbind.go's BodyPath binding, the POST-body twin of
			// this route's own GET "team_scoped" QueryParam binding
			// above) -- exercises the SAME resolve_repo_filter_ids team
			// branch against a team that genuinely resolves to repos, the
			// same request shape a real team-scoped client sends.
			Name: "team_scoped",
			Body: map[string]any{
				"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
			Parity:     workUnitsParity,
		},
		{
			// repo scope via filters.what.repos -- the POST-only field
			// _filters_from_query (GET) never populates -- bound to
			// filters/options' own live repo_id, same BodyPath reasoning
			// as team_scoped above.
			Name: "repo_scoped",
			Body: map[string]any{
				"filters": map[string]any{"what": map[string]any{"repos": []string{"11111111-1111-1111-1111-111111111111"}}},
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", BodyPath: "filters.what.repos"}},
			Parity:     workUnitsParity,
		},
		{
			// why.work_category exercises _split_category_filters'
			// theme/subcategory split (SplitCategoryFilters) and
			// _matches_category_filter's own post-fetch narrowing --
			// both GET-unreachable (GET has no query param for it).
			Name: "work_category_filter",
			Body: map[string]any{
				"filters": map[string]any{"why": map[string]any{"work_category": []string{"feature_delivery.new_capability"}}},
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// include_textual=false, POST's own body-typed twin of
			// GET's "include_textual_false" sibling.
			Name: "include_textual_false",
			Body: map[string]any{
				"filters":         map[string]any{},
				"include_textual": false,
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// explicit limit -- `payload.limit or 200` (api/main.py:620)
			// keeps a genuinely-set, non-zero limit unchanged, unlike
			// the "missing/null/0 falls back to 200" path
			// "default_filters" above already exercises implicitly.
			Name: "explicit_limit",
			Body: map[string]any{
				"filters": map[string]any{},
				"limit":   5,
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// Confirmed live (pydantic_validation_error.go): a body with
			// no "filters" key produces byte-parity 422 `missing`
			// envelopes on both planes.
			Name:                "missing_filters",
			Body:                map[string]any{},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
	},
}
