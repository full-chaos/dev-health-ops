package goapiproof

import "net/url"

// This file holds GET+POST /api/v1/opportunities' own corpus content --
// requests and (where a mechanism's own shape admits one)
// baseline-defect citations. restcorpus.go registers the two vars below
// into restEndpointSpecs with a two-line addition; restRunOrder and
// restmounted.go's mountedRESTPaths each carry one further line.
// Nothing else in those shared files changes here.
//
// opportunitiesNumericLeaves declares ZERO numeric leaves: every field
// OpportunitiesResponse can reach (id, title, rationale, evidence_links,
// suggested_experiments) is a string or a list of strings -- the one
// numeric value the route computes, delta_pct, is only ever interpolated
// into the "rationale" STRING (services/opportunities.py:80-83,
// "{delta.delta_pct:.0f}%"), never emitted as its own JSON leaf.
// NumericLeavesDeclared is still set (with empty FloatTierB/
// FloatExactLeaves/IntegerLeaves) so a future numeric field added to this
// shape would fail UndeclaredNumericLeaves rather than silently comparing
// Tier A -- the same reason a route with a genuinely empty declaration
// still sets the marker (Options.NumericLeavesDeclared's own doc
// comment).
var opportunitiesNumericLeaves = Options{
	NumericLeavesDeclared: true,
}

var opportunitiesGetEndpointSpec = RESTEndpointSpec{
	Method: "GET",
	Path:   "/api/v1/opportunities",
	Requests: []RESTRequest{
		{
			// scope_type/scope_id/range_days/compare_days/start_date/
			// end_date all take opportunities()'s own Pydantic defaults
			// (main.py:1186-1191): org scope, 14/14 day windows.
			Name:                "opportunities_default_org",
			Query:               url.Values{},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: opportunitiesNumericLeaves,
		},
		{
			// team scope, scope_id bound at run time to filters/options'
			// own live team_id -- same producer/binding shape home's own
			// "home_team_scoped" entry uses (this route composes
			// build_home_response the same way).
			Name:                "opportunities_team_scoped",
			Query:               url.Values{"scope_type": {"team"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     opportunitiesNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
		},
		{
			// repo scope, scope_id bound to filters/options' own live
			// repo_id.
			Name:                "opportunities_repo_scoped",
			Query:               url.Values{"scope_type": {"repo"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     opportunitiesNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
		},
		{
			// _filters_from_query's own ScopeFilter(level=scope_type)
			// construction raises for any scope_type outside the Literal
			// set, INSIDE opportunities()'s own try block -- the same
			// generic `except Exception: 503`, matching home's own
			// "invalid_scope_type_is_503" precedent for the identical
			// helper.
			Name:                "invalid_scope_type_is_503",
			Query:               url.Values{"scope_type": {"bogus"}},
			WantCandidateStatus: 503, WantBaselineStatus: 503,
			BodyMode: RESTBodyModeJSON,
		},
		{
			Name:                "bad_range_days_is_422",
			Query:               url.Values{"range_days": {"abc"}},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
	},
}

var opportunitiesPostEndpointSpec = RESTEndpointSpec{
	Method: "POST",
	Path:   "/api/v1/opportunities",
	Requests: []RESTRequest{
		{
			Name:                "opportunities_default_org",
			Body:                map[string]any{"filters": map[string]any{}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: opportunitiesNumericLeaves,
		},
		{
			// POST's own scope id has no body-path id-binding mechanism
			// (RESTIDBinding only resolves into a query param or a path
			// param) -- so, unlike the GET entries above, this carries a
			// fixed, neutral, almost-certainly-nonexistent team id rather
			// than a live one, matching home's own "home_team_scoped" POST
			// entry's identical precedent and reasoning.
			Name:                "opportunities_team_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []any{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: opportunitiesNumericLeaves,
		},
		{
			Name:                "opportunities_repo_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []any{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: opportunitiesNumericLeaves,
		},
		{
			// HomeRequest's ONE field ("filters") is required with no
			// default -- confirmed live shape (pydantic_validation_error.go),
			// matching home's own "missing_filters" precedent for a body
			// missing a required top-level field.
			Name:                "missing_filters",
			Body:                map[string]any{},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
		{
			Name:                "invalid_scope_level",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "bogus"}}},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
	},
}
