package goapiproof

import (
	"net/url"
	"time"
)

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

// opportunitiesTeamScopeSubsetDefect declares the team-scoped candidate
// list as a bounded subset of the baseline's own organization-wide list,
// paired by TITLE rather than the response's own "id" field: id is
// assigned by RANK POSITION within one response (confirmed against a
// live capture: the SAME opportunity, byte-identical title/rationale/
// evidence_links/suggested_experiments, carried "id":"opp-2" in the
// organization-wide baseline and "id":"opp-1" in the team-scoped
// candidate, because it ranked second org-wide and first within the
// team), so it is not a stable identity across two differently-scoped
// responses and is excluded from both KeyFields and EqualLeaves here.
// title is the one field this route's own opportunity-selection can be
// expected to hold stable across a scope narrowing that keeps an
// opportunity relevant at all (services/opportunities.py labels each
// candidate cause by its own metric, one title per metric); rationale,
// evidence_links and suggested_experiments are declared EQUAL because
// this route computes no per-item aggregate a narrower scope could
// legitimately shrink -- opportunitiesNumericLeaves' own doc comment
// above already establishes that delta_pct, this route's one numeric
// value, is only ever interpolated into the rationale STRING, never its
// own leaf, so an admitted opportunity's rationale is expected to read
// identically whether the underlying delta was computed org-wide or over
// the team's own narrower repository set for a genuinely stable driver;
// a rationale that in fact differs (a real percentage change) simply
// fails the equality check and stays outside, correctly.
var opportunitiesTeamScopeSubsetDefect = BaselineDefect{
	Ticket: "CHAOS-5920",
	Reason: "GET/POST /api/v1/opportunities' team scope resolves a team's repositories from team_repo_ownership through one shared condition (internal/queryapi/teamscope.RepoCondition), while the reference plane resolves the same scope from user_metrics_daily.team_id (resolve_repo_ids_for_teams, api/queries/scopes.py) and, for this route's own home-response composition, additionally drops the filter outright before an org_id ever reaches it -- either way the baseline answers organization-wide for a request the candidate answers over the team's own narrower repository set. For a genuinely narrower team the candidate's item list is therefore a subset of the baseline's own list, paired by title rather than the response's own rank-assigned id (see this var's own doc comment). Go is correct.",
	Paths:  []string{"data.items"},
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:    "data.items",
		KeyFields:   []string{"title"},
		EqualLeaves: []string{"rationale", "evidence_links", "suggested_experiments"},
	},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's and at least one opportunity the whole organization raises does not also apply to the team's own narrower repository set; a team owning every repository, or a window whose every raised opportunity applies unchanged to the team, leaves the two lists identical, and shows no divergence under this Path",
}

// opportunitiesTeamScopedParity is opportunitiesNumericLeaves plus
// opportunitiesTeamScopeSubsetDefect -- shared by GET and POST's own
// opportunities_team_scoped entries.
var opportunitiesTeamScopedParity = Options{
	NumericLeavesDeclared: true,
	BaselineDefects:       []BaselineDefect{opportunitiesTeamScopeSubsetDefect},
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
			// build_home_response the same way). Timeout raised above the
			// run default for the same reason as home's own
			// "home_team_scoped" entry: the baseline leg's team-scope
			// repository resolution is an extra read the org-scope default
			// entry above never makes.
			//
			// The two planes resolve that repository set from different
			// tables, and the home builder this route composes additionally
			// drops the filter outright on the baseline for a repo-scoped
			// metric, so a genuinely narrower team's candidate item list is
			// a bounded subset of the baseline's own organization-wide list
			// (opportunitiesTeamScopeSubsetDefect above). It is visible on
			// the wire as an opportunity the baseline raises from an
			// organization-wide movement and the candidate does not raise
			// for the team.
			Name:                "opportunities_team_scoped",
			Query:               url.Values{"scope_type": {"team"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     opportunitiesTeamScopedParity,
			IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
			Timeout:    180 * time.Second,
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
			// team scope, bound at run time to filters/options' own live
			// team_id via restidbind.go's BodyPath binding -- the POST-body
			// twin of the QueryParam binding this route's own GET
			// "opportunities_team_scoped" entry above uses, matching home's
			// own "home_team_scoped" POST entry's identical precedent. Same
			// bounded-subset finding as its GET twin above.
			Name:                "opportunities_team_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     opportunitiesTeamScopedParity,
			IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
		},
		{
			// repo scope, bound to filters/options' own live repo_id, same
			// BodyPath and reasoning as opportunities_team_scoped above.
			Name:                "opportunities_repo_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []string{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     opportunitiesNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", BodyPath: "filters.scope.ids"}},
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
