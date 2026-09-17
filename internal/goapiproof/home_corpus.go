package goapiproof

import (
	"net/url"
	"time"
)

// This file holds GET+POST /api/v1/home's own corpus content --
// requests, numeric-leaf declarations and (where a mechanism's own
// shape admits one) baseline-defect citations. restcorpus.go registers
// the two vars below into restEndpointSpecs with a two-line addition;
// restRunOrder and restmounted.go's mountedRESTPaths each carry one
// further line. Nothing else in those shared files changes here.

// homeNumericLeaves is every numeric leaf this route's response can
// reach, named float or integer and traced to its producing query, per
// entry, for every 200-status request below (Options.
// NumericLeavesDeclared).
//
//   - data.freshness.coverage.* and data.rework_theme_allocation.
//     allocation_pct: genuine ratios (a count divided by a count,
//     multiplied by 100) -- float by construction, whatever the
//     numerator/denominator's own integer provenance.
//   - data.data_confidence.coverage_pct: the arithmetic mean of the
//     three coverage ratios above -- float for the same reason.
//   - data.deltas.delta_pct: (current-previous)/previous*100 -- always
//     a float computation.
//   - data.deltas.value and data.deltas.spark.value: this ONE wire path
//     is shared by all eleven _METRICS entries (services/home.py), whose
//     producing aggregates are NOT uniform -- churn/throughput/
//     deploy_freq sum() an integer count column, while cycle_time/
//     review_latency/wip_saturation/change_failure_rate/rework_ratio/
//     pr_rework_ratio/ci_success avg() (or, for pr_rework_ratio, a
//     weighted ratio of) a Float64 column. Declared float for the whole
//     path: the more permissive classification is the honest one for a
//     path that mixes genuine float aggregates with integer sums, and
//     tolerating a sum-typed row costs nothing since an exact integer
//     value differing by less than FloatTierB's own tolerance is not a
//     real divergence either way.
//   - data.rework_theme_allocation.allocation: sum(work_items_completed)
//     (api/queries/metrics.py fetch_rework_theme_allocation) -- a bare
//     sum of a count column, not a merged floating aggregate.
//   - data.rework_theme_allocation.prs_merged / .churn_loc: the same
//     table's sum(prs_merged) / sum(churn_loc) -- bare count sums.
//   - data.signals.evidence_count: len(evidence) / a row count -- never
//     a floating aggregate.
var homeNumericLeaves = Options{
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.freshness.coverage.repos_covered_pct":            "fetch_coverage's covered/total ratio *100 (api/queries/freshness.py) -- a genuine ratio.",
		"data.freshness.coverage.prs_linked_to_issues_pct":     "fetch_coverage's linked/total ratio *100 -- a genuine ratio.",
		"data.freshness.coverage.issues_with_cycle_states_pct": "fetch_coverage's with_cycle/total ratio *100 -- a genuine ratio.",
		"data.data_confidence.coverage_pct":                    "_coverage_pct_from_coverage's mean of the three coverage ratios above (services/home.py) -- float.",
		"data.deltas.delta_pct":                                "delta_pct's (current-previous)/previous*100 (api/utils/numeric.py) -- always a float computation.",
		"data.deltas.value":                                    "shared by all eleven _METRICS entries, whose producing aggregates mix sum() over integer columns and avg()/ratio over Float64 columns on this ONE wire path -- declared float for the whole path (see this var's own doc comment).",
		"data.deltas.spark.value":                              "the same per-row mixed provenance as data.deltas.value, one point per day.",
		"data.rework_theme_allocation.allocation_pct":          "fetch_rework_theme_allocation's allocation/total ratio *100 (api/queries/metrics.py) -- a genuine ratio.",
	},
	IntegerLeaves: map[string]string{
		"data.rework_theme_allocation.allocation": "fetch_rework_theme_allocation's sum(work_items_completed) -- a bare count sum.",
		"data.rework_theme_allocation.prs_merged": "the same query's sum(prs_merged) -- a bare count sum.",
		"data.rework_theme_allocation.churn_loc":  "the same query's sum(churn_loc) -- a bare count sum.",
		"data.signals.evidence_count":             "a row count / len(evidence) (services/home.py) -- never a floating aggregate.",
	},
	// data.events.ts is EventItem.ts (schemas.py's `EventItem(ts=datetime.
	// now(timezone.utc), ...)`, services/home.py) -- a per-request wall-
	// clock read on BOTH planes, so two calls to the SAME plane already
	// disagree on it; cross-plane equality proves nothing.
	// StochasticLeafClass (compare.go/stochasticleaf.go) cannot admit this
	// leaf: its lookup refuses any path that crosses a list
	// (stochasticleaf.go's own "%q crosses a list" check), and "events" is
	// a list -- there is no wildcard or subtree form. VolatileFields is
	// this package's only mechanism that reaches an array leaf (its
	// dotted-path key is index-free by construction), and it excludes the
	// leaf from comparison ENTIRELY: it does not itself assert a wire
	// format the way StochasticLeafClass's type/order checks would. This
	// PR's own cmd/query-api/internal/home wire-format test is what pins
	// the byte shape instead.
	VolatileFields: map[string]string{
		"data.events.ts": "datetime.now(timezone.utc) read fresh per request on both planes (services/home.py) -- drawn, not derived, so it cannot agree across calls, same plane or cross-plane. Ticket: CHAOS-5907",
	},
}

// homeGetEndpointSpec and homePostEndpointSpec's happy-path requests
// carry NO BaselineDefect citation. Three ReplacingMergeTree reads in
// the Python plane omit dedup entirely (fetch_coverage's two
// work_item_cycle_times reads, fetch_source_statuses' ci_pipeline_runs
// branch, _resolve_scope_labels' repos/teams reads) and are fixed here
// (internal/home's own queries_freshness.go/queries_signals.go doc
// comments, both reading the affected tables FINAL) -- but none of the
// three reaches a leaf a shape can admit under this package's coverage
// rules:
//
//   - fetch_coverage's two ratios (prs_linked_to_issues_pct,
//     issues_with_cycle_states_pct) have numerator AND denominator drawn
//     from the SAME undeduped scan, and different physical versions of
//     one work item can carry different work_scope_id/cycle_time_hours
//     values -- the ratio has no guaranteed direction. Left uncovered.
//   - fetch_source_statuses' ci_pipeline_runs branch reaches only
//     max(last_synced) and a count()>0 HAVING gate, both dedup-INVARIANT
//     (an extra physical version cannot move a max(), and cannot flip a
//     count from zero since a duplicate requires an existing row to
//     duplicate) -- no possible divergence, so no citation at all.
//   - _resolve_scope_labels' stale repos/teams display name is a
//     categorical swap (one string for another), which no numeric or
//     keyed-direction shape can admit. Left uncovered.
var homeGetEndpointSpec = RESTEndpointSpec{
	Method: "GET",
	Path:   "/api/v1/home",
	Requests: []RESTRequest{
		{
			// scope_type/scope_id/range_days/compare_days/start_date/
			// end_date all take home()'s own Pydantic defaults
			// (main.py:494-499): org scope, 14/14 day windows.
			Name:                "home_default_org",
			Query:               url.Values{},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: homeNumericLeaves,
		},
		{
			// team scope, scope_id bound at run time to filters/options'
			// own live team_id -- same producer/binding shape quadrant's
			// own "cycle_throughput_team_scoped" entry uses. Timeout raised
			// above the run default: the baseline (Python) leg resolves the
			// team's own repository set before it reads this route's
			// metrics (resolve_repo_ids_for_teams, api/queries/scopes.py),
			// a read the org-scope default entry above never makes, and
			// that extra read can outrun the run's own default budget.
			Name:                "home_team_scoped",
			Query:               url.Values{"scope_type": {"team"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     homeNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
			Timeout:    180 * time.Second,
		},
		{
			// repo scope, scope_id bound to filters/options' own live
			// repo_id.
			Name:                "home_repo_scoped",
			Query:               url.Values{"scope_type": {"repo"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     homeNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
		},
		{
			// _filters_from_query's own ScopeFilter(level=scope_type)
			// construction raises for any scope_type outside the Literal
			// set, INSIDE home()'s own try block -- the same generic
			// `except Exception: 503`, matching sankey's own
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

var homePostEndpointSpec = RESTEndpointSpec{
	Method: "POST",
	Path:   "/api/v1/home",
	Requests: []RESTRequest{
		{
			Name:                "home_default_org",
			Body:                map[string]any{"filters": map[string]any{}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: homeNumericLeaves,
		},
		{
			// team scope, bound at run time to filters/options' own live
			// team_id via restidbind.go's BodyPath binding -- the POST-body
			// twin of the QueryParam binding home's own GET "home_team_scoped"
			// entry above uses, same shape investment_explain's own
			// "team_scoped" POST entry already established for this
			// mechanism.
			Name:                "home_team_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     homeNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
		},
		{
			// repo scope, bound to filters/options' own live repo_id, same
			// BodyPath and reasoning as home_team_scoped above.
			Name:                "home_repo_scoped",
			Body:                map[string]any{"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []string{"ABC-123"}}}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     homeNumericLeaves,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", BodyPath: "filters.scope.ids"}},
		},
		{
			// HomeRequest's ONE field ("filters") is required with no
			// default -- confirmed live shape (pydantic_validation_error.go),
			// matching sankey's own "missing_mode_and_filters" precedent
			// for a body missing a required top-level field.
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
