package goapiproof

import (
	"net/url"
	"time"
)

// This file holds POST /api/v1/work-units/{work_unit_id}/explain's own
// corpus content. restcorpus.go registers the var below into
// restEndpointSpecs with a one-line addition, and restRunOrder carries one
// further line; nothing else in that shared file changes here.
//
// EVERY REQUEST NAMES llm_provider EXPLICITLY, AND THAT IS LOAD-BEARING.
// A proof run executes against production on both planes, and this is an
// LLM route: left at the endpoint's own `llm_provider="auto"` default, the
// deployment's configured provider decides, which would mean a real
// provider call and a real `llm_token_usage` row on the baseline leg for
// every run. Only two names cannot do either -- "mock" answers from a
// canned payload, "none" answers without constructing a provider at all --
// and neither reaches write_llm_token_usage, whose own guard excludes the
// raw string "mock" and which the none path returns before.
// TestWorkUnitExplainRequestsNameANonCallingProvider enforces that on
// every request below, so a later edit cannot quietly turn a proof run
// into a billed provider call.
//
// WHY MOST REQUESTS EXPECT 404. An explanation needs a work unit that
// exists, and only the entries that bind one ask for it: no literal id is
// a real unit in any org, so the id has to come from a live response. The
// negative, scope and validation branches use the LITERAL "{work_unit_id}"
// path segment instead -- the same mechanism, for the same reason,
// documented at this file's sibling people/{person_id}/summary spec's own
// person_not_found entry: RESTRequest has no per-request path override, so
// a request with no PathParam binding sends the placeholder verbatim, both
// planes decode it back to that same literal string, and no work unit can
// ever carry it. That turns the no-real-id constraint into the negative
// path proof rather than blocking it.
//
// ONE INTERMITTENT, STRUCTURALLY UNCOVERABLE MECHANISM is deliberately NOT
// declared as a BaselineDefect. This port's reads compose the shared
// latest-investments source, which excludes a work unit a later regrouping
// run retired; the reference plane's read does not. The bound entries take
// their id from the reference plane's OWN list, so whenever that list's
// first unit happens to be a retired one, this plane answers 404 where the
// reference answers 200 -- a STATUS difference. Parity.BaselineDefects
// never touches the status code (it admits leaf differences only), and
// StatusDivergenceReason states a deliberate Go-side status CHOICE, which
// this is not. So such a run shows up as a visible refusal naming both
// statuses: expected while that mechanism exists, never silenced, and
// absent for any org with no retired unit at the head of its own list.
var workUnitExplainParity = Options{
	// This response carries no numeric leaf at all: every field is a
	// string, a bool, an object of strings or a list of strings
	// (api/models/schemas.py's WorkUnitExplanation). NumericLeavesDeclared
	// is set with all three numeric tables empty on purpose -- it makes any
	// numeric leaf that ever appears here a reported, undeclared one rather
	// than a silently exact comparison.
	NumericLeavesDeclared: true,
}

// workUnitExplainLiveUnit binds the work_unit_id path segment to GET
// /api/v1/work-units' own default_window response, which runs earlier in
// restRunOrder.
var workUnitExplainLiveUnit = []RESTIDBinding{{Producer: "work_unit_id", PathParam: "work_unit_id"}}

var workUnitExplainPostEndpointSpec = RESTEndpointSpec{
	Method: "POST",
	Path:   "/api/v1/work-units/{work_unit_id}/explain",
	Requests: []RESTRequest{
		{
			// The one request that reaches a real explanation. Under the
			// mock provider every leaf of it is a pure function of the
			// precomputed row: the canned completion carries no markdown
			// section headers, so the response parser matches no SUMMARY
			// section, falls through to its first-paragraph default and
			// returns the whole completion text as `summary` -- which makes
			// this entry a byte-level comparison of the mock's own payload
			// as well as of the parser's other five fields.
			Name:                "mock_explains_a_live_work_unit",
			Query:               url.Values{"llm_provider": {"mock"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitExplainParity,
			IDBindings: workUnitExplainLiveUnit,
		},
		{
			// A provider resolving to "none" answers 200 with
			// ai_generated=false and every text field empty
			// (work_unit_explain.py's own early return), which is a
			// DIFFERENT branch of the same endpoint, not a degraded form of
			// the entry above. Bound to the same live unit, though this
			// branch reads nothing from it but its id.
			Name:                "provider_none_empty_explanation",
			Query:               url.Values{"llm_provider": {"none"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitExplainParity,
			IDBindings: workUnitExplainLiveUnit,
		},
		{
			// A widened window: range_days defaults to 14, and the time
			// filter is the only other thing that narrows which unit the
			// read can see. A unit visible in the default window is still
			// visible in a wider one, so this stays a 200.
			Name:                "range_days_90",
			Query:               url.Values{"llm_provider": {"mock"}, "range_days": {"90"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitExplainParity,
			IDBindings: workUnitExplainLiveUnit,
		},
		{
			// The not-found body interpolates the caller's own id, so this
			// compares that interpolation as well as the status -- and it
			// is the only entry here that exercises the one error body on
			// this route carrying caller-supplied text.
			Name:                "absent_work_unit",
			Query:               url.Values{"llm_provider": {"mock"}},
			WantCandidateStatus: 404, WantBaselineStatus: 404,
			BodyMode: RESTBodyModeJSON,
		},
		{
			// team scope: resolve_repo_filter_ids' team branch, which runs
			// BEFORE the read and so is executed on both planes even though
			// no unit is found. scope_id is bound to the live team_id every
			// other team-scoped request in this corpus binds to, so the
			// branch resolves real repositories rather than an empty set.
			//
			// Timeout raised above the run default for the same reason the
			// work-units GET's own team-scoped entry raises it: the
			// baseline leg's team-to-repo resolution is an extra read no
			// org-scope entry makes, and it can outrun the default budget.
			// That read happens before the not-found check, so this entry
			// pays for it even though it finds no unit.
			Name:                "team_scoped_absent_work_unit",
			Query:               url.Values{"llm_provider": {"mock"}, "scope_type": {"team"}},
			WantCandidateStatus: 404, WantBaselineStatus: 404,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
			Timeout:    180 * time.Second,
		},
		{
			// repo scope, scope_id bound to the live repo_id -- the other
			// branch of the same resolver.
			Name:                "repo_scoped_absent_work_unit",
			Query:               url.Values{"llm_provider": {"mock"}, "scope_type": {"repo"}},
			WantCandidateStatus: 404, WantBaselineStatus: 404,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
		},
		{
			// An unknown scope_type is NOT a validation error: the
			// reference constructs ScopeFilter(level=scope_type) inside the
			// endpoint's own try block, so the literal error it raises is
			// swallowed by the generic handler and answered as 503
			// "Explanation unavailable".
			Name:                "unknown_scope_type",
			Query:               url.Values{"llm_provider": {"mock"}, "scope_type": {"not-a-real-scope-level"}},
			WantCandidateStatus: 503, WantBaselineStatus: 503,
			BodyMode: RESTBodyModeJSON,
		},
		{
			// The shared int_parsing validator, which answers before the
			// provider is resolved and before any read.
			Name:                "invalid_range_days",
			Query:               url.Values{"llm_provider": {"mock"}, "range_days": {"not-a-number"}},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
		{
			// The shared date validator, on one of the other two typed
			// parameters this endpoint declares.
			Name:                "invalid_start_date",
			Query:               url.Values{"llm_provider": {"mock"}, "start_date": {"not-a-date"}},
			WantCandidateStatus: 422, WantBaselineStatus: 422,
			BodyMode: RESTBodyModeJSON,
		},
	},
}
