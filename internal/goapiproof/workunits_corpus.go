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

// workUnitsTeamScopeSubsetDefect declares the team-scoped candidate list
// as a bounded subset of the baseline's own organization-wide list: the
// two planes resolve a team's repositories from different tables (this
// route's own team_scoped request doc comment below states the two
// tables), so a genuinely narrower team reads fewer work units on the
// candidate side by design. Every field workUnitInvestmentWire carries
// besides its own key (work_unit_id) is an intrinsic, per-unit property
// this route's own assembly computes the same way regardless of scope --
// RepoIDs narrows WHICH units the read returns, never resums a returned
// unit's own effort/investment/evidence fields across a repository set
// (cmd/query-api/workunits_route.go's own RepoIDs wiring is a WHERE-filter
// input only) -- so every one of those fields is declared EQUAL, never
// bounded: this route carries no per-unit sum-type leaf a narrower scope
// could legitimately shrink.
//
// What this citation does NOT certify: that an admitted baseline-only
// work unit is absent from the candidate BECAUSE it falls outside the
// team's repositories, as opposed to the route's own, separately
// undeclared work_unit_supersessions/evidence-quotes dedup divergences
// this file's own package doc comment states above (points 1 and 2) --
// both of which can also manifest as a baseline-only list element. This
// shape cannot tell the two mechanisms apart from the wire alone; it
// admits either one under the same "baseline carries an element the
// candidate's list does not" rule. Accepted here because the two
// mechanisms differ enormously in scale in practice (a scope narrowing to
// one team's repositories typically removes most of an organization's
// units; an unmerged dedup exclusion removes at most the handful of
// units a single unmerged physical row affects) and in cause (this
// citation's own mechanism is a permanent property of a genuinely
// narrower team, never tied to an unmerged row), not because the two are
// structurally distinguishable here.
var workUnitsTeamScopeSubsetDefect = BaselineDefect{
	Ticket: "CHAOS-5920",
	Reason: "GET/POST /api/v1/work-units' team scope resolves a team's repositories from team_repo_ownership through one shared condition (cmd/query-api/internal/teamscope.RepoCondition), while the reference plane resolves the same scope from user_metrics_daily.team_id (resolve_repo_ids_for_teams, api/queries/scopes.py) -- a per-author-attribution table, not a repository-ownership one. For a genuinely narrower team the candidate's list is therefore a subset of the baseline's own organization-wide list: every unit the candidate returns is also in the baseline's list, under the same work_unit_id, with identical work_unit_type/work_unit_name/time_range/effort/investment/evidence_quality/evidence -- none of which this route's own assembly resums across a repository set (see this var's own doc comment). Go is correct.",
	Paths:  []string{"data"},
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:    "data",
		KeyFields:   []string{"work_unit_id"},
		EqualLeaves: []string{"work_unit_type", "work_unit_name", "time_range", "effort", "investment", "evidence_quality", "evidence"},
	},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's; a team that happens to own every repository in the organization leaves the two lists identical, and a narrow enough window can return zero units on both legs -- either way there is nothing under data for this citation to explain, and a comparison taken then shows no divergence",
}

// workUnitsTeamScopeOrderInsensitiveLists keys `data` by work_unit_id for
// team_scoped alone (never workUnitsParity's own org-wide/repo-scoped
// entries, which carry no membership-subset defect and stay positional):
// workUnitsTeamScopeSubsetDefect's own TeamRepoSubsetShape can only ever
// admit a genuine membership gap as a ShapePresence finding (one
// work_unit_id present in baseline, absent in candidate) when `data`
// itself is compared by key rather than position (compareList,
// compare.go) -- a raw positional compareList reports a length mismatch
// only when the two lists are actually different LENGTHS, which a gap at
// or before the shared default LIMIT never produces (both legs still
// saturate it), and otherwise cascades one baseline-only unit into a
// positional value mismatch on every following index instead of the one
// admittable presence difference.
//
// structuralAgreementFailure's own list-alignment rule (structural.go)
// reads a route's declared identity from the SAME two places this
// package already declares one: a request's own DedupListPath/
// DedupKeyFields (the synthetic key InjectRESTDedupKeys writes), or a
// BaselineDefect's own TeamRepoSubsetShape.KeyFields at a matching
// ListPath. Read-only audit of every TeamRepoSubsetShape this package
// declares whose KeyFields is not literal "id" -- each one is keyed for
// structural alignment by that rule:
//   - GET/POST /api/v1/work-units team_scoped -- `data`, work_unit_id
//     (this entry). Gains a NEW OrderInsensitiveLists entry here too
//     (below): without it, compareList's own VALUE comparison stays
//     positional even once the structural pre-check stops misfiring, and
//     the one genuine gap cascades into a wall of positional value
//     mismatches instead of the one admittable ShapePresence finding.
//   - investment/sunburst, the three heatmap variants (repo touchpoints,
//     hotspot risk, review-wait density), sankey nodes/links (both
//     modes), investment/flow nodes/links (both modes) -- each already
//     declares its own OrderInsensitiveLists entry at the same
//     ListPath/KeyFields for an unrelated ordering reason, so only their
//     structural alignment was ever the gap; this rule closes it with no
//     further corpus change.
//   - GET/POST /api/v1/drilldown/prs and GET/POST /api/v1/opportunities
//     team_scoped -- neither declares an OrderInsensitiveLists entry at
//     their own TeamRepoSubsetShape's ListPath, so they carry the SAME
//     two-part gap this route did; both are also declared Intermittent
//     with a doc comment stating the citation has never yet been
//     exercised against a genuinely narrowing live window, unlike this
//     route's own confirmed recurrence. Left undeclared here: closing
//     them needs the same live confirmation this route already has, not
//     assumed from the shape alone.
//
// explain's own contributors/drivers team_scoped subset defects key by
// literal "id" already, so they were already aligned correctly before
// this rule existed and are unaffected by it.
var workUnitsTeamScopeOrderInsensitiveLists = []OrderInsensitiveList{
	{
		Path:      "data",
		KeyFields: []string{"work_unit_id"},
		Reason:    "team_scoped's own list membership can genuinely differ by one work_unit_id with no length change while both legs saturate the shared default LIMIT; keying by work_unit_id turns that into the ShapePresence finding workUnitsTeamScopeSubsetDefect's own TeamRepoSubsetShape is built to admit, instead of a positional cascade.",
		Ticket:    "CHAOS-5920",
	},
}

// workUnitsTeamScopedParity is workUnitsParity plus
// workUnitsTeamScopeSubsetDefect, with `data` keyed by work_unit_id -- see
// workUnitsTeamScopeOrderInsensitiveLists' own doc comment for why the
// keying is required for the defect to admit anything at all. Shared by
// GET and POST's own team_scoped entries.
var workUnitsTeamScopedParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            workUnitsFloats,
	FloatExactLeaves:      workUnitsFloatExact,
	BaselineDefects:       []BaselineDefect{workUnitsTeamScopeSubsetDefect},
	OrderInsensitiveLists: workUnitsTeamScopeOrderInsensitiveLists,
}

var workUnitsGetEndpointSpec = RESTEndpointSpec{
	Method: "GET",
	Path:   "/api/v1/work-units",
	Requests: []RESTRequest{
		{
			Name:                "default_window",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
			// Produces the work_unit_id that POST
			// /api/v1/work-units/{work_unit_id}/explain's own bound entries
			// consume. This response is a bare JSON array of work-unit
			// records, so there is no ListPath to walk -- the array IS the
			// decoded body -- and work_unit_id is already a bare string on
			// the wire, so no JoinField is needed either.
			Produces: []RESTIDProducer{{Name: "work_unit_id", IDField: "work_unit_id"}},
		},
		{
			Name:                "range_days_90",
			Query:               url.Values{"range_days": {"90"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON, Parity: workUnitsParity,
		},
		{
			// team scope, scope_id bound at run time to filters/
			// options' own live team_id -- the same live id every other
			// team-scoped GET request in this corpus binds to. Timeout
			// raised above the run default: the baseline leg's own
			// team-to-repo resolution is an extra read the org-scope
			// default entry never makes and can outrun the default budget.
			//
			// The two planes resolve that team's repositories from
			// different tables, so a genuinely narrower team's candidate
			// list is a bounded subset of the baseline's own
			// organization-wide list (workUnitsTeamScopeSubsetDefect
			// above). It is visible on the wire as a LIST LENGTH: the
			// baseline answers over the whole organization, the candidate
			// over the team's repositories.
			Name:                "team_scoped",
			Query:               url.Values{"scope_type": {"team"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			Parity:     workUnitsTeamScopedParity,
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
			// above) -- exercises the same team branch against a team that
			// genuinely resolves to repositories, the same request shape a
			// real team-scoped client sends. Same bounded-subset finding as
			// its GET twin above.
			Name: "team_scoped",
			Body: map[string]any{
				"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
			},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
			Parity:     workUnitsTeamScopedParity,
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
