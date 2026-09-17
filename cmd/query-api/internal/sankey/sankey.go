// Package sankey is the Go port of GET+POST /api/v1/sankey
// (api/services/sankey.py's build_sankey_response, api/queries/sankey.py's
// five ClickHouse readers, api/queries/scopes.py's build_scope_filter_multi
// and api/services/filtering.py's resolve_repo_filter_ids/time_window).
//
// SCOPE: all four SANKEY_DEFINITIONS entries (investment, expense, state,
// hotspot) are ported, including the schema-presence guard
// (_tables_present/_columns_present) each mode's builder runs before
// issuing its own reads -- a missing table or column degrades to an empty
// (nodes=[], links=[]) result rather than an error, matching Python.
//
// ERROR SHAPE: build_sankey_response's own body raises only a plain
// ValueError for an unrecognised mode; every other failure this package
// surfaces is a plain error too. Both route handlers (sankey_get,
// sankey_post) catch every exception the same way -- a blanket
// `except Exception: raise HTTPException(503, "Data unavailable")`, with
// no narrower except-ValueError branch the way investment_flow's sibling
// route has. This port's route layer follows suit: ANY error BuildResponse
// returns becomes a 503, never a 400/404 -- there is no RequestError type
// in this package at all, unlike quadrant/heatmap's siblings.
//
// DEDUP (class ruling, this service): every ReplacingMergeTree table this
// package reads -- repos, work_item_cycle_times, work_item_state_
// durations_daily, file_metrics_daily, and the shared work_unit_
// investments dedup this package reuses from the analytics package -- is
// read FINAL (or, for work_unit_investments, via the analytics package's
// own argMax-tuple dedup), org_id filtered inside the same statement.
// api/queries/sankey.py reads repos with a plain (undeduped) JOIN in two
// of its five readers, and work_item_cycle_times with no FINAL or other
// dedup at all in a third -- each a declared Python-plane defect (see
// this package's queries.go doc comments and internal/goapiproof/
// restcorpus.go's own sankey declarations). work_item_metrics_daily and
// work_item_state_durations_daily's Python readers already dedup
// correctly (FINAL and a per-key argMax(items_touched, computed_at)
// respectively, the latter's projected column being non-nullable per
// DDL, so no null-skip risk) -- no divergence there.
package sankey

import (
	"context"
	"errors"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the narrow ClickHouse read capability this package
// needs -- same shape/convention as every other ported REST route's
// package-local interface.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ErrUnavailable is returned when BuildResponse is called with a nil client.
var ErrUnavailable = errors.New("sankey: clickhouse client unavailable")

const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return "SETTINGS max_execution_time = 30"
}

// definition ports SankeyDefinition (services/sankey.py:26-30).
type definition struct {
	Label       string
	Description string
	Unit        string
}

// definitions ports SANKEY_DEFINITIONS (services/sankey.py:33-56) verbatim,
// same declaration order.
var definitions = map[string]definition{
	"investment": {
		Label:       "Investment flow",
		Description: "Where effort allocates across initiatives, areas, issue types, and work items.",
		Unit:        "items",
	},
	"expense": {
		Label:       "Investment expense",
		Description: "How planned effort converts into unplanned work, rework, and rewrites.",
		Unit:        "items",
	},
	"state": {
		Label:       "State flow",
		Description: "Execution paths that reveal stalls, loops, and retry patterns.",
		Unit:        "items",
	},
	"hotspot": {
		Label:       "Code hotspot flow",
		Description: "Where change concentrates from repos to files and change intent.",
		Unit:        "changes",
	},
}

const (
	maxInvestmentItems = 60
	maxHotspotRows     = 150
)

// Params is BuildResponse's input. Mode is read verbatim from the
// caller (the route layer normalizes the "hotpot" typo the same way
// build_sankey_response does, see normalizeMode below); WorkCategory/
// WhatRepos/ScopeIDs/ScopeLevel are filters.why.work_category/
// filters.what.repos/filters.scope.ids/filters.scope.level, already
// extracted from the request's raw filters object by the route layer,
// the same convention drilldown/investment_explain's own route files use.
// StartDay/EndDay are time_window's own (start_day, end_day) pair, ALREADY
// resolved against window_start/window_end (services/sankey.py's
// _apply_window_to_filters, applied by the route layer before calling
// BuildResponse -- the one piece of business logic this package leaves to
// its caller, matching drilldown_prs_route.go's own timeWindow call-site
// convention).
type Params struct {
	Mode         string
	ScopeLevel   string // default "org"
	ScopeIDs     []string
	WhatRepos    []string
	WorkCategory []string
	StartDay     time.Time
	EndDay       time.Time
}

// normalizeMode ports the `if mode == "hotpot": mode = "hotspot"` typo fix
// (services/sankey.py:547-548).
func normalizeMode(mode string) string {
	if mode == "hotpot" {
		return "hotspot"
	}
	return mode
}

// Response is the wire shape of SankeyResponse (api/models/schemas.py:
// 561-577) restricted to the fields build_sankey_response ever sets
// (mode/nodes/links/unit/label/description); the remaining nine fields
// (team_coverage, repo_coverage, distinct_team_targets,
// distinct_repo_targets, chosen_mode, coverage, unassigned_reasons,
// flow_mode, drill_category, top_n_repos) are ALWAYS null on the wire for
// this route -- SankeyResponse's own Pydantic defaults, never populated
// by build_sankey_response (only investment_flow/investment_flow_repo_team,
// a different pair of routes, ever set them). Every one of those nine
// fields is still emitted, as an explicit JSON null, matching FastAPI's
// default (non-exclude_none) response_model serialization -- so every
// field below is a nilable type with no `omitempty`.
type Response struct {
	Mode                string             `json:"mode"`
	Nodes               []Node             `json:"nodes"`
	Links               []Link             `json:"links"`
	Unit                *string            `json:"unit"`
	Label               *string            `json:"label"`
	Description         *string            `json:"description"`
	TeamCoverage        *float64           `json:"team_coverage"`
	RepoCoverage        *float64           `json:"repo_coverage"`
	DistinctTeamTargets *int               `json:"distinct_team_targets"`
	DistinctRepoTargets *int               `json:"distinct_repo_targets"`
	ChosenMode          *string            `json:"chosen_mode"`
	Coverage            map[string]float64 `json:"coverage"`
	UnassignedReasons   map[string]int     `json:"unassigned_reasons"`
	FlowMode            *string            `json:"flow_mode"`
	DrillCategory       *string            `json:"drill_category"`
	TopNRepos           *int               `json:"top_n_repos"`
}

// Node is the wire shape of SankeyNode (schemas.py:549-552). Value is
// always null -- _touch_node (services/sankey.py:105-112) never sets it.
type Node struct {
	Name  string   `json:"name"`
	Group *string  `json:"group"`
	Value *float64 `json:"value"`
}

// Link is the wire shape of SankeyLink (schemas.py:555-558).
type Link struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Value  float64 `json:"value"`
}

func strPtr(s string) *string { return &s }

// BuildResponse ports build_sankey_response (services/sankey.py:537-599).
func BuildResponse(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	if client == nil {
		return nil, ErrUnavailable
	}

	mode := normalizeMode(params.Mode)
	def, ok := definitions[mode]
	if !ok {
		return nil, errUnknownMode(mode)
	}

	scopeLevel := params.ScopeLevel
	if scopeLevel == "" {
		scopeLevel = "org"
	}

	var nodes []Node
	var links []Link
	var err error

	// One instant for the whole response: a repo-keyed mode builds several
	// statements from one scope, so they resolve one team membership. The
	// expense and state modes key on a team column and take no instant.
	asOf := time.Now().UTC()

	switch mode {
	case "investment":
		nodes, links, err = buildInvestmentFlow(ctx, client, params.StartDay, params.EndDay, scopeLevel, params.ScopeIDs, params.WhatRepos, params.WorkCategory, orgID, asOf)
	case "expense":
		nodes, links, err = buildExpenseFlow(ctx, client, params.StartDay, params.EndDay, scopeLevel, params.ScopeIDs, params.WhatRepos, orgID)
	case "state":
		nodes, links, err = buildStateFlow(ctx, client, params.StartDay, params.EndDay, scopeLevel, params.ScopeIDs, params.WhatRepos, orgID)
	case "hotspot":
		nodes, links, err = buildHotspotFlow(ctx, client, params.StartDay, params.EndDay, scopeLevel, params.ScopeIDs, params.WhatRepos, orgID, asOf)
	}
	if err != nil {
		return nil, err
	}
	if nodes == nil {
		nodes = []Node{}
	}
	if links == nil {
		links = []Link{}
	}

	return &Response{
		Mode:        mode,
		Nodes:       nodes,
		Links:       links,
		Unit:        strPtr(def.Unit),
		Label:       strPtr(def.Label),
		Description: strPtr(def.Description),
	}, nil
}

type unknownModeError struct{ mode string }

func (e *unknownModeError) Error() string { return "sankey: unknown mode: " + e.mode }

func errUnknownMode(mode string) error { return &unknownModeError{mode: mode} }
