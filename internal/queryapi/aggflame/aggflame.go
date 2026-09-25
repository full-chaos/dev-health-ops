// Package aggflame is the Go port of GET /api/v1/flame/aggregated
// (src/dev_health_ops/api/services/aggregated_flame.py's
// build_aggregated_flame_response and its three per-mode tree builders,
// api/queries/aggregated_flame.py's six ClickHouse readers).
//
// Python source read at this branch's base:
//   - api/main.py -- the flame_aggregated() view (mode/date-window/filter
//     query params, _reject_comparative_params, the mode enum 400, the
//     outer try/except -> 503 fallback, and the limit/min_value clamps
//     `min(max(limit, 1), 1000)` / `max(min_value, 0)`, applied in the
//     ROUTE, not here -- see flame_aggregated_route.go).
//   - api/services/aggregated_flame.py -- build_aggregated_flame_response
//     and _build_cycle_breakdown_tree/_build_code_hotspots_tree/
//     _build_throughput_tree, _sanitize_label.
//   - api/queries/aggregated_flame.py -- fetch_cycle_breakdown,
//     fetch_cycle_milestones, fetch_code_hotspots, fetch_repo_names,
//     fetch_throughput, fetch_throughput_by_type.
//   - api/models/schemas.py -- AggregatedFlameNode/ApproximationInfo/
//     AggregatedFlameMeta/AggregatedFlameResponse, the wire shape.
//
// DEAD CODE, NOT PORTED: services/aggregated_flame.py declares
// _CYCLE_STATUS_ORDER and _status_sort_key but never calls
// _status_sort_key anywhere in the file (confirmed by reading the whole
// module) -- the cycle-breakdown category/child ordering this package
// implements (buildCycleBreakdownTree) is entirely value-sorted, matching
// what Python's own _build_cycle_breakdown_tree actually does. Porting an
// unused sort key would be new behaviour with nothing to parity-check it
// against.
//
// DATA-LAYER NOTE (ClickHouse ReplacingMergeTree dedup, class ruling):
//   - work_item_state_durations_daily is ReplacingMergeTree(computed_at)
//     (001_metrics_v2.sql, converted by migration 096), sort-keyed
//     (org_id, provider, work_scope_id, team_id, status, day). Python's
//     own fetch_cycle_breakdown/fetch_cycle_milestones already dedup with
//     an inner argMax(col, computed_at) GROUP BY the full sort key before
//     summing -- this port keeps that exact shape (matching the identical,
//     already-reviewed pattern in internal/operatingreview's
//     fetchStateDurations), no divergence.
//   - file_metrics_daily is ReplacingMergeTree(computed_at)
//     (001_metrics_v2.sql, converted by migration 096), sort-keyed
//     (org_id, repo_id, day, path). Python's fetch_code_hotspots reads it
//     through dedup_from() (clickhouse_dedup.py), which for this table
//     resolves to an `ORDER BY computed_at DESC LIMIT 1 BY` subquery, not
//     argMax/FINAL. This port uses an inner argMax(churn, computed_at)
//     GROUP BY (repo_id, day, path) subquery instead -- same one-row-per-
//     key-per-day result, never a bare LIMIT-1-BY dedup shape, per the
//     class ruling that a ReplacingMergeTree read never dedups with
//     LIMIT-1-BY alone.
//   - repos is ReplacingMergeTree(last_synced) (000_raw_tables.sql),
//     sort-keyed (org_id, id) since migration 027. Python's own
//     fetch_repo_names reads it with NEITHER FINAL nor an ORDER-BY-DESC
//     dedup -- a bare `WHERE id IN (...) AND org_id = ...` -- but its
//     WHERE already narrows to exactly the (org_id, id) sort key for each
//     row, the same "WHERE already specifies the whole sort key, so
//     ORDER-BY-LIMIT-1 and FINAL agree" situation flame_route.go's sibling
//     package documents for fetch_deployment. This port reads `repos
//     FINAL` anyway, for the same uniform "always FINAL" convention
//     every other ReplacingMergeTree reader in this service follows -- a
//     style choice, not a declared divergence.
//   - work_item_cycle_times is ReplacingMergeTree(computed_at)
//     (001_metrics_v2.sql), and Python's own fetch_throughput/
//     fetch_throughput_by_type already read it `AS wct FINAL` -- this
//     port matches that, no divergence.
//   - work_item_team_attributions: this port's own copy of
//     PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE (api/queries/
//     investment.py) is a verbatim, byte-for-byte port -- the same copy
//     internal/quadrant already carries independently (this binary's
//     established "own copy per file" convention, not a shared package).
//
// CONFIRMED PYTHON DEAD-PARAMETER QUIRKS (ported verbatim, not fixed --
// these are output-contract behaviours, not an RMT-dedup class issue this
// port is authorised to change):
//   - throughput mode: main.py's flame_aggregated() accepts a
//     work_scope_id query param, but build_aggregated_flame_response's
//     throughput branch never forwards it to fetch_throughput_by_type or
//     fetch_throughput, and never adds it to meta.filters either -- it has
//     zero effect on a throughput request. This port's buildThroughput
//     does not read Params.WorkScopeID at all.
//   - throughput mode: fetch_throughput's own filters list DOES have a
//     work_scope_id branch, but the caller (build_aggregated_flame_response)
//     never passes work_scope_id into it, so that branch is unreachable
//     in practice; likewise fetch_throughput/fetch_throughput_by_type both
//     accept a repo_id parameter that neither function's WHERE clause
//     ever references (confirmed by reading api/queries/
//     aggregated_flame.py in full) -- repo_id reaches only meta.filters
//     (build_aggregated_flame_response's own `if repo_id:
//     filters_used["repo_id"] = repo_id`), never the query. This port's
//     fetchThroughput/fetchThroughputByType take no repo_id parameter at
//     all; buildThroughput still records Params.RepoID into meta.filters.
//   - code_hotspots/throughput modes never read Params.Provider; only
//     cycle_breakdown does (main.py's own per-mode wiring).
package aggflame

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// declares independently (quadrant.QueryClient, operatingreview.QueryClient).
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// RequestError carries an HTTP status the way Python's HTTPException does,
// so the route layer can answer the same status code
// build_aggregated_flame_response/flame_aggregated() would raise for the
// same bad input, without this package importing net/http.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(msg string) error { return &RequestError{Status: 400, Message: msg} }

// AsRequestError extracts a *RequestError's status/message, or reports
// ok=false for any other error -- the route layer's 503 fallback
// (main.py's own `except Exception: raise HTTPException(503, "Data
// unavailable")`, main.py:872-873).
func AsRequestError(err error) (*RequestError, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return nil, false
}

// Params is BuildResponse's input -- the query params GET
// /api/v1/flame/aggregated takes (main.py:807-819), already normalised by
// the route layer the way main.py's own view function body normalises
// them BEFORE calling build_aggregated_flame_response: StartDay/EndDay
// are the resolved [start, end) window (main.py:844-854, not the raw
// optional query values), and Limit/MinValue already carry the
// `min(max(limit, 1), 1000)` / `max(min_value, 0)` clamps (main.py:
// 867-868).
type Params struct {
	Mode        string
	StartDay    time.Time
	EndDay      time.Time
	TeamID      string // "" means absent, matching `team_id or None`
	RepoID      string
	Provider    string
	WorkScopeID string
	Limit       int
	MinValue    int
}

// Node is AggregatedFlameNode's wire shape (schemas.py:601-605). Children
// is always a non-nil (possibly empty) slice: Python's own `children:
// list[...] = []` default never serialises as JSON null.
type Node struct {
	Name     string  `json:"name"`
	Value    float64 `json:"value"`
	Children []Node  `json:"children"`
}

// ApproximationInfo is ApproximationInfo's wire shape (schemas.py:
// 608-612). Method is a pointer so an unset value serialises as JSON
// null, matching Pydantic's `method: str | None = None` (this app sets no
// response_model_exclude_none anywhere -- main.py:217 -- so None fields
// are always emitted, never dropped).
type ApproximationInfo struct {
	Used   bool    `json:"used"`
	Method *string `json:"method"`
}

// Meta is AggregatedFlameMeta's wire shape (schemas.py:615-621).
// WindowStart/WindowEnd are pre-formatted "YYYY-MM-DD" strings, the same
// convention internal/quadrant's own Point.WindowStart/WindowEnd already
// use for a Python `date` field (never a custom civil-date type). Filters
// and Notes are always non-nil so they serialise as `{}`/`[]`, matching
// Python's `filters: dict = {}` / `notes: list = []` defaults.
type Meta struct {
	WindowStart   string                    `json:"window_start"`
	WindowEnd     string                    `json:"window_end"`
	Filters       pyjson.OrderedMap[string] `json:"filters"`
	Notes         []string                  `json:"notes"`
	Approximation ApproximationInfo         `json:"approximation"`
}

// Response is AggregatedFlameResponse's wire shape (schemas.py:624-628).
type Response struct {
	Mode string `json:"mode"`
	Unit string `json:"unit"`
	Root Node   `json:"root"`
	Meta Meta   `json:"meta"`
}

// BuildResponse ports build_aggregated_flame_response (services/
// aggregated_flame.py:287-464) together with flame_aggregated()'s own
// mode-enum check (main.py:838-842) -- the same "own the enum
// validation" shape internal/quadrant.BuildResponse already follows for
// its own `type` query param, even though Python enforces the two checks
// in different files.
func BuildResponse(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	switch params.Mode {
	case "cycle_breakdown":
		return buildCycleBreakdown(ctx, client, orgID, params)
	case "code_hotspots":
		return buildCodeHotspots(ctx, client, orgID, params)
	case "throughput":
		return buildThroughput(ctx, client, orgID, params)
	default:
		return nil, badRequest("mode must be 'cycle_breakdown', 'code_hotspots', or 'throughput'")
	}
}

func windowMeta(params Params, filters pyjson.OrderedMap[string], notes []string, approx ApproximationInfo) Meta {
	return Meta{
		WindowStart:   params.StartDay.Format("2006-01-02"),
		WindowEnd:     params.EndDay.Format("2006-01-02"),
		Filters:       filters,
		Notes:         notes,
		Approximation: approx,
	}
}

// buildCycleBreakdown ports build_aggregated_flame_response's
// "cycle_breakdown" branch (services/aggregated_flame.py:309-370).
func buildCycleBreakdown(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	rows, err := fetchCycleBreakdown(ctx, client, orgID, params.StartDay, params.EndDay, params.TeamID, params.Provider, params.WorkScopeID)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_cycle_breakdown: %w", err)
	}

	notes := []string{}
	approx := ApproximationInfo{}
	var root Node

	if len(rows) == 0 {
		milestoneRows, err := fetchCycleMilestones(ctx, client, orgID, params.StartDay, params.EndDay, params.TeamID, params.Provider, params.WorkScopeID)
		if err != nil {
			return nil, fmt.Errorf("aggflame: fetch_cycle_milestones: %w", err)
		}
		if len(milestoneRows) > 0 {
			notes = append(notes, "Detailed state transition data unavailable. Using milestone-based approximation.")
			method := "milestones"
			approx = ApproximationInfo{Used: true, Method: &method}
			remapped := make([]cycleBreakdownRow, len(milestoneRows))
			for i, r := range milestoneRows {
				remapped[i] = cycleBreakdownRow{
					Status:     r.Milestone,
					TotalHours: r.AvgHours * float64(r.TotalItems),
				}
			}
			root = buildCycleBreakdownTree(remapped)
		} else {
			root = Node{Name: "Cycle Time", Value: 0, Children: []Node{}}
		}
	} else {
		root = buildCycleBreakdownTree(rows)
	}

	// aggregated_flame.py fills filters_used in this order.
	filters := pyjson.NewOrderedMap[string]()
	if params.TeamID != "" {
		filters.Set("team_id", params.TeamID)
	}
	if params.Provider != "" {
		filters.Set("provider", params.Provider)
	}
	if params.WorkScopeID != "" {
		filters.Set("work_scope_id", params.WorkScopeID)
	}

	return &Response{
		Mode: "cycle_breakdown",
		Unit: "hours",
		Root: root,
		Meta: windowMeta(params, filters, notes, approx),
	}, nil
}

// buildCodeHotspots ports build_aggregated_flame_response's
// "code_hotspots" branch (services/aggregated_flame.py:372-412).
func buildCodeHotspots(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	rows, err := fetchCodeHotspots(ctx, client, orgID, params.StartDay, params.EndDay, params.RepoID, params.Limit, params.MinValue)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_code_hotspots: %w", err)
	}

	notes := []string{}
	var root Node
	repoNames := map[string]string{}

	if len(rows) == 0 {
		notes = append(notes, "No file churn data available for this window/scope.")
		root = Node{Name: "Code Churn", Value: 0, Children: []Node{}}
	} else {
		repoIDs := make([]string, 0, len(rows))
		for _, r := range rows {
			if r.RepoID != "" {
				repoIDs = append(repoIDs, r.RepoID)
			}
		}
		repoNames, err = fetchRepoNames(ctx, client, orgID, repoIDs)
		if err != nil {
			return nil, fmt.Errorf("aggflame: fetch_repo_names: %w", err)
		}
		root = buildCodeHotspotsTree(rows, repoNames)
	}

	// aggregated_flame.py fills filters_used in this order.
	filters := pyjson.NewOrderedMap[string]()
	if params.RepoID != "" {
		filters.Set("repo_id", params.RepoID)
	}

	return &Response{
		Mode: "code_hotspots",
		Unit: "loc",
		Root: root,
		Meta: windowMeta(params, filters, notes, ApproximationInfo{}),
	}, nil
}

// buildThroughput ports build_aggregated_flame_response's "throughput"
// branch (services/aggregated_flame.py:414-464) -- the `else` branch of
// Python's if/elif/else, reached for any mode other than
// "cycle_breakdown"/"code_hotspots"; BuildResponse's own switch above
// already restricts that to exactly "throughput".
func buildThroughput(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	rows, err := fetchThroughputByType(ctx, client, orgID, params.StartDay, params.EndDay, params.TeamID, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_throughput_by_type: %w", err)
	}

	notes := []string{}
	approx := ApproximationInfo{}

	if len(rows) == 0 {
		rows, err = fetchThroughput(ctx, client, orgID, params.StartDay, params.EndDay, params.TeamID, params.Limit)
		if err != nil {
			return nil, fmt.Errorf("aggflame: fetch_throughput: %w", err)
		}
		notes = append(notes, "Work type classification unavailable. Using team-based grouping.")
		method := "inferred"
		approx = ApproximationInfo{Used: true, Method: &method}
	}

	var root Node
	if len(rows) == 0 {
		notes = append(notes, "No throughput data available for this window/scope.")
		root = Node{Name: "Work Delivered", Value: 0, Children: []Node{}}
	} else {
		root = buildThroughputTree(rows)
	}

	// aggregated_flame.py fills filters_used in this order.
	filters := pyjson.NewOrderedMap[string]()
	if params.TeamID != "" {
		filters.Set("team_id", params.TeamID)
	}
	if params.RepoID != "" {
		filters.Set("repo_id", params.RepoID)
	}

	return &Response{
		Mode: "throughput",
		Unit: "items",
		Root: root,
		Meta: windowMeta(params, filters, notes, approx),
	}, nil
}

// sanitizeLabel ports _sanitize_label (services/aggregated_flame.py:
// 55-59).
func sanitizeLabel(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "(unknown)"
	}
	return trimmed
}

// capitalizeWorkType ports Python's `w_type.capitalize()`
// (services/aggregated_flame.py:270) -- upper-cases the first rune,
// lower-cases the rest, matching str.capitalize()'s ASCII-range
// behaviour for the work-item type strings this reads (feature/bug/
// chore/etc).
func capitalizeWorkType(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	return strings.ToUpper(string(r[0])) + strings.ToLower(string(r[1:]))
}

// cycleStatusCategory ports the inline if/elif chain inside
// _build_cycle_breakdown_tree (services/aggregated_flame.py:91-100).
func cycleStatusCategory(status string) string {
	switch status {
	case "in_progress", "in progress", "coding", "development":
		return "Active Work"
	case "review", "in_review", "code review", "pr_review":
		return "Review"
	case "backlog", "ready", "waiting", "queue", "pending":
		return "Waiting"
	case "blocked", "on_hold", "on hold":
		return "Blocked"
	default:
		return "Other"
	}
}

// cycleCategoryOrder is the fixed insertion order Python's own
// `categories: dict[str, list] = {...}` literal declares (services/
// aggregated_flame.py:77-83) -- a Python dict preserves this order, so a
// Go map (unordered) cannot stand in for it; only categories with at
// least one row survive into category_nodes, in this order before the
// final value-sort.
var cycleCategoryOrder = []string{"Active Work", "Review", "Waiting", "Blocked", "Other"}

// buildCycleBreakdownTree ports _build_cycle_breakdown_tree
// (services/aggregated_flame.py:62-136).
func buildCycleBreakdownTree(rows []cycleBreakdownRow) Node {
	categories := map[string][]cycleBreakdownRow{}
	for _, row := range rows {
		status := strings.ToLower(strings.TrimSpace(row.Status))
		if row.TotalHours <= 0 {
			continue
		}
		cat := cycleStatusCategory(status)
		categories[cat] = append(categories[cat], row)
	}

	categoryNodes := []Node{}
	for _, catName := range cycleCategoryOrder {
		catRows := categories[catName]
		if len(catRows) == 0 {
			continue
		}
		sorted := make([]cycleBreakdownRow, len(catRows))
		copy(sorted, catRows)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].TotalHours > sorted[j].TotalHours })

		children := make([]Node, 0, len(sorted))
		var catValue float64
		for _, r := range sorted {
			children = append(children, Node{Name: sanitizeLabel(r.Status), Value: r.TotalHours, Children: []Node{}})
			catValue += r.TotalHours
		}
		categoryNodes = append(categoryNodes, Node{Name: catName, Value: catValue, Children: children})
	}

	sort.SliceStable(categoryNodes, func(i, j int) bool { return categoryNodes[i].Value > categoryNodes[j].Value })

	var rootValue float64
	for _, n := range categoryNodes {
		rootValue += n.Value
	}

	return Node{Name: "Cycle Time", Value: rootValue, Children: categoryNodes}
}

// pathTreeNode is one node of the per-repo path tree _build_code_hotspots_tree
// builds via its nested `path_tree` dict (services/aggregated_flame.py:
// 165-184) before converting it to AggregatedFlameNode via _tree_to_node.
// order preserves first-seen segment insertion order, matching a Python
// dict's own iteration order.
type pathTreeNode struct {
	value    float64
	children map[string]*pathTreeNode
	order    []string
}

func newPathTreeNode() *pathTreeNode {
	return &pathTreeNode{children: map[string]*pathTreeNode{}}
}

// buildCodeHotspotsTree ports _build_code_hotspots_tree
// (services/aggregated_flame.py:139-230).
func buildCodeHotspotsTree(rows []hotspotRow, repoNames map[string]string) Node {
	repoFiles := map[string][]hotspotRow{}
	var repoOrder []string
	for _, row := range rows {
		repoID := row.RepoID
		if repoID == "" {
			repoID = "(unknown)"
		}
		if _, seen := repoFiles[repoID]; !seen {
			repoOrder = append(repoOrder, repoID)
		}
		repoFiles[repoID] = append(repoFiles[repoID], row)
	}

	repoNodes := []Node{}
	for _, repoID := range repoOrder {
		files := repoFiles[repoID]
		repoName := repoNames[repoID]
		if repoName == "" {
			repoName = repoID
		}

		root := newPathTreeNode()
		for _, file := range files {
			filePath := file.FilePath
			if filePath == "" {
				filePath = "(unknown)"
			}
			segments := []string{}
			for _, seg := range strings.Split(filePath, "/") {
				if seg != "" {
					segments = append(segments, seg)
				}
			}
			if len(segments) == 0 {
				segments = []string{filePath}
			}

			current := root
			for i, seg := range segments {
				child, ok := current.children[seg]
				if !ok {
					child = newPathTreeNode()
					current.children[seg] = child
					current.order = append(current.order, seg)
				}
				if i == len(segments)-1 {
					child.value += file.TotalChurn
				} else {
					current = child
				}
			}
		}

		dirNodes := make([]Node, 0, len(root.order))
		for _, dirName := range root.order {
			dirNodes = append(dirNodes, pathTreeToNode(dirName, root.children[dirName]))
		}
		sort.SliceStable(dirNodes, func(i, j int) bool { return dirNodes[i].Value > dirNodes[j].Value })

		var repoValue float64
		for _, n := range dirNodes {
			repoValue += n.Value
		}
		repoNodes = append(repoNodes, Node{Name: sanitizeLabel(repoName), Value: repoValue, Children: dirNodes})
	}

	sort.SliceStable(repoNodes, func(i, j int) bool { return repoNodes[i].Value > repoNodes[j].Value })

	var rootValue float64
	for _, n := range repoNodes {
		rootValue += n.Value
	}
	return Node{Name: "Code Churn", Value: rootValue, Children: repoNodes}
}

// pathTreeToNode ports _tree_to_node (services/aggregated_flame.py:186-203).
func pathTreeToNode(name string, t *pathTreeNode) Node {
	children := make([]Node, 0, len(t.order))
	for _, childName := range t.order {
		children = append(children, pathTreeToNode(childName, t.children[childName]))
	}
	sort.SliceStable(children, func(i, j int) bool { return children[i].Value > children[j].Value })

	var childrenValue float64
	for _, c := range children {
		childrenValue += c.Value
	}

	return Node{Name: sanitizeLabel(name), Value: t.value + childrenValue, Children: children}
}

// buildThroughputTree ports _build_throughput_tree
// (services/aggregated_flame.py:233-284). Python's per-child name/value
// expressions (`row.get("team_name") or row.get("repo_name") or
// row.get("team_id")`, `row.get("items_completed") or
// row.get("throughput") or 0`) fall back to keys neither
// fetchThroughputRow-producing query ever populates (team_name and
// items_completed are always present and non-empty/non-zero-typed for
// both readers this package has) -- see this package's own doc comment
// for the confirmation. This port reads TeamName/ItemsCompleted directly.
func buildThroughputTree(rows []throughputRow) Node {
	typeGroups := map[string][]throughputRow{}
	var typeOrder []string
	for _, row := range rows {
		wType := row.WorkType
		if wType == "" {
			wType = "unclassified"
		}
		if _, seen := typeGroups[wType]; !seen {
			typeOrder = append(typeOrder, wType)
		}
		typeGroups[wType] = append(typeGroups[wType], row)
	}

	typeNodes := []Node{}
	for _, wType := range typeOrder {
		typeRows := typeGroups[wType]
		sorted := make([]throughputRow, len(typeRows))
		copy(sorted, typeRows)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ItemsCompleted > sorted[j].ItemsCompleted })

		children := make([]Node, 0, len(sorted))
		var typeValue float64
		for _, r := range sorted {
			children = append(children, Node{Name: sanitizeLabel(r.TeamName), Value: r.ItemsCompleted, Children: []Node{}})
			typeValue += r.ItemsCompleted
		}
		typeNodes = append(typeNodes, Node{Name: capitalizeWorkType(wType), Value: typeValue, Children: children})
	}

	sort.SliceStable(typeNodes, func(i, j int) bool { return typeNodes[i].Value > typeNodes[j].Value })

	var rootValue float64
	for _, n := range typeNodes {
		rootValue += n.Value
	}
	return Node{Name: "Work Delivered", Value: rootValue, Children: typeNodes}
}
