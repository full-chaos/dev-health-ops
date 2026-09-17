// Per-mode business logic -- ports _build_investment_flow/
// _build_expense_flow/_build_state_flow/_build_hotspot_flow (services/
// sankey.py:236-534).
package sankey

import (
	"context"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func combineScope(sqlA string, bindingsA []dhclickhouse.Binding, sqlB string, bindingsB []dhclickhouse.Binding) (string, []dhclickhouse.Binding) {
	return sqlA + sqlB, append(append([]dhclickhouse.Binding{}, bindingsA...), bindingsB...)
}

// buildInvestmentFlow ports _build_investment_flow (services/sankey.py:
// 236-297).
func buildInvestmentFlow(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeLevel string, scopeIDs, whatRepos, workCategory []string, orgID string, asOf time.Time) ([]Node, []Link, error) {
	if !tablesPresent(ctx, client, []string{"work_unit_investments"}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "work_unit_investments", []string{
		"theme_distribution_json", "effort_value", "from_ts", "to_ts", "repo_id",
	}) {
		return nil, nil, nil
	}

	repoFilterSQL, repoBindings, err := repoScopeFilter(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID, "repo_id", asOf)
	if err != nil {
		return nil, nil, err
	}
	themes := categoryThemeFilters(workCategory)
	var categoryFilterSQL string
	var categoryBindings []dhclickhouse.Binding
	if len(themes) > 0 {
		categoryFilterSQL = " AND theme_kv.1 IN {themes:Array(String)}"
		categoryBindings = []dhclickhouse.Binding{{Name: "themes", Value: themes}}
	}
	scopeFilterSQL, scopeBindings := combineScope(repoFilterSQL, repoBindings, categoryFilterSQL, categoryBindings)

	rows, err := fetchInvestmentFlowItems(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, maxInvestmentItems, orgID)
	if err != nil {
		return nil, nil, err
	}

	nodes := newNodeAccumulator()
	edges := newEdgeAccumulator()
	for _, row := range rows {
		if row.Value <= 0 {
			continue
		}
		source := normalizeLabel(row.Source, true, "Unassigned")
		target := normalizeLabel(row.Target, true, "Other")
		nodes.touchNode(source, strPtr("initiative"))
		nodes.touchNode(target, strPtr("project"))
		edges.addEdge(source, target, row.Value)
	}
	return nodes.nodes(), edges.links(), nil
}

// buildExpenseFlow ports _build_expense_flow (services/sankey.py:300-384).
func buildExpenseFlow(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]Node, []Link, error) {
	if !tablesPresent(ctx, client, []string{"work_item_metrics_daily", "work_item_cycle_times"}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "work_item_metrics_daily", []string{
		"day", "new_items_count", "new_bugs_count", "items_completed", "bug_completed_ratio", "team_id", "work_scope_id",
	}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "work_item_cycle_times", []string{"day", "status", "team_id", "work_scope_id"}) {
		return nil, nil, nil
	}

	const teamColumn = "ifNull(nullIf(team_id, ''), 'unassigned')"
	teamFilterSQL, teamBindings := teamScopeFilter(scopeLevel, scopeIDs, teamColumn)
	workFilterSQL, workBindings := workScopeFilter(scopeLevel, scopeIDs, whatRepos, "work_scope_id")
	scopeFilterSQL, scopeBindings := combineScope(teamFilterSQL, teamBindings, workFilterSQL, workBindings)

	rows, err := fetchExpenseCounts(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, nil
	}
	newBugs := rows[0].NewBugs
	bugCompleted := rows[0].BugCompletedEstimate

	canceledItems, err := fetchExpenseAbandoned(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, nil, err
	}

	unplanned := maxFloat(0, newBugs)
	rework := maxFloat(0, minFloat(unplanned, bugCompleted))
	abandoned := maxFloat(0, minFloat(rework, canceledItems))

	nodes := newNodeAccumulator()
	edges := newEdgeAccumulator()
	nodes.touchNode("Planned work", strPtr("planned"))
	nodes.touchNode("Unplanned work", strPtr("unplanned"))
	nodes.touchNode("Rework", strPtr("rework"))
	nodes.touchNode("Abandonment / rewrite", strPtr("abandonment"))
	edges.addEdge("Planned work", "Unplanned work", unplanned)
	edges.addEdge("Unplanned work", "Rework", rework)
	edges.addEdge("Rework", "Abandonment / rewrite", abandoned)
	return nodes.nodes(), edges.links(), nil
}

// stateLabels ports the status_labels dict (services/sankey.py:432-441).
var stateLabels = map[string]string{
	"backlog":     "Backlog",
	"todo":        "Todo",
	"in_progress": "In Progress",
	"in_review":   "In Review",
	"blocked":     "Blocked",
	"done":        "Done",
	"canceled":    "Canceled",
	"unknown":     "Unknown",
}

// stateLabel ports _label (services/sankey.py:443-444): a status_labels
// hit, else Title-Cased with underscores turned to spaces.
func stateLabel(status string) string {
	if label, ok := stateLabels[status]; ok {
		return label
	}
	words := strings.Split(strings.ReplaceAll(status, "_", " "), " ")
	for i, w := range words {
		if w == "" {
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return strings.Join(words, " ")
}

// buildStateFlow ports _build_state_flow (services/sankey.py:387-473).
func buildStateFlow(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]Node, []Link, error) {
	if !tablesPresent(ctx, client, []string{"work_item_state_durations_daily"}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "work_item_state_durations_daily", []string{
		"day", "status", "items_touched", "team_id", "work_scope_id",
	}) {
		return nil, nil, nil
	}

	const teamColumn = "ifNull(nullIf(team_id, ''), 'unassigned')"
	teamFilterSQL, teamBindings := teamScopeFilter(scopeLevel, scopeIDs, teamColumn)
	workFilterSQL, workBindings := workScopeFilter(scopeLevel, scopeIDs, whatRepos, "work_scope_id")
	scopeFilterSQL, scopeBindings := combineScope(teamFilterSQL, teamBindings, workFilterSQL, workBindings)

	rows, err := fetchStateStatusCounts(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, nil, err
	}

	var statusOrder []string
	statusCounts := map[string]float64{}
	for _, row := range rows {
		if row.ItemsTouched <= 0 {
			continue
		}
		statusRaw := strings.ToLower(normalizeLabel(row.Status, true, "unknown"))
		if _, ok := statusCounts[statusRaw]; !ok {
			statusOrder = append(statusOrder, statusRaw)
		}
		statusCounts[statusRaw] += row.ItemsTouched
	}

	backlog := statusCounts["backlog"]
	todo := statusCounts["todo"]
	inProgress := statusCounts["in_progress"]
	inReview := statusCounts["in_review"]
	done := statusCounts["done"]
	blocked := statusCounts["blocked"]
	canceled := statusCounts["canceled"]

	flowBacklog := minFloat(backlog, todo)
	flowTodo := minFloat(todo, inProgress)
	blockedFlow := minFloat(inProgress, blocked)
	remaining := maxFloat(0, inProgress-blockedFlow)
	reviewFlow := minFloat(remaining, inReview)
	remaining = maxFloat(0, remaining-reviewFlow)
	canceledFlow := minFloat(remaining, canceled)
	doneFlow := minFloat(reviewFlow, done)

	nodes := newNodeAccumulator()
	edges := newEdgeAccumulator()
	for _, status := range statusOrder {
		nodes.touchNode(stateLabel(status), strPtr("state"))
	}
	edges.addEdge(stateLabel("backlog"), stateLabel("todo"), flowBacklog)
	edges.addEdge(stateLabel("todo"), stateLabel("in_progress"), flowTodo)
	edges.addEdge(stateLabel("in_progress"), stateLabel("blocked"), blockedFlow)
	edges.addEdge(stateLabel("in_progress"), stateLabel("in_review"), reviewFlow)
	edges.addEdge(stateLabel("in_progress"), stateLabel("canceled"), canceledFlow)
	edges.addEdge(stateLabel("in_review"), stateLabel("done"), doneFlow)
	return nodes.nodes(), edges.links(), nil
}

// buildHotspotFlow ports _build_hotspot_flow (services/sankey.py:476-534).
func buildHotspotFlow(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeLevel string, scopeIDs, whatRepos []string, orgID string, asOf time.Time) ([]Node, []Link, error) {
	if !tablesPresent(ctx, client, []string{"file_metrics_daily", "repos"}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "file_metrics_daily", []string{"repo_id", "day", "path", "churn"}) {
		return nil, nil, nil
	}
	if !columnsPresent(ctx, client, "repos", []string{"id", "repo"}) {
		return nil, nil, nil
	}

	scopeFilterSQL, scopeBindings, err := repoScopeFilter(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID, "metrics.repo_id", asOf)
	if err != nil {
		return nil, nil, err
	}

	rows, err := fetchHotspotRows(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, maxHotspotRows, orgID)
	if err != nil {
		return nil, nil, err
	}

	nodes := newNodeAccumulator()
	edges := newEdgeAccumulator()
	for _, row := range rows {
		if row.Churn <= 0 {
			continue
		}
		repo := normalizeLabel(row.Repo, true, "Unknown repo")
		directory := normalizeLabel(row.Directory, true, "(root)")
		filePath := normalizeLabel(row.FilePath, true, "unknown file")
		changeType := normalizeLabel(row.ChangeType, true, "feature")

		directoryLabel := repo + " / " + directory
		fileLabel := repo + " / " + filePath

		nodes.touchNode(repo, strPtr("repo"))
		nodes.touchNode(directoryLabel, strPtr("directory"))
		nodes.touchNode(fileLabel, strPtr("file"))
		nodes.touchNode(changeType, strPtr("change_type"))

		edges.addEdge(repo, directoryLabel, row.Churn)
		edges.addEdge(directoryLabel, fileLabel, row.Churn)
		edges.addEdge(fileLabel, changeType, row.Churn)
	}
	return nodes.nodes(), edges.links(), nil
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
