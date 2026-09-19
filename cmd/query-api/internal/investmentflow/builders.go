// Node/link assembly -- ports api/services/investment_flow.py's
// _get_repo_rollup_map, _build_team_burden_sankey, _build_team_theme_
// subcategory_repo_sankey, and the two builders' own inline node/link
// loops in build_investment_flow_response's dynamic-mode fallback and
// build_investment_repo_team_flow_response.
//
// ORDERING: every accumulator below is insertion-ordered (a Go slice +
// map pair standing in for Python's dict, which has preserved insertion
// order since 3.7) because two DIFFERENT Python builders rely on that
// order in two DIFFERENT ways this file must not conflate:
//   - _build_team_burden_sankey/_build_team_theme_subcategory_repo_sankey
//     accumulate link_totals keyed by (source, target), THEN iterate that
//     dict in first-insertion order to build the links list (no sort by
//     value) and compute each node's value as max(incoming, outgoing)
//     AFTER every link is known.
//   - the dynamic-mode fallback appends ONE SankeyLink per qualifying
//     row directly (its rows already arrive grouped by (source, target)
//     from the SQL's own GROUP BY, so no further accumulation is needed),
//     and a node's value is the running SUM of every value that touched
//     it, updated as each row is processed, not a final max(incoming,
//     outgoing) pass.
//   - the repo-team builder keeps that running-sum node shape, but its
//     rows are grouped by (subcategory, repo, team), one level finer than
//     either link it draws: several rows reach the same subcategory->repo
//     or repo->team pair. It accumulates its links through the SAME
//     orderedEdges accumulator the link_totals builders use, so the
//     response carries one link per (source, target), valued at the sum
//     of every row that reaches it, in first-insertion order.
//
// Reusing cmd/query-api/internal/sankey's own nodeAccumulator/
// edgeAccumulator (response.go) would silently apply THAT package's own
// rules (max-of-incoming/outgoing node values, links sorted descending by
// value) to code paths whose Python source does neither -- so this
// package keeps its own, deliberately different, accumulator shapes.
package investmentflow

import (
	"math"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/sankey"
)

const (
	unassignedTeam = "unassigned"
	unassignedRepo = "unassigned"
	// unassignedTargetLabel is the literal "unassigned" edgeStats compares
	// a row's target against -- team_rows' target and repo_rows' target
	// share the SAME literal fallback value (UNASSIGNED_TEAM ==
	// UNASSIGNED_REPO == "unassigned" in Python too), so one constant
	// serves both without implying either row shape's own semantics.
	unassignedTargetLabel = "unassigned"
	unassignedTeamLabel   = "Unassigned team"
	unassignedRepoLabel   = "Unassigned repo"
	otherReposLabel       = "Other repos"
)

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }
func intPtr(i int) *int           { return &i }

// safeFloat mirrors safe_float's non-finite guard (api/utils/numeric.py:
// 22-36) for a value that has already been scanned as a Go float64 --
// there is no TypeError/ValueError branch to reproduce since ClickHouse's
// Float64 column always decodes to a float64, only the isfinite check
// carries over.
func safeFloat(v float64) float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return v
}

// --- ordered accumulators ------------------------------------------------

// orderedEdgeKey is the Go analogue of link_totals' `dict[tuple[str,str],
// float]` key.
type orderedEdgeKey struct{ Source, Target string }

// orderedEdges accumulates a value per (source, target) pair, insertion
// order preserved -- backs _build_team_burden_sankey/_build_team_theme_
// subcategory_repo_sankey's own link_totals dict, and buildRepoTeamSankey's
// links.
type orderedEdges struct {
	order  []orderedEdgeKey
	values map[orderedEdgeKey]float64
}

func newOrderedEdges() *orderedEdges {
	return &orderedEdges{values: map[orderedEdgeKey]float64{}}
}

func (e *orderedEdges) add(source, target string, value float64) {
	key := orderedEdgeKey{source, target}
	if _, ok := e.values[key]; !ok {
		e.order = append(e.order, key)
	}
	e.values[key] += value
}

// links returns one sankey.Link per accumulated (source, target) pair, in
// first-insertion order, dropping a non-positive total (defensive: every
// caller adds only value > 0).
func (e *orderedEdges) links() []sankey.Link {
	out := make([]sankey.Link, 0, len(e.order))
	for _, key := range e.order {
		value := e.values[key]
		if value <= 0 {
			continue
		}
		out = append(out, sankey.Link{Source: key.Source, Target: key.Target, Value: value})
	}
	return out
}

// nodePresence tracks first-touch node name/group, insertion order
// preserved, with NO value -- callers of this accumulator compute each
// node's value in a separate pass (max(incoming, outgoing) after every
// link is known), matching _build_team_burden_sankey/_build_team_theme_
// subcategory_repo_sankey's own two-pass shape (add_node, then a final
// loop that overwrites node.value).
type nodePresence struct {
	order []string
	group map[string]string
}

func newNodePresence() *nodePresence {
	return &nodePresence{group: map[string]string{}}
}

func (p *nodePresence) add(name, group string) {
	if _, ok := p.group[name]; ok {
		return
	}
	p.group[name] = group
	p.order = append(p.order, name)
}

// nodeRunningTotal tracks first-touch node name/group AND a value that
// accumulates as each row is processed -- backs the dynamic-mode fallback
// and the repo-team builder's own single-pass add_node()+`node.value +=
// value` shape (different from nodePresence's two-pass max(incoming,
// outgoing), see this file's own package doc comment).
type nodeRunningTotal struct {
	order  []string
	byName map[string]*sankey.Node
}

func newNodeRunningTotal() *nodeRunningTotal {
	return &nodeRunningTotal{byName: map[string]*sankey.Node{}}
}

func (a *nodeRunningTotal) add(name, group string, value float64) {
	n, ok := a.byName[name]
	if !ok {
		n = &sankey.Node{Name: name, Group: strPtr(group), Value: floatPtr(0)}
		a.byName[name] = n
		a.order = append(a.order, name)
	}
	*n.Value += value
}

func (a *nodeRunningTotal) nodes() []sankey.Node {
	out := make([]sankey.Node, 0, len(a.order))
	for _, name := range a.order {
		out = append(out, *a.byName[name])
	}
	return out
}

// --- _get_repo_rollup_map (api/services/investment_flow.py:44-67) -------

type repoValuePair struct {
	Repo  string
	Value float64
}

// repoRollupMap ports _get_repo_rollup_map. Python's sorted(..., reverse=
// True) is a STABLE sort, so ties keep repo_totals' own insertion order
// (first-seen order in rows) -- sort.SliceStable with that same
// first-seen ordering as input order reproduces it exactly.
func repoRollupMap(pairs []repoValuePair, topNRepos int) map[string]string {
	order := make([]string, 0, len(pairs))
	totals := map[string]float64{}
	for _, p := range pairs {
		repo := p.Repo
		if repo == "" {
			repo = unassignedRepo
		}
		if repo == unassignedRepo {
			continue
		}
		value := safeFloat(p.Value)
		if value <= 0 {
			continue
		}
		if _, ok := totals[repo]; !ok {
			order = append(order, repo)
		}
		totals[repo] += value
	}

	sorted := append([]string(nil), order...)
	sort.SliceStable(sorted, func(i, j int) bool { return totals[sorted[i]] > totals[sorted[j]] })

	n := topNRepos
	if n < 1 {
		n = 1
	}
	if n > len(sorted) {
		n = len(sorted)
	}
	topRepos := map[string]bool{}
	for i := 0; i < n; i++ {
		topRepos[sorted[i]] = true
	}

	rollup := make(map[string]string, len(order))
	for _, repo := range order {
		if topRepos[repo] {
			rollup[repo] = repo
		} else {
			rollup[repo] = otherReposLabel
		}
	}
	return rollup
}

// --- _build_team_burden_sankey (api/services/investment_flow.py:70-131) -

// teamBurdenRow is the common (team, category-or-subcategory, repo,
// value) shape both fetch_investment_team_category_repo_edges' and
// fetch_investment_team_subcategory_repo_edges' rows are converted to
// before this builder -- Python's own row dicts carry either a
// "category" or a "subcategory" key depending on the caller, read via
// row.get(category_key); this port always reads the Category field,
// with categoryGroup carrying the "category"-vs-"subcategory" node-group
// distinction category_key itself drove in Python.
type teamBurdenRow struct {
	Team     string
	Category string
	Repo     string
	Value    float64
}

func teamCategoryRepoRowsToBurden(rows []teamCategoryRepoEdgeRow) []teamBurdenRow {
	out := make([]teamBurdenRow, len(rows))
	for i, r := range rows {
		out[i] = teamBurdenRow{Team: r.Team, Category: r.Category, Repo: r.Repo, Value: r.Value}
	}
	return out
}

func teamSubcategoryRepoRowsToBurden(rows []teamSubcategoryRepoEdgeRow) []teamBurdenRow {
	out := make([]teamBurdenRow, len(rows))
	for i, r := range rows {
		out[i] = teamBurdenRow{Team: r.Team, Category: r.Subcategory, Repo: r.Repo, Value: r.Value}
	}
	return out
}

func teamBurdenRepoValues(rows []teamBurdenRow) []repoValuePair {
	out := make([]repoValuePair, len(rows))
	for i, r := range rows {
		out[i] = repoValuePair{Repo: r.Repo, Value: r.Value}
	}
	return out
}

// buildTeamBurdenSankey ports _build_team_burden_sankey. categoryGroup is
// the node group category_label_fn's own rows are tagged with -- "category"
// when the caller's category_key was "category" (team_category_repo),
// "subcategory" otherwise (the team_subcategory_repo else-branch).
func buildTeamBurdenSankey(rows []teamBurdenRow, categoryGroup string, categoryLabelFn func(string) string, topNRepos int) ([]sankey.Node, []sankey.Link) {
	presence := newNodePresence()
	edges := newOrderedEdges()
	rollup := repoRollupMap(teamBurdenRepoValues(rows), topNRepos)

	for _, row := range rows {
		teamRaw := row.Team
		if teamRaw == "" {
			teamRaw = unassignedTeam
		}
		teamLabel := teamRaw
		if teamRaw == unassignedTeam {
			teamLabel = unassignedTeamLabel
		}

		categoryRaw := row.Category
		repoRaw := row.Repo
		if repoRaw == "" {
			repoRaw = unassignedRepo
		}
		value := safeFloat(row.Value)
		if categoryRaw == "" || value <= 0 {
			continue
		}

		var repoLabel string
		switch {
		case repoRaw == unassignedRepo:
			repoLabel = unassignedRepoLabel
		default:
			if mapped, ok := rollup[repoRaw]; ok {
				repoLabel = mapped
			} else {
				repoLabel = repoRaw
			}
		}
		categoryLabel := categoryLabelFn(categoryRaw)

		presence.add(teamLabel, "team")
		presence.add(categoryLabel, categoryGroup)
		presence.add(repoLabel, "repo")

		edges.add(teamLabel, categoryLabel, value)
		edges.add(categoryLabel, repoLabel, value)
	}

	return finishPresenceEdges(presence, edges)
}

// finishPresenceEdges is the shared second pass _build_team_burden_
// sankey/_build_team_theme_subcategory_repo_sankey both run after their
// own accumulation loop: build the links list in link_totals' own
// insertion order (dropping any non-positive total, defensive -- every
// accumulated value here is already a sum of value > 0 additions), sum
// incoming/outgoing per node name from that SAME links list, then set
// every node's value to max(incoming, outgoing).
func finishPresenceEdges(presence *nodePresence, edges *orderedEdges) ([]sankey.Node, []sankey.Link) {
	incoming := map[string]float64{}
	outgoing := map[string]float64{}
	links := edges.links()
	for _, link := range links {
		outgoing[link.Source] = outgoing[link.Source] + link.Value
		incoming[link.Target] = incoming[link.Target] + link.Value
	}

	nodes := make([]sankey.Node, 0, len(presence.order))
	for _, name := range presence.order {
		value := math.Max(incoming[name], outgoing[name])
		nodes = append(nodes, sankey.Node{Name: name, Group: strPtr(presence.group[name]), Value: floatPtr(value)})
	}
	return nodes, links
}

// --- _build_team_theme_subcategory_repo_sankey (investment_flow.py:
// 134-197) --------------------------------------------------------------

// buildTeamThemeSubcategoryRepoSankey ports _build_team_theme_
// subcategory_repo_sankey: the 4-level team -> theme -> subcategory ->
// repo flow, parsing each row's "theme.subcategory" key itself (unlike
// buildTeamBurdenSankey, which takes an already-resolved category).
func buildTeamThemeSubcategoryRepoSankey(rows []teamSubcategoryRepoEdgeRow, topNRepos int) ([]sankey.Node, []sankey.Link) {
	presence := newNodePresence()
	edges := newOrderedEdges()
	rollup := repoRollupMap(teamBurdenRepoValues(teamSubcategoryRepoRowsToBurden(rows)), topNRepos)

	for _, row := range rows {
		teamRaw := row.Team
		if teamRaw == "" {
			teamRaw = unassignedTeam
		}
		teamLabel := teamRaw
		if teamRaw == unassignedTeam {
			teamLabel = unassignedTeamLabel
		}

		subcategoryRaw := row.Subcategory
		repoRaw := row.Repo
		if repoRaw == "" {
			repoRaw = unassignedRepo
		}
		value := safeFloat(row.Value)
		if subcategoryRaw == "" || value <= 0 {
			continue
		}

		var themeLabel, subcategoryLabel string
		if idx := strings.Index(subcategoryRaw, "."); idx >= 0 {
			themeKey, subKey := subcategoryRaw[:idx], subcategoryRaw[idx+1:]
			themeLabel = formatThemeLabel(themeKey)
			subcategoryLabel = titleCase(subKey)
		} else {
			themeLabel = titleCase(subcategoryRaw)
			subcategoryLabel = subcategoryRaw
		}

		var repoLabel string
		if repoRaw == unassignedRepo {
			repoLabel = unassignedRepoLabel
		} else if mapped, ok := rollup[repoRaw]; ok {
			repoLabel = mapped
		} else {
			repoLabel = repoRaw
		}

		presence.add(teamLabel, "team")
		presence.add(themeLabel, "category")
		presence.add(subcategoryLabel, "subcategory")
		presence.add(repoLabel, "repo")

		edges.add(teamLabel, themeLabel, value)
		edges.add(themeLabel, subcategoryLabel, value)
		edges.add(subcategoryLabel, repoLabel, value)
	}

	return finishPresenceEdges(presence, edges)
}

// --- dynamic-mode fallback (build_investment_flow_response's own inline
// loop, investment_flow.py:~430-470) -------------------------------------

// buildDynamicFlowSankey ports the per-row node/link loop
// build_investment_flow_response's dynamic (non-flow_mode) branch runs
// directly after choosing chosenMode ("team"/"repo_scope"/"fallback").
// Unlike buildTeamBurdenSankey/buildTeamThemeSubcategoryRepoSankey, rows
// already arrive pre-grouped by (source, target) from the SQL's own
// GROUP BY, so this appends ONE link per qualifying row with no further
// accumulation, and each node's value is a running sum updated as rows
// are processed (nodeRunningTotal), never a final max(incoming,
// outgoing) pass.
func buildDynamicFlowSankey(rows []edgeRow, chosenMode string) ([]sankey.Node, []sankey.Link) {
	nodes := newNodeRunningTotal()
	links := make([]sankey.Link, 0)
	for _, row := range rows {
		sourceKey := row.Source
		target := row.Target
		value := safeFloat(row.Value)
		if sourceKey == "" || target == "" || value <= 0 {
			continue
		}

		sourceLabel := formatSubcategoryLabel(sourceKey)
		nodes.add(sourceLabel, "subcategory", value)

		if chosenMode != "fallback" {
			targetGroup := "repo"
			if chosenMode == "team" {
				targetGroup = "team"
			}
			nodes.add(target, targetGroup, value)
			links = append(links, sankey.Link{Source: sourceLabel, Target: target, Value: value})
		}
	}
	return nodes.nodes(), links
}

// --- build_investment_repo_team_flow_response's own loop (investment_
// flow.py:~560-606) -------------------------------------------------------

// buildRepoTeamSankey ports build_investment_repo_team_flow_response's
// node/link loop directly (there is no separate top-level Python
// function for it, the same way buildDynamicFlowSankey above ports an
// inline loop rather than a named function). Same running-total node
// shape as buildDynamicFlowSankey. Links differ from the Python loop,
// which appends one link per row: a row is one (subcategory, repo, team)
// group, so that loop repeats a repo->team link once per subcategory and
// a subcategory->repo link once per team. Here every link is accumulated
// through orderedEdges, so each (source, target) pair appears once,
// valued at the sum of the rows that reach it, in first-insertion order.
// Node values are the same either way: each node already sums every row
// that touches it.
func buildRepoTeamSankey(rows []repoTeamEdgeRow) ([]sankey.Node, []sankey.Link) {
	nodes := newNodeRunningTotal()
	edges := newOrderedEdges()
	for _, row := range rows {
		subKey := row.Subcategory
		if subKey == "" {
			continue
		}
		value := safeFloat(row.Value)
		if value <= 0 {
			continue
		}
		sourceLabel := formatSubcategoryLabel(subKey)
		nodes.add(sourceLabel, "subcategory", value)

		repoLabel := row.Repo
		if repoLabel == "" {
			repoLabel = unassignedRepo
		}
		teamLabel := strings.TrimSpace(row.Team)
		if teamLabel == "" {
			teamLabel = unassignedTeam
		}

		if repoLabel == unassignedRepo && teamLabel != unassignedTeam {
			nodes.add(teamLabel, "team", value)
			edges.add(sourceLabel, teamLabel, value)
			continue
		}

		nodes.add(repoLabel, "repo", value)
		edges.add(sourceLabel, repoLabel, value)

		if teamLabel != unassignedTeam {
			nodes.add(teamLabel, "team", value)
			edges.add(repoLabel, teamLabel, value)
		}
	}
	return nodes.nodes(), edges.links()
}

// --- shared coverage-stats computation (investment_flow.py:322-341, the
// dynamic-mode fetch's own get_stats closure is separate, see
// edgeStats below) --------------------------------------------------------

// teamRepoValueRow is the common (team, repo, value) projection the
// flow_mode branch's own post-fetch stats block reads -- ignoring
// whichever third dimension (category/subcategory) that branch's row
// type also carries.
type teamRepoValueRow struct {
	Team  string
	Repo  string
	Value float64
}

func teamCategoryRepoRowsToStats(rows []teamCategoryRepoEdgeRow) []teamRepoValueRow {
	out := make([]teamRepoValueRow, len(rows))
	for i, r := range rows {
		out[i] = teamRepoValueRow{Team: r.Team, Repo: r.Repo, Value: r.Value}
	}
	return out
}

func teamSubcategoryRepoRowsToStats(rows []teamSubcategoryRepoEdgeRow) []teamRepoValueRow {
	out := make([]teamRepoValueRow, len(rows))
	for i, r := range rows {
		out[i] = teamRepoValueRow{Team: r.Team, Repo: r.Repo, Value: r.Value}
	}
	return out
}

// coverageStats ports build_investment_flow_response's flow_mode-branch
// post-fetch block (investment_flow.py:322-341): total_value/team_
// coverage/repo_coverage/distinct_team_targets/distinct_repo_targets, all
// computed straight from rows -- deliberately NOT the same statistic as
// buildTeamBurdenSankey's own node values, which is why this is a
// separate pass over the raw rows rather than a byproduct of building
// the sankey nodes.
func coverageStats(rows []teamRepoValueRow) (teamCoverage, repoCoverage float64, distinctTeamTargets, distinctRepoTargets int) {
	var totalValue, assignedTeamValue, assignedRepoValue float64
	teamSeen := map[string]bool{}
	repoSeen := map[string]bool{}
	for _, row := range rows {
		value := safeFloat(row.Value)
		totalValue += value
		team := row.Team
		if team == "" {
			team = unassignedTeam
		}
		if team != unassignedTeam {
			assignedTeamValue += value
			teamSeen[team] = true
		}
		repo := row.Repo
		if repo == "" {
			repo = unassignedRepo
		}
		if repo != unassignedRepo {
			assignedRepoValue += value
			repoSeen[repo] = true
		}
	}
	if totalValue > 0 {
		teamCoverage = assignedTeamValue / totalValue
		repoCoverage = assignedRepoValue / totalValue
	}
	return teamCoverage, repoCoverage, len(teamSeen), len(repoSeen)
}

// edgeStats ports the dynamic-mode fallback's own get_stats closure
// (investment_flow.py:~408-417): total_val is summed over EVERY row
// regardless of target, short-circuiting to (0, 0) when it is exactly
// zero -- distinct from coverageStats' own total, which is a genuinely
// different Python function (get_stats is a local closure, not a shared
// helper) even though the shapes look similar.
func edgeStats(rows []edgeRow) (coverage float64, distinctTargets int) {
	var totalVal float64
	for _, row := range rows {
		totalVal += safeFloat(row.Value)
	}
	if totalVal == 0 {
		return 0, 0
	}
	var assignedVal float64
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Target != unassignedTargetLabel {
			assignedVal += safeFloat(row.Value)
			seen[row.Target] = true
		}
	}
	return assignedVal / totalVal, len(seen)
}
