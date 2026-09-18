package goapiproof

import "testing"

// This file exercises the ACTUAL corpus Options vars this ticket adds
// (restcorpus.go), not the underlying shapes in isolation (teamreposubset_
// test.go/dictkeydirection_test.go already do that) -- proving each
// route's own field-name/leaf choice (EqualLeaves vs BoundedLeavesAllKeys,
// which Paths, which KeyFields) is wired to the actual live Options an
// admissible team-scoped request carries. Every test here isolates one
// declaration: disabling ITS OWN Equal/Bounded choice in restcorpus.go
// (swapping EqualLeaves for BoundedLeavesAllKeys or vice versa, or
// dropping AdmitBaselineOnlyKeys) turns the matching test red.

// --- heatmap ---------------------------------------------------------

// TestHeatmapRepoTouchpointsTeamScopedParity_NarrowerTeamAdmitted pins
// heatmapRepoTouchpointsTeamScopeSubsetDefect's own EqualLeaves choice: a
// team-scoped candidate missing one repo's own (x, y) bucket entirely,
// with every KEPT bucket's value UNCHANGED (repo_touchpoints' own GROUP
// BY already confines a bucket to one repo), is a bounded subset.
func TestHeatmapRepoTouchpointsTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-08-01", "full-chaos/ops", 12)+","+heatmapCell("2026-08-01", "full-chaos/other-team-repo", 40)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-08-01", "full-chaos/ops", 12)))

	result := Compare(baseline, candidate, heatmapRepoTouchpointsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a kept repo's own unchanged bucket plus a whole missing non-team repo bucket is a bounded subset: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// NOTE: a "kept bucket's value must be EQUAL, not bounded" negative test
// does not distinguish anything observable for repo_touchpoints/
// hotspot_risk: heatmapDedupParity's own sibling KeyedDirectionShape
// already admits ANY baseline>candidate value difference at a matched
// (x, y) key, mechanism-agnostically, regardless of this ticket's own
// EqualLeaves choice -- the SAME overlap
// investmentTeamScopeDictSubsetDefects' own doc comment (restcorpus.go)
// already documents for the dict shapes. EqualLeaves is still the
// CORRECT, precise claim (repo_touchpoints/hotspot_risk's own GROUP BY
// already confines a bucket to one repo, so nothing should ever need
// bounding there), but its own value coverage is redundant with that
// sibling in practice; this ticket's own new, non-redundant contribution
// for these two metrics is the MEMBERSHIP admission the positive test
// above pins.

// TestHeatmapHotspotRiskTeamScopedParity_NarrowerTeamAdmitted is
// heatmapHotspotRiskTeamScopeSubsetDefect's own positive twin.
func TestHeatmapHotspotRiskTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-08-01", "full-chaos/ops:file.go", 30)+","+heatmapCell("2026-08-01", "full-chaos/other-team-repo:file.go", 90)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-08-01", "full-chaos/ops:file.go", 30)))

	result := Compare(baseline, candidate, heatmapHotspotRiskTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHeatmapReviewWaitDensityTeamScopedParity_NarrowerTeamShrinksKeptValue
// pins heatmapReviewWaitDensityTeamScopeSubsetDefect's own
// BoundedLeavesAllKeys choice: review_wait_density's bucket sums across
// EVERY repo sharing an (hour, weekday), so a matched key's own value
// GENUINELY SHRINKS (never merely disappears) under a narrower team
// scope -- the opposite shape from repo_touchpoints/hotspot_risk above.
func TestHeatmapReviewWaitDensityTeamScopedParity_NarrowerTeamShrinksKeptValue(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("09", "Mon", 12.5)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("09", "Mon", 4.0)))

	result := Compare(baseline, candidate, heatmapReviewWaitDensityTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a shared (hour, weekday) bucket's own value shrinking under a narrower team scope must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHeatmapReviewWaitDensityTeamScopedParity_GrownKeptValueStaysOutside
// is the negative twin: candidate > baseline at a matched bucket must
// never be admitted, in either direction.
func TestHeatmapReviewWaitDensityTeamScopedParity_GrownKeptValueStaysOutside(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("09", "Mon", 4.0)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("09", "Mon", 12.5)))

	result := Compare(baseline, candidate, heatmapReviewWaitDensityTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// --- sankey (plain GET/POST /api/v1/sankey) ---------------------------

func sankeyTeamScopeBody(nodes, links string) string {
	return `{"data":{"mode":"investment","nodes":[` + nodes + `],"links":[` + links + `],"unit":null,"label":null,"description":null,"team_coverage":null,"repo_coverage":null,"distinct_team_targets":null,"distinct_repo_targets":null}}`
}

func sankeyTeamScopeNode(name, group string) string {
	return `{"name":"` + name + `","group":"` + group + `","value":null}`
}

func sankeyTeamScopeLink(source, target string, value float64) string {
	return `{"source":"` + source + `","target":"` + target + `","value":` + jsonFloat(value) + `}`
}

// TestSankeyInvestmentTeamScopedParity_NarrowerTeamAdmitted pins
// sankeyNodesTeamScopeSubsetDefect/sankeyLinksTeamScopeSubsetDefect: a
// team-scoped candidate missing the whole non-team-repo node/edge, with
// the kept (theme, repo) edge's own value UNCHANGED (investment mode's
// own GROUP BY source, target already confines an edge to one repo), is
// a bounded subset.
func TestSankeyInvestmentTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("Feature Delivery", "initiative")+","+sankeyTeamScopeNode("full-chaos/ops", "project")+","+sankeyTeamScopeNode("full-chaos/other-team-repo", "project"),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500)+","+sankeyTeamScopeLink("Feature Delivery", "full-chaos/other-team-repo", 900),
	))
	candidate := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("Feature Delivery", "initiative")+","+sankeyTeamScopeNode("full-chaos/ops", "project"),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500),
	))

	result := Compare(baseline, candidate, sankeyInvestmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestSankeyInvestmentTeamScopedParity_ShrunkenKeptLinkValueStaysOutside
// pins the EqualLeaves choice: a KEPT (source, target) edge's own value
// must stay EXACTLY equal, since it is already confined to one repo by
// the query's own GROUP BY -- a shrink there must not be silently
// admitted as if it were a multi-repo sum.
func TestSankeyInvestmentTeamScopedParity_ShrunkenKeptLinkValueStaysOutside(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("Feature Delivery", "initiative")+","+sankeyTeamScopeNode("full-chaos/ops", "project"),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500),
	))
	candidate := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("Feature Delivery", "initiative")+","+sankeyTeamScopeNode("full-chaos/ops", "project"),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 200),
	))

	result := Compare(baseline, candidate, sankeyInvestmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a single-repo edge's own value must be EQUAL, not merely bounded: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestSankeyHotspotTeamScopedParity_NarrowerTeamAdmitted is
// sankeyNodesTeamScopeSubsetDefect/sankeyLinksTeamScopeSubsetDefect's own
// positive proof under hotspot mode's Parity specifically (a DIFFERENT
// Options value, sankeyHotspotTeamScopedParity, than the investment-mode
// test above).
func TestSankeyHotspotTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("full-chaos/ops", "repo")+","+sankeyTeamScopeNode("full-chaos/other-team-repo", "repo"),
		sankeyTeamScopeLink("full-chaos/ops", "full-chaos/ops / src", 50)+","+sankeyTeamScopeLink("full-chaos/other-team-repo", "full-chaos/other-team-repo / src", 80),
	))
	candidate := snapshotFromJSON(t, sankeyTeamScopeBody(
		sankeyTeamScopeNode("full-chaos/ops", "repo"),
		sankeyTeamScopeLink("full-chaos/ops", "full-chaos/ops / src", 50),
	))

	result := Compare(baseline, candidate, sankeyHotspotTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// --- investment (dict shapes) -----------------------------------------

// TestInvestmentTeamScopedParity_ThemeEntirelyMissingFromTeamAdmitted
// pins investmentTeamScopeDictSubsetDefects' own AdmitBaselineOnlyKeys
// opt-in: a theme with zero effort inside the team's own repositories is
// ABSENT from the candidate map entirely (never merely smaller) --
// BuildResponse's own `row.Value > 0` guard -- and must be admitted.
func TestInvestmentTeamScopedParity_ThemeEntirelyMissingFromTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"theme_distribution":{"feature_delivery":1200.0,"maintenance":300.0}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"theme_distribution":{"feature_delivery":1200.0}}}`)

	result := Compare(baseline, candidate, investmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a theme entirely missing from the team-scoped candidate must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestInvestmentTeamScopedParity_CandidateOnlyThemeNeverAdmitted pins
// the opt-in's own refusal direction, wired through the LIVE corpus
// Options rather than a hand-built shape (dictkeydirection_test.go
// already pins the shape itself): a theme present ONLY in the candidate
// contradicts the subset claim and must stay outside.
func TestInvestmentTeamScopedParity_CandidateOnlyThemeNeverAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"theme_distribution":{"feature_delivery":1200.0}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"theme_distribution":{"feature_delivery":1200.0,"maintenance":300.0}}}`)

	result := Compare(baseline, candidate, investmentTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// --- investment/sunburst ------------------------------------------------

// TestInvestmentSunburstTeamScopedParity_NarrowerTeamAdmitted pins
// investmentSunburstTeamScopeSubsetDefect: a slice for a repository
// outside the team is entirely missing, and the KEPT slice's own value
// is unchanged (scope = one specific repo per key already).
func TestInvestmentSunburstTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":[{"theme":"feature_delivery","subcategory":"feature_delivery.enablement","scope":"full-chaos/ops","value":500.0},{"theme":"feature_delivery","subcategory":"feature_delivery.enablement","scope":"full-chaos/other-team-repo","value":900.0}]}`)
	candidate := snapshotFromJSON(t, `{"data":[{"theme":"feature_delivery","subcategory":"feature_delivery.enablement","scope":"full-chaos/ops","value":500.0}]}`)

	result := Compare(baseline, candidate, investmentSunburstTeamScopedParityWithLimit(500))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// --- investment/flow ----------------------------------------------------

func investmentFlowTeamScopeBody(nodes, links string) string {
	return `{"data":{"mode":"investment","nodes":[` + nodes + `],"links":[` + links + `],"unit":null,"label":null,"description":null,"team_coverage":null,"repo_coverage":null,"distinct_team_targets":null,"distinct_repo_targets":null}}`
}

// investmentFlowTeamScopeNode carries a REAL numeric value -- unlike
// plain GET/POST /api/v1/sankey (sankeyTeamScopeNode, always null),
// investment/flow's own node builders populate a real running total
// (nodeRunningTotal.add, investmentflow/builders.go).
func investmentFlowTeamScopeNode(name, group string, value float64) string {
	return `{"name":"` + name + `","group":"` + group + `","value":` + jsonFloat(value) + `}`
}

// TestInvestmentFlowDynamicTeamScopedParity_CategoryNodeValueShrinksAdmitted
// pins investmentFlowNodesTeamScopeSubsetDefect's own BoundedLeavesAllKeys
// choice: buildDynamicFlowSankey's own "subcategory" source node
// accumulates a RUNNING SUM across every repo its own edges reach
// (nodeRunningTotal.add), so a narrower team scope genuinely SHRINKS a
// kept category node's own value (never merely removes it) -- the
// opposite of a single-repo leaf node.
func TestInvestmentFlowDynamicTeamScopedParity_CategoryNodeValueShrinksAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("Feature Delivery", "subcategory", 1400)+","+investmentFlowTeamScopeNode("full-chaos/ops", "repo", 500)+","+investmentFlowTeamScopeNode("full-chaos/other-team-repo", "repo", 900),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500)+","+sankeyTeamScopeLink("Feature Delivery", "full-chaos/other-team-repo", 900),
	))
	// team scope narrows to full-chaos/ops alone: the category node's own
	// running sum shrinks to just that repo's own contribution (1400 ->
	// 500), and the other repo's own leaf node/edge disappears entirely.
	candidate := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("Feature Delivery", "subcategory", 500)+","+investmentFlowTeamScopeNode("full-chaos/ops", "repo", 500),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500),
	))

	result := Compare(baseline, candidate, investmentFlowDynamicTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestInvestmentFlowDynamicTeamScopedParity_GrownNodeValueStaysOutside is
// the negative twin: a node's own value reading candidate > baseline
// must never be admitted, in either direction.
func TestInvestmentFlowDynamicTeamScopedParity_GrownNodeValueStaysOutside(t *testing.T) {
	baseline := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("Feature Delivery", "subcategory", 500)+","+investmentFlowTeamScopeNode("full-chaos/ops", "repo", 500),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500),
	))
	candidate := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("Feature Delivery", "subcategory", 900)+","+investmentFlowTeamScopeNode("full-chaos/ops", "repo", 900),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 900),
	))

	result := Compare(baseline, candidate, investmentFlowDynamicTeamScopedParity)
	// Both the (Feature Delivery, full-chaos/ops) link and BOTH nodes grow
	// candidate > baseline in this fixture -- three findings, none
	// admitted in either direction.
	if result.DifferencesOutsideBaselineDefect != 3 {
		t.Fatalf("outside = %d, want 3: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestInvestmentFlowRepoTeamTeamScopedParity_NarrowerTeamAdmitted proves
// the SAME two declarations reused on POST /api/v1/investment/flow/
// repo-team's OWN Options value (investmentFlowRepoTeamTeamScopedParity),
// not just investment/flow's.
func TestInvestmentFlowRepoTeamTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("full-chaos/ops", "repo", 500)+","+investmentFlowTeamScopeNode("full-chaos/other-team-repo", "repo", 900),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500)+","+sankeyTeamScopeLink("Feature Delivery", "full-chaos/other-team-repo", 900),
	))
	candidate := snapshotFromJSON(t, investmentFlowTeamScopeBody(
		investmentFlowTeamScopeNode("full-chaos/ops", "repo", 500),
		sankeyTeamScopeLink("Feature Delivery", "full-chaos/ops", 500),
	))

	result := Compare(baseline, candidate, investmentFlowRepoTeamTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// --- drilldown/prs ------------------------------------------------------

func drilldownPRsTeamScopeItem(repoID string, number int, title, author string, latencyHours int) string {
	return `{"repo_id":"` + repoID + `","number":` + jsonFloat(float64(number)) + `,"title":"` + title + `","author_name":"` + author + `","created_at":"2026-08-15T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":` + jsonFloat(float64(latencyHours)) + `}`
}

func drilldownPRsTeamScopeBody(items string) string {
	return `{"data":{"items":[` + items + `]}}`
}

// TestDrilldownPRsTeamScopedParity_NarrowerTeamAdmitted pins
// drilldownPRsTeamScopeSubsetDefect: a team-scoped candidate missing an
// entire PR from a non-team repository, with the kept PR's own fields
// UNCHANGED (every field besides the (repo_id, number) key is a per-PR
// property, never resummed), is a bounded subset.
func TestDrilldownPRsTeamScopedParity_NarrowerTeamAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, drilldownPRsTeamScopeBody(
		drilldownPRsTeamScopeItem("11111111-1111-1111-1111-111111111111", 42, "Fix bug", "alice", 3)+","+
			drilldownPRsTeamScopeItem("22222222-2222-2222-2222-222222222222", 7, "Other team's PR", "bob", 5)))
	candidate := snapshotFromJSON(t, drilldownPRsTeamScopeBody(
		drilldownPRsTeamScopeItem("11111111-1111-1111-1111-111111111111", 42, "Fix bug", "alice", 3)))

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestDrilldownPRsTeamScopedParity_EqualPopulationIsACleanMatch pins the
// case this ticket actually verified in production: the team owns every
// repository with a pull request in the bound window, so baseline and
// candidate carry the IDENTICAL list -- a clean match with nothing for
// this shape to admit at all, never a narrowing.
func TestDrilldownPRsTeamScopedParity_EqualPopulationIsACleanMatch(t *testing.T) {
	body := drilldownPRsTeamScopeBody(
		drilldownPRsTeamScopeItem("11111111-1111-1111-1111-111111111111", 42, "Fix bug", "alice", 3) + "," +
			drilldownPRsTeamScopeItem("22222222-2222-2222-2222-222222222222", 7, "Team PR too", "bob", 5))
	baseline := snapshotFromJSON(t, body)
	candidate := snapshotFromJSON(t, body)

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestDrilldownPRsTeamScopedParity_ChangedKeptFieldStaysOutside pins the
// EqualLeaves choice: every field besides the key is a per-PR property,
// never resummed across a repository set, so a matched PR's own title
// changing must NOT be silently admitted.
func TestDrilldownPRsTeamScopedParity_ChangedKeptFieldStaysOutside(t *testing.T) {
	baseline := snapshotFromJSON(t, drilldownPRsTeamScopeBody(
		drilldownPRsTeamScopeItem("11111111-1111-1111-1111-111111111111", 42, "Fix bug", "alice", 3)))
	candidate := snapshotFromJSON(t, drilldownPRsTeamScopeBody(
		drilldownPRsTeamScopeItem("11111111-1111-1111-1111-111111111111", 42, "A different title", "alice", 3)))

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
