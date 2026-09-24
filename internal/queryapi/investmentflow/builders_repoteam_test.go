package investmentflow

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/sankey"
)

// perRowRepoTeamLinks is the reference plane's own link loop
// (build_investment_repo_team_flow_response): one link per qualifying row
// and link level, no accumulation, so a (source, target) pair that several
// rows reach appears once per row.
func perRowRepoTeamLinks(rows []repoTeamEdgeRow) []sankey.Link {
	links := make([]sankey.Link, 0)
	for _, row := range rows {
		value := safeFloat(row.Value)
		if row.Subcategory == "" || value <= 0 {
			continue
		}
		source := formatSubcategoryLabel(row.Subcategory)
		repo := row.Repo
		if repo == "" {
			repo = unassignedRepo
		}
		team := strings.TrimSpace(row.Team)
		if team == "" {
			team = unassignedTeam
		}
		if repo == unassignedRepo && team != unassignedTeam {
			links = append(links, sankey.Link{Source: source, Target: team, Value: value})
			continue
		}
		links = append(links, sankey.Link{Source: source, Target: repo, Value: value})
		if team != unassignedTeam {
			links = append(links, sankey.Link{Source: repo, Target: team, Value: value})
		}
	}
	return links
}

// perRowRepoTeamNodes is the reference plane's own node loop: a running
// total per node over every qualifying row that touches it.
func perRowRepoTeamNodes(rows []repoTeamEdgeRow) []sankey.Node {
	nodes := newNodeRunningTotal()
	for _, row := range rows {
		value := safeFloat(row.Value)
		if row.Subcategory == "" || value <= 0 {
			continue
		}
		nodes.add(formatSubcategoryLabel(row.Subcategory), "subcategory", value)
		repo := row.Repo
		if repo == "" {
			repo = unassignedRepo
		}
		team := strings.TrimSpace(row.Team)
		if team == "" {
			team = unassignedTeam
		}
		if repo == unassignedRepo && team != unassignedTeam {
			nodes.add(team, "team", value)
			continue
		}
		nodes.add(repo, "repo", value)
		if team != unassignedTeam {
			nodes.add(team, "team", value)
		}
	}
	return nodes.nodes()
}

// sumCopiesByEndpoints collapses links sharing (source, target) into one
// link valued at the sum of the copies, in list order, placed where the
// pair first appears -- the whole-key accounting the proof comparator's
// BaselineCopySumShape applies to the reference plane's body.
func sumCopiesByEndpoints(links []sankey.Link) []sankey.Link {
	type key struct{ source, target string }
	index := map[key]int{}
	out := make([]sankey.Link, 0, len(links))
	for _, link := range links {
		k := key{link.Source, link.Target}
		if i, ok := index[k]; ok {
			out[i].Value = out[i].Value + link.Value
			continue
		}
		index[k] = len(out)
		out = append(out, link)
	}
	return out
}

// TestRepoTeamSankeySumsLinksSharingEndpoints pins the captured
// production shape: one repository reached from several subcategories
// (its repo->team link repeats once per subcategory in the reference
// body) and one repository owned by two teams (its subcategory->repo link
// repeats once per team).
func TestRepoTeamSankeySumsLinksSharingEndpoints(t *testing.T) {
	rows := []repoTeamEdgeRow{
		{Subcategory: "feature_delivery.roadmap", Repo: "org/ask-dev", Team: "TeamA", Value: 21395.666688859463},
		{Subcategory: "feature_delivery.customer", Repo: "org/ask-dev", Team: "TeamA", Value: 2364},
		{Subcategory: "risk.security", Repo: "org/ask-dev", Team: "TeamA", Value: 50.33333483338356},
		{Subcategory: "maintenance.debt", Repo: "org/ops", Team: "TeamA", Value: 7},
		{Subcategory: "maintenance.debt", Repo: "org/ops", Team: "TeamB", Value: 5},
	}
	nodes, links := buildRepoTeamSankey(rows)

	want := []sankey.Link{
		{Source: "Feature Delivery · Roadmap", Target: "org/ask-dev", Value: 21395.666688859463},
		{Source: "org/ask-dev", Target: "TeamA", Value: 21395.666688859463 + 2364 + 50.33333483338356},
		{Source: "Feature Delivery · Customer", Target: "org/ask-dev", Value: 2364},
		{Source: "Risk · Security", Target: "org/ask-dev", Value: 50.33333483338356},
		{Source: "Maintenance · Debt", Target: "org/ops", Value: 7 + 5},
		{Source: "org/ops", Target: "TeamA", Value: 7},
		{Source: "org/ops", Target: "TeamB", Value: 5},
	}
	if !reflect.DeepEqual(links, want) {
		t.Fatalf("links =\n%+v\nwant\n%+v", links, want)
	}
	if got := perRowRepoTeamNodes(rows); !reflect.DeepEqual(nodeView(nodes), nodeView(got)) {
		t.Fatalf("nodes = %v, want the per-row running totals %v", nodeView(nodes), nodeView(got))
	}
	if n := len(perRowRepoTeamLinks(rows)); n != 10 {
		t.Fatalf("reference per-row link count = %d, want 10 (the fixture must carry repeated endpoints)", n)
	}
}

func nodeView(nodes []sankey.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		group := ""
		if n.Group != nil {
			group = *n.Group
		}
		value := 0.0
		if n.Value != nil {
			value = *n.Value
		}
		out[i] = fmt.Sprintf("%s/%s/%v", n.Name, group, value)
	}
	return out
}

// TestRepoTeamSankeyLinkIdentityByEnumeration proves, over every subset of
// a small row alphabet (every (subcategory, repo, team) combination the
// builder's branches distinguish, including a skipped empty subcategory,
// the unassigned repo and team fallbacks, and non-positive values), that:
//   - no two links share (source, target);
//   - the links equal the reference plane's per-row links with copies
//     sharing (source, target) summed in list order, value for value and
//     in the same first-appearance order;
//   - the nodes equal the reference plane's per-row running totals.
func TestRepoTeamSankeyLinkIdentityByEnumeration(t *testing.T) {
	subcategories := []string{"feature_delivery.roadmap", "risk.security", ""}
	repos := []string{"repoA", "repoB", "unassigned", ""}
	teams := []string{"teamA", "teamB", "unassigned", " "}
	values := []float64{3, 0.1, 7.25, 0, -1, 1e-3}
	var alphabet []repoTeamEdgeRow
	for _, s := range subcategories {
		for _, r := range repos {
			for _, tm := range teams {
				alphabet = append(alphabet, repoTeamEdgeRow{Subcategory: s, Repo: r, Team: tm, Value: values[len(alphabet)%len(values)]})
			}
		}
	}
	// 48 rows: enumerate every subset of a sliding window of 16 rows
	// (2^16 subsets per window) so every row meets every other row it can
	// share a link with inside some window.
	const window = 16
	checked := 0
	for start := 0; start+window <= len(alphabet); start += window / 2 {
		part := alphabet[start : start+window]
		for mask := 0; mask < 1<<window; mask++ {
			var rows []repoTeamEdgeRow
			for i := range part {
				if mask&(1<<i) != 0 {
					rows = append(rows, part[i])
				}
			}
			nodes, links := buildRepoTeamSankey(rows)
			seen := map[[2]string]bool{}
			for _, link := range links {
				k := [2]string{link.Source, link.Target}
				if seen[k] {
					t.Fatalf("rows %+v: two links share (%q, %q)", rows, link.Source, link.Target)
				}
				seen[k] = true
			}
			if want := sumCopiesByEndpoints(perRowRepoTeamLinks(rows)); !reflect.DeepEqual(links, want) {
				t.Fatalf("rows %+v:\nlinks %+v\nwant  %+v", rows, links, want)
			}
			if want := perRowRepoTeamNodes(rows); !reflect.DeepEqual(nodeView(nodes), nodeView(want)) {
				t.Fatalf("rows %+v: nodes %v, want %v", rows, nodeView(nodes), nodeView(want))
			}
			checked++
		}
	}
	if checked != 5*(1<<window) {
		t.Fatalf("checked %d row sets, want %d", checked, 5*(1<<window))
	}
}
