package units

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"
)

// referenceGraph is one org's edges across the four providers. Issue ids use
// each provider's work item shape; pull request work items (GitHub pull
// requests, GitLab merge requests) are issues too. The graph holds:
//   - per provider, an issue whose only links are issue -> issue links of
//     every dependency kind (relates, blocks, duplicates, parent/child);
//   - per provider, a pull request unit (with an issue -> incident link) and a
//     commit unit that a relates link would fuse if issue links joined;
//   - a chain of relates links across all providers that, joined, is one
//     component far over the cap.
type referenceGraph struct {
	edges []Edge
	// joined lists the node sets the joining links (pull request, commit)
	// must produce; every other endpoint must be a one-node unit.
	joined [][]NodeKey
}

var referenceProviders = []struct{ name, issue, prItem, prNode, commit string }{
	{"jira", "jira:ACME-%d", "ghpr:acme/web#%d", "0b6c0000-0000-4000-8000-000000000001#pr%d", "0b6c0000-0000-4000-8000-000000000001@j%d"},
	{"linear", "linear:3f0c%04d-0000-4000-8000-000000000000", "ghpr:acme/api#%d", "0b6c0000-0000-4000-8000-000000000002#pr%d", "0b6c0000-0000-4000-8000-000000000002@l%d"},
	{"github", "gh:acme/web#%d", "ghpr:acme/cli#%d", "0b6c0000-0000-4000-8000-000000000003#pr%d", "0b6c0000-0000-4000-8000-000000000003@g%d"},
	{"gitlab", "gitlab:acme/api#%d", "glmr:acme/api!%d", "1c7d0000-0000-4000-8000-000000000004#pr%d", "1c7d0000-0000-4000-8000-000000000004@m%d"},
}

func newReferenceGraph() referenceGraph {
	var graph referenceGraph
	add := func(st, si, kind, tt, ti string, conf float64) {
		graph.edges = append(graph.edges, Edge{
			EdgeID:     fmt.Sprintf("%x", sha256.Sum256([]byte(st+"|"+si+"|"+kind+"|"+tt+"|"+ti))),
			SourceType: st, SourceID: si, TargetType: tt, TargetID: ti, Confidence: conf,
		})
	}
	issue := func(id string) NodeKey { return NodeKey{Type: "issue", ID: id} }
	var chain []string
	for _, p := range referenceProviders {
		lone, blocked, dup, parent, child := fmt.Sprintf(p.issue, 1), fmt.Sprintf(p.issue, 2), fmt.Sprintf(p.issue, 3), fmt.Sprintf(p.issue, 4), fmt.Sprintf(p.issue, 5)
		add("issue", lone, "relates", "issue", blocked, 0.9)
		add("issue", lone, "blocks", "issue", blocked, 0.9)
		add("issue", dup, "duplicates", "issue", lone, 0.9)
		add("issue", parent, "parent_of", "issue", child, 1.0)

		// A pull request implements its work item and a tracker issue; a
		// commit references another tracker issue. A relates link between the
		// two units and one from each unit to the lone issue must not fuse
		// anything.
		prItem, tracked, committed := fmt.Sprintf(p.prItem, 7), fmt.Sprintf(p.issue, 6), fmt.Sprintf(p.issue, 8)
		prNode, commit := fmt.Sprintf(p.prNode, 7), fmt.Sprintf(p.commit, 8)
		add("pr", prNode, "implements", "issue", prItem, 1.0)
		add("pr", prNode, "references", "issue", tracked, 0.9)
		add("pr", prNode, "contains", "commit", commit+"x", 0.9)
		// An issue -> incident link has an issue source but is not an issue
		// link: it still joins.
		add("issue", tracked, "linked_incident", "incident", "incident:"+p.name, 0.9)
		add("commit", commit, "references", "issue", committed, 0.85)
		add("issue", tracked, "relates", "issue", committed, 0.9)
		add("issue", prItem, "relates", "issue", lone, 0.9)
		add("issue", committed, "blocks", "issue", lone, 0.9)
		graph.joined = append(graph.joined,
			[]NodeKey{{Type: "pr", ID: prNode}, issue(prItem), issue(tracked), {Type: "commit", ID: commit + "x"}, {Type: "incident", ID: "incident:" + p.name}},
			[]NodeKey{{Type: "commit", ID: commit}, issue(committed)},
		)
		for n := 0; n < 60; n++ {
			chain = append(chain, fmt.Sprintf(p.issue, 100+n))
		}
	}
	for i := 0; i+1 < len(chain); i++ {
		add("issue", chain[i], "relates", "issue", chain[i+1], 0.9)
	}
	return graph
}

func liveReferenceOptions(cap int) BuildOptions {
	return BuildOptions{MaxComponentNodes: &cap, ProtectPullRequestLinks: true, IssueLinksAreReferences: true}
}

func endpointsOf(edges []Edge) map[NodeKey]struct{} {
	nodes := make(map[NodeKey]struct{})
	for _, edge := range edges {
		nodes[edge.source()] = struct{}{}
		nodes[edge.target()] = struct{}{}
	}
	return nodes
}

func sortedNodes(nodes []NodeKey) []NodeKey {
	out := append([]NodeKey(nil), nodes...)
	sort.Slice(out, func(i, j int) bool { return nodeLess(out[i], out[j]) })
	return out
}

// Every endpoint of the input is a node of exactly one unit, the units glued
// by pull request and commit links are exactly the expected node sets, and
// every other endpoint is a one-node unit.
func TestEveryWorkItemOfTheInputHasExactlyOneUnit(t *testing.T) {
	graph := newReferenceGraph()
	stats := &BuildStats{}
	components := BuildComponentsWithOptions(graph.edges, liveReferenceOptions(150), stats)

	count := make(map[NodeKey]int)
	byID := make(map[string][]NodeKey)
	for _, component := range components {
		for _, node := range component.Nodes {
			count[node]++
		}
		byID[WorkUnitID(component.Nodes)] = component.Nodes
	}
	endpoints := endpointsOf(graph.edges)
	for node := range endpoints {
		if count[node] != 1 {
			t.Errorf("%s:%s is in %d units, want exactly 1", node.Type, node.ID, count[node])
		}
	}
	for node := range count {
		if _, ok := endpoints[node]; !ok {
			t.Errorf("%s:%s is a unit node but no input edge names it", node.Type, node.ID)
		}
	}

	inJoined := make(map[NodeKey]struct{})
	for _, want := range graph.joined {
		got, ok := byID[WorkUnitID(want)]
		if !ok {
			t.Errorf("no unit with node set %v", sortedNodes(want))
			continue
		}
		if len(got) != len(want) {
			t.Errorf("unit %v has %d nodes, want %d", sortedNodes(got), len(got), len(want))
		}
		for _, node := range want {
			inJoined[node] = struct{}{}
		}
	}
	wantOneNode := len(endpoints) - len(inJoined)
	oneNode := 0
	for _, component := range components {
		if len(component.Nodes) == 1 {
			oneNode++
		}
	}
	if oneNode != wantOneNode {
		t.Errorf("one-node units = %d, want %d (every endpoint outside a joined unit)", oneNode, wantOneNode)
	}
	if len(components) != len(graph.joined)+wantOneNode {
		t.Errorf("units = %d, want %d", len(components), len(graph.joined)+wantOneNode)
	}
	if stats.ReferenceOnlyUnits != wantOneNode {
		t.Errorf("stats.ReferenceOnlyUnits = %d, want %d", stats.ReferenceOnlyUnits, wantOneNode)
	}
	issueLinks := 0
	for _, edge := range graph.edges {
		if edge.SourceType == "issue" && edge.TargetType == "issue" {
			issueLinks++
		}
	}
	if stats.ReferenceLinks != issueLinks {
		t.Errorf("stats.ReferenceLinks = %d, want %d", stats.ReferenceLinks, issueLinks)
	}
	if stats.OversizedComponents != 0 || stats.DroppedEdges != 0 {
		t.Errorf("the 240-issue relates chain reached the split (oversized=%d dropped=%d): issue links counted toward a unit", stats.OversizedComponents, stats.DroppedEdges)
	}
}

// No unit holds an issue -> issue link in its edge bundle, and every unit's
// nodes are connected by its own bundle alone: no unit is joined only by an
// issue link.
func TestNoUnitIsJoinedByAnIssueToIssueLink(t *testing.T) {
	graph := newReferenceGraph()
	for _, component := range BuildComponentsWithOptions(graph.edges, liveReferenceOptions(150), nil) {
		for _, edge := range component.Edges {
			if isIssueToIssueLink(edge) {
				t.Errorf("unit %v lists the issue link %s -> %s as evidence", sortedNodes(component.Nodes), edge.SourceID, edge.TargetID)
			}
		}
		index := newNodeIndex()
		for _, node := range component.Nodes {
			index.intern(node)
		}
		if groups := connectedComponents(index, component.Edges); len(groups) != 1 {
			t.Errorf("unit %v falls into %d parts on its own edges: something other than its edges joined it", sortedNodes(component.Nodes), len(groups))
		}
	}
}

// The cap stays a safety valve: a component joined by pull request and commit
// links alone that is over the cap still splits, and issue links on top of it
// neither grow it nor change its split.
func TestTheCapStillSplitsAJoiningComponentAndIgnoresIssueLinks(t *testing.T) {
	var edges []Edge
	add := func(st, si, tt, ti string, conf float64) {
		edges = append(edges, Edge{
			EdgeID:     fmt.Sprintf("%x", sha256.Sum256([]byte(st+"|"+si+"|"+tt+"|"+ti))),
			SourceType: st, SourceID: si, TargetType: tt, TargetID: ti, Confidence: conf,
		})
	}
	for i := 0; i < 30; i++ {
		add("commit", fmt.Sprintf("repo@c%d", i), "issue", fmt.Sprintf("jira:A-%d", i), 0.85)
		add("commit", fmt.Sprintf("repo@c%d", i), "issue", fmt.Sprintf("jira:A-%d", i+1), 0.85)
	}
	add("pr", "repo#pr1", "issue", "jira:A-0", 1.0)
	withoutLinks := append([]Edge(nil), edges...)
	for i := 0; i < 30; i++ {
		add("issue", fmt.Sprintf("jira:A-%d", i), "issue", fmt.Sprintf("jira:B-%d", i), 0.9)
	}

	baseStats, stats := &BuildStats{}, &BuildStats{}
	base := BuildComponentsWithOptions(withoutLinks, liveReferenceOptions(20), baseStats)
	got := BuildComponentsWithOptions(edges, liveReferenceOptions(20), stats)
	if baseStats.OversizedComponents != 1 || stats.OversizedComponents != 1 {
		t.Fatalf("oversized = %d / %d, want 1 / 1: the joining component must still reach the split", baseStats.OversizedComponents, stats.OversizedComponents)
	}
	if baseStats.DroppedEdges != stats.DroppedEdges {
		t.Errorf("dropped edges %d with issue links, %d without: issue links changed the split", stats.DroppedEdges, baseStats.DroppedEdges)
	}
	ids := make(map[string]bool)
	for _, component := range got {
		if len(component.Nodes) > 20 {
			t.Errorf("unit of %d nodes over the cap 20", len(component.Nodes))
		}
		ids[WorkUnitID(component.Nodes)] = true
	}
	for _, component := range base {
		if !ids[WorkUnitID(component.Nodes)] {
			t.Errorf("unit %v of the joining component changed when issue links were added", sortedNodes(component.Nodes))
		}
	}
	if len(got) != len(base)+30 {
		t.Errorf("units = %d, want %d (the split units plus 30 one-node units for jira:B-*)", len(got), len(base)+30)
	}
}

// The reference rule is opt-in: BuildComponents, the form the frozen Python
// goldens call, still joins on issue links.
func TestBuildComponentsStillJoinsOnIssueLinks(t *testing.T) {
	edges := []Edge{
		{EdgeID: "e1", SourceType: "issue", SourceID: "jira:A-1", TargetType: "issue", TargetID: "jira:A-2", Confidence: 0.9},
	}
	if got := BuildComponents(edges, nil, false, nil); len(got) != 1 || len(got[0].Nodes) != 2 {
		t.Fatalf("BuildComponents gave %d units, want one unit of both issues", len(got))
	}
	cap := 150
	if got := BuildComponentsWithOptions(edges, BuildOptions{MaxComponentNodes: &cap, IssueLinksAreReferences: true}, nil); len(got) != 2 {
		t.Fatalf("with IssueLinksAreReferences: %d units, want 2 one-node units", len(got))
	}
}

// Unit ids do not depend on edge input order, also for one-node units.
func TestReferenceUnitIDsDoNotDependOnEdgeOrder(t *testing.T) {
	graph := newReferenceGraph()
	want := make(map[string]bool)
	for _, component := range BuildComponentsWithOptions(graph.edges, liveReferenceOptions(150), nil) {
		want[WorkUnitID(component.Nodes)] = true
	}
	reversed := make([]Edge, len(graph.edges))
	for i, edge := range graph.edges {
		reversed[len(graph.edges)-1-i] = edge
	}
	got := BuildComponentsWithOptions(reversed, liveReferenceOptions(150), nil)
	if len(got) != len(want) {
		t.Fatalf("units = %d reversed, %d forward", len(got), len(want))
	}
	for _, component := range got {
		if !want[WorkUnitID(component.Nodes)] {
			t.Errorf("unit %v exists only for the reversed input", sortedNodes(component.Nodes))
		}
	}
}
