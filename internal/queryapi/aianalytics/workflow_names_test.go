package aianalytics

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8113: the nodes of aiWorkflowDrilldown carry a display name, from the one
// resolver the Work Graph edge ends are named with (workgraph.NodeDisplayNames).
// The field is Go-only: the recorded Python cases hold none (the oracle strips
// it), and the tests below pin it.
//
// The ids of the fixture have the forms the writers store: a pull request is
// "{repo_uuid}:{number}" (aiworkflow.prIDFor), a deployment and an incident are
// the provider's own opaque ids.

const (
	wfRepo       = "aaaaaaaa-1111-4111-8111-111111111111"
	wfPR         = wfRepo + ":7"
	wfUnnamedPR  = wfRepo + ":8"
	wfDeployment = "dep-9001"
	wfIncident   = "INC-42"
	wfRun        = "0123456789abcdef0123456789abcdef"
)

func wfEdge(id, srcType, srcID, tgtType, tgtID string) map[string]any {
	return map[string]any{
		"edge_id": id, "source_type": srcType, "source_id": srcID, "target_type": tgtType, "target_id": tgtID,
		"edge_type": "x", "confidence": float64(1), "source": "s", "evidence": "{}", "provider": "github", "repo_id": nil,
	}
}

// namedWorkflowClient is an issue whose AI workflow run made two pull requests;
// one was reviewed and deployed, and the deployment is linked to an incident.
func namedWorkflowClient() *fixtureClientC {
	return &fixtureClientC{
		c: oracleCCase{Name: "names/chain", Kind: "workflow", Hops: [][]map[string]any{
			{wfEdge("e1", "issue", "ABC-1", "ai_workflow_run", wfRun)},
			{wfEdge("e2", "ai_workflow_run", wfRun, "pr", wfPR), wfEdge("e3", "ai_workflow_run", wfRun, "pr", wfUnnamedPR)},
			{wfEdge("e4", "pr", wfPR, "review_outcome", "rev-1"), wfEdge("e5", "pr", wfPR, "deployment", wfDeployment)},
			{wfEdge("e6", "deployment", wfDeployment, "incident", wfIncident)},
		}},
		namePRs:         [][]any{{wfRepo, uint32(7), "Add the retry budget"}},
		nameDeployments: [][]any{{wfDeployment, "production"}},
		nameIncidents:   [][]any{{wfIncident, "resolved", "Checkout errors"}},
	}
}

func runWorkflow(t *testing.T, client *fixtureClientC) *model.AIWorkflowDrilldownResult {
	t.Helper()
	got, err := WorkflowDrilldown(context.Background(), client, "org-1", model.AIWorkflowRootTypeInputIssue, "ABC-1", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func nodeNames(t *testing.T, nodes []model.AIWorkflowGraphNodeOut) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, n := range nodes {
		key := n.NodeType + " " + n.NodeID
		if _, dup := out[key]; dup {
			t.Fatalf("node %q is listed twice", key)
		}
		out[key] = nameOrNil(n.DisplayName)
	}
	return out
}

func TestWorkflowDrilldown_NodesCarryDisplayNames(t *testing.T) {
	got := nodeNames(t, runWorkflow(t, namedWorkflowClient()).Nodes)
	want := map[string]string{
		"issue ABC-1":                "ABC-1",                      // a readable issue key is its own name
		"ai_workflow_run " + wfRun:   "<nil>",                      // no catalogue names a run
		"pr " + wfPR:                 "Add the retry budget",       // the pull request's title
		"pr " + wfUnnamedPR:          "<nil>",                      // not in the catalogue: never its id
		"review_outcome rev-1":       "<nil>",                      // no catalogue names a review outcome
		"deployment " + wfDeployment: "production deploy",          // the environment
		"incident " + wfIncident:     "Checkout errors (Resolved)", // title and status
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("node names\n got  %v\n want %v", got, want)
	}
}

// The flag says whether the node's TYPE carries a name, so a client can tell a
// gap (a null name of a type that carries one) from a type with no name.
func TestWorkflowDrilldown_NodesSayWhetherTheirTypeCarriesAName(t *testing.T) {
	want := map[string]bool{
		"issue ABC-1":                true,
		"ai_workflow_run " + wfRun:   false,
		"pr " + wfPR:                 true,
		"pr " + wfUnnamedPR:          true, // no name in the catalogue: a gap, and the flag says so
		"review_outcome rev-1":       false,
		"deployment " + wfDeployment: true,
		"incident " + wfIncident:     true,
	}
	for _, catalogue := range []string{"full", "empty"} {
		client := namedWorkflowClient()
		if catalogue == "empty" {
			client.namePRs, client.nameDeployments, client.nameIncidents = nil, nil, nil
		}
		got := map[string]bool{}
		for _, n := range runWorkflow(t, client).Nodes {
			got[n.NodeType+" "+n.NodeID] = n.NameExpected
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s catalogue: nameExpected\n got  %v\n want %v", catalogue, got, want)
		}
	}
}

// A name is never an id that holds a repository UUID: a pull request the
// catalogue does not name stays without a name.
func TestWorkflowDrilldown_ANameIsNeverARawID(t *testing.T) {
	client := namedWorkflowClient()
	client.namePRs, client.nameDeployments, client.nameIncidents = nil, nil, nil
	for _, n := range runWorkflow(t, client).Nodes {
		if n.NodeType == "issue" {
			continue
		}
		if n.DisplayName != nil {
			t.Errorf("%s %s: displayName = %q with empty catalogues, want nil", n.NodeType, n.NodeID, *n.DisplayName)
		}
	}
}

// The web names an edge end by the node of the same type and id: every end that
// has an id must have exactly one node, also when the edge limit cuts the walk.
func TestWorkflowDrilldown_EveryEdgeEndHasItsNode(t *testing.T) {
	check := func(t *testing.T, got *model.AIWorkflowDrilldownResult) int {
		t.Helper()
		nodes := nodeNames(t, got.Nodes)
		ends := 0
		for _, e := range got.Edges {
			for _, end := range [][2]string{{e.SourceType, e.SourceID}, {e.TargetType, e.TargetID}} {
				if end[1] == "" {
					continue
				}
				ends++
				if _, ok := nodes[end[0]+" "+end[1]]; !ok {
					t.Errorf("edge %s: end %s %s has no node", e.EdgeID, end[0], end[1])
				}
			}
		}
		return ends
	}
	t.Run("named chain", func(t *testing.T) {
		if check(t, runWorkflow(t, namedWorkflowClient())) != 12 {
			t.Fatal("the chain must have six edges with two ends each")
		}
	})
	t.Run("named chain cut by the limit", func(t *testing.T) {
		got, err := WorkflowDrilldown(context.Background(), namedWorkflowClient(), "org-1", model.AIWorkflowRootTypeInputIssue, "ABC-1", 5, 2)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Partial || check(t, got) == 0 {
			t.Fatalf("want a partial walk with edges, got partial=%v edges=%d", got.Partial, len(got.Edges))
		}
	})
	measured := 0
	for name, c := range loadCases(t) {
		if c.Kind != "workflow" || c.Error != nil {
			continue
		}
		t.Run(name, func(t *testing.T) {
			got, err := WorkflowDrilldown(context.Background(), &fixtureClientC{c: c}, "org-1", model.AIWorkflowRootTypeInput(c.RootType), c.RootID, c.Depth, c.Limit)
			if err != nil {
				t.Fatal(err)
			}
			measured += check(t, got)
		})
	}
	if measured == 0 {
		t.Fatal("the recorded cases gave no edge end: nothing was measured")
	}
}

// One read per node type that has a node, bound to the caller's org, kept apart
// from the edge reads the Python calls are compared with.
func TestWorkflowDrilldown_NameReadsAreOnePerTypeAndOrgBound(t *testing.T) {
	client := namedWorkflowClient()
	runWorkflow(t, client)
	reads := map[string]int{}
	for i, st := range client.nameStatements {
		which := workflowNameRead(st)
		reads[which]++
		if strings.Count(st, "org_id = {org_id:String}") < 1 {
			t.Errorf("%s name read has no org predicate:\n%s", which, st)
		}
		if v, _ := binding(client.nameBindings[i], "org_id"); v != "org-1" {
			t.Errorf("%s name read binds org_id=%v, want org-1", which, v)
		}
		switch which {
		case "pr":
			if v, _ := binding(client.nameBindings[i], "repo_ids"); !reflect.DeepEqual(v, []string{wfRepo}) {
				t.Errorf("repo_ids = %v, want the one repository of both pull requests", v)
			}
			if v, _ := binding(client.nameBindings[i], "pr_numbers"); !reflect.DeepEqual(v, []string{"7", "8"}) {
				t.Errorf("pr_numbers = %v, want [7 8]", v)
			}
		case "deployment":
			if v, _ := binding(client.nameBindings[i], "dep_ids"); !reflect.DeepEqual(v, []string{wfDeployment}) {
				t.Errorf("dep_ids = %v", v)
			}
		case "incident":
			if v, _ := binding(client.nameBindings[i], "inc_ids"); !reflect.DeepEqual(v, []string{wfIncident}) {
				t.Errorf("inc_ids = %v", v)
			}
		}
	}
	if want := map[string]int{"pr": 1, "deployment": 1, "incident": 1}; !reflect.DeepEqual(reads, want) {
		t.Errorf("name reads = %v, want %v", reads, want)
	}
	for _, st := range client.statements {
		if workflowNameRead(st) != "" {
			t.Errorf("a name read is recorded among the edge reads:\n%s", st)
		}
	}

	// A walk with no pull request, deployment or incident node reads no name.
	issueOnly := &fixtureClientC{c: oracleCCase{Kind: "workflow", Hops: [][]map[string]any{
		{wfEdge("e1", "issue", "ABC-1", "ai_workflow_run", wfRun)},
	}}}
	runWorkflow(t, issueOnly)
	if len(issueOnly.nameStatements) != 0 {
		t.Errorf("%d name read(s) for a walk with only issue and run nodes", len(issueOnly.nameStatements))
	}
}

// A name is a label: a name read that fails leaves the nodes of that type
// without a name and keeps every node and every edge.
func TestWorkflowDrilldown_AFailedNameReadKeepsTheNodes(t *testing.T) {
	whole := runWorkflow(t, namedWorkflowClient())
	for which, lost := range map[string]string{
		"pr":         "pr " + wfPR,
		"deployment": "deployment " + wfDeployment,
		"incident":   "incident " + wfIncident,
	} {
		t.Run(which, func(t *testing.T) {
			client := namedWorkflowClient()
			client.nameErr = map[string]error{which: errors.New("boom")}
			got := runWorkflow(t, client)
			if len(got.Nodes) != len(whole.Nodes) || len(got.Edges) != len(whole.Edges) {
				t.Fatalf("want %d nodes and %d edges, got %d and %d", len(whole.Nodes), len(whole.Edges), len(got.Nodes), len(got.Edges))
			}
			names, wholeNames := nodeNames(t, got.Nodes), nodeNames(t, whole.Nodes)
			for key, want := range wholeNames {
				if key == lost {
					want = "<nil>"
				}
				if names[key] != want {
					t.Errorf("%s: displayName = %s, want %s", key, names[key], want)
				}
			}
		})
	}
}
