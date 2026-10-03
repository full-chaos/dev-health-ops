package workgraph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// CHAOS-8113: NodeDisplayNames names nodes whose type is known, with the three
// reads the Work Graph edge ends are named with.

const (
	nnRepoA = "aaaaaaaa-1111-4111-8111-111111111111"
	nnRepoB = "bbbbbbbb-2222-4222-8222-222222222222"
)

// nodeNameClient answers the three name reads by statement and records them.
type nodeNameClient struct {
	prs         [][]any // repo_id, number (uint32), title
	deployments [][]any // deployment_id, environment
	incidents   [][]any // id, status, title
	fail        map[string]error
	statements  map[string][]string
	bindings    map[string][][]clickhouse.Binding
	unmatched   []string
}

func (c *nodeNameClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	kind := wgKind(statement)
	if c.statements == nil {
		c.statements, c.bindings = map[string][]string{}, map[string][][]clickhouse.Binding{}
	}
	c.statements[kind] = append(c.statements[kind], statement)
	c.bindings[kind] = append(c.bindings[kind], bindings)
	if err := c.fail[kind]; err != nil {
		return nil, err
	}
	switch kind {
	case "pr":
		return &fakeRowScanner{rows: c.prs}, nil
	case "dep":
		return &fakeRowScanner{rows: c.deployments}, nil
	case "inc":
		return &fakeRowScanner{rows: c.incidents}, nil
	}
	c.unmatched = append(c.unmatched, statement)
	return nil, errors.New("nodeNameClient: unscripted statement")
}

func catalogueClient() *nodeNameClient {
	return &nodeNameClient{
		prs:         [][]any{{nnRepoA, uint32(7), "Add the retry budget"}, {nnRepoB, uint32(12), "  Trim me  "}},
		deployments: [][]any{{"dep-9001", "production"}, {"dep-empty-env", "  "}},
		incidents:   [][]any{{"INC-42", "resolved", "Checkout errors"}, {"INC-43", "triggered", ""}},
	}
}

func named(names []*string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = "<nil>"
		if n != nil {
			out[i] = *n
		}
	}
	return out
}

func TestNodeDisplayNames_NamesByNodeType(t *testing.T) {
	nodes := []struct {
		ref  NodeRef
		want string
		why  string
	}{
		{NodeRef{"pr", nnRepoA + ":7"}, "Add the retry budget", "a pull request in the typed edge tables' form"},
		{NodeRef{"pr", nnRepoB + "#pr12"}, "Trim me", "a pull request in the work_graph_edges form"},
		{NodeRef{"PR", "  " + nnRepoA + ":7  "}, "Add the retry budget", "type and id are trimmed, the type's case is ignored"},
		{NodeRef{"pr", nnRepoA + ":99"}, "<nil>", "a pull request the catalogue does not hold: never its id"},
		{NodeRef{"pr", "r:7"}, "<nil>", "a pull request id with no repository UUID cannot be read"},
		{NodeRef{"pr", nnRepoA}, "<nil>", "a bare repository UUID is not a pull request id"},
		{NodeRef{"deployment", "dep-9001"}, "production deploy", "an opaque provider id, named by its environment"},
		{NodeRef{"deployment", "dep-empty-env"}, "<nil>", "a deployment with no environment has no name"},
		{NodeRef{"deployment", "dep-unknown"}, "<nil>", "a deployment the catalogue does not hold"},
		{NodeRef{"incident", "INC-42"}, "Checkout errors (Resolved)", "title and status"},
		{NodeRef{"incident", "INC-43"}, "incident (Triggered)", "an incident with no title"},
		{NodeRef{"incident", "INC-unknown"}, "<nil>", "an incident the catalogue does not hold"},
		{NodeRef{"issue", "ABC-1"}, "ABC-1", "a readable issue key is its own name"},
		{NodeRef{"issue", "linear:CHAOS-12"}, "linear:CHAOS-12", "a provider-prefixed key is readable"},
		{NodeRef{"issue", nnRepoA}, "<nil>", "an issue id that is a UUID"},
		{NodeRef{"issue", "gh:" + nnRepoA + "#3"}, "<nil>", "an issue id that holds a UUID"},
		{NodeRef{"issue", "0123456789abcdef0123456789abcdef"}, "<nil>", "an issue id that is an opaque hash"},
		{NodeRef{"review_outcome", "rev-1"}, "<nil>", "no catalogue names a review outcome, readable id or not"},
		{NodeRef{"ai_workflow_run", "run-readable"}, "<nil>", "no catalogue names a run, readable id or not"},
		{NodeRef{"issue", "   "}, "<nil>", "an empty id"},
		{NodeRef{"incident", ""}, "<nil>", "an empty id"},
	}
	refs := make([]NodeRef, len(nodes))
	for i, n := range nodes {
		refs[i] = n.ref
	}
	client := catalogueClient()
	got := named(NodeDisplayNames(context.Background(), client, "org-1", refs))
	if len(got) != len(nodes) {
		t.Fatalf("want one entry per node (%d), got %d", len(nodes), len(got))
	}
	for i, n := range nodes {
		if got[i] != n.want {
			t.Errorf("%s %q = %s, want %s (%s)", n.ref.Type, n.ref.ID, got[i], n.want, n.why)
		}
	}
	if len(client.unmatched) != 0 {
		t.Errorf("unexpected statements: %v", client.unmatched)
	}
}

func TestNodeDisplayNames_OneReadPerTypeBoundToTheOrg(t *testing.T) {
	client := catalogueClient()
	NodeDisplayNames(context.Background(), client, "org-1", []NodeRef{
		{"pr", nnRepoB + ":12"}, {"pr", nnRepoA + ":7"}, {"pr", nnRepoA + ":7"},
		{"deployment", "dep-b"}, {"deployment", "dep-a"},
		{"incident", "INC-2"}, {"incident", "INC-1"},
		{"issue", "ABC-1"},
	})
	want := map[string]map[string]any{
		"pr":  {"repo_ids": []string{nnRepoA, nnRepoB}, "pr_numbers": []string{"7", "12"}},
		"dep": {"dep_ids": []string{"dep-a", "dep-b"}},
		"inc": {"inc_ids": []string{"INC-1", "INC-2"}},
	}
	for kind, bound := range want {
		if len(client.statements[kind]) != 1 {
			t.Fatalf("%s: %d read(s), want exactly one for all the nodes of the type", kind, len(client.statements[kind]))
		}
		st, b := client.statements[kind][0], client.bindings[kind][0]
		if !strings.Contains(st, "org_id = {org_id:String}") {
			t.Errorf("%s read has no org predicate:\n%s", kind, st)
		}
		if v, _ := bindingValue(b, "org_id"); v != "org-1" {
			t.Errorf("%s read binds org_id=%v, want org-1", kind, v)
		}
		for name, wantValue := range bound {
			if v, _ := bindingValue(b, name); !reflect.DeepEqual(v, wantValue) {
				t.Errorf("%s read binds %s=%v, want %v", kind, name, v, wantValue)
			}
		}
	}
	if len(client.statements) != 3 {
		t.Errorf("statement kinds = %d, want the three name reads only", len(client.statements))
	}

	// No node of a type, no read of its catalogue; no org or no node, no read at all.
	for name, run := range map[string]func(c *nodeNameClient) []*string{
		"only issue and run nodes": func(c *nodeNameClient) []*string {
			return NodeDisplayNames(context.Background(), c, "org-1", []NodeRef{{"issue", "ABC-1"}, {"ai_workflow_run", "r"}})
		},
		"no org": func(c *nodeNameClient) []*string {
			return NodeDisplayNames(context.Background(), c, "", []NodeRef{{"pr", nnRepoA + ":7"}})
		},
		"no nodes": func(c *nodeNameClient) []*string {
			return NodeDisplayNames(context.Background(), c, "org-1", nil)
		},
	} {
		c := catalogueClient()
		got := run(c)
		if len(c.statements) != 0 {
			t.Errorf("%s: %d statement kind(s) read, want none", name, len(c.statements))
		}
		if name == "no org" && (len(got) != 1 || got[0] != nil) {
			t.Errorf("no org: got %v, want one nil entry", named(got))
		}
	}
}

func TestNodeDisplayNames_AFailedReadLeavesOnlyThatTypeUnnamed(t *testing.T) {
	refs := []NodeRef{{"pr", nnRepoA + ":7"}, {"deployment", "dep-9001"}, {"incident", "INC-42"}, {"issue", "ABC-1"}}
	whole := named(NodeDisplayNames(context.Background(), catalogueClient(), "org-1", refs))
	if want := []string{"Add the retry budget", "production deploy", "Checkout errors (Resolved)", "ABC-1"}; !reflect.DeepEqual(whole, want) {
		t.Fatalf("with every read working: %v, want %v", whole, want)
	}
	for kind, index := range map[string]int{"pr": 0, "dep": 1, "inc": 2} {
		client := catalogueClient()
		client.fail = map[string]error{kind: errors.New("boom")}
		got := named(NodeDisplayNames(context.Background(), client, "org-1", refs))
		want := append([]string(nil), whole...)
		want[index] = "<nil>"
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s read fails: %v, want %v", kind, got, want)
		}
	}
}

// One table says which types carry a name. A type carries a name exactly when
// NodeDisplayNames can give a node of that type one, so the flag and the name
// cannot disagree; and the set is the four types the SDL states.
func TestNodeTypeCarriesName_FollowsTheTableTheNamesComeFrom(t *testing.T) {
	sample := map[string]string{
		"pr": nnRepoA + ":7", "deployment": "dep-9001", "incident": "INC-42", "issue": "ABC-1",
		"review_outcome": "rev-1", "ai_workflow_run": "run-readable", "diff": "d1", "commit": "abc", "file": "a.go", "": "x",
	}
	carries := map[string]bool{}
	for typ, id := range sample {
		name := NodeDisplayNames(context.Background(), catalogueClient(), "org-1", []NodeRef{{typ, id}})[0]
		if got := NodeTypeCarriesName(typ); got != (name != nil) {
			t.Errorf("%q: NodeTypeCarriesName = %v, but a full catalogue gives the name %v", typ, got, named([]*string{name}))
		}
		if NodeTypeCarriesName(typ) {
			carries[typ] = true
		}
	}
	if want := map[string]bool{"pr": true, "deployment": true, "incident": true, "issue": true}; !reflect.DeepEqual(carries, want) {
		t.Errorf("types that carry a name = %v, want %v", carries, want)
	}
	if len(namedNodeTypes) != 4 {
		t.Errorf("the table holds %d types, want the four the SDL states", len(namedNodeTypes))
	}
	if !NodeTypeCarriesName("  PR ") {
		t.Error("the type is trimmed and its case ignored, as NodeDisplayNames does")
	}
}

// resolvePRDisplayNames reads only the id forms it is given: the edge ends of
// workGraphEdges pass the "#pr" form alone, as the reference does, so the typed
// tables' colon form stays unresolved there.
func TestResolvePRDisplayNames_ReadsOnlyTheFormsItIsGiven(t *testing.T) {
	colon := map[string]struct{}{nnRepoA + ":7": {}}
	client := catalogueClient()
	resolved := map[string]string{}
	resolvePRDisplayNames(context.Background(), client, "org-1", colon, resolved, prEdgeIDRe)
	if len(resolved) != 0 || len(client.statements) != 0 {
		t.Fatalf("the #pr form alone resolved a colon id: %v (%d statement kinds)", resolved, len(client.statements))
	}
	resolvePRDisplayNames(context.Background(), client, "org-1", colon, resolved, prEdgeIDRe, prColonIDRe)
	if resolved[nnRepoA+":7"] != "Add the retry budget" {
		t.Fatalf("both forms: %v", resolved)
	}

	// The edge path: a colon id never reaches the pull request read.
	edges := catalogueClient()
	got := batchResolveDisplayNames(context.Background(), edges, "org-1", []edgeEndpoint{{sourceID: nnRepoA + ":7", sourceType: "pr"}})
	if len(got) != 0 || len(edges.statements) != 0 {
		t.Fatalf("workGraphEdges resolved a colon id: %v (%d statement kinds)", got, len(edges.statements))
	}
}
