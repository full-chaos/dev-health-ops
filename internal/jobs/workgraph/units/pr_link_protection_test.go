package units

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// providerPR is one pull request (GitHub) or merge request (GitLab) and the
// work item that holds it, in the id shapes the real producers mint.
type providerPR struct {
	prNode   string // "<repo uuid>#pr<number>": the pull request NODE id, same for both providers
	workItem string // the pull request held as a WORK ITEM id
	churn    float64
}

const prLinkCap = 40

// oversizedPRGraph builds one connected component far over the cap. Native
// issue -> issue "relates" links (confidence 0.9) glue 150 tracker issues in a
// chain; every pull request implements its work item by an explicit_text link
// (0.9, below the component's max of 1.0 set by one native link); every work
// item is related to a chain issue, so the work items sit in the giant
// component. GitHub and GitLab alternate. Edge ids are hashes, as in the real
// table, so the legacy drop order is arbitrary between the two edge kinds.
func oversizedPRGraph() ([]Edge, []providerPR) {
	var edges []Edge
	add := func(st, si, tt, ti, kind string, conf float64) {
		edges = append(edges, Edge{
			EdgeID: fmt.Sprintf("%x", sha256.Sum256([]byte(st+"|"+si+"|"+kind+"|"+tt+"|"+ti))), SourceType: st, SourceID: si,
			TargetType: tt, TargetID: ti, Confidence: conf,
		})
	}
	const chain = 150
	for i := 0; i+1 < chain; i++ {
		add("issue", fmt.Sprintf("jira:TRK-%d", i), "issue", fmt.Sprintf("jira:TRK-%d", i+1), "relates", 0.9)
	}
	var prs []providerPR
	for k := 0; k < 80; k++ {
		var pr providerPR
		if k%2 == 0 {
			pr = providerPR{prNode: fmt.Sprintf("0b6c0000-0000-4000-8000-%012d#pr%d", k, k+1), workItem: fmt.Sprintf("ghpr:acme/web#%d", k+1)}
		} else {
			pr = providerPR{prNode: fmt.Sprintf("1c7d0000-0000-4000-8000-%012d#pr%d", k, k+1), workItem: fmt.Sprintf("glmr:acme/api!%d", k+1)}
		}
		pr.churn = float64(10 * (k + 1))
		add("pr", pr.prNode, "issue", pr.workItem, "implements", 0.9)
		add("issue", pr.workItem, "issue", fmt.Sprintf("jira:TRK-%d", (k*7)%chain), "relates", 0.9)
		prs = append(prs, pr)
	}
	// One native link sets the component's max confidence to 1.0, so every
	// 0.9 explicit_text link is droppable by the legacy split.
	add("pr", "native-pr#pr1", "issue", "jira:TRK-3", "implements", 1.0)
	return edges, prs
}

func fragmentOf(components []Component, node NodeKey) int {
	for index, component := range components {
		for _, candidate := range component.Nodes {
			if candidate == node {
				return index
			}
		}
	}
	return -1
}

func buildWith(edges []Edge, protect bool, stats *BuildStats) []Component {
	cap := prLinkCap
	return BuildComponentsWithOptions(edges, BuildOptions{MaxComponentNodes: &cap, ProtectPullRequestLinks: protect}, stats)
}

// A pull request and the issue it implements by an explicit_text link stay in
// ONE unit when the component is over the cap: the unit owns its pull request
// node, so the effort rule finds the pull request churn.
func TestOversizedSplitKeepsPullRequestWithItsWorkItem(t *testing.T) {
	edges, prs := oversizedPRGraph()
	stats := &BuildStats{}
	components := buildWith(edges, true, stats)
	if stats.OversizedComponents != 1 {
		t.Fatalf("fixture must be one oversized component, got %d", stats.OversizedComponents)
	}
	if stats.DroppedNodes != 0 {
		t.Fatalf("hub removal must not be needed here, dropped %d nodes", stats.DroppedNodes)
	}
	for _, component := range components {
		if len(component.Nodes) > prLinkCap {
			t.Fatalf("fragment of %d nodes is over the cap %d", len(component.Nodes), prLinkCap)
		}
	}
	for _, pr := range prs {
		prFragment := fragmentOf(components, NodeKey{Type: "pr", ID: pr.prNode})
		itemFragment := fragmentOf(components, NodeKey{Type: "issue", ID: pr.workItem})
		if prFragment < 0 || prFragment != itemFragment {
			t.Fatalf("%s and its pull request node are in different units (%d vs %d)", pr.workItem, itemFragment, prFragment)
		}
	}
}

// The state the system exists to reach: the unit of a pull request work item
// has effort = the pull request churn, for GitHub and GitLab alike.
func TestUnitOfPullRequestWorkItemHasPullRequestChurnAsEffort(t *testing.T) {
	edges, prs := oversizedPRGraph()
	churn := map[string]float64{}
	for _, pr := range prs {
		churn[pr.prNode] = pr.churn
	}
	// effortOf returns the unit's effort and the churn of the pull request
	// nodes the unit owns (the unit may own several pull requests: surviving
	// issue links join units).
	effortOf := func(components []Component, workItem string) (Effort, float64) {
		index := fragmentOf(components, NodeKey{Type: "issue", ID: workItem})
		var input EffortInput
		owned := 0.0
		for _, node := range components[index].Nodes {
			switch node.Type {
			case "issue":
				input.IssueIDs = append(input.IssueIDs, node.ID)
			case "pr":
				input.PRIDs = append(input.PRIDs, node.ID)
				owned += churn[node.ID]
			}
		}
		input.PRChurn = churn
		return ComputeEffort(input), owned
	}
	fixed := buildWith(edges, true, nil)
	legacy := buildWith(edges, false, nil)
	zeroLegacy := 0
	for _, pr := range prs {
		got, owned := effortOf(fixed, pr.workItem)
		if got.Metric != EffortMetricChurnLOC || got.Value != owned || got.Value < pr.churn {
			t.Fatalf("%s: effort %+v, want churn_loc %v (>= its own pull request churn %v)", pr.workItem, got, owned, pr.churn)
		}
		if legacyEffort, _ := effortOf(legacy, pr.workItem); legacyEffort.Value == 0 {
			zeroLegacy++
		}
	}
	// Strength of the fixture: the legacy split must reproduce the defect, or
	// this test could not fail on the old behaviour.
	if zeroLegacy == 0 {
		t.Fatal("legacy split severed no pull request link: the fixture no longer reproduces CHAOS-9267")
	}
}

// Same edges in = same units out, whatever the input order.
func TestProtectedSplitIsDeterministicUnderInputOrder(t *testing.T) {
	edges, _ := oversizedPRGraph()
	canon := func(components []Component) string {
		parts := make([]string, 0, len(components))
		for _, component := range components {
			ids := make([]string, 0, len(component.Nodes))
			for _, node := range component.Nodes {
				ids = append(ids, node.Type+"|"+node.ID)
			}
			sort.Strings(ids)
			parts = append(parts, strings.Join(ids, ","))
		}
		sort.Strings(parts)
		return strings.Join(parts, ";")
	}
	want := canon(buildWith(edges, true, nil))
	random := rand.New(rand.NewSource(9267))
	for round := 0; round < 25; round++ {
		shuffled := append([]Edge(nil), edges...)
		random.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if got := canon(buildWith(shuffled, true, nil)); got != want {
			t.Fatalf("round %d: units differ for the same edges in another order", round)
		}
	}
}

// BuildComponents (the form the frozen Python goldens call) must stay the
// legacy split: the declared divergence is opt-in.
func TestBuildComponentsStaysLegacySplit(t *testing.T) {
	edges, _ := oversizedPRGraph()
	cap := prLinkCap
	legacyForm := BuildComponents(edges, &cap, false, nil)
	explicit := buildWith(edges, false, nil)
	if len(legacyForm) != len(explicit) {
		t.Fatalf("BuildComponents gives %d units, options(false) gives %d", len(legacyForm), len(explicit))
	}
	if len(legacyForm) == len(buildWith(edges, true, nil)) {
		t.Fatal("legacy and protected splits give the same unit count: the divergence is not visible")
	}
}

// Both live consumers must group identically (two consumers that disagree mint
// different work_unit_ids). Every non-test caller of the builder outside this
// package must pass units.ProtectPullRequestLinks and
// units.IssueLinksAreReferences, and none may call the legacy form.
func TestEveryLiveConsumerPassesTheSharedProtectionFlag(t *testing.T) {
	if !ProtectPullRequestLinks {
		t.Fatal("ProtectPullRequestLinks must be true for the live consumers")
	}
	if !IssueLinksAreReferences {
		t.Fatal("IssueLinksAreReferences must be true for the live consumers")
	}
	root := filepath.Join("..", "..", "..") // internal/
	withOptions, flagged, referenced := 0, 0, 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.Contains(filepath.ToSlash(path), "jobs/workgraph/units/") {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		text := string(raw)
		if strings.Contains(text, "units.BuildComponents(") {
			t.Errorf("%s calls the legacy units.BuildComponents; live consumers must use BuildComponentsWithOptions", path)
		}
		if strings.Contains(text, "units.BuildComponentsWithOptions(") {
			withOptions++
			if strings.Contains(text, "ProtectPullRequestLinks: units.ProtectPullRequestLinks") {
				flagged++
			}
			if strings.Contains(text, "IssueLinksAreReferences: units.IssueLinksAreReferences") {
				referenced++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if withOptions < 2 {
		t.Fatalf("found %d live consumers of BuildComponentsWithOptions, want at least 2 (materializer and membership backfill): the scan measured nothing", withOptions)
	}
	if flagged != withOptions {
		t.Fatalf("%d of %d live consumers pass the shared flag", flagged, withOptions)
	}
	if referenced != withOptions {
		t.Fatalf("%d of %d live consumers pass units.IssueLinksAreReferences", referenced, withOptions)
	}
}

// Only pull request -> issue links are protected. Components glued by
// commit -> issue links or pull request -> commit links must still split
// under the cap by dropping those links, with no hub removed.
func TestOnlyPullRequestIssueLinksAreProtected(t *testing.T) {
	var edges []Edge
	add := func(st, si, tt, ti string, conf float64) {
		edges = append(edges, Edge{
			EdgeID:     fmt.Sprintf("%x", sha256.Sum256([]byte(st+"|"+si+"|"+tt+"|"+ti))),
			SourceType: st, SourceID: si, TargetType: tt, TargetID: ti, Confidence: conf,
		})
	}
	// Chain A: commits referencing issues, 0.85, 61 nodes in one line.
	for i := 0; i < 30; i++ {
		add("commit", fmt.Sprintf("repo@c%d", i), "issue", fmt.Sprintf("jira:A-%d", i), 0.85)
		add("commit", fmt.Sprintf("repo@c%d", i), "issue", fmt.Sprintf("jira:A-%d", i+1), 0.85)
	}
	// Chain B: pull requests containing commits, 0.9, 61 nodes in one line.
	for i := 0; i < 30; i++ {
		add("pr", fmt.Sprintf("repo#pr%d", i), "commit", fmt.Sprintf("repo@b%d", i), 0.9)
		add("pr", fmt.Sprintf("repo#pr%d", i), "commit", fmt.Sprintf("repo@b%d", i+1), 0.9)
	}
	// One native link at 1.0 sets the max confidence of the whole graph.
	add("pr", "repo#pr0", "issue", "jira:A-0", 1.0)
	stats := &BuildStats{}
	components := buildWith(edges, true, stats)
	if stats.OversizedComponents != 1 {
		t.Fatalf("fixture must be one oversized component, got %d", stats.OversizedComponents)
	}
	if stats.DroppedNodes != 0 {
		t.Fatalf("commit and pull request -> commit links must be dropped before any node is: %d nodes removed", stats.DroppedNodes)
	}
	for _, component := range components {
		if len(component.Nodes) > prLinkCap {
			t.Fatalf("fragment of %d nodes is over the cap %d", len(component.Nodes), prLinkCap)
		}
	}
}
