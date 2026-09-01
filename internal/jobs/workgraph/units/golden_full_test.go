package units

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The frozen pair: a captured work_graph_edges input and the components the
// DEPLOYED Python builder produces from it.
const (
	goldenEdgesFixture = "workgraph_components_split_edges.json"
	goldenFixture      = "workgraph_components_split_python_golden.json"
)

// goldenEdgesDocument mirrors the columnar frozen input. The rows are stored one
// array per edge (field order declared in Columns) purely to keep the fixture
// small; the encoding is lossless.
type goldenEdgesDocument struct {
	Columns           []string `json:"columns"`
	Edges             [][]any  `json:"edges"`
	MaxComponentNodes int      `json:"max_component_nodes"`
	OrgID             string   `json:"org_id"`
}

// goldenDocument mirrors the shape
// tests/fixtures/generate_workgraph_components_python_golden.py renders.
type goldenDocument struct {
	Components        []goldenComponent `json:"components"`
	MaxComponentNodes int               `json:"max_component_nodes"`
	SourceEdges       string            `json:"source_edges"`
	Stats             goldenStats       `json:"stats"`
}

type goldenComponent struct {
	WorkUnitID string     `json:"work_unit_id"`
	Nodes      [][]string `json:"nodes"`
	EdgeIDs    []string   `json:"edge_ids"`
}

type goldenStats struct {
	OversizedComponents int `json:"oversized_components"`
	DroppedEdges        int `json:"dropped_edges"`
	DroppedNodes        int `json:"dropped_nodes"`
}

// TestBuildComponentsMatchesFrozenPythonGoldenExhaustively is the parity proof
// for CHAOS-4441's shared grouping core.
//
// It compares EVERY component and EVERY field -- id, node set, edge bundle,
// component order, and the split's three counters -- not a hand-picked subset.
// A subset assertion here would be a test that cannot fail in the way that
// matters: the whole hazard is that one component out of 1,601 groups
// differently and silently re-addresses rows in another table
// (backfill.py:113-127). See TestGoldenComparisonCatchesPlantedDefects for the
// falsification of this comparator itself.
func TestBuildComponentsMatchesFrozenPythonGoldenExhaustively(t *testing.T) {
	edges, golden := loadGoldenPair(t)

	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, stats)

	if golden.SourceEdges != goldenEdgesFixture {
		t.Fatalf(
			"golden was generated from %q but this test feeds it %q -- the pair is mismatched",
			golden.SourceEdges, goldenEdgesFixture,
		)
	}
	if len(live) != len(golden.Components) {
		t.Fatalf("component count: python %d, go %d", len(golden.Components), len(live))
	}

	liveComponents := make([]goldenComponent, len(live))
	for position, component := range live {
		liveComponents[position] = toGoldenComponent(component)
	}

	for _, failure := range diffAgainstGolden(golden, liveComponents, *stats, maxComponentNodes) {
		t.Error(failure)
	}
}

// diffAgainstGolden is the comparator, factored out of the test that uses it so
// TestGoldenComparisonCatchesPlantedDefects can falsify it directly. A
// comparator only asserted against a passing input is itself untested -- the
// differential-oracle lesson this repo already paid for (CHAOS-3033).
//
// Returns one message per divergence; empty means parity.
func diffAgainstGolden(
	golden goldenDocument, live []goldenComponent, stats BuildStats, maxComponentNodes int,
) []string {
	failures := make([]string, 0)

	if len(live) != len(golden.Components) {
		return append(failures, fmt.Sprintf(
			"component count: python %d, go %d", len(golden.Components), len(live),
		))
	}

	// Compared IN ORDER, not as sets. Component order is contract: partitioned
	// materialization dispatches numeric component_indexes and each chunk worker
	// re-derives the list, so index N must name the same component on both
	// planes or chunks silently skip / double-categorize units
	// (queries.py:46-51).
	reported := 0
	for position := range live {
		if reflect.DeepEqual(golden.Components[position], live[position]) {
			continue
		}
		reported++
		if reported <= 8 {
			failures = append(failures, fmt.Sprintf(
				"component %d mismatch:\npython: %s\ngo:     %s",
				position, renderForDiff(golden.Components[position]), renderForDiff(live[position]),
			))
		}
	}
	if reported > 8 {
		failures = append(failures, fmt.Sprintf("... and %d further component mismatches", reported-8))
	}

	expectedStats := BuildStats{
		OversizedComponents: golden.Stats.OversizedComponents,
		DroppedEdges:        golden.Stats.DroppedEdges,
		DroppedNodes:        golden.Stats.DroppedNodes,
	}
	if stats != expectedStats {
		failures = append(failures, fmt.Sprintf("stats: python %+v, go %+v", expectedStats, stats))
	}

	// The cap must be ENFORCED, not merely configured: the split exists because
	// one unbounded component dominated the whole Investment allocation chart
	// (CHAOS-2775). A port that produced the right ids while leaving an
	// oversized component would pass every assertion above.
	for position, component := range live {
		if len(component.Nodes) > maxComponentNodes {
			failures = append(failures, fmt.Sprintf(
				"component %d has %d nodes, exceeding the cap of %d",
				position, len(component.Nodes), maxComponentNodes,
			))
		}
	}
	return failures
}

// TestGoldenComparisonCatchesPlantedDefects falsifies the comparator.
//
// Every mutation below is a real way this port could be wrong, applied to a
// KNOWN-GOOD live result. If any of them slips through, the exhaustive test
// above is decorative and the parity claim in the PR body is worthless.
func TestGoldenComparisonCatchesPlantedDefects(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, stats)

	baseline := make([]goldenComponent, len(live))
	for position, component := range live {
		baseline[position] = toGoldenComponent(component)
	}
	if failures := diffAgainstGolden(golden, baseline, *stats, maxComponentNodes); len(failures) != 0 {
		t.Fatalf("baseline is not clean, cannot falsify from it: %v", failures)
	}

	clone := func() []goldenComponent {
		copied := make([]goldenComponent, len(baseline))
		for position, component := range baseline {
			copied[position] = goldenComponent{
				WorkUnitID: component.WorkUnitID,
				Nodes:      append([][]string(nil), component.Nodes...),
				EdgeIDs:    append([]string(nil), component.EdgeIDs...),
			}
		}
		return copied
	}
	// A component with more than one node, so node-level mutations are visible.
	multiNode := -1
	for position, component := range baseline {
		if len(component.Nodes) > 1 {
			multiNode = position
			break
		}
	}
	if multiNode < 0 {
		t.Fatal("fixture has no multi-node component; the node mutations below would be vacuous")
	}

	for _, plant := range []struct {
		name    string
		mutate  func([]goldenComponent) []goldenComponent
		stats   func(BuildStats) BuildStats
		capOnly bool
	}{
		{
			name: "flipped hash separator",
			mutate: func(components []goldenComponent) []goldenComponent {
				// "|" -> ":" in the join is the classic re-implementation slip.
				nodes := live[multiNode].Nodes
				tokens := make([]string, 0, len(nodes))
				for _, node := range nodes {
					tokens = append(tokens, node.Type+":"+node.ID)
				}
				sort.Strings(tokens)
				sum := sha256.Sum256([]byte(strings.Join(tokens, ":")))
				components[multiNode].WorkUnitID = hex.EncodeToString(sum[:])
				return components
			},
		},
		{
			name: "dropped a node type from the hashed set",
			mutate: func(components []goldenComponent) []goldenComponent {
				// The exact CHAOS-4441 trap: treating the node universe as
				// issue/pr/commit and silently discarding feature_flag,
				// incident, escalation_policy, operational_service nodes.
				components[multiNode].Nodes = components[multiNode].Nodes[1:]
				return components
			},
		},
		{
			name: "reordered two components",
			mutate: func(components []goldenComponent) []goldenComponent {
				components[0], components[1] = components[1], components[0]
				return components
			},
		},
		{
			name: "dropped one edge from a bundle",
			mutate: func(components []goldenComponent) []goldenComponent {
				for position, component := range components {
					if len(component.EdgeIDs) > 1 {
						components[position].EdgeIDs = component.EdgeIDs[1:]
						return components
					}
				}
				t.Fatal("no component with more than one edge; this mutation is vacuous")
				return components
			},
		},
		{
			name:   "off-by-one in the split edge-drop prefix",
			mutate: func(components []goldenComponent) []goldenComponent { return components },
			stats: func(stats BuildStats) BuildStats {
				stats.DroppedEdges++
				return stats
			},
		},
		{
			name:   "hub removal silently skipped",
			mutate: func(components []goldenComponent) []goldenComponent { return components },
			stats: func(stats BuildStats) BuildStats {
				stats.DroppedNodes = 0
				return stats
			},
		},
	} {
		t.Run(plant.name, func(t *testing.T) {
			mutated := plant.mutate(clone())
			mutatedStats := *stats
			if plant.stats != nil {
				mutatedStats = plant.stats(mutatedStats)
			}
			failures := diffAgainstGolden(golden, mutated, mutatedStats, maxComponentNodes)
			if len(failures) == 0 {
				t.Fatal("the comparator accepted a planted defect: it does not prove parity")
			}
		})
	}
}

// TestSplitCapIsLoadBearing is the implementation-level mutation: run the real
// builder with a cap that genuinely changes the answer and show the golden
// rejects the result. This proves the golden is sensitive to the SPLIT, not
// only to the connected-component grouping underneath it.
//
// The cap used is 155, not 151. A first version of this test used cap+1 and
// failed -- correctly. Python reproduces that: on this fixture caps 140, 150
// and 151 all yield exactly 1,601 components with identical stats, because the
// largest fragment the split leaves is 134 nodes, so every cap in that plateau
// is the same configuration. Recorded because it is operationally useful: the
// deployed value could be retuned anywhere in that band with no effect on any
// work_unit_id, and a "we changed the cap" claim in that range is a no-op.
func TestSplitCapIsLoadBearing(t *testing.T) {
	edges, golden := loadGoldenPair(t)

	widened := 155
	stats := &BuildStats{}
	live := BuildComponents(edges, &widened, stats)

	components := make([]goldenComponent, len(live))
	for position, component := range live {
		components[position] = toGoldenComponent(component)
	}
	if len(diffAgainstGolden(golden, components, *stats, golden.MaxComponentNodes)) == 0 {
		t.Fatal(
			"raising the cap to 155 changed nothing the golden can see; " +
				"the fixture does not actually constrain the split",
		)
	}
}

// TestBuildComponentsMatchesPythonAcrossCaps is a second cross-language check
// that costs no fixture bytes.
//
// The expected values were measured by running the DEPLOYED Python
// build_components over this same frozen input at each cap (2026-09-01). It
// covers what the single-cap golden cannot: that the split responds to the cap
// the same way on both planes, including the 140/150/151 plateau and the
// monotonic-in-the-right-direction behaviour of dropped_nodes as the cap moves.
func TestBuildComponentsMatchesPythonAcrossCaps(t *testing.T) {
	edges, _ := loadGoldenPair(t)

	for _, expected := range []struct {
		cap                 int
		components          int
		droppedNodes        int
		droppedEdges        int
		oversizedComponents int
		largestComponent    int
	}{
		{cap: 120, components: 1607, droppedNodes: 244, droppedEdges: 4, oversizedComponents: 1, largestComponent: 115},
		{cap: 140, components: 1601, droppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 150, components: 1601, droppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 151, components: 1601, droppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 155, components: 1598, droppedNodes: 241, droppedEdges: 4, oversizedComponents: 1, largestComponent: 155},
		{cap: 160, components: 1595, droppedNodes: 240, droppedEdges: 4, oversizedComponents: 1, largestComponent: 159},
	} {
		t.Run(fmt.Sprintf("cap_%d", expected.cap), func(t *testing.T) {
			maxNodes := expected.cap
			stats := &BuildStats{}
			live := BuildComponents(edges, &maxNodes, stats)

			if len(live) != expected.components {
				t.Errorf("components: python %d, go %d", expected.components, len(live))
			}
			if stats.DroppedNodes != expected.droppedNodes {
				t.Errorf("dropped_nodes: python %d, go %d", expected.droppedNodes, stats.DroppedNodes)
			}
			if stats.DroppedEdges != expected.droppedEdges {
				t.Errorf("dropped_edges: python %d, go %d", expected.droppedEdges, stats.DroppedEdges)
			}
			if stats.OversizedComponents != expected.oversizedComponents {
				t.Errorf("oversized_components: python %d, go %d", expected.oversizedComponents, stats.OversizedComponents)
			}
			largest := 0
			for _, component := range live {
				if len(component.Nodes) > largest {
					largest = len(component.Nodes)
				}
			}
			if largest != expected.largestComponent {
				t.Errorf("largest component: python %d, go %d", expected.largestComponent, largest)
			}
			if largest > expected.cap {
				t.Errorf("cap %d not enforced: largest component has %d nodes", expected.cap, largest)
			}
		})
	}
}

// TestFrozenGoldenExercisesTheHubRemovalPath guards the FIXTURE, not the code.
//
// This edge capture is the only input available that reaches phase (b) of the
// split -- the node-destroying removeHubs path (CHAOS-4758). A later capture
// taken after the edge-confidence policy change will not reach it at all, since
// that policy exists precisely to stop hub removal firing. If someone
// "refreshes" this fixture with newer data, every assertion above still passes
// while the most dangerous function in the package silently loses all coverage.
// This test makes that swap fail loudly instead.
func TestFrozenGoldenExercisesTheHubRemovalPath(t *testing.T) {
	_, golden := loadGoldenPair(t)

	if golden.Stats.OversizedComponents < 1 {
		t.Fatalf(
			"fixture %s no longer contains an oversized component: the split path is untested. "+
				"Do not replace this capture -- add a second one.",
			goldenEdgesFixture,
		)
	}
	if golden.Stats.DroppedNodes < 1 {
		t.Fatalf(
			"fixture %s no longer reaches removeHubs (dropped_nodes=%d): the node-destroying "+
				"phase (b) of the split is untested. Do not replace this capture -- add a second one.",
			goldenEdgesFixture, golden.Stats.DroppedNodes,
		)
	}
	if golden.Stats.DroppedEdges < 1 {
		t.Fatalf(
			"fixture %s no longer reaches the edge-drop phase (a) of the split", goldenEdgesFixture,
		)
	}
}

// TestWorkUnitIDMatchesPythonVectors pins the hash itself across the two
// languages, independently of the component builder.
//
// The expected digests were produced by running
// dev_health_ops.utils.normalization.work_unit_id on this checkout's Python
// (2026-09-01). The golden file proves grouping AND hashing together; this
// proves hashing alone, so a failure says which of the two moved. The vectors
// deliberately cover the cases a re-implementation gets wrong: a prefix-
// colliding node type, an id that itself contains the ':' and '|' delimiters,
// non-ASCII (the hash is over UTF-8 bytes), and the empty set.
func TestWorkUnitIDMatchesPythonVectors(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		nodes    []NodeKey
		expected string
	}{
		{
			name:     "prefix colliding types",
			nodes:    []NodeKey{{Type: "incident", ID: "z"}, {Type: "incident0", ID: "a"}},
			expected: "ce8bfdb9ee27df67e492cfc58d64825f36e8cb280888466df5d6762608db6879",
		},
		{
			name: "ids containing the delimiter",
			nodes: []NodeKey{
				{Type: "issue", ID: "linear:CHAOS-4441"},
				{Type: "pr", ID: "7b9583ee-4d24-2be7-4d09-34f815bebdd7#pr288"},
				{Type: "commit", ID: "7b9583ee-4d24-2be7-4d09-34f815bebdd7@deadbeef"},
			},
			expected: "9bb1503cd0255ca385533a1b2817bdf7f0ae74109727b8af6443e6291d07d640",
		},
		{
			// Also the first component of the frozen golden, so this vector
			// cross-checks the fixture against a hand-run of the producer.
			name:     "feature flag node",
			nodes:    []NodeKey{{Type: "feature_flag", ID: "29f8c3eb5b736ef6a33ea44be0ea4d892b3fedc9e7a65b23e2a95664e735a0b9"}},
			expected: "d0d4d6eabc8bd9296049a4ab50b60a1cb91cfeeb9071a2bf6283b359860d6f73",
		},
		{
			name:     "non ascii id",
			nodes:    []NodeKey{{Type: "issue", ID: "\u00fcn\u00efcod\u00e9:\u03a9"}},
			expected: "de21b64aef16a13819b41d17a3594231e069762414f68ebb6ef4cdbf5e4077a0",
		},
		{
			name:     "empty node set",
			nodes:    nil,
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := WorkUnitID(testCase.nodes); got != testCase.expected {
				t.Errorf("WorkUnitID = %s, python = %s", got, testCase.expected)
			}
			reversed := make([]NodeKey, 0, len(testCase.nodes))
			for position := len(testCase.nodes) - 1; position >= 0; position-- {
				reversed = append(reversed, testCase.nodes[position])
			}
			if got := WorkUnitID(reversed); got != testCase.expected {
				t.Errorf("WorkUnitID is not order-independent: %s", got)
			}
		})
	}
}

// TestWorkUnitIDSortsRenderedTokensNotPairs pins WHY the sort is over rendered
// tokens rather than (type, id) pairs.
//
// CORRECTION TO AN EARLIER CLAIM IN THIS PORT: I first asserted that the pair
// ordering diverges for the "incident" / "incident_timeline_event" pair this org
// actually carries. It does not, and this test caught me -- '_' (0x5F) sorts
// ABOVE ':' (0x3A), so for every node type currently in use the two orderings
// agree and the choice is unobservable today. The divergence is real but LATENT:
// it needs a type whose next character sorts BELOW ':', i.e. a digit. "incident"
// vs "incident0" token-sorts as [incident0:a, incident:z] and pair-sorts as
// [incident:z, incident0:a] -- different join order, different digest. Pinning
// it now means a future numeric-suffixed node type cannot silently re-address
// every unit that contains one.
func TestWorkUnitIDSortsRenderedTokensNotPairs(t *testing.T) {
	nodes := []NodeKey{{Type: "incident", ID: "z"}, {Type: "incident0", ID: "a"}}

	pairSorted := append([]NodeKey(nil), nodes...)
	sort.SliceStable(pairSorted, func(left, right int) bool { return nodeLess(pairSorted[left], pairSorted[right]) })

	if WorkUnitID(nodes) == hashJoinedTokens(pairSorted) {
		t.Fatal(
			"token-sorted and pair-sorted hashing agree on a digit-suffixed type pair; " +
				"this test can no longer detect the divergence it exists to pin",
		)
	}
}

// hashJoinedTokens hashes nodes in the order GIVEN, without WorkUnitID's sort,
// so a test can compare the digest of one ordering against another.
func hashJoinedTokens(nodes []NodeKey) string {
	tokens := make([]string, 0, len(nodes))
	for _, node := range nodes {
		tokens = append(tokens, node.Type+":"+node.ID)
	}
	sum := sha256.Sum256([]byte(strings.Join(tokens, "|")))
	return hex.EncodeToString(sum[:])
}

func toGoldenComponent(component Component) goldenComponent {
	nodes := make([][]string, 0, len(component.Nodes))
	for _, node := range component.Nodes {
		nodes = append(nodes, []string{node.Type, node.ID})
	}
	// The generator emits `sorted([list(node) for node in ...])`, i.e. Python
	// list comparison over [type, id]; sort the same way so the comparison is of
	// content, not of traversal order (which WorkUnitID makes irrelevant anyway).
	sort.SliceStable(nodes, func(left, right int) bool {
		if nodes[left][0] != nodes[right][0] {
			return nodes[left][0] < nodes[right][0]
		}
		return nodes[left][1] < nodes[right][1]
	})

	edgeIDs := make([]string, 0, len(component.Edges))
	seen := make(map[string]struct{}, len(component.Edges))
	for _, edge := range component.Edges {
		if edge.EdgeID == "" {
			continue
		}
		if _, ok := seen[edge.EdgeID]; ok {
			continue
		}
		seen[edge.EdgeID] = struct{}{}
		edgeIDs = append(edgeIDs, edge.EdgeID)
	}
	sort.Strings(edgeIDs)

	return goldenComponent{
		WorkUnitID: WorkUnitID(component.Nodes),
		Nodes:      nodes,
		EdgeIDs:    edgeIDs,
	}
}

func loadGoldenPair(t *testing.T) ([]Edge, goldenDocument) {
	t.Helper()
	root := repositoryRootPath(t)

	rawEdges, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", goldenEdgesFixture))
	if err != nil {
		t.Fatalf("read frozen edge input: %v", err)
	}
	var edgesDocument goldenEdgesDocument
	if err := json.Unmarshal(rawEdges, &edgesDocument); err != nil {
		t.Fatalf("decode frozen edge input: %v", err)
	}

	rawGolden, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", goldenFixture))
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(rawGolden, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}

	return decodeGoldenEdges(t, edgesDocument), golden
}

// decodeGoldenEdges rebuilds the edge rows from the columnar fixture, PRESERVING
// FILE ORDER. The order is the source query's ORDER BY over the full identity
// key and is load-bearing -- discoverComponents walks edges in input order.
func decodeGoldenEdges(t *testing.T, document goldenEdgesDocument) []Edge {
	t.Helper()
	column := make(map[string]int, len(document.Columns))
	for position, name := range document.Columns {
		column[name] = position
	}
	for _, required := range []string{
		"edge_id", "source_type", "source_id", "target_type", "target_id", "confidence",
	} {
		if _, ok := column[required]; !ok {
			t.Fatalf("frozen edge input is missing the %q column", required)
		}
	}

	text := func(row []any, name string) string {
		value, ok := row[column[name]].(string)
		if !ok {
			t.Fatalf("column %q is not a string in row %v", name, row)
		}
		return value
	}

	edges := make([]Edge, 0, len(document.Edges))
	for _, row := range document.Edges {
		if len(row) != len(document.Columns) {
			t.Fatalf("row has %d values but %d columns are declared", len(row), len(document.Columns))
		}
		edges = append(edges, Edge{
			EdgeID:     text(row, "edge_id"),
			SourceType: text(row, "source_type"),
			SourceID:   text(row, "source_id"),
			TargetType: text(row, "target_type"),
			TargetID:   text(row, "target_id"),
			Confidence: ConfidenceFromValue(row[column["confidence"]]),
		})
	}
	return edges
}

func renderForDiff(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<unrenderable: %v>", err)
	}
	if len(encoded) > 600 {
		return fmt.Sprintf("%s... (%d bytes)", encoded[:600], len(encoded))
	}
	return string(encoded)
}

// repositoryRootPath walks up from this package to the checkout root (the
// directory containing go.mod), matching the helper the repo_user_commit golden
// test established.
func repositoryRootPath(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}
