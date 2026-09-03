package units

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
// for CHAOS-4441's shared grouping core, adapted by CHAOS-4771, run with
// partitionHubs=true -- the FIXED behaviour, not what is live in production
// until the WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED flip (see
// BuildComponents's doc). See
// TestBuildComponentsMatchesFrozenPythonGoldenExhaustivelyLegacyDeletePath
// below for the partitionHubs=false counterpart -- what actually runs today.
//
// It compares EVERY golden component -- id, node set, edge bundle -- against
// live Go output, plus the split's counters and the cap. A subset assertion
// here would be a test that cannot fail in the way that matters: the whole
// hazard is that one component out of 1,601 groups differently and silently
// re-addresses rows in another table (backfill.py:113-127). See
// TestGoldenComparisonCatchesPlantedDefects for the falsification of this
// comparator itself.
//
// CHAOS-4771 changed what "matches" means: see diffAgainstGoldenPartitioned's
// doc for why the comparison is now keyed by work_unit_id instead of position.
func TestBuildComponentsMatchesFrozenPythonGoldenExhaustively(t *testing.T) {
	edges, golden := loadGoldenPair(t)

	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, true, stats)

	if golden.SourceEdges != goldenEdgesFixture {
		t.Fatalf(
			"golden was generated from %q but this test feeds it %q -- the pair is mismatched",
			golden.SourceEdges, goldenEdgesFixture,
		)
	}

	liveComponents := make([]goldenComponent, len(live))
	for position, component := range live {
		liveComponents[position] = toGoldenComponent(component)
	}

	for _, failure := range diffAgainstGoldenPartitioned(golden, liveComponents, *stats, maxComponentNodes) {
		t.Error(failure)
	}
}

// TestBuildComponentsMatchesFrozenPythonGoldenExhaustivelyLegacyDeletePath is
// the partitionHubs=false counterpart above: this is the exact pre-CHAOS-4771
// comparator (restored, not weakened), proving the flag's DEFAULT still
// bit-matches the LIVE Python materializer -- the property the flag-gate
// exists to protect until CHAOS-4924 cutover flips it.
func TestBuildComponentsMatchesFrozenPythonGoldenExhaustivelyLegacyDeletePath(t *testing.T) {
	edges, golden := loadGoldenPair(t)

	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, false, stats)

	if golden.SourceEdges != goldenEdgesFixture {
		t.Fatalf(
			"golden was generated from %q but this test feeds it %q -- the pair is mismatched",
			golden.SourceEdges, goldenEdgesFixture,
		)
	}

	liveComponents := make([]goldenComponent, len(live))
	for position, component := range live {
		liveComponents[position] = toGoldenComponent(component)
	}

	for _, failure := range diffAgainstGoldenLegacyDelete(golden, liveComponents, *stats, maxComponentNodes) {
		t.Error(failure)
	}
}

// diffAgainstGolden is the comparator, factored out of the test that uses it so
// TestGoldenComparisonCatchesPlantedDefects can falsify it directly. A
// comparator only asserted against a passing input is itself untested -- the
// differential-oracle lesson this repo already paid for (CHAOS-3033).
//
// CHAOS-4771 KEYS THE EXISTENCE/CONTENT CHECK BY work_unit_id, NOT RAW
// POSITION -- a deliberate change from the pre-fix comparator, not a loosening
// of it. Before this fix, live and golden were produced by the IDENTICAL
// algorithm, so positional equality and content equality were the same claim.
// Now that removeHubs partitions instead of deletes, live carries extra
// singleton components that the golden's own sorted-node-tuple ordering
// (components.py:301-303) can insert ANYWHERE in the list -- a single extra
// element near the front shifts every following RAW index, so comparing raw
// positions would report thousands of "mismatches" that are really just index
// drift, hiding any real defect in the noise.
//
// CORRECTED (codex round 1 on #2172, P2): an earlier version of this comment
// went further and dropped order-checking ENTIRELY, which was an
// overcorrection -- component order IS still load-bearing (partitioned
// materialization dispatches numeric component_indexes, chquery.go:24), and
// throwing the whole invariant away to dodge the singleton-shift problem let a
// real reordering bug (e.g. two components swapped) pass silently. What
// actually survives the fix, provably: BuildComponents's final sort is ONE
// stable total order over the combined (survivors + new singletons) list, so
// two survivors' RELATIVE order to each other cannot flip just because
// singletons were interleaved between them -- only an actual reordering bug
// can do that. So this comparator checks order too, on the golden-matching
// SUBSET of live (singletons excluded, since they have no golden position to
// compare against) -- see the ORDER check below, keyed by filtering live to
// golden's ids while preserving live's own sequence, not by raw index.
//
// The invariant that survives the fix, unconditionally: removeHubs's
// hub-selection loop and the reduced active graph it recomputes each round are
// BYTE-FOR-BYTE the pre-fix algorithm (see components.go) -- only what happens
// to a chosen hub changed. So every golden component must still appear,
// IDENTICALLY and in golden's relative ORDER, somewhere in live, and live's
// only permitted extra output is one singleton fragment per node the golden's
// own dropped_nodes counted.
//
// Returns one message per divergence; empty means parity.
func diffAgainstGoldenPartitioned(
	golden goldenDocument, live []goldenComponent, stats BuildStats, maxComponentNodes int,
) []string {
	failures := make([]string, 0)

	liveByID := make(map[string]goldenComponent, len(live))
	for _, component := range live {
		liveByID[component.WorkUnitID] = component
	}
	goldenByID := make(map[string]struct{}, len(golden.Components))
	for _, component := range golden.Components {
		goldenByID[component.WorkUnitID] = struct{}{}
	}

	missing := 0
	for _, expected := range golden.Components {
		got, ok := liveByID[expected.WorkUnitID]
		if ok && reflect.DeepEqual(expected, got) {
			continue
		}
		missing++
		if missing <= 8 {
			renderedGot := "<absent from live>"
			if ok {
				renderedGot = renderForDiff(got)
			}
			failures = append(failures, fmt.Sprintf(
				"golden component %s missing or changed:\npython: %s\ngo:     %s",
				expected.WorkUnitID, renderForDiff(expected), renderedGot,
			))
		}
	}
	if missing > 8 {
		failures = append(failures, fmt.Sprintf("... and %d further missing/changed golden components", missing-8))
	}

	// ORDER, among the golden-matching subset of live (codex round 1 on #2172,
	// P2): the earlier version of this comparator dropped position-checking
	// entirely, reasoning that the new singleton fragments can sort anywhere
	// and shift every following index -- true, but it threw out a real
	// invariant along with the shifting one. Component order is load-bearing:
	// partitioned materialization dispatches numeric component_indexes
	// (chquery.go:24) and each chunk worker re-derives the list, so index N
	// must name the same component on both planes. What survives the fix,
	// provably: BuildComponents's final sort is a single GLOBAL sort by node
	// key over the combined (survivors + new singletons) list -- a stable
	// total order, so two survivors whose relative order was A-before-B in
	// golden cannot come out B-before-A in live merely because singletons were
	// interleaved between them; only an ACTUAL reordering bug could do that.
	// So: filter live down to just the components matching a golden
	// work_unit_id, preserve live's own order, and assert that sequence
	// equals golden.Components exactly -- singletons are correctly excluded
	// here (they have no golden counterpart) and checked separately below.
	// ORDER, among the golden-matching subset of live (codex round 1 on #2172,
	// P2): the earlier version of this comparator dropped position-checking
	// entirely, reasoning that the new singleton fragments can sort anywhere
	// and shift every following index -- true, but it threw out a real
	// invariant along with the shifting one. Component order is load-bearing:
	// partitioned materialization dispatches numeric component_indexes
	// (chquery.go:24) and each chunk worker re-derives the list, so index N
	// must name the same component on both planes. What survives the fix,
	// provably: BuildComponents's final sort is a single GLOBAL sort by node
	// key over the combined (survivors + new singletons) list -- a stable
	// total order, so two survivors whose relative order was A-before-B in
	// golden cannot come out B-before-A in live merely because singletons were
	// interleaved between them; only an ACTUAL reordering bug could do that.
	// So: filter live down to just the components matching a golden
	// work_unit_id, preserve live's own order, and assert that sequence
	// equals golden.Components exactly -- singletons are correctly excluded
	// here (they have no golden counterpart) and checked separately below.
	matchingLiveInOrder := make([]goldenComponent, 0, len(golden.Components))
	for _, component := range live {
		if _, inGolden := goldenByID[component.WorkUnitID]; inGolden {
			matchingLiveInOrder = append(matchingLiveInOrder, component)
		}
	}
	reordered := 0
	for position := range golden.Components {
		if position >= len(matchingLiveInOrder) {
			break // component-count mismatch is reported separately below.
		}
		if golden.Components[position].WorkUnitID != matchingLiveInOrder[position].WorkUnitID {
			reordered++
			if reordered <= 8 {
				failures = append(failures, fmt.Sprintf(
					"component order: golden position %d is %s, go's matching-subset position %d is %s",
					position, golden.Components[position].WorkUnitID, position, matchingLiveInOrder[position].WorkUnitID,
				))
			}
		}
	}
	if reordered > 8 {
		failures = append(failures, fmt.Sprintf("... and %d further order mismatches", reordered-8))
	}

	// live's only permitted addition over golden: one singleton fragment
	// (exactly one node, zero edges) per node golden.Stats.DroppedNodes counted.
	// Anything else extra -- a multi-node component, or an edge on a
	// "singleton" -- is a real divergence, not the partition fix, and a count
	// that doesn't match dropped_nodes means a hub was invented or lost.
	extraSingletons := 0
	unexpectedExtras := 0
	for _, component := range live {
		if _, inGolden := goldenByID[component.WorkUnitID]; inGolden {
			continue
		}
		if len(component.Nodes) == 1 && len(component.EdgeIDs) == 0 {
			extraSingletons++
			continue
		}
		unexpectedExtras++
		if unexpectedExtras <= 8 {
			failures = append(failures, fmt.Sprintf(
				"unexpected non-singleton component absent from golden: %s", renderForDiff(component),
			))
		}
	}
	if unexpectedExtras > 8 {
		failures = append(failures, fmt.Sprintf("... and %d further unexpected extra components", unexpectedExtras-8))
	}
	if extraSingletons != golden.Stats.DroppedNodes {
		failures = append(failures, fmt.Sprintf(
			"extra singleton components: golden dropped_nodes %d, go produced %d",
			golden.Stats.DroppedNodes, extraSingletons,
		))
	}
	if len(live) != len(golden.Components)+golden.Stats.DroppedNodes {
		failures = append(failures, fmt.Sprintf(
			"component count: expected %d (golden %d + dropped_nodes %d), go %d",
			len(golden.Components)+golden.Stats.DroppedNodes, len(golden.Components), golden.Stats.DroppedNodes, len(live),
		))
	}

	// DroppedNodes is CHAOS-4771's regression guard: nodes are partitioned, not
	// deleted, post-fix, so this must read 0 regardless of what golden recorded
	// (see BuildStats's doc). PartitionedHubs is the new signal and must equal
	// exactly what golden called dropped_nodes -- same hubs, different fate.
	expectedStats := BuildStats{
		OversizedComponents: golden.Stats.OversizedComponents,
		DroppedEdges:        golden.Stats.DroppedEdges,
		DroppedNodes:        0,
		PartitionedHubs:     golden.Stats.DroppedNodes,
	}
	if stats != expectedStats {
		failures = append(failures, fmt.Sprintf("stats: expected %+v, go %+v", expectedStats, stats))
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

// diffAgainstGoldenLegacyDelete is the comparator for partitionHubs=false --
// the flag's default, and what actually runs in production until
// WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED flips (see BuildComponents's
// doc). This IS the pre-CHAOS-4771 comparator, restored rather than weakened:
// on this branch live and golden are STILL produced by the identical
// algorithm, so positional equality and content equality are still the same
// claim, and a work_unit_id-keyed comparison (diffAgainstGoldenPartitioned)
// would be a strictly weaker check here for no reason -- it stays positional
// on purpose.
func diffAgainstGoldenLegacyDelete(
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
		PartitionedHubs:     0,
	}
	if stats != expectedStats {
		failures = append(failures, fmt.Sprintf("stats: python %+v, go %+v", expectedStats, stats))
	}

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

// TestGoldenComparisonCatchesPlantedDefects falsifies the PARTITIONED
// (partitionHubs=true) comparator. See
// TestLegacyDeleteComparisonCatchesPlantedDefects below for the legacy
// (partitionHubs=false) counterpart.
//
// Every mutation below is a real way this port could be wrong, applied to a
// KNOWN-GOOD live result. If any of them slips through, the exhaustive test
// above is decorative and the parity claim in the PR body is worthless.
func TestGoldenComparisonCatchesPlantedDefects(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, true, stats)

	baseline := make([]goldenComponent, len(live))
	for position, component := range live {
		baseline[position] = toGoldenComponent(component)
	}
	if failures := diffAgainstGoldenPartitioned(golden, baseline, *stats, maxComponentNodes); len(failures) != 0 {
		t.Fatalf("baseline is not clean, cannot falsify from it: %v", failures)
	}

	clone := func() []goldenComponent {
		copied := make([]goldenComponent, len(baseline))
		for position, component := range baseline {
			copied[position] = goldenComponent{
				WorkUnitID: component.WorkUnitID,
				// slices.Clone, not append(T(nil), s...): the latter silently
				// downgrades a non-nil EMPTY slice to nil (append with zero
				// elements to add returns its nil-vs-not input verbatim through
				// no realloc), which made every planted-defect mutation below
				// pass for the WRONG reason -- reflect.DeepEqual sees
				// "edge_ids": null diverge from golden's "edge_ids": [] on
				// whichever zero-edge component this clone happened to
				// disturb, regardless of what the mutation under test actually
				// changed (found while adding the reordered-components case:
				// it "passed" even with the order check deleted). slices.Clone
				// preserves nil vs non-nil-empty exactly.
				Nodes:   slices.Clone(component.Nodes),
				EdgeIDs: slices.Clone(component.EdgeIDs),
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
			// codex round 1 on #2172, P2: the ORDER check this mutation targets
			// did not exist before this round -- swapping two golden-matching
			// (non-singleton) components passed silently, while
			// component_indexes is numeric and load-bearing downstream
			// (chquery.go:24). Swaps the FIRST TWO golden-matching components it
			// finds, deliberately not touching any singleton (a singleton swap
			// would be a no-op for the order check, since singletons have no
			// golden position to compare against).
			name: "reordered two golden-matching components",
			mutate: func(components []goldenComponent) []goldenComponent {
				goldenIDs := make(map[string]struct{}, len(golden.Components))
				for _, component := range golden.Components {
					goldenIDs[component.WorkUnitID] = struct{}{}
				}
				first := -1
				for position, component := range components {
					if _, inGolden := goldenIDs[component.WorkUnitID]; !inGolden {
						continue
					}
					if first < 0 {
						first = position
						continue
					}
					components[first], components[position] = components[position], components[first]
					return components
				}
				t.Fatal("fewer than two golden-matching components; this mutation is vacuous")
				return components
			},
		},
		{
			// CHAOS-4771's own regression: a partitioned hub silently reverted to
			// the pre-fix deletion. If this slips through, the comparator cannot
			// tell the fix from the bug it replaced.
			name: "a partitioned hub reverted to deletion",
			mutate: func(components []goldenComponent) []goldenComponent {
				goldenIDs := make(map[string]struct{}, len(golden.Components))
				for _, component := range golden.Components {
					goldenIDs[component.WorkUnitID] = struct{}{}
				}
				for position, component := range components {
					if _, inGolden := goldenIDs[component.WorkUnitID]; inGolden {
						continue
					}
					return append(components[:position], components[position+1:]...)
				}
				t.Fatal("no extra (hub) component to delete; this mutation is vacuous")
				return components
			},
		},
		{
			// A spurious component neither the golden nor a legitimate partitioned
			// hub explains -- e.g. a hub duplicated into two fragments instead of
			// one.
			name: "an unexplained extra component invented",
			mutate: func(components []goldenComponent) []goldenComponent {
				return append(components, goldenComponent{
					WorkUnitID: "invented-not-a-real-work-unit-id",
					Nodes:      [][]string{{"issue", "invented-node-a"}, {"issue", "invented-node-b"}},
				})
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
			// Post-fix, DroppedNodes is expected to be 0 unconditionally (see
			// BuildStats's doc) -- so a REAPPEARING nonzero value, or a
			// PartitionedHubs count that stops matching golden's dropped_nodes,
			// is the defect to catch now.
			name:   "PartitionedHubs count drifts from golden's dropped_nodes",
			mutate: func(components []goldenComponent) []goldenComponent { return components },
			stats: func(stats BuildStats) BuildStats {
				stats.PartitionedHubs++
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
			failures := diffAgainstGoldenPartitioned(golden, mutated, mutatedStats, maxComponentNodes)
			if len(failures) == 0 {
				t.Fatal("the comparator accepted a planted defect: it does not prove parity")
			}
		})
	}
}

// TestLegacyDeleteComparisonCatchesPlantedDefects falsifies
// diffAgainstGoldenLegacyDelete (partitionHubs=false), mirroring
// TestGoldenComparisonCatchesPlantedDefects above. Reuses the mutation shapes
// that apply unchanged to a positional comparator; skips the two
// partition-specific ones (a hub reverted to deletion, or a spurious extra
// component) that only make sense once removeHubs can add fragments -- this
// comparator's live path never does, so those two would be vacuous here. Adds
// one legacy-specific case instead: dropped_nodes silently forced to 0, the
// exact CHAOS-4758 regression this comparator exists to still catch on the
// path that is actually live in production today.
func TestLegacyDeleteComparisonCatchesPlantedDefects(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, false, stats)

	baseline := make([]goldenComponent, len(live))
	for position, component := range live {
		baseline[position] = toGoldenComponent(component)
	}
	if failures := diffAgainstGoldenLegacyDelete(golden, baseline, *stats, maxComponentNodes); len(failures) != 0 {
		t.Fatalf("baseline is not clean, cannot falsify from it: %v", failures)
	}

	clone := func() []goldenComponent {
		copied := make([]goldenComponent, len(baseline))
		for position, component := range baseline {
			copied[position] = goldenComponent{
				WorkUnitID: component.WorkUnitID,
				// slices.Clone, not append(T(nil), s...): the latter silently
				// downgrades a non-nil EMPTY slice to nil (append with zero
				// elements to add returns its nil-vs-not input verbatim through
				// no realloc), which made every planted-defect mutation below
				// pass for the WRONG reason -- reflect.DeepEqual sees
				// "edge_ids": null diverge from golden's "edge_ids": [] on
				// whichever zero-edge component this clone happened to
				// disturb, regardless of what the mutation under test actually
				// changed (found while adding the reordered-components case:
				// it "passed" even with the order check deleted). slices.Clone
				// preserves nil vs non-nil-empty exactly.
				Nodes:   slices.Clone(component.Nodes),
				EdgeIDs: slices.Clone(component.EdgeIDs),
			}
		}
		return copied
	}
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
		name   string
		mutate func([]goldenComponent) []goldenComponent
		stats  func(BuildStats) BuildStats
	}{
		{
			name: "flipped hash separator",
			mutate: func(components []goldenComponent) []goldenComponent {
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
			// The CHAOS-4758 regression itself: hub deletion silently stops
			// happening (or stops being counted) on the path that is actually
			// live in production. This is the one case that most needs this
			// comparator, since it is the only one still watching this branch.
			name:   "hub deletion silently skipped or uncounted",
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
			failures := diffAgainstGoldenLegacyDelete(golden, mutated, mutatedStats, maxComponentNodes)
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
	live := BuildComponents(edges, &widened, true, stats)

	components := make([]goldenComponent, len(live))
	for position, component := range live {
		components[position] = toGoldenComponent(component)
	}
	if len(diffAgainstGoldenPartitioned(golden, components, *stats, golden.MaxComponentNodes)) == 0 {
		t.Fatal(
			"raising the cap to 155 changed nothing the golden can see; " +
				"the fixture does not actually constrain the split",
		)
	}
}

// TestSplitCapIsLoadBearingLegacyDeletePath mirrors the above for
// partitionHubs=false -- the cap must still be load-bearing on the branch
// that is actually live in production.
func TestSplitCapIsLoadBearingLegacyDeletePath(t *testing.T) {
	edges, golden := loadGoldenPair(t)

	widened := 155
	stats := &BuildStats{}
	live := BuildComponents(edges, &widened, false, stats)

	components := make([]goldenComponent, len(live))
	for position, component := range live {
		components[position] = toGoldenComponent(component)
	}
	if len(diffAgainstGoldenLegacyDelete(golden, components, *stats, golden.MaxComponentNodes)) == 0 {
		t.Fatal(
			"raising the cap to 155 changed nothing the golden can see; " +
				"the fixture does not actually constrain the split",
		)
	}
}

// TestBuildComponentsMatchesPythonAcrossCaps is a second cross-language check
// that costs no fixture bytes.
//
// pythonDroppedNodes and pythonComponents were measured by running the
// DEPLOYED (pre-fix) Python build_components over this same frozen input at
// each cap (2026-09-01) -- kept as the reference for what phase (a) and the
// oversized-component discovery do, which CHAOS-4771 does not touch. It covers
// what the single-cap golden cannot: that the split responds to the cap the
// same way on both planes, including the 140/150/151 plateau.
//
// CHAOS-4771: post-fix Go no longer matches pythonComponents directly -- it
// additionally emits one singleton fragment per node Python would have
// dropped, so the expected Go count is pythonComponents+pythonDroppedNodes,
// stats.DroppedNodes must read 0, and stats.PartitionedHubs must equal
// pythonDroppedNodes exactly (same hubs found, different fate). See
// diffAgainstGolden's doc for why this is the correct post-fix contract.
func TestBuildComponentsMatchesPythonAcrossCaps(t *testing.T) {
	edges, _ := loadGoldenPair(t)

	for _, expected := range []struct {
		cap                 int
		pythonComponents    int
		pythonDroppedNodes  int
		droppedEdges        int
		oversizedComponents int
		largestComponent    int
	}{
		{cap: 120, pythonComponents: 1607, pythonDroppedNodes: 244, droppedEdges: 4, oversizedComponents: 1, largestComponent: 115},
		{cap: 140, pythonComponents: 1601, pythonDroppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 150, pythonComponents: 1601, pythonDroppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 151, pythonComponents: 1601, pythonDroppedNodes: 242, droppedEdges: 4, oversizedComponents: 1, largestComponent: 134},
		{cap: 155, pythonComponents: 1598, pythonDroppedNodes: 241, droppedEdges: 4, oversizedComponents: 1, largestComponent: 155},
		{cap: 160, pythonComponents: 1595, pythonDroppedNodes: 240, droppedEdges: 4, oversizedComponents: 1, largestComponent: 159},
	} {
		// Distinct input-node universe, computed once per cap iteration (the
		// input `edges` is the same fixture throughout, so this is invariant
		// across both branches below -- computed here rather than twice).
		inputNodes := make(map[NodeKey]struct{})
		for _, edge := range edges {
			inputNodes[edge.source()] = struct{}{}
			inputNodes[edge.target()] = struct{}{}
		}

		t.Run(fmt.Sprintf("cap_%d/partitioned", expected.cap), func(t *testing.T) {
			maxNodes := expected.cap
			stats := &BuildStats{}
			live := BuildComponents(edges, &maxNodes, true, stats)

			expectedGoComponents := expected.pythonComponents + expected.pythonDroppedNodes
			if len(live) != expectedGoComponents {
				t.Errorf("components: expected %d (python %d + dropped %d), go %d",
					expectedGoComponents, expected.pythonComponents, expected.pythonDroppedNodes, len(live))
			}
			if stats.DroppedNodes != 0 {
				t.Errorf("dropped_nodes: expected 0 (partitionHubs=true partitions, never deletes), go %d", stats.DroppedNodes)
			}
			if stats.PartitionedHubs != expected.pythonDroppedNodes {
				t.Errorf("partitioned_hubs: expected %d (= python dropped_nodes), go %d", expected.pythonDroppedNodes, stats.PartitionedHubs)
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

			// Node conservation (CHAOS-4771 acceptance #2): every node the split
			// saw lands in exactly one output fragment. Cheapest place to check
			// it per-cap is a sum, since every fragment here is disjoint by
			// construction (connectedComponents partitions its input node set).
			totalNodes := 0
			for _, component := range live {
				totalNodes += len(component.Nodes)
			}
			if totalNodes != len(inputNodes) {
				t.Errorf("node conservation: %d distinct input nodes, %d nodes across all output fragments", len(inputNodes), totalNodes)
			}
		})

		// The gate's counterpart: prove the LEGACY (default, live-in-production)
		// branch still matches the deployed Python numbers exactly, at every cap
		// this fixture is measured at -- not just the single cap the exhaustive
		// golden test covers.
		t.Run(fmt.Sprintf("cap_%d/legacy_delete", expected.cap), func(t *testing.T) {
			maxNodes := expected.cap
			stats := &BuildStats{}
			live := BuildComponents(edges, &maxNodes, false, stats)

			if len(live) != expected.pythonComponents {
				t.Errorf("components: python %d, go %d", expected.pythonComponents, len(live))
			}
			if stats.DroppedNodes != expected.pythonDroppedNodes {
				t.Errorf("dropped_nodes: python %d, go %d", expected.pythonDroppedNodes, stats.DroppedNodes)
			}
			if stats.PartitionedHubs != 0 {
				t.Errorf("partitioned_hubs: expected 0 (partitionHubs=false never partitions), go %d", stats.PartitionedHubs)
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

			// Contrast with the partitioned branch above: on THIS branch, node
			// conservation is expected to FAIL, by exactly pythonDroppedNodes --
			// that shortfall IS the CHAOS-4758 defect, still live on this path
			// until the flag flips. Asserting the shortfall (not just noting it)
			// keeps this test from silently passing if the legacy path ever stops
			// dropping nodes for the wrong reason (e.g. a bug that also breaks
			// node conservation in a DIFFERENT way that happens to sum to zero).
			totalNodes := 0
			for _, component := range live {
				totalNodes += len(component.Nodes)
			}
			shortfall := len(inputNodes) - totalNodes
			if shortfall != expected.pythonDroppedNodes {
				t.Errorf(
					"legacy path's node shortfall: expected %d (= python dropped_nodes), got %d (%d input nodes, %d output nodes)",
					expected.pythonDroppedNodes, shortfall, len(inputNodes), totalNodes,
				)
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

// TestPartitionFixLeavesNoNodeBehind is CHAOS-4771 acceptance #1 and #2 on the
// SAME fixture TestFrozenGoldenExercisesTheHubRemovalPath just proved reaches
// removeHubs: node conservation is asserted as the INVARIANT (every distinct
// input node appears in EXACTLY ONE output fragment), not as a counter reading
// zero -- a counter that reads zero is not the same claim as a counter that
// cannot be wrong, because a bug that both drops one node and double-counts
// another can leave a naive counter at zero.
func TestPartitionFixLeavesNoNodeBehind(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	if golden.Stats.DroppedNodes < 1 {
		t.Fatalf("fixture no longer reaches removeHubs; this test needs a run that does (see TestFrozenGoldenExercisesTheHubRemovalPath)")
	}

	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, true, stats)

	if stats.DroppedNodes != 0 {
		t.Errorf("dropped_nodes: expected 0 post-fix, got %d", stats.DroppedNodes)
	}
	if stats.PartitionedHubs != golden.Stats.DroppedNodes {
		t.Errorf("partitioned_hubs: expected %d (= golden's pre-fix dropped_nodes), got %d", golden.Stats.DroppedNodes, stats.PartitionedHubs)
	}

	inputNodes := make(map[NodeKey]struct{})
	for _, edge := range edges {
		inputNodes[edge.source()] = struct{}{}
		inputNodes[edge.target()] = struct{}{}
	}

	seenIn := make(map[NodeKey]int)
	totalOutputNodes := 0
	for _, component := range live {
		if len(component.Nodes) > maxComponentNodes {
			t.Errorf("cap %d not enforced: a component has %d nodes", maxComponentNodes, len(component.Nodes))
		}
		for _, node := range component.Nodes {
			seenIn[node]++
			totalOutputNodes++
		}
	}

	if totalOutputNodes != len(inputNodes) {
		t.Errorf("node conservation: %d distinct input nodes, %d nodes across all output fragments", len(inputNodes), totalOutputNodes)
	}
	for node := range inputNodes {
		if count := seenIn[node]; count != 1 {
			t.Errorf("node %+v appears in %d output fragments, want exactly 1", node, count)
		}
	}
	for node := range seenIn {
		if _, wasInput := inputNodes[node]; !wasInput {
			t.Errorf("node %+v appears in output but was never in the input", node)
		}
	}
}

// TestLegacyDeletePathStillDropsNodes is the CONTRAST to
// TestPartitionFixLeavesNoNodeBehind above, on the SAME fixture: this is
// "node-conservation test on the gated branch" the other direction -- proving
// the flag's DEFAULT (partitionHubs=false, what is actually live today) still
// reproduces the CHAOS-4758 data loss exactly, not accidentally fixed by some
// other change. A gate whose "off" position quietly stopped dropping nodes
// would defeat the whole reason it exists: two consumers could then disagree
// about whether hubs are dropped for a reason unrelated to the flag they both
// read.
func TestLegacyDeletePathStillDropsNodes(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	if golden.Stats.DroppedNodes < 1 {
		t.Fatalf("fixture no longer reaches removeHubs; this test needs a run that does (see TestFrozenGoldenExercisesTheHubRemovalPath)")
	}

	stats := &BuildStats{}
	maxComponentNodes := golden.MaxComponentNodes
	live := BuildComponents(edges, &maxComponentNodes, false, stats)

	if stats.DroppedNodes != golden.Stats.DroppedNodes {
		t.Errorf("dropped_nodes: expected %d (matching golden, unchanged), got %d", golden.Stats.DroppedNodes, stats.DroppedNodes)
	}
	if stats.PartitionedHubs != 0 {
		t.Errorf("partitioned_hubs: expected 0 (partitionHubs=false never partitions), got %d", stats.PartitionedHubs)
	}

	inputNodes := make(map[NodeKey]struct{})
	for _, edge := range edges {
		inputNodes[edge.source()] = struct{}{}
		inputNodes[edge.target()] = struct{}{}
	}
	totalOutputNodes := 0
	for _, component := range live {
		totalOutputNodes += len(component.Nodes)
	}
	shortfall := len(inputNodes) - totalOutputNodes
	if shortfall != golden.Stats.DroppedNodes {
		t.Errorf(
			"node shortfall: expected %d (= golden's dropped_nodes), got %d (%d input nodes, %d output nodes)",
			golden.Stats.DroppedNodes, shortfall, len(inputNodes), totalOutputNodes,
		)
	}
}

// TestBuildComponentsIsDeterministic guards the component-ORDER contract
// CHAOS-4771 could not preserve against the pre-fix Python golden's position
// (see diffAgainstGolden's doc) by proving it a different way: run-to-run
// stability. Partitioned materialization dispatches numeric component_indexes
// and each chunk worker re-derives the list (queries.py:46-51), so index N
// must name the same component every time BuildComponents runs on the same
// input -- a property this checks directly rather than by comparison to a
// reference that no longer has an opinion about where the new singleton
// fragments belong.
func TestBuildComponentsIsDeterministic(t *testing.T) {
	edges, golden := loadGoldenPair(t)
	maxComponentNodes := golden.MaxComponentNodes

	for _, partitionHubs := range []bool{true, false} {
		t.Run(fmt.Sprintf("partitionHubs=%v", partitionHubs), func(t *testing.T) {
			first := BuildComponents(edges, &maxComponentNodes, partitionHubs, &BuildStats{})
			second := BuildComponents(edges, &maxComponentNodes, partitionHubs, &BuildStats{})

			if len(first) != len(second) {
				t.Fatalf("component count is not stable across runs: %d then %d", len(first), len(second))
			}
			for position := range first {
				firstRendered := toGoldenComponent(first[position])
				secondRendered := toGoldenComponent(second[position])
				if !reflect.DeepEqual(firstRendered, secondRendered) {
					t.Fatalf("component %d is not stable across runs:\nfirst:  %s\nsecond: %s",
						position, renderForDiff(firstRendered), renderForDiff(secondRendered))
				}
			}
		})
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
