package units

import (
	"math"
	"sort"
	"strings"
	"testing"
)

// Non-finite confidences are reachable in production: work_graph_edges.confidence
// is a plain `Float32`, not `Nullable(Float32)` with a checked writer, so a NaN
// or an infinity can be stored and read back. The frozen golden cannot catch any
// divergence they cause -- all 6,040 of its confidences are finite (1.0 or
// 0.8999999761581421) -- so the split's treatment of them is pinned here
// against separately measured Python output.
//
// Expected values were produced by running the DEPLOYED Python
// work_graph/investment/components.py build_components over each construction
// (2026-09-01), not by reading Go's answer back.
//
// The reason this matters is that Python's partition of a component's edges is
// written as two INDEPENDENT comprehensions, not an if/else:
//
//	protected  = [e for e in edges if conf(e) >= max_confidence]
//	droppable  = [e for e in edges if conf(e) <  max_confidence]
//
// For NaN both predicates are false, so the edge lands in NEITHER list and is
// silently excluded from grouping. A Go port written as the natural
// `if conf >= max { protected } else { droppable }` routes it to droppable
// instead, which changes which edges survive the split and therefore changes
// work_unit_ids -- silently, since the two tables that share those ids are
// written by two different jobs.
func TestSplitMatchesPythonOnNonFiniteConfidence(t *testing.T) {
	edge := func(id, source, target string, confidence float64) Edge {
		return Edge{
			EdgeID: id, SourceType: "issue", SourceID: source,
			TargetType: "issue", TargetID: target, Confidence: confidence,
		}
	}

	for _, testCase := range []struct {
		name     string
		edges    []Edge
		cap      int
		expected []string // "node,node|edge,edge" per component, canonically sorted
		stats    BuildStats
		why      string
	}{
		{
			// NaN arrives FIRST, so Python's max() -- which keeps its running
			// result whenever `candidate > result` is false, and every
			// comparison against NaN is false -- returns NaN itself. Both
			// partition predicates then fail for every edge, so the whole
			// component is stripped of edges and shatters into singletons.
			name: "nan first poisons max_confidence and excludes every edge",
			edges: []Edge{
				edge("z", "a", "b", math.NaN()),
				edge("a", "b", "c", 1.0),
				edge("m", "c", "d", 0.5),
			},
			cap:      2,
			expected: []string{"issue:a|", "issue:b|", "issue:c|", "issue:d|"},
			stats:    BuildStats{OversizedComponents: 1, DroppedEdges: 0, DroppedNodes: 0},
			why:      "max_confidence is NaN, so protected and droppable are both empty",
		},
		{
			// A finite max, with one NaN edge in the middle: only the NaN edge
			// is excluded. This isolates the partition branch from the
			// max-poisoning above -- the COMPONENTS come out the same either
			// way here, and only dropped_edges (0 vs 1) reveals a wrong port.
			name: "nan with a finite max is excluded from both partitions",
			edges: []Edge{
				edge("e1", "a", "b", 1.0),
				edge("e2", "b", "c", math.NaN()),
				edge("e3", "c", "d", 0.5),
			},
			cap:      2,
			expected: []string{"issue:a,issue:b|e1", "issue:c,issue:d|e3"},
			stats:    BuildStats{OversizedComponents: 1, DroppedEdges: 0, DroppedNodes: 0},
			why:      "the NaN edge is in neither partition, so it never reaches kept_edges",
		},
		{
			// Control: +Inf is ordinary. It compares normally, becomes the max,
			// and its edge is protected while the finite ones are droppable.
			// Included so a fix for NaN cannot quietly break infinities.
			name: "positive infinity behaves as an ordinary maximum",
			edges: []Edge{
				edge("e1", "a", "b", math.Inf(1)),
				edge("e2", "b", "c", 1.0),
				edge("e3", "c", "d", 0.5),
			},
			cap:      2,
			expected: []string{"issue:a,issue:b|e1", "issue:c|", "issue:d|"},
			stats:    BuildStats{OversizedComponents: 1, DroppedEdges: 2, DroppedNodes: 0},
			why:      "+Inf is the max, so its edge is protected and both finite edges drop",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			maxNodes := testCase.cap
			stats := &BuildStats{}
			live := BuildComponents(testCase.edges, &maxNodes, stats)

			rendered := make([]string, 0, len(live))
			for _, component := range live {
				nodes := make([]string, 0, len(component.Nodes))
				for _, node := range component.Nodes {
					nodes = append(nodes, node.Type+":"+node.ID)
				}
				sort.Strings(nodes)
				edgeIDs := make([]string, 0, len(component.Edges))
				for _, componentEdge := range component.Edges {
					edgeIDs = append(edgeIDs, componentEdge.EdgeID)
				}
				sort.Strings(edgeIDs)
				rendered = append(rendered, strings.Join(nodes, ",")+"|"+strings.Join(edgeIDs, ","))
			}
			sort.Strings(rendered)

			expected := append([]string(nil), testCase.expected...)
			sort.Strings(expected)

			if strings.Join(rendered, "  ") != strings.Join(expected, "  ") {
				t.Errorf(
					"components differ from Python (%s):\npython: %v\ngo:     %v",
					testCase.why, expected, rendered,
				)
			}
			if *stats != testCase.stats {
				t.Errorf("stats differ from Python: python %+v, go %+v", testCase.stats, *stats)
			}
		})
	}
}
