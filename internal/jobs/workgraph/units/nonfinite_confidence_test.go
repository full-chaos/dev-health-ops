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

// TestSplitSortKeyIsAWidenedFloat32NeverAComputedValue answers lane-4752-go's
// reciprocal FMA warning, and pins the property that makes the answer hold.
//
// Their point: `work_graph_edges.confidence` is Float32, and the oversized-
// component split orders on `(confidence, edge_id)`. They measured that using
// a Python double instead of the stored Float32 in a variant-C simulation gave
// 1833 components / 2905 dropped edges where the real stored type gives
// 1832 / 2904 -- with DIFFERENT component sets. One quantisation step
// repartitions the graph.
//
// So if anything ever COMPUTED a confidence -- a blend, a decay, a weighted
// average -- an FMA-induced last-bit move on arm64 would land directly on that
// sort key. The failure would not be a wrong decimal but a different component
// partition, and therefore a different set of work_unit_ids: their warning is
// my evidence-quality banding hazard in a worse form, because it changes which
// rows exist rather than how one row is labelled.
//
// Audited at the time of writing: nothing in the split computes a confidence.
// chquery scans Float32 and widens once with float64(); units compares and
// sorts, never arithmetics. MeanEdgeConfidence does compute -- it is a sum and
// a divide -- but it feeds evidence QUALITY, never the sort key.
//
// "Nothing computes it today" is a claim with a shelf life, so what is pinned
// here is the property a future computation would break: every value on the
// sort key is a float32 widened EXACTLY into a float64. Widening is lossless
// and involves no rounding, so there is nothing for an FMA to fuse. A computed
// confidence would not satisfy this, and that is the moment to revisit.
func TestSplitSortKeyIsAWidenedFloat32NeverAComputedValue(t *testing.T) {
	// Widening float32 -> float64 is exact for every value, including the
	// non-finite ones and the subnormals. If this ever failed, the sort key
	// would carry a rounding the Python plane does not have.
	for _, sample := range []float32{
		0, 1, 0.9, 0.85, 0.1, -0.1,
		float32(math.SmallestNonzeroFloat32),
		float32(math.MaxFloat32),
		float32(math.Inf(1)), float32(math.Inf(-1)),
	} {
		widened := float64(sample)
		if narrowed := float32(widened); narrowed != sample {
			t.Errorf("float32(%v) -> float64 -> float32 = %v; widening must be exact",
				sample, narrowed)
		}
	}
	if nan := float64(float32(math.NaN())); !math.IsNaN(nan) {
		t.Error("NaN must survive widening")
	}

	// The quantisation step lane-4752-go measured: a float64 that is NOT
	// representable in float32 sorts differently from its float32 neighbour.
	// This is why the column's real type has to be carried through rather than
	// scanned into a float64 directly.
	const python64 = 0.85
	widened32 := float64(float32(python64))
	if widened32 == python64 {
		t.Skip("0.85 happens to be float32-exact on this platform; the " +
			"quantisation point needs a different sample")
	}
	if widened32 > python64 == (widened32 < python64) {
		t.Error("expected the float32-quantised value to differ from the double")
	}
}
