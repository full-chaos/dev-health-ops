package units

import (
	"sort"
	"strconv"
)

// Edge is one deduplicated work_graph_edges row, reduced to the fields the
// component builder reads.
//
// Confidence is float64 here while the ClickHouse column is Float32. That is
// deliberate and is the same widening Python gets: its driver hands
// _edge_confidence a Python float (an IEEE double) holding the exact float32
// value. Decoders MUST read the column as float32 and widen, never parse the
// decimal text as float64 -- 0.9 as a Float32 widened to double is
// 0.89999997615814208984375, which is NOT equal to the double 0.9, and edges
// are partitioned by `confidence >= maxConfidence` and ordered by
// (confidence, edge_id). Getting this wrong changes which edges are protected
// from the split and therefore changes work_unit_ids.
type Edge struct {
	EdgeID     string
	SourceType string
	SourceID   string
	TargetType string
	TargetID   string
	Confidence float64
}

func (edge Edge) source() NodeKey { return NodeKey{Type: edge.SourceType, ID: edge.SourceID} }
func (edge Edge) target() NodeKey { return NodeKey{Type: edge.TargetType, ID: edge.TargetID} }

// Component is one work unit: its node set and the edge bundle that produced it.
type Component struct {
	Nodes []NodeKey
	Edges []Edge
}

// BuildStats is the port of components.py:46-63 ComponentBuildStats.
//
// The oversized-component split is never allowed to be silent: these counters
// are surfaced in the run stats of both the materializer and the membership
// backfill. In Python they are accompanied only by logger.warning calls
// (components.py:214-220, 271-276), which is why CHAOS-4758 -- 242 real nodes
// deleted from org 70d529e0's graph -- went unnoticed. The Go side additionally
// emits them as counters (see the executor's observer), so a recurrence trips a
// metric rather than needing someone to read worker logs.
type BuildStats struct {
	OversizedComponents int
	DroppedEdges        int
	DroppedNodes        int
}

// ConfidenceFromValue ports components.py:72-83 _edge_confidence, the coercion
// applied to whatever the ClickHouse driver returns for the confidence column.
//
// Kept exported and in this package (rather than inlined at the decode site) so
// the materializer and the membership backfill coerce identically -- the same
// reason Python keeps it beside the builder. Note the bool case: Python checks
// isinstance(value, bool) BEFORE the numeric branch, because bool is a subclass
// of int there, so True coerces to 0.0 and not 1.0. Preserved exactly.
func ConfidenceFromValue(value any) float64 {
	switch typed := value.(type) {
	case bool:
		return 0.0
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case string:
		parsed, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0.0
		}
		return parsed
	default:
		return 0.0
	}
}

// nodeIndex is an insertion-ordered set of nodes.
//
// This type exists because Go map iteration is randomized and Python dict
// iteration is insertion-ordered, and _discover_components walks its adjacency
// dict directly (components.py:147). Ranging over a Go map there would pick a
// different DFS start node on every run, producing a different component ORDER
// -- and component order is load-bearing: partitioned materialization
// dispatches numeric component_indexes and each chunk worker re-derives the
// list, so index N must name the same component in the dispatcher and in the
// worker (queries.py:46-51). A map would make chunks silently skip or
// double-categorize units.
type nodeIndex struct {
	order    []NodeKey
	position map[NodeKey]int
}

func newNodeIndex() *nodeIndex {
	return &nodeIndex{position: make(map[NodeKey]int)}
}

// intern returns the slot for node, appending it if this is its first sighting.
func (index *nodeIndex) intern(node NodeKey) int {
	if slot, ok := index.position[node]; ok {
		return slot
	}
	slot := len(index.order)
	index.order = append(index.order, node)
	index.position[node] = slot
	return slot
}

func (index *nodeIndex) has(node NodeKey) bool {
	_, ok := index.position[node]
	return ok
}

// connectedComponents ports components.py:90-123 _connected_components:
// union-find over nodes, using only edges whose BOTH endpoints are in nodes.
//
// Grouping is content-based, so the node SET of each component is independent
// of input iteration order -- the property work_unit_id relies on.
//
// DELIBERATE, UNOBSERVABLE ORDER DIVERGENCE: Python builds its groups by
// iterating a set of tuples, so both the order of the returned fragments and
// the order of nodes within a fragment follow string-hash order, which
// PYTHONHASHSEED randomizes between runs. This implementation iterates the
// insertion-ordered node slice instead, which is deterministic. That is safe
// because every consumer normalizes: splitOversizedComponent sorts its result
// by each fragment's sorted node list (components.py:301-303), removeHubs feeds
// that same sort, degrees/max/min are order-independent, and WorkUnitID sorts
// its tokens. Python could not have relied on the order it produces, because it
// does not have a stable one.
func connectedComponents(nodes *nodeIndex, edges []Edge) [][]NodeKey {
	parent := make([]int, len(nodes.order))
	for slot := range parent {
		parent[slot] = slot
	}

	// Path-halving find, mirroring components.py:105-111.
	var find func(int) int
	find = func(slot int) int {
		root := slot
		for parent[root] != root {
			root = parent[root]
		}
		for parent[slot] != root {
			parent[slot], slot = root, parent[slot]
		}
		return root
	}

	for _, edge := range edges {
		source, target := edge.source(), edge.target()
		if !nodes.has(source) || !nodes.has(target) {
			continue
		}
		rootA, rootB := find(nodes.position[source]), find(nodes.position[target])
		if rootA != rootB {
			parent[rootB] = rootA
		}
	}

	groupSlots := make(map[int]int)
	groups := make([][]NodeKey, 0)
	for slot, node := range nodes.order {
		root := find(slot)
		groupSlot, ok := groupSlots[root]
		if !ok {
			groupSlot = len(groups)
			groupSlots[root] = groupSlot
			groups = append(groups, nil)
		}
		groups[groupSlot] = append(groups[groupSlot], node)
	}
	return groups
}

// discoverComponents ports components.py:126-167 _discover_components: raw
// connected components with their deduplicated edge bundles.
//
// This is the historical materialize._build_components traversal, preserved so
// that for graphs WITHOUT any oversized component the output -- including
// component order and per-component edge lists -- is byte-identical to the
// pre-CHAOS-2775 behaviour.
//
// Three ordering details are contract, not incidental:
//   - adjacency is walked in first-sighting order (Python dict insertion order),
//     which is edge input order -- hence nodeIndex rather than a map;
//   - the DFS uses a LIFO stack and appends on POP, so component_nodes is in
//     pop order, not push order;
//   - a component's edge bundle keeps the FIRST occurrence of each edge_id and
//     is emitted in first-sighting order.
func discoverComponents(edges []Edge) []Component {
	index := newNodeIndex()
	adjacency := make([][]NodeKey, 0)
	edgesByNode := make([][]Edge, 0)

	grow := func(node NodeKey) int {
		slot := index.intern(node)
		for len(adjacency) <= slot {
			adjacency = append(adjacency, nil)
			edgesByNode = append(edgesByNode, nil)
		}
		return slot
	}

	for _, edge := range edges {
		source, target := edge.source(), edge.target()
		sourceSlot, targetSlot := grow(source), grow(target)
		adjacency[sourceSlot] = append(adjacency[sourceSlot], target)
		adjacency[targetSlot] = append(adjacency[targetSlot], source)
		edgesByNode[sourceSlot] = append(edgesByNode[sourceSlot], edge)
		edgesByNode[targetSlot] = append(edgesByNode[targetSlot], edge)
	}

	visited := make([]bool, len(index.order))
	components := make([]Component, 0)

	for startSlot, startNode := range index.order {
		if visited[startSlot] {
			continue
		}
		stack := []NodeKey{startNode}
		visited[startSlot] = true

		componentNodes := make([]NodeKey, 0)
		componentEdges := make([]Edge, 0)
		seenEdgeIDs := make(map[string]struct{})

		for len(stack) > 0 {
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			componentNodes = append(componentNodes, current)

			currentSlot := index.position[current]
			for _, edge := range edgesByNode[currentSlot] {
				// Python: `if edge_id and edge_id not in component_edges` --
				// an edge with an empty id is never bundled, and never
				// suppresses a later one.
				if edge.EdgeID == "" {
					continue
				}
				if _, seen := seenEdgeIDs[edge.EdgeID]; seen {
					continue
				}
				seenEdgeIDs[edge.EdgeID] = struct{}{}
				componentEdges = append(componentEdges, edge)
			}
			for _, neighbor := range adjacency[currentSlot] {
				neighborSlot := index.position[neighbor]
				if visited[neighborSlot] {
					continue
				}
				visited[neighborSlot] = true
				stack = append(stack, neighbor)
			}
		}
		components = append(components, Component{Nodes: componentNodes, Edges: componentEdges})
	}
	return components
}

// degrees ports components.py:170-177 _degrees.
func degrees(edges []Edge, within *nodeIndex) map[NodeKey]int {
	degree := make(map[NodeKey]int, len(within.order))
	for _, node := range within.order {
		degree[node] = 0
	}
	for _, edge := range edges {
		source, target := edge.source(), edge.target()
		if within.has(source) && within.has(target) {
			degree[source]++
			degree[target]++
		}
	}
	return degree
}

// removeHubs ports components.py:180-220 _remove_hubs: drop the highest-degree
// hub nodes until every fragment fits maxNodes.
//
// THIS FUNCTION DESTROYS DATA, AND THAT IS THE PORTED BEHAVIOUR. A removed node
// is dropped ENTIRELY -- excluded from all output fragments, its incident edges
// gone with it. On org 70d529e0 today it deletes 242 nodes (240 issues, 2 PRs),
// roughly 10% of the graph, which then appear in no work unit at all; a PR whose
// issue was deleted is orphaned into a one-node PR-only unit with no team
// bridge, which is what surfaces on the Investment page as "Unassigned team".
// That defect is CHAOS-4758 and is deliberately NOT fixed here (see the package
// doc). This port must reproduce it exactly so the fix can be measured against
// a proven-equivalent baseline.
//
// Deterministic: among the nodes of every still-oversized fragment, the node
// with the highest degree is removed, ties broken by the SMALLEST node id.
// Terminates because every iteration shrinks the active node set.
func removeHubs(nodes *nodeIndex, edges []Edge, maxNodes int, stats *BuildStats) [][]NodeKey {
	active := newNodeIndex()
	for _, node := range nodes.order {
		active.intern(node)
	}

	for {
		// Python rebuilds active_edges from the CURRENT active set each round
		// (`active.discard(hub)` shrinks it), so edges incident to an already
		// removed hub disappear from consideration. Filtering against a set that
		// still contained the dropped hubs would happen to yield the same
		// fragments -- connectedComponents ignores edges whose endpoints it does
		// not know, and degrees only counts within the oversized set -- but it
		// would be correct by accident, and it would keep re-scanning edges that
		// can no longer matter. Shrink the set, exactly as Python does.
		activeEdges := make([]Edge, 0, len(edges))
		for _, edge := range edges {
			if active.has(edge.source()) && active.has(edge.target()) {
				activeEdges = append(activeEdges, edge)
			}
		}
		fragments := connectedComponents(active, activeEdges)

		oversized := newNodeIndex()
		for _, fragment := range fragments {
			if len(fragment) > maxNodes {
				for _, node := range fragment {
					oversized.intern(node)
				}
			}
		}
		if len(oversized.order) == 0 {
			return fragments
		}

		degree := degrees(activeEdges, oversized)
		maxDegree := 0
		for position, node := range oversized.order {
			if position == 0 || degree[node] > maxDegree {
				maxDegree = degree[node]
			}
		}
		// Python: min(node for node in oversized_nodes if degree == max_degree).
		// The minimum is over the (type, id) TUPLE, so type is compared first
		// and id only breaks a type tie -- not over the rendered "type:id" token
		// the hash sorts by. The two orders can differ; this is the ported one.
		var hub NodeKey
		haveHub := false
		for _, node := range oversized.order {
			if degree[node] != maxDegree {
				continue
			}
			if !haveHub || nodeLess(node, hub) {
				hub, haveHub = node, true
			}
		}

		active = withoutNode(active, hub)
		if stats != nil {
			stats.DroppedNodes++
		}
	}
}

// withoutNode returns the active set minus one node, preserving sighting order.
// This is Python's set.discard; the order is preserved because
// connectedComponents walks it (see its own note on the divergence that makes
// order unobservable downstream).
func withoutNode(active *nodeIndex, removed NodeKey) *nodeIndex {
	rebuilt := newNodeIndex()
	for _, node := range active.order {
		if node == removed {
			continue
		}
		rebuilt.intern(node)
	}
	return rebuilt
}

// nodeLess is Python's tuple comparison for (node_type, node_id).
func nodeLess(left, right NodeKey) bool {
	if left.Type != right.Type {
		return left.Type < right.Type
	}
	return left.ID < right.ID
}

// splitOversizedComponent ports components.py:223-304
// _split_oversized_component.
//
// Phase (a), edge drop: edges strictly below the component's MAX confidence are
// dropped in (confidence, edge_id) order; the smallest prefix of that ordering
// whose removal makes every fragment fit is found by binary search (fragment
// size is monotonically non-increasing as more edges are dropped). Edges tied at
// the max confidence are NEVER dropped in this phase -- which is exactly why
// CHAOS-4758 bites today: 4,383 of this org's 4,387 edges in the giant component
// sit at confidence 1.0, so phase (a) is near-vacuous and control falls through
// to the node-destroying phase (b).
//
// Phase (b): whatever the edge phase cannot resolve -- a fragment held together
// purely by max-confidence edges -- goes to removeHubs.
func splitOversizedComponent(nodes []NodeKey, edges []Edge, maxNodes int, stats *BuildStats) []Component {
	nodeSet := newNodeIndex()
	for _, node := range nodes {
		nodeSet.intern(node)
	}

	maxConfidence := 0.0
	for position, edge := range edges {
		if position == 0 || edge.Confidence > maxConfidence {
			maxConfidence = edge.Confidence
		}
	}

	protected := make([]Edge, 0, len(edges))
	droppable := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		if edge.Confidence >= maxConfidence {
			protected = append(protected, edge)
			continue
		}
		droppable = append(droppable, edge)
	}
	sort.SliceStable(droppable, func(left, right int) bool {
		if droppable[left].Confidence != droppable[right].Confidence {
			return droppable[left].Confidence < droppable[right].Confidence
		}
		return droppable[left].EdgeID < droppable[right].EdgeID
	})

	keptAfter := func(dropCount int) []Edge {
		kept := make([]Edge, 0, len(protected)+len(droppable)-dropCount)
		kept = append(kept, protected...)
		kept = append(kept, droppable[dropCount:]...)
		return kept
	}
	fits := func(dropCount int) bool {
		for _, fragment := range connectedComponents(nodeSet, keptAfter(dropCount)) {
			if len(fragment) > maxNodes {
				return false
			}
		}
		return true
	}

	low, high := 0, len(droppable)
	for low < high {
		middle := (low + high) / 2
		if fits(middle) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	dropCount := low
	if stats != nil {
		stats.DroppedEdges += dropCount
	}

	keptEdges := keptAfter(dropCount)
	fragmentNodeLists := make([][]NodeKey, 0)
	for _, fragment := range connectedComponents(nodeSet, keptEdges) {
		if len(fragment) <= maxNodes {
			fragmentNodeLists = append(fragmentNodeLists, fragment)
			continue
		}
		fragmentIndex := newNodeIndex()
		for _, node := range fragment {
			fragmentIndex.intern(node)
		}
		fragmentNodeLists = append(fragmentNodeLists, removeHubs(fragmentIndex, keptEdges, maxNodes, stats)...)
	}

	result := make([]Component, 0, len(fragmentNodeLists))
	for _, fragmentNodes := range fragmentNodeLists {
		fragmentSet := newNodeIndex()
		for _, node := range fragmentNodes {
			fragmentSet.intern(node)
		}
		fragmentEdges := make([]Edge, 0)
		for _, edge := range keptEdges {
			if fragmentSet.has(edge.source()) && fragmentSet.has(edge.target()) {
				fragmentEdges = append(fragmentEdges, edge)
			}
		}
		result = append(result, Component{Nodes: fragmentNodes, Edges: fragmentEdges})
	}

	// Deterministic fragment order, independent of set-iteration order: sort by
	// the fragment's SORTED node tuple (components.py:301-303). Sorting the key
	// rather than the fragment itself matters -- the emitted Nodes slice keeps
	// its traversal order.
	sortKeys := make([][]NodeKey, len(result))
	for position, component := range result {
		sorted := append([]NodeKey(nil), component.Nodes...)
		sort.SliceStable(sorted, func(left, right int) bool { return nodeLess(sorted[left], sorted[right]) })
		sortKeys[position] = sorted
	}
	order := make([]int, len(result))
	for position := range order {
		order[position] = position
	}
	sort.SliceStable(order, func(left, right int) bool {
		return nodeListLess(sortKeys[order[left]], sortKeys[order[right]])
	})
	ordered := make([]Component, len(result))
	for position, source := range order {
		ordered[position] = result[source]
	}
	return ordered
}

// nodeListLess is Python's list comparison: element-wise, then shorter-first.
func nodeListLess(left, right []NodeKey) bool {
	for position := 0; position < len(left) && position < len(right); position++ {
		if left[position] != right[position] {
			return nodeLess(left[position], right[position])
		}
	}
	return len(left) < len(right)
}

// BuildComponents ports components.py:307-344 build_components: the work-unit
// components of a work_graph_edges row set.
//
// This is the SINGLE implementation shared by the materializer and the
// membership backfill, so their component sets -- and therefore their
// work_unit_id hashes -- stay identical for identical edge input. Callers must
// not re-derive grouping locally; see the package doc for why.
//
// maxComponentNodes nil resolves through ResolveMaxComponentNodes (explicit >
// environment > default). stats may be nil.
//
// Heuristic edges are excluded UPSTREAM, at the fetch_work_graph_edges choke
// point, not here (queries.py:25-35) -- this function groups whatever it is
// given, and a caller that fetches without that filter will percolate thousands
// of unrelated nodes into one component.
func BuildComponents(edges []Edge, maxComponentNodes *int, stats *BuildStats) []Component {
	maxNodes := ResolveMaxComponentNodes(maxComponentNodes)

	result := make([]Component, 0)
	for _, discovered := range discoverComponents(edges) {
		// list(dict.fromkeys(component_nodes)): dedupe, keeping first sighting.
		unitNodes := make([]NodeKey, 0, len(discovered.Nodes))
		seen := make(map[NodeKey]struct{}, len(discovered.Nodes))
		for _, node := range discovered.Nodes {
			if _, ok := seen[node]; ok {
				continue
			}
			seen[node] = struct{}{}
			unitNodes = append(unitNodes, node)
		}

		if len(unitNodes) <= maxNodes {
			result = append(result, Component{Nodes: unitNodes, Edges: discovered.Edges})
			continue
		}
		if stats != nil {
			stats.OversizedComponents++
		}
		result = append(result, splitOversizedComponent(unitNodes, discovered.Edges, maxNodes, stats)...)
	}
	return result
}
