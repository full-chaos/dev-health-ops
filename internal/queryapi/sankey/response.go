// Node/edge assembly shared by every mode's builder -- ports
// _normalize_label/_add_edge/_touch_node/_links_from_edges (services/
// sankey.py:86-122).
package sankey

import (
	"sort"
	"strings"
)

// edgeKey is the Go analogue of Python's `dict[tuple[str,str], float]`
// edges accumulator key.
type edgeKey struct {
	Source, Target string
}

// nodeAccumulator preserves first-touch insertion order, the same
// property a Python dict (nodes: dict[str, SankeyNode]) gives _touch_node
// for free.
type nodeAccumulator struct {
	order  []string
	byName map[string]Node
}

func newNodeAccumulator() *nodeAccumulator {
	return &nodeAccumulator{byName: map[string]Node{}}
}

// touchNode ports _touch_node (services/sankey.py:105-112).
func (a *nodeAccumulator) touchNode(name string, group *string) {
	if _, ok := a.byName[name]; ok {
		return
	}
	a.byName[name] = Node{Name: name, Group: group}
	a.order = append(a.order, name)
}

func (a *nodeAccumulator) nodes() []Node {
	out := make([]Node, 0, len(a.order))
	for _, name := range a.order {
		out = append(out, a.byName[name])
	}
	return out
}

// edgeAccumulator preserves first-touch insertion order for edges, the
// tiebreak _links_from_edges' stable sort relies on (services/sankey.py:
// 115-122, Python's list.sort is stable).
type edgeAccumulator struct {
	order  []edgeKey
	values map[edgeKey]float64
}

func newEdgeAccumulator() *edgeAccumulator {
	return &edgeAccumulator{values: map[edgeKey]float64{}}
}

// addEdge ports _add_edge (services/sankey.py:93-102): a non-positive
// value is dropped outright, never accumulated.
func (a *edgeAccumulator) addEdge(source, target string, value float64) {
	if value <= 0 {
		return
	}
	key := edgeKey{Source: source, Target: target}
	if _, ok := a.values[key]; !ok {
		a.order = append(a.order, key)
	}
	a.values[key] += value
}

// links ports _links_from_edges (services/sankey.py:115-122): every
// accumulated edge (value already guaranteed > 0 by addEdge, so the
// `if value > 0` filter there is a no-op here, ported anyway for a
// faithful copy), sorted descending by value, ties broken by first-touch
// insertion order (stable sort).
func (a *edgeAccumulator) links() []Link {
	out := make([]Link, 0, len(a.order))
	for _, key := range a.order {
		value := a.values[key]
		if value > 0 {
			out = append(out, Link{Source: key.Source, Target: key.Target, Value: value})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Value > out[j].Value })
	return out
}

// normalizeLabel ports _normalize_label (services/sankey.py:86-90).
// present is false for Python's None; an all-whitespace string also
// falls back, matching Python's `.strip()` emptiness check.
func normalizeLabel(value string, present bool, fallback string) string {
	if !present {
		return fallback
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
