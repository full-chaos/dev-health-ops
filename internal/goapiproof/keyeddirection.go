package goapiproof

import "strings"

// KeyedDirectionShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// invariant a strictly ADDITIVE row-dedup mechanism can honestly prove
// per keyed element of a list: the reference (baseline) plane's value can
// only be pulled UP by counting extra unmerged physical row versions the
// candidate (Go) plane's FINAL/argMax-deduped read already collapses,
// never down -- so a real instance always leaves baseline STRICTLY
// GREATER than candidate at that key, and nothing about the mechanism
// bounds by how much.
//
// This is deliberately WEAKER than SankeyRepoFanoutShape's own
// integer-multiplier claim: it is for a mechanism where a list element
// (a heatmap cell, a single fixed sankey edge) can be the SUM of MANY
// unrelated source rows, only SOME of which carry the affected table's
// own unmerged duplicate -- a heatmap weekday/hour cell mixes PRs and
// commits from many different repos, for instance, so no single clean
// multiplier of the cell's own total is provable in general, even though
// a particular capture may happen to show one. Claiming a multiplier
// here would be a claim the mechanism does not actually support; claiming
// only direction is the strongest claim that IS honest, per the ruling.
//
// The shape this type verifies, over the two DECODED lists at ListPath,
// per key:
//
//  1. KeyFields are the query's OWN GROUP BY columns for this list --
//     declared here, not inferred, so a reader can check them directly
//     against the SQL the mechanism cites. They must be the SAME fields,
//     in the SAME order, as a paired OrderInsensitiveList declaration for
//     this exact ListPath: a Finding's own Detail only carries the
//     "[key=...] " prefix this shape parses when the list is ALSO
//     declared order-insensitive with matching KeyFields (see
//     compareListByKey/orderInsensitiveKey) -- this shape does not
//     itself re-derive that pairing.
//  2. Keys, when non-empty, restricts admission to EXACTLY the declared
//     key tuples -- e.g. the one fixed (source, target) pair a mechanism
//     is proven to touch (sankeyCycleTimesDedupParity's Rework ->
//     Abandonment / rewrite edge, the ONLY edge buildExpenseFlow derives
//     from canceled_items). A difference at any OTHER key stays outside
//     this citation, exactly as an unrestricted mechanism (heatmapDedup
//     Parity's cells, where every cell can in principle be touched)
//     leaves nil/empty and admits any key.
//  3. A key present in both lists, with a numeric ValueField on both
//     sides, admits iff baseline is STRICTLY greater than candidate. No
//     magnitude bound: a shift of one part in a billion and a shift of
//     ten times the value are both explained the same way, because the
//     mechanism itself carries no bound on how many extra rows an
//     unmerged table can hold. A key present on only one side is a
//     structural difference (see compare.go's leafDifference gate) and
//     is never reached here.
//
// What this shape CANNOT catch: a real Go-side regression that happens
// to move the SAME key's value in the SAME direction (candidate below
// baseline) while touching nothing else -- for example a Go bug that
// UNDER-counts. Direction alone cannot tell that apart from a genuine
// instance of this mechanism; only the sign is checked, never disproved.
// This is a known, accepted limit of what a two-body comparison can tell
// apart at this granularity, not an oversight -- the same limit
// SupersessionSkewShape's own doc comment states for its whole-comparison
// direction check.
type KeyedDirectionShape struct {
	// ListPath is the dotted, index-free path to the list itself, e.g.
	// "data.cells" or "data.links".
	ListPath string
	// ValueField is the element's own numeric field name, e.g. "value".
	ValueField string
	// ValuePath is the leaf path findings carry for that field -- must
	// equal one of the defect's own Paths entries, e.g. "data.cells.value".
	ValuePath string
	// KeyFields names the query's own GROUP BY columns identifying one
	// element -- see the type doc comment's rule 1.
	KeyFields []string
	// Keys, when non-empty, restricts admission to exactly these key
	// tuples (index-free values, in KeyFields order) -- see rule 2. Nil
	// or empty admits every key the list carries.
	Keys [][]string
}

// keyedDirectionPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// lists at ListPath.
type keyedDirectionPlan struct {
	shape *KeyedDirectionShape
	// valid is false when the list could not even be read, key-indexed,
	// on both sides. A plan that is not valid admits NOTHING -- the safe
	// default, never a guess.
	valid bool
	// admittedKeys is the set of "\x1f"-joined key tuples this plan
	// admits.
	admittedKeys map[string]bool
}

// buildKeyedDirectionPlan evaluates every rule KeyedDirectionShape
// documents against one comparison's decoded baseline/candidate `data`
// values.
func buildKeyedDirectionPlan(shape *KeyedDirectionShape, baselineData, candidateData any) *keyedDirectionPlan {
	plan := &keyedDirectionPlan{shape: shape, admittedKeys: map[string]bool{}}

	baseList, ok1 := listAtDottedPath(baselineData, shape.ListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.ListPath)
	if !ok1 || !ok2 {
		return plan
	}

	index := func(list []any) (map[string]float64, bool) {
		out := make(map[string]float64, len(list))
		for _, element := range list {
			key, ok := orderInsensitiveKey(element, shape.KeyFields)
			if !ok {
				return nil, false
			}
			object := element.(map[string]any)
			value, ok := asFloat(object[shape.ValueField])
			if !ok {
				continue
			}
			out[key] = value
		}
		return out, true
	}
	baseValues, ok3 := index(baseList)
	candValues, ok4 := index(candList)
	if !ok3 || !ok4 {
		return plan
	}
	plan.valid = true

	allowed := map[string]bool{}
	restricted := len(shape.Keys) > 0
	if restricted {
		for _, tuple := range shape.Keys {
			allowed[strings.Join(tuple, "\x1f")] = true
		}
	}

	for key, candValue := range candValues {
		if restricted && !allowed[key] {
			continue
		}
		baseValue, ok := baseValues[key]
		if !ok {
			continue
		}
		if baseValue > candValue {
			plan.admittedKeys[key] = true
		}
	}

	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *keyedDirectionPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	if tieredPath(finding.Path) != p.shape.ValuePath {
		return false
	}
	key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
	if !ok {
		return false
	}
	return p.admittedKeys[key]
}
