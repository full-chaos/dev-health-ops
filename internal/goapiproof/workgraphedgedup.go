package goapiproof

import "encoding/json"

// WorkGraphEdgeDedupShape, set on a BaselineDefect, replaces that defect's
// blanket "any leaf difference under Paths is covered" rule with a
// shape-specific admission for ONE mechanism: the baseline plane reading a
// ReplacingMergeTree edge table without collapsing unmerged physical
// versions of the same logical edge, so its page holds more than one row
// for some edge ids while the candidate plane's page holds exactly one.
// Positional comparison then reports every field from the first repeated
// id onward as a difference, because every later element shifted by one
// slot -- not because any field's VALUE is wrong.
//
// The shape this type verifies, over the two DECODED edge lists:
//
//  1. Every id repeated in the baseline list names a group of elements
//     that are, ignoring order, equal to one another field for field: the
//     SAME logical edge inserted more than once, not a real per-field
//     disagreement hiding behind a shared id. A number compares by
//     VALUE, not by literal form, so a plane that always writes a whole
//     number as "1" and one that sometimes writes it as "1.0" still agree
//     -- that is a formatting choice, never the difference this rule
//     exists to catch.
//  2. The candidate list carries no repeated id at all -- the mechanism
//     this shape explains is one-sided. A repeated id on the candidate
//     side is a different, unexplained condition, and nothing here may
//     admit it.
//  3. For every id the two lists share, the (deduplicated) baseline
//     element and the candidate element are equal, field for field, by
//     the same by-value comparison as rule 1. An id present on only one
//     side is outside this rule -- see below.
//  4. At least one id was actually repeated in the baseline and at least
//     one id was actually shared between the two lists -- a plan built
//     from a comparison with neither has nothing to admit and stays
//     invalid, the same safe default every shape in this package uses.
//
// An id present in the candidate list but never in the baseline list is
// NOT a contradiction of this shape: a page-length budget spent on
// baseline duplicate rows means the baseline simply never reached that
// edge within the page, exactly as a deduplicated candidate page reaches
// further than a raw one for the same request. Rule 3 only asks that
// where the two pages DO overlap, by id, their content agrees -- which is
// the fact that tells a genuine field-level regression apart from the
// duplicate-row mechanism: a regression changes a shared id's value, and
// rule 3 catches it by leaving the whole plan invalid.
//
// Anything outside these four rules -- a repeated baseline id whose copies
// disagree, a repeated candidate id, or a shared id whose content
// disagrees -- is NOT covered: classifyBaselineDefects reports it as an
// ordinary difference outside the citation, exactly like any other
// uncited mismatch.
type WorkGraphEdgeDedupShape struct {
	// EdgesListPath is the dotted, index-free path to the edge LIST
	// itself (no trailing field name), e.g. "data.workGraphEdges.edges".
	EdgesListPath string
	// IDField is the object field that identifies one logical edge within
	// an element of the list at EdgesListPath, e.g. "edgeId".
	IDField string
}

// workGraphEdgeDedupPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// edge lists.
type workGraphEdgeDedupPlan struct {
	// valid is a WHOLE-COMPARISON verdict, same discipline as
	// coverageShiftPlan: true only when every rule the type doc comment
	// states holds. false admits NOTHING.
	valid bool
}

// admits reports whether one Finding is covered by this plan. Every
// finding this shape is ever asked about already lies under
// EdgesListPath (defectCovers filters to the defect's own Paths before
// this is called), so -- exactly like coverageShiftPlan -- the verdict is
// the same whole-comparison answer for every one of them.
func (p *workGraphEdgeDedupPlan) admits(Finding) bool {
	return p != nil && p.valid
}

// buildWorkGraphEdgeDedupPlan evaluates every rule WorkGraphEdgeDedupShape
// documents against one comparison's two decoded edge lists.
func buildWorkGraphEdgeDedupPlan(shape *WorkGraphEdgeDedupShape, baselineData, candidateData any) *workGraphEdgeDedupPlan {
	plan := &workGraphEdgeDedupPlan{}

	baseList, ok1 := listAtDottedPath(baselineData, shape.EdgesListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.EdgesListPath)
	if !ok1 || !ok2 {
		return plan
	}

	baseGroups := map[string][]map[string]any{}
	for _, element := range baseList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
		}
		baseGroups[id] = append(baseGroups[id], object)
	}

	// Rule 1: every repeated baseline id's copies agree with one another.
	baseUnique := make(map[string]map[string]any, len(baseGroups))
	hasDuplicate := false
	for id, group := range baseGroups {
		first := group[0]
		for _, other := range group[1:] {
			if !jsonValuesEqual(first, other) {
				return plan
			}
		}
		if len(group) > 1 {
			hasDuplicate = true
		}
		baseUnique[id] = first
	}
	if !hasDuplicate {
		return plan
	}

	// Rule 2: the candidate carries no repeated id.
	candByID := make(map[string]map[string]any, len(candList))
	for _, element := range candList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
		}
		if _, already := candByID[id]; already {
			return plan
		}
		candByID[id] = object
	}

	// Rule 3: every SHARED id agrees, content for content. An id present
	// on only one side is outside this rule -- see the type doc comment.
	shared := 0
	for id, candObject := range candByID {
		baseObject, ok := baseUnique[id]
		if !ok {
			continue
		}
		shared++
		if !jsonValuesEqual(baseObject, candObject) {
			return plan
		}
	}
	if shared == 0 {
		return plan
	}

	plan.valid = true
	return plan
}

// edgeObjectAndID reads one edge list element as a decoded JSON object
// plus its identifying field, treated as opaque strings throughout --
// this shape never interprets an edge's fields, only compares them
// whole.
func edgeObjectAndID(element any, idField string) (map[string]any, string, bool) {
	object, ok := element.(map[string]any)
	if !ok {
		return nil, "", false
	}
	id, ok := object[idField].(string)
	if !ok || id == "" {
		return nil, "", false
	}
	return object, id, true
}

// jsonValuesEqual compares two values decoded from JSON (via
// DecodeSnapshot, so every number is a json.Number) the same way the rest
// of this package's comparator does: a number compares by parsed VALUE,
// through asFloat (compare.go), never by its literal text, so "1" and
// "1.0" agree exactly as compareNumber already treats them as one Tier-A
// value. reflect.DeepEqual has no such normalization -- it would read the
// two planes' own formatting conventions for a whole number as a content
// disagreement, which is a different plane's serializer, not a duplicate
// row whose fields actually differ. Every other JSON kind (object, list,
// string, bool, null) compares structurally, recursively.
func jsonValuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if aNum, ok := a.(json.Number); ok {
		bNum, ok := b.(json.Number)
		if !ok {
			return false
		}
		aFloat, aOK := asFloat(aNum)
		bFloat, bOK := asFloat(bNum)
		if aOK && bOK {
			return aFloat == bFloat
		}
		return aNum == bNum
	}
	if aMap, ok := a.(map[string]any); ok {
		bMap, ok := b.(map[string]any)
		if !ok || len(aMap) != len(bMap) {
			return false
		}
		for key, aValue := range aMap {
			bValue, present := bMap[key]
			if !present || !jsonValuesEqual(aValue, bValue) {
				return false
			}
		}
		return true
	}
	if aList, ok := a.([]any); ok {
		bList, ok := b.([]any)
		if !ok || len(aList) != len(bList) {
			return false
		}
		for i := range aList {
			if !jsonValuesEqual(aList[i], bList[i]) {
				return false
			}
		}
		return true
	}
	return a == b
}

// listAtDottedPath reads a JSON list at a dotted, index-free path (the
// same form BaselineDefect.Paths uses) under `data`. ok is false when the
// path does not resolve to a list -- the caller treats that as "cannot
// evaluate the shape at all" rather than guessing a partial one.
func listAtDottedPath(root any, dottedPath string) ([]any, bool) {
	value, ok := navigateSegments(root, citedSegments(dottedPath))
	if !ok {
		return nil, false
	}
	list, ok := value.([]any)
	return list, ok
}
