package goapiproof

import "strings"

// DuplicateCollapseLengthShape, set on a BaselineDefect, admits a list's
// own ShapeLength finding for the ONE additional consequence
// WorkGraphEdgeDedupShape's own per-id admission (workgraphedgedup.go)
// cannot reach: a raw list-LENGTH finding (compare.go's ShapeLength) is
// never dispatched through WorkGraphEdgeDedupShape's own admits() --
// owningID resolves only an INDEXED element or a declared
// TrailingCursorPath, never the bare list itself -- and compare.go's own
// structural-admission gate does not name WorkGraphEdgeDedupShape at all
// (see that gate's own doc comment in classifyBaselineDefects). This
// shape names the ONE length-level fact a duplicate-row mechanism can
// make honest: collapsing baseline's own duplicate physical row
// versions, id for id, reproduces the candidate list's own content
// exactly, length included. It never changes WorkGraphEdgeDedupShape
// itself -- it recomputes the same per-id facts independently, over the
// SAME two decoded lists, so the two shapes can be declared side by
// side on the same BaselineDefect list without either depending on the
// other's own plan.
//
// The shape this type verifies, over the two DECODED lists at ListPath:
//
//  1. Every physical copy of an id repeated in the baseline list is,
//     ignoring order, BYTE-IDENTICAL to every other copy of that id --
//     jsonValuesEqual's own by-value comparison (a number compares by
//     parsed value, a timestamp by parsed instant), never merely "close
//     enough". A single disagreeing pair anywhere refuses the WHOLE
//     plan: this shape makes one claim about the WHOLE list's own
//     length, not a per-id one, so it cannot selectively exclude one
//     disagreeing id's copies from a length that still needs a global
//     count.
//  2. The candidate list carries no repeated id at all -- a repeated
//     candidate id is a different, unexplained condition, the same
//     one-sided gate WorkGraphEdgeDedupShape's own rule 3 applies.
//  3. Every id present in BOTH lists is byte-identical between its
//     (deduplicated) baseline copy and its candidate element -- a real
//     Go-side regression on a shared id (a dropped or altered field)
//     must never hide behind this admission.
//  4. Every id in the candidate list is present in baseline, and every
//     (deduplicated) baseline id is present in the candidate list -- the
//     two ID SETS are identical once baseline's duplicates collapse.
//     This is strictly stronger than a bare length-equality check: two
//     lists of the same length built from entirely different rows would
//     pass a count comparison alone while still hiding a real,
//     unexplained substitution, and rule 4 refuses that case even though
//     rules 1-3 never see it (they only ever compare ids that already
//     match). Given rule 4, the list's own dedup(baseline)-vs-candidate
//     LENGTH equality follows automatically; this shape asserts it
//     directly rather than trusting the arithmetic alone.
//
// What this shape CANNOT catch: a duplicate-row mechanism that ALSO
// drops or substitutes an unrelated, non-duplicated row -- rule 4's own
// set-equality check refuses that case rather than guessing which half
// of the length difference the duplication actually explains.
//
// RequestLimit's own precondition (rule 0, evaluated before rules 1-4):
// when RequestLimit is set, a baseline whose own raw length REACHES it
// refuses outright, before rule 4's own set-equality is even computed.
// Rule 4 alone is not sufficient here: both legs of a comparison are
// requested under the SAME limit, so if the TRUE population exceeds
// RequestLimit on BOTH planes, candidate's own list is independently
// truncated at RequestLimit too -- reading FINAL, with no duplication,
// candidate's own truncated list is exactly its own top-RequestLimit
// distinct rows in order, and baseline's own dedup, if honestly
// ordered, names the SAME top rows. Rule 4's set-equality would then
// hold EXACTLY, admitting a match that only proves the two leading
// pages agree, never that the whole population was compared -- the
// claim this shape's own RequestLimit callers make. A baseline
// genuinely SHORT of RequestLimit carries no such ambiguity (ClickHouse
// itself would have returned more rows had more existed), so the
// precondition never refuses that case.
type DuplicateCollapseLengthShape struct {
	// ListPath is the dotted, index-free path to the list itself, e.g.
	// "data.items".
	ListPath string
	// IDField is the object field that identifies one logical element
	// within an element of the list at ListPath, e.g.
	// RESTDedupKeyField for a REST route's synthetic dedup key.
	IDField string
	// RequestLimit, when > 0, is the request's own effective LIMIT the
	// baseline was captured under -- see rule 0's own doc comment above.
	// Zero (the default, every entry declared before this field existed)
	// disables rule 0 entirely, preserving that entry's own unchanged
	// behavior: a bare exact-collapse claim with no premise about
	// whether a further, unobserved page exists beyond what was
	// captured.
	RequestLimit int
	// CopyRule, when declared, replaces rules 1 and 3's byte-identity
	// with DuplicateCopyRule (duplicatecopyrule.go): an id's copies may
	// disagree in the rule's named fields, and the candidate element must
	// be the copy a FINAL read serves. Rule 4 already requires every
	// baseline id in the candidate, so every repeated id is judged
	// against its candidate row. Nil keeps rules 1 and 3 as stated.
	CopyRule *DuplicateCopyRule
	// CountPath, when set, is the dotted path of a sibling total-count
	// leaf (for example "data.workGraphEdges.totalCount") that the
	// resolver derives from the list it returns. When the plan applies,
	// a value difference at that path is admitted only if each leg's own
	// count equals that leg's own list length -- a count the resolver
	// derives from its list moves with the list, and a count that does
	// not (a candidate count that disagrees with its own list) stays
	// outside. Empty keeps the shape's length-only behaviour.
	CountPath string
	// OrderField, when set, is a numeric field of each candidate element
	// by which the candidate list must be weakly DESCENDING, with equal
	// values ascending by OrderTieField (for work_graph_edges:
	// confidence DESC, edgeId ASC, the order both planes sort by). A
	// candidate that is not in that order refuses the whole plan. Empty
	// keeps the shape without an order rule.
	OrderField    string
	OrderTieField string
	// OwnsPage, when set, makes this shape the ONE owner of every finding on
	// a page it applies to, not only the list length and the count: every
	// element finding under ListPath (a positional value difference, a
	// baseline-only element) and, when CursorPath is set, the trailing
	// cursor. The plan then also requires the baseline's distinct ids, in
	// first-occurrence order, to equal the candidate's ids in order, so
	// every positional difference is a consequence of a repeated baseline
	// row and nothing else. The page is decided as a whole: a page this
	// shape refuses leaves all of its findings outside, and a sibling
	// declaration that would absorb them positionally must apply only to
	// pages this shape does not own (see RequestLimit).
	OwnsPage bool
	// CursorPath is the dotted path of the page's trailing cursor leaf (for
	// example "data.workGraphEdges.pageInfo.endCursor"). Used only with
	// OwnsPage: a difference there is admitted when the candidate's cursor
	// names its own last element.
	CursorPath string
}

// duplicateCollapseLengthPlan is one comparison's fully-evaluated
// admission decision, built once per defect from the two decoded lists.
type duplicateCollapseLengthPlan struct {
	shape   *DuplicateCollapseLengthShape
	applies bool
	// countAdmitted is true when the shape declares a CountPath and each
	// leg's count equals its own list length.
	countAdmitted bool
	// cursorAdmitted is true when the shape owns the page, declares a
	// CursorPath and the candidate's cursor names its own last element.
	cursorAdmitted bool
}

// candidateInDeclaredOrder reports whether the candidate list is weakly
// descending by OrderField with ties ascending by OrderTieField.
func (s *DuplicateCollapseLengthShape) candidateInDeclaredOrder(list []any) bool {
	if s.OrderField == "" {
		return true
	}
	var prevValue float64
	var prevTie string
	for i, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return false
		}
		value, ok := asFloat(object[s.OrderField])
		if !ok {
			return false
		}
		tie, _ := object[s.OrderTieField].(string)
		if i > 0 {
			if value > prevValue || (value == prevValue && tie <= prevTie) {
				return false
			}
		}
		prevValue, prevTie = value, tie
	}
	return true
}

// countEqualsLength reads the leaf at CountPath in root and reports
// whether it is a number equal to wantLen.
func countEqualsLength(root any, countPath string, wantLen int) bool {
	value, ok := navigateSegments(root, citedSegments(countPath))
	if !ok {
		return false
	}
	count, ok := asFloat(value)
	return ok && count == float64(wantLen)
}

// buildDuplicateCollapseLengthPlan evaluates every rule
// DuplicateCollapseLengthShape documents against one comparison's two
// decoded lists.
func buildDuplicateCollapseLengthPlan(shape *DuplicateCollapseLengthShape, baselineData, candidateData any) *duplicateCollapseLengthPlan {
	plan := &duplicateCollapseLengthPlan{shape: shape}

	baseList, ok1 := listAtDottedPath(baselineData, shape.ListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.ListPath)
	if !ok1 || !ok2 {
		return plan
	}

	// Rule 0: RequestLimit's own precondition -- see the field's own doc
	// comment. A baseline whose own raw length REACHES (or, defensively,
	// exceeds) RequestLimit refuses before anything else is computed.
	if shape.RequestLimit > 0 && len(baseList) >= shape.RequestLimit {
		return plan
	}

	baseGroups := map[string][]map[string]any{}
	var baseOrder []string
	for _, element := range baseList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
		}
		if _, seen := baseGroups[id]; !seen {
			baseOrder = append(baseOrder, id)
		}
		baseGroups[id] = append(baseGroups[id], object)
	}

	// Rule 1: every physical copy of a repeated id agrees, byte for
	// byte. A single-copy id trivially satisfies this (nothing to
	// disagree with). Under a declared CopyRule the copies are judged
	// against the candidate row in rule 3 instead.
	hasDuplicate := false
	for _, group := range baseGroups {
		if len(group) > 1 {
			hasDuplicate = true
		}
		if shape.CopyRule.declared() {
			continue
		}
		first := group[0]
		for _, other := range group[1:] {
			if !jsonValuesEqual(first, other) {
				return plan
			}
		}
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

	// Rule 3: every id shared between the two (deduplicated) sides
	// agrees, byte for byte, or under a declared CopyRule the candidate
	// row is the copy a FINAL read serves.
	for id, group := range baseGroups {
		candObject, ok := candByID[id]
		if !ok {
			continue
		}
		if !shape.CopyRule.candidateIsServedCopy(group, candObject) {
			return plan
		}
	}

	// Rule 4: the two ID SETS are identical once baseline's duplicates
	// collapse -- never merely the same count.
	for id := range candByID {
		if _, ok := baseGroups[id]; !ok {
			return plan
		}
	}
	for id := range baseGroups {
		if _, ok := candByID[id]; !ok {
			return plan
		}
	}
	if len(baseGroups) != len(candList) {
		return plan
	}
	if !shape.candidateInDeclaredOrder(candList) {
		return plan
	}
	if shape.OwnsPage {
		// Every positional finding is explained by a repeated baseline row
		// only when removing the repeats leaves the candidate's own
		// sequence, id for id.
		for i, element := range candList {
			_, id, _ := edgeObjectAndID(element, shape.IDField)
			if baseOrder[i] != id {
				return plan
			}
		}
	}

	plan.applies = true
	if shape.OwnsPage && shape.CursorPath != "" {
		last := ""
		if len(candList) > 0 {
			_, last, _ = edgeObjectAndID(candList[len(candList)-1], shape.IDField)
		}
		if cursor, ok := navigateSegments(candidateData, citedSegments(shape.CursorPath)); ok {
			if text, isText := cursor.(string); isText && text == last {
				plan.cursorAdmitted = true
			}
		}
	}
	if shape.CountPath != "" {
		plan.countAdmitted = countEqualsLength(baselineData, shape.CountPath, len(baseList)) &&
			countEqualsLength(candidateData, shape.CountPath, len(candList))
	}
	return plan
}

// admits reports whether one Finding is covered by this plan: only the
// list's own ShapeLength finding, and only when every rule above held.
func (p *duplicateCollapseLengthPlan) admits(finding Finding) bool {
	if p == nil || !p.applies {
		return false
	}
	if p.countAdmitted && p.shape.CountPath != "" && finding.Path == "$."+p.shape.CountPath {
		return true
	}
	if p.shape.OwnsPage {
		tiered := tieredPath(finding.Path)
		if tiered == p.shape.ListPath || strings.HasPrefix(tiered, p.shape.ListPath+".") {
			return true
		}
		if p.cursorAdmitted && finding.Path == "$."+p.shape.CursorPath {
			return true
		}
	}
	return finding.Shape == ShapeLength && tieredPath(finding.Path) == p.shape.ListPath
}
