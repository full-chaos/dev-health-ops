package goapiproof

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
}

// duplicateCollapseLengthPlan is one comparison's fully-evaluated
// admission decision, built once per defect from the two decoded lists.
type duplicateCollapseLengthPlan struct {
	shape   *DuplicateCollapseLengthShape
	applies bool
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
	for _, element := range baseList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
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

	plan.applies = true
	return plan
}

// admits reports whether one Finding is covered by this plan: only the
// list's own ShapeLength finding, and only when every rule above held.
func (p *duplicateCollapseLengthPlan) admits(finding Finding) bool {
	if p == nil || !p.applies {
		return false
	}
	return finding.Shape == ShapeLength && tieredPath(finding.Path) == p.shape.ListPath
}
