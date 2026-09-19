package goapiproof

import "time"

// PageBoundary names the one row position where a reference page at its
// limit can hold only part of an id's physical copies. The reference
// reads a ReplacingMergeTree table without FINAL and ORDER BY SortField
// DESC LIMIT Limit; copies of one id share its SortField value, so when
// the page ends inside that value's rows, the copies past the limit are
// cut off. The candidate reads FINAL and serves the version the merge
// keeps, which can be a cut-off copy. The page then holds a copy of the
// boundary id that the candidate row does not equal.
//
// Invariant: a candidate row is admitted against baseline copies it does
// not equal only when its id is the id of the last row of a baseline page
// at its limit, the page is ordered SortField DESC, no other id shares
// that row's SortField value, every on-page copy of that id agrees with
// the others, and the candidate row differs from them only on the declared
// copy-rule field set; every other row keeps the whole-copy rule
// (DuplicateCopyRule.candidateIsServedCopy).
//
// Limit: the rule infers that the cut-off copy exists; it cannot see it.
// Which copy the merge keeps is the table's version column, which no body
// carries, so a candidate row that differs from the on-page copy only
// inside the copy-rule set is not told apart from one that serves a value
// no stored version holds.
type PageBoundary struct {
	// SortField is the timestamp field the route orders DESC by.
	SortField string
	// Limit is the request's own effective limit; zero claims no page cut.
	Limit int
	// CopyRule names the fields the boundary row may differ on. A shape
	// whose own copy judgement is byte identity sets it here; a check with
	// its own CopyRule passes that one.
	CopyRule *DuplicateCopyRule
}

// copyAdmission is which rule admitted a candidate row against its id's
// baseline copies.
type copyAdmission int

const (
	copyRefused copyAdmission = iota
	// copyServed: the whole-copy rule (candidateIsServedCopy).
	copyServed
	// copyCutBoundary: the page-boundary rule, only where copyServed
	// refuses.
	copyCutBoundary
)

// cutBoundaryID returns the id of baseList's last row when the list is at
// or over Limit, ordered SortField DESC (ties permitted), and no other id
// carries that row's SortField value: a page cut inside the last id's
// copies is a claim about an ordered page, so a page out of order has no
// boundary. ok is false otherwise, including for a nil boundary, a row
// without an id or an unparseable SortField value.
func (b *PageBoundary) cutBoundaryID(baseList []any, idField string) (string, bool) {
	if b == nil || b.Limit <= 0 || len(baseList) < b.Limit {
		return "", false
	}
	ids := make([]string, len(baseList))
	values := make([]time.Time, len(baseList))
	for i, element := range baseList {
		_, id, ok := edgeObjectAndID(element, idField)
		if !ok {
			return "", false
		}
		value, ok := duplicateCollapsePageCutSortValue(element, b.SortField)
		if !ok {
			return "", false
		}
		ids[i], values[i] = id, value
	}
	last := len(baseList) - 1
	for i := 0; i < last; i++ {
		if values[i+1].After(values[i]) {
			return "", false
		}
		if ids[i] != ids[last] && values[i].Equal(values[last]) {
			return "", false
		}
	}
	return ids[last], true
}

// admitCopy judges one candidate row against its id's baseline copies:
// the whole-copy rule (served, under that rule) first, then, for the cut
// boundary id alone, the page-boundary rule under the boundary's own
// CopyRule.
func (b *PageBoundary) admitCopy(served *DuplicateCopyRule, group []map[string]any, candidate map[string]any, cutBoundary bool) copyAdmission {
	if served.candidateIsServedCopy(group, candidate) {
		return copyServed
	}
	if cutBoundary && b != nil && b.CopyRule.candidateIsCutBoundaryCopy(group, candidate) {
		return copyCutBoundary
	}
	return copyRefused
}

// candidateIsCutBoundaryCopy reports whether candidate can be a copy cut
// off past the page limit, given the on-page copies in group: the rule
// names at least one field, every on-page copy carries the candidate's key
// set and equals every other on-page copy, every field outside the rule's
// fields agrees with the candidate, and a write-once field populated on
// the page carries the same value on the candidate (a later write never
// turns it back to null or to another value).
func (r *DuplicateCopyRule) candidateIsCutBoundaryCopy(group []map[string]any, candidate map[string]any) bool {
	if !r.declared() || len(group) == 0 || candidate == nil {
		return false
	}
	first := group[0]
	for _, copyObject := range group {
		if !sameKeySet(copyObject, candidate) || !jsonValuesEqual(copyObject, first) {
			return false
		}
	}
	for _, field := range r.WriteOnceFields {
		held, present := first[field]
		if present && held != nil && !jsonValuesEqual(held, candidate[field]) {
			return false
		}
	}
	fields := append(append([]string{}, r.WriteOnceFields...), r.RewrittenFields...)
	return jsonValuesEqual(withoutFields(first, fields), withoutFields(candidate, fields))
}
