package goapiproof

import (
	"fmt"
	"time"
)

// CandidateAccounting is a route family's one candidate-side check, set on
// a BaselineDefect (BaselineDefect.Accounting) and evaluated by the
// comparator before that defect admits anything, whatever its shape. The
// comparator compares list elements by position only while the two lists
// have the same length, and a duplicate row on the baseline shifts every
// later position, so a shape's own rules judge baseline copies and never
// see the candidate's own rows as a whole. This check holds the invariant:
// every candidate row is accounted for, meaning it equals one whole
// physical baseline copy of the same id under CopyRule. The candidate is
// ordered by the route's sort key. The only unaccounted rows permitted are
// candidate rows beyond the baseline's reach after a page cut.
type CandidateAccounting struct {
	// ListPath is the dotted, index-free path to the list, e.g.
	// "data.items".
	ListPath string
	// IDField identifies one logical row, e.g. RESTDedupKeyField.
	IDField string
	// SortField is the timestamp field the route orders DESC by.
	SortField string
	// CopyRule judges a candidate row against the baseline copies of its
	// id; nil requires every copy and the candidate to be equal.
	CopyRule *DuplicateCopyRule
	// PageLimit is the request's own effective limit. A baseline at or
	// over it is page-cut, and only then may the candidate carry ids the
	// baseline never reached. Zero claims no page cut.
	PageLimit int
	// AllowDropped waives clause d alone, for a declaration whose
	// mechanism is a narrower candidate population (a team scope's own
	// repositories): a baseline id may be absent from the candidate, and
	// nothing else is waived.
	AllowDropped bool
}

// checkLists evaluates check over the lists at ListPath. A list absent on
// one plane is a structural finding the comparator reports on its own,
// which no citation covers; absent on both, there is no row to account
// for.
func (a *CandidateAccounting) checkLists(baselineData, candidateData any) (bool, string) {
	if a == nil {
		return true, ""
	}
	baseList, ok1 := listAtDottedPath(baselineData, a.ListPath)
	candList, ok2 := listAtDottedPath(candidateData, a.ListPath)
	if !ok1 || !ok2 {
		return true, ""
	}
	return a.check(baseList, candList)
}

// holds reports whether every candidate row is accounted for -- see check.
func (a *CandidateAccounting) holds(baseList, candList []any) bool {
	ok, _ := a.check(baseList, candList)
	return ok
}

// check evaluates the invariant and names the first violation:
//
//	a. every element of both lists is an object carrying a non-empty id;
//	b. candidate ids are unique;
//	c. the candidate is monotone DESC by SortField, every value parseable;
//	d. every baseline id is present in the candidate (waived alone by
//	   AllowDropped);
//	e. every candidate row whose id is in the baseline is a copy CopyRule
//	   lets a FINAL read serve;
//	f. a candidate-only id appears only on a page-cut baseline, and only
//	   after the candidate's last shared row;
//	g. the candidate is no longer than PageLimit when PageLimit is set.
func (a *CandidateAccounting) check(baseList, candList []any) (bool, string) {
	if a == nil {
		return true, ""
	}
	if a.PageLimit > 0 && len(candList) > a.PageLimit {
		return false, fmt.Sprintf("candidate carries %d rows, over the request's limit %d", len(candList), a.PageLimit)
	}
	baseGroups := map[string][]map[string]any{}
	for i, element := range baseList {
		object, id, ok := edgeObjectAndID(element, a.IDField)
		if !ok {
			return false, fmt.Sprintf("baseline row %d carries no id", i)
		}
		baseGroups[id] = append(baseGroups[id], object)
	}
	if !monotoneDescending(candList, a.SortField) {
		return false, "candidate rows are not ordered " + a.SortField + " DESC"
	}
	seen := make(map[string]bool, len(candList))
	lastShared, firstOnly := -1, -1
	for i, element := range candList {
		object, id, ok := edgeObjectAndID(element, a.IDField)
		if !ok {
			return false, fmt.Sprintf("candidate row %d carries no id", i)
		}
		if seen[id] {
			return false, fmt.Sprintf("candidate repeats id %q", id)
		}
		seen[id] = true
		group, shared := baseGroups[id]
		if !shared {
			if firstOnly < 0 {
				firstOnly = i
			}
			continue
		}
		if !a.CopyRule.candidateIsServedCopy(group, object) {
			return false, fmt.Sprintf("candidate row %q equals no baseline copy under the declared %s rule", id, a.CopyRule.describe())
		}
		lastShared = i
	}
	if len(baseGroups) == 0 {
		if firstOnly >= 0 {
			return false, "candidate carries rows the empty baseline never reached"
		}
		return true, ""
	}
	for id := range baseGroups {
		if !seen[id] && !a.AllowDropped {
			return false, fmt.Sprintf("baseline id %q is absent from the candidate", id)
		}
	}
	if firstOnly < 0 {
		return true, ""
	}
	if a.PageLimit <= 0 || len(baseList) < a.PageLimit {
		return false, fmt.Sprintf("candidate row %d carries an id the baseline lacks on an uncut page", firstOnly)
	}
	if firstOnly < lastShared {
		return false, fmt.Sprintf("candidate row %d carries an id the baseline lacks inside the shared rows", firstOnly)
	}
	return true, ""
}

// monotoneDescending reports whether every element of list carries a
// parseable timestamp at sortField and no element is strictly later than
// the one before it.
func monotoneDescending(list []any, sortField string) bool {
	var previous time.Time
	for i, element := range list {
		ts, ok := duplicateCollapsePageCutSortValue(element, sortField)
		if !ok {
			return false
		}
		if i > 0 && ts.After(previous) {
			return false
		}
		previous = ts
	}
	return true
}
