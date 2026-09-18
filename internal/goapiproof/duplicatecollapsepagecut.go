package goapiproof

import "time"

// DuplicateCollapsePageCutShape, set on a BaselineDefect, admits a list's
// own ShapeLength finding for the SECOND, more severe consequence a
// repos-join duplicate-row fan-out produces on a LIMIT-bounded page that
// the route's own limit is small enough to actually fill:
// DuplicateCollapseLengthShape (dedupcollapselength.go) only ever admits
// the exact-collapse case, `collapse(baseline) == candidate`, because its
// own rule 4 refuses whenever the candidate's own id set carries anything
// the baseline's page never reached at all. When the baseline page is
// truncated AT the route's own limit, every duplicate copy spends one
// extra slot that a genuinely distinct, later-ranked element would
// otherwise have occupied -- the candidate page, reading the same table
// FINAL, reaches further and lists distinct ids the baseline's own page
// never got to. This shape names that mechanism directly instead of
// leaving it as an unexplained length difference, and never touches
// DuplicateCollapseLengthShape's own declared entries or their tests.
//
// Established from two real production captures (team_scoped GET/POST
// /api/v1/drilldown/prs), one PARTIAL and one ALL-DOUBLED: baseline 50
// items at the route's own limit either way. PARTIAL: 28 distinct ids,
// 22 of them duplicated exactly 2x each, byte-identical; candidate 36
// items, its own 36 distinct ids a strict superset of the baseline's
// 28 -- all 28 recur, in the SAME relative order, as candidate's own
// first 28 elements, the 8 remaining candidate ids in candidate's own
// tail (short of the 22 wasted slots because the true, deduplicated
// population, 36, is itself under the route's own limit, 50).
// ALL-DOUBLED: 25 distinct ids across TWO repos, every one of the 25
// duplicated exactly 2x each, byte-identical -- the window's own
// population entirely covered by repos holding an unmerged repos row at
// capture time, not merely a subset of it; candidate 36 items, dedup
// (baseline)'s own 25 ids as its literal prefix, 11 candidate-only ids
// in the tail. Both captures fall within the SAME rules below -- the
// shape has no premise that a partial, rather than total, duplicate
// coverage of the baseline page is required.
//
// The shape this type verifies, over the two DECODED lists at ListPath:
//
//  1. The baseline list is EXACTLY Limit long -- the actual effective
//     LIMIT this specific request carried (see Limit's own doc comment,
//     the same convention LimitDisplacementShape's own field already
//     establishes). Anything else refuses the whole plan: a baseline
//     short of its own limit has no truncation at all, and
//     DuplicateCollapseLengthShape's own exact-collapse rule is the
//     correct citation for that case, not this one.
//
//  2. Every physical copy of an id repeated in the baseline list is,
//     ignoring order, byte-identical to every other copy -- the SAME
//     by-value comparison (jsonValuesEqual) DuplicateCollapseLengthShape's
//     own rule 1 applies. A single disagreeing pair anywhere refuses the
//     whole plan. At least one id must actually be repeated, or this
//     shape has nothing to admit and does not apply.
//
//     2b. The baseline list, read whole INCLUDING every duplicate copy in
//     its own captured physical position, is weakly monotone by
//     SortField, DESC (ties permitted) -- a defensive input check, not a
//     premise rule 6's own proof needs: two physical copies of one id
//     share the SAME SortField value by rule 2, so an honest ORDER BY
//     ... DESC capture can only ever tie there, never strictly violate
//     this order. A baseline body that does refuses the whole plan
//     rather than treating it as this mechanism's own genuine capture.
//
//  3. The candidate list carries no repeated id at all -- the SAME
//     one-sided gate DuplicateCollapseLengthShape's own rule 2 applies.
//
//  4. dedup(baseline) -- one representative copy per distinct id, in
//     FIRST-OCCURRENCE order, which is the baseline's own sort order
//     since every duplicate copy of one id is byte-identical (rule 2) --
//     is candidate's own LITERAL PREFIX: the first len(dedup(baseline))
//     elements of the candidate list carry the SAME ids in the SAME
//     order, and each carries content byte-identical to its baseline
//     representative. Anything else -- a baseline-distinct id missing
//     from the candidate, a shared id whose content differs, or a
//     candidate-only id appearing inside the shared prefix's own range --
//     refuses the whole plan. This rule alone says nothing about ORDER:
//     an id-set match at each position is silent on whether the
//     candidate's own SortField content actually places every row where
//     the route's own ORDER BY would -- rule 6 is the one rule that
//     checks that.
//
//  5. The candidate list is never LONGER than Limit. This is the same
//     count argument that motivated this shape -- E = (baseline length)
//     - len(dedup(baseline)), the count of page slots the duplicate
//     copies spent, versus C = (candidate length) - len(dedup(baseline)),
//     the count of candidate-only ids rule 4 already confirmed sit in
//     the shared prefix's tail -- reduced algebraically, not a separate
//     check: rule 1 already pins baseline length to Limit, so E =
//     Limit - len(dedup(baseline)), and C compares to E exactly as
//     candidate length compares to Limit (C == E iff candidate length ==
//     Limit; C < E iff candidate length < Limit; C > E iff candidate
//     length > Limit) -- there is no baseline/candidate pair where the
//     count argument and this length check disagree. A candidate longer
//     than Limit is structurally impossible under the route's own shared
//     LIMIT value and, were it ever observed, refuses outright rather
//     than silently admitting more than the mechanism could produce.
//
//  6. The candidate list, read whole, is monotone by SortField in the
//     route's own DESC direction (ties permitted, a strict increase
//     anywhere refuses) -- the actual computed check rule 4's id-set
//     match alone cannot make: a set of ids matching at each position
//     says nothing about whether a candidate-only row's own CONTENT
//     genuinely belongs at or after the page boundary, or was displaced
//     there by an unrelated ordering regression. SortField is read as a
//     timestamp (parseTimestamp, the same wire-form-tolerant parser
//     jsonValuesEqual's own comparisons already rely on for these rows);
//     a missing or unparseable SortField value anywhere refuses the
//     whole plan rather than guessing.
//
//     PROOF that rule 6 alone (never independently re-reading the
//     baseline's own SortField at all) already establishes "every
//     candidate-only row orders at or after the baseline's own last
//     row": let m be the SortField value of dedup(baseline)'s own LAST
//     entry, which by rule 4 is candidate's own element at position
//     len(dedup(baseline))-1. Rule 6's own monotonicity, applied to the
//     WHOLE candidate list, forces every later candidate position --
//     every candidate-only row -- to a SortField value <= m. Separately,
//     baseline's own ACTUAL last physical row is a copy of ONE of
//     dedup(baseline)'s own (at most Limit) distinct ids, sharing that
//     id's own representative value; that value is one of the SAME set
//     rule 6's own monotonicity (via rule 4's prefix identity) already
//     forces to be >= m (m being their minimum by construction, once
//     rule 6 holds). So baseline's own actual last row's value is always
//     >= m, and every candidate-only row's value is always <= m --
//     therefore always <= baseline's own actual last row's value too, a
//     CONSEQUENCE of rules 4 and 6 together, never a fact this shape
//     needs to compute separately by reading the baseline's own
//     SortField.
//
// What this shape CANNOT catch: a genuine Go-side drop of a page-worthy
// element (rule 4's own prefix-identity check refuses that case outright,
// because the missing id breaks the literal-prefix property), a genuine
// Go-side over-fetch that returns more distinct ids than the route's own
// limit allows (rule 5's refusal), and a genuine Go-side ordering
// regression anywhere in the candidate list (rule 6's refusal). NO
// property of a candidate-only tail row (one of the C ids rule 5's own
// count argument places past dedup(baseline)'s own prefix) is a content
// check, IDField and SortField included: rule 3 checks that IDField is
// UNIQUE within the candidate, never that it names a real, correct
// element; rule 6 checks that SortField is a MONOTONE sequence, never
// that any one value is the row's true one. Uniqueness and monotonicity
// are the only STRUCTURAL properties this shape can ever verify for such
// a row, and they hold for an arbitrary wrong-but-unique id or an
// arbitrary wrong-but-ordered timestamp exactly as they hold for a
// correct one. This is not a gap this shape leaves open by choice: it is
// the mechanism's own limit -- baseline's page was truncated BEFORE
// reaching that row at all, so no baseline value for ANY of its fields,
// including its own id, exists anywhere in this comparison to check
// against. A Go-side defect confined to such a row is real, distinct
// from the truncation this shape names, and this shape does not, and
// structurally cannot, admit it away -- compareList's own per-element
// comparison (compare.go, the `compareJSON` loop) never runs at all once
// the two lists' lengths differ; compareList's own OrderInsensitiveList
// branch is the one path that compares field-by-field across a length
// difference, and is not this route's own declaration (an ORDER
// BY-sorted page, not an order-insensitive keyed set), so no OTHER shape
// or the blanket per-leaf rule reaches a candidate-only tail row at all.
type DuplicateCollapsePageCutShape struct {
	// ListPath is the dotted, index-free path to the list itself, e.g.
	// "data.items".
	ListPath string
	// IDField is the object field that identifies one logical element
	// within an element of the list at ListPath, e.g. RESTDedupKeyField.
	IDField string
	// SortField is the object field the route's own live query orders
	// DESC by, e.g. "created_at" (drilldown/prs.go's own `ORDER BY
	// pr.created_at DESC`, api/queries/drilldown.py's identical `ORDER
	// BY created_at DESC` -- the route accepts a `sort` request field but
	// never reads it, so every admissible request through this route
	// reaches the SAME hardcoded field). Read as a timestamp (see rule
	// 6's own doc comment); a route using this shape with a numeric sort
	// field is out of this declaration's own scope.
	SortField string
	// Limit is the effective LIMIT this specific request's own query
	// carried -- the request's own limit value when it sent one,
	// otherwise the route's own default (the identical convention
	// LimitDisplacementShape's own Limit field already establishes). It
	// is a property of ONE request, not the route in general, so a
	// caller composing per-request Options sets a fresh copy of this
	// field for every corpus entry that can reach a different limit.
	Limit int
}

// duplicateCollapsePageCutPlan is one comparison's fully-evaluated
// admission decision, built once per defect from the two decoded lists.
type duplicateCollapsePageCutPlan struct {
	shape   *DuplicateCollapsePageCutShape
	applies bool
}

// buildDuplicateCollapsePageCutPlan evaluates every rule
// DuplicateCollapsePageCutShape documents against one comparison's two
// decoded lists.
func buildDuplicateCollapsePageCutPlan(shape *DuplicateCollapsePageCutShape, baselineData, candidateData any) *duplicateCollapsePageCutPlan {
	plan := &duplicateCollapsePageCutPlan{shape: shape}

	// A non-positive Limit is caught here for clarity (a misconfigured
	// declaration, stated as its own condition) but is structurally
	// redundant with rule 1 immediately below: len() is never negative,
	// so len(baseList) != shape.Limit already refuses for ANY Limit <= 0
	// except Limit == 0 with an EMPTY baseList, a case rule 2's own
	// hasDuplicate gate (an empty list has no duplicate) catches next.
	// No constructible input distinguishes this check's own removal from
	// one of those two surviving it -- proven by direct mutation -- kept
	// as the type's own explicit, named precondition rather than an
	// implicit consequence of arithmetic a reader must re-derive.
	if shape.Limit <= 0 {
		return plan
	}

	// listAtDottedPath's own decode failure is likewise redundant with
	// rule 1: a failed decode reads as a nil/empty list (length 0), and
	// Limit is already confirmed > 0 above, so len(baseList) != shape.Limit
	// refuses unconditionally. Kept as this shape's own explicit "cannot
	// evaluate" precondition, matching the discipline every other decode
	// failure in this file states directly rather than leaving to an
	// arithmetic side effect.
	baseList, ok1 := listAtDottedPath(baselineData, shape.ListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.ListPath)
	if !ok1 || !ok2 {
		return plan
	}

	// Rule 1.
	if len(baseList) != shape.Limit {
		return plan
	}

	// Rule 2: dedup(baseline) in first-occurrence order; every duplicate
	// id's own copies byte-identical.
	baseGroups := map[string][]map[string]any{}
	seen := map[string]bool{}
	var dedupOrder []string
	dedupRepresentative := map[string]map[string]any{}
	for _, element := range baseList {
		// edgeObjectAndID's own "not ok" case (a malformed element, or
		// one carrying no usable id) is NOT re-checked here with its own
		// early exit: it returns id="", object=nil. Every baseline element
		// -- malformed or not -- ends up represented somewhere in
		// dedupOrder, which rule 4 below compares position for position
		// against the candidate's own prefix; a spurious "" entry can
		// never match a well-formed candidate id there (edgeObjectAndID
		// itself refuses an empty id on the candidate side, so candidate
		// never produces a matching ""), so this always surfaces as a
		// rule-4 id-set mismatch instead of a dedicated early exit here.
		// Proven unreachable as an INDEPENDENT guard by direct mutation:
		// removing an early exit at this exact point never changes this
		// plan's own observable admit/refuse verdict on any constructible
		// input -- the "a guard no input can reach is deleted, not kept"
		// discipline. This does NOT extend to the candidate's own element
		// decode (rule 3 below): a malformed CANDIDATE element in the
		// TAIL (past rule 4's own prefix range) is never examined by rule
		// 4 at all, so that decode failure is its own, independently
		// load-bearing check.
		object, id, _ := edgeObjectAndID(element, shape.IDField)
		baseGroups[id] = append(baseGroups[id], object)
		if !seen[id] {
			seen[id] = true
			dedupOrder = append(dedupOrder, id)
			dedupRepresentative[id] = object
		}
	}
	hasDuplicate := false
	for _, group := range baseGroups {
		if len(group) > 1 {
			hasDuplicate = true
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

	// Rule 2b: the baseline's own RAW physical sequence (duplicates
	// included, in the order the route's own capture returned them) is
	// weakly monotone by SortField, DESC, ties permitted -- the same
	// check rule 6 applies to the candidate, applied here to the OTHER
	// leg. This is never load-bearing for rule 6's own proof (every
	// baseline row's SortField value is >= m there regardless of the
	// baseline's own physical order -- see that proof's own text, which
	// invokes only id membership in dedup(baseline), never a position
	// comparison between two baseline rows): it is a defensive input
	// check, refusing a baseline body no honest ORDER BY ... DESC
	// execution could ever produce (two physical copies of one id share
	// the SAME SortField value by rule 2, so honest duplicates are
	// always ties, never a strict violation) before this shape treats it
	// as a genuine capture of the declared mechanism at all.
	baseSort := make([]time.Time, len(baseList))
	for i, element := range baseList {
		ts, ok := duplicateCollapsePageCutSortValue(element, shape.SortField)
		if !ok {
			return plan
		}
		baseSort[i] = ts
	}
	for i := 1; i < len(baseSort); i++ {
		if baseSort[i].After(baseSort[i-1]) {
			return plan
		}
	}

	// Rule 3: the candidate carries no repeated id.
	candByID := make(map[string]map[string]any, len(candList))
	candOrder := make([]string, 0, len(candList))
	for _, element := range candList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
		}
		if _, already := candByID[id]; already {
			return plan
		}
		candByID[id] = object
		candOrder = append(candOrder, id)
	}

	// Rule 4: dedup(baseline) is candidate's own literal prefix.
	if len(candOrder) < len(dedupOrder) {
		return plan
	}
	for i, id := range dedupOrder {
		if candOrder[i] != id {
			return plan
		}
		if !jsonValuesEqual(dedupRepresentative[id], candByID[id]) {
			return plan
		}
	}

	// Rule 5: the candidate never exceeds the route's own limit. This is
	// the count argument (candidate-only ids vs. wasted duplicate slots)
	// reduced algebraically, not a separate check: rule 1 already pins
	// baseline length to Limit, and rule 4 already pins dedup(baseline)'s
	// own length as a fixed floor under the candidate's length, so
	// C = len(candidate)-len(dedup(baseline)) and E = Limit-len(dedup(baseline))
	// compare EXACTLY as len(candidate) compares to Limit -- there is no
	// way to reach C < E at a candidate length that also equals Limit, or
	// C > E at a candidate length that does not exceed it. Checking the
	// length directly says the same thing without a branch nothing can
	// ever reach.
	if len(candList) > shape.Limit {
		return plan
	}

	// Rule 6: the candidate is monotone by SortField, DESC, ties
	// permitted. This alone already establishes "every candidate-only
	// row orders at or after the baseline's own last row" -- see the
	// type doc comment's own proof, which reads no SortField value from
	// the baseline side (rule 2b's own baseline read exists purely as a
	// defensive input check on the OTHER leg, not as a premise this
	// proof depends on).
	candSort := make([]time.Time, len(candList))
	for i, element := range candList {
		ts, ok := duplicateCollapsePageCutSortValue(element, shape.SortField)
		if !ok {
			return plan
		}
		candSort[i] = ts
	}
	for i := 1; i < len(candSort); i++ {
		if candSort[i].After(candSort[i-1]) {
			return plan
		}
	}

	plan.applies = true
	return plan
}

// duplicateCollapsePageCutSortValue reads one list element's own
// SortField as a timestamp. ok is false when the element is not an
// object, the field is absent, not a string, or does not parse under
// parseTimestamp -- the caller treats that as "cannot evaluate rule 6 at
// all" rather than guessing an order.
func duplicateCollapsePageCutSortValue(element any, sortField string) (time.Time, bool) {
	// This type assertion is structurally redundant with rule 3's own
	// identical assertion (edgeObjectAndID, above): rule 3 already
	// refuses on ANY non-object candidate element, anywhere in the list,
	// before rule 6's own loop ever calls this function. Kept anyway as
	// this function's own contract, not this file's: a private helper
	// that reads an arbitrary list element must never assume a caller
	// upstream already validated its shape, or a future caller (or a
	// reordering of rules 3/6) turns a graceful "cannot evaluate" into a
	// panic.
	object, ok := element.(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	// A field that is ABSENT and a field that is JSON `null` are the same
	// Go value here (a nil interface): map indexing a Go map gives the
	// zero value either way, so this ok check is itself redundant with
	// the string assertion right below it (raw.(string) on nil also
	// fails) -- kept as ITS OWN named case ("the field is absent") rather
	// than folded into "the field is not a string", so a reader sees the
	// SAME distinction rule 6's own doc comment states without having to
	// re-derive it from Go's own zero-value behaviour.
	raw, ok := object[sortField]
	if !ok {
		return time.Time{}, false
	}
	str, ok := raw.(string)
	if !ok {
		return time.Time{}, false
	}
	return parseTimestamp(str)
}

// admits reports whether one Finding is covered by this plan: only the
// list's own ShapeLength finding, and only when every rule above held.
func (p *duplicateCollapsePageCutPlan) admits(finding Finding) bool {
	if p == nil || !p.applies {
		return false
	}
	return finding.Shape == ShapeLength && tieredPath(finding.Path) == p.shape.ListPath
}
