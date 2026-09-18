package goapiproof

import "strings"

// BoundaryTieShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to ONE
// structural consequence a genuinely UNORDERED tie at a LIMIT-bounded
// list's own cutoff can produce: when strictly more rows share the
// list's own trailing ORDER BY value than remain before LIMIT cuts the
// list off, each plane's own ClickHouse execution admits an unspecified
// subset of that tied group into the returned list -- never guaranteed
// the SAME subset, because neither plane's own SQL carries a secondary
// sort key to break the tie deterministically (see the citing
// BaselineDefect's own Reason for the two query citations). There is no
// "correct" member of a tie to prefer: unlike LimitDisplacementShape,
// this mechanism has no value-inflating root cause and no direction.
//
// Established from source for GET/POST /api/v1/drilldown/issues:
// fetch_issues (api/queries/drilldown.py) and fetchIssuesQuery
// (internal/drilldown/issues.go) both carry `ORDER BY wct.completed_at
// DESC` with no secondary sort, then `LIMIT :limit`, identically on both
// planes -- so a completed_at value shared by more rows than remain
// under the limit can put a DIFFERENT subset of that tied group on each
// plane's own list, never merely a different ORDER of the same subset
// (the sibling OrderInsensitiveList declaration for the same ListPath
// already clears a pure reordering of a SHARED subset on its own, with
// no shape needed -- this type exists for what that keyed pairing still
// reports: a key present on only one side).
//
// The shape this type verifies, over the two DECODED lists at ListPath:
//
//  1. Both lists are EXACTLY Limit long -- the actual effective LIMIT
//     this specific request carried. Either length off Limit refuses
//     the whole plan: a list short of the limit has no boundary at all,
//     the same "no boundary, no displacement" reasoning
//     LimitDisplacementShape's own rule 1 already applies.
//  2. The baseline-only and candidate-only key sets (by KeyFields, the
//     SAME pairing key the sibling OrderInsensitiveList declaration
//     uses) are equal in size -- always true when both lists are the
//     same length and share every other key, so this is a structural
//     sanity check, not a new constraint. Both empty admits nothing
//     (valid, but there is nothing to cover this run -- the
//     Intermittent state).
//  3. Every baseline-only key's own TieField value, and every
//     candidate-only key's own TieField value, normalize (see
//     normalizeTieValue) to ONE SHARED value -- the single tie both
//     planes' own arbitrary cut fell inside. A baseline-only or
//     candidate-only key whose own TieField value does not normalize,
//     or does not match that shared value, refuses the WHOLE plan: a
//     mixed cause (this mechanism plus something else) is never
//     partially admitted.
//
// WHAT THIS SHAPE DOES NOT, AND CANNOT, CERTIFY: that the admitted keys
// are genuinely interchangeable beyond sharing TieField -- e.g. two rows
// tied on completed_at but only one of them actually belongs to the
// requested scope would look identical to this shape. That premise
// rests on the citing BaselineDefect's own Reason (both planes' SQL
// applies the SAME WHERE/scope predicates ahead of ORDER BY), never
// independently measured here.
type BoundaryTieShape struct {
	// ListPath is the dotted, index-free path to the list, e.g.
	// "data.items".
	ListPath string
	// KeyFields names the field(s) that together key one row -- the SAME
	// KeyFields the sibling OrderInsensitiveList declaration for this
	// same ListPath uses, e.g. []string{"work_item_id"}.
	KeyFields []string
	// TieField is the row's own ORDER BY field whose shared value
	// explains the swap, e.g. "completed_at".
	TieField string
	// Limit is the effective LIMIT this specific request's own query
	// carried -- the same per-request contract LimitDisplacementShape.
	// Limit documents.
	Limit int
}

// boundaryTieRow is one decoded list element's own key/TieField
// projection. tie is the RAW decoded value at TieField -- typically a
// string, but possibly nil (a genuinely Nullable column) or absent --
// normalizeTieValue is the one place that is resolved into a comparable
// string.
type boundaryTieRow struct {
	tie any
}

// boundaryTieRowSet is one plane's fully-decoded, key-indexed list.
type boundaryTieRowSet struct {
	order []string
	byKey map[string]boundaryTieRow
}

// boundaryTiePlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded lists.
type boundaryTiePlan struct {
	valid                 bool
	admittedBaselineOnly  map[string]bool
	admittedCandidateOnly map[string]bool
}

// buildBoundaryTiePlan evaluates every rule BoundaryTieShape documents.
// Unlike LimitDisplacementShape it reads nothing from any OTHER defect's
// own `covered` state -- the two decoded lists are all this mechanism
// ever needs -- so, like every other self-contained shape in this
// package, it runs in classifyBaselineDefects' first pass, not the
// second.
func buildBoundaryTiePlan(shape *BoundaryTieShape, baselineData, candidateData any) *boundaryTiePlan {
	plan := &boundaryTiePlan{admittedBaselineOnly: map[string]bool{}, admittedCandidateOnly: map[string]bool{}}

	baseRows, ok1 := decodeBoundaryTieRows(baselineData, shape)
	candRows, ok2 := decodeBoundaryTieRows(candidateData, shape)
	if !ok1 || !ok2 {
		return plan
	}

	// Rule 1.
	if len(baseRows.order) != shape.Limit || len(candRows.order) != shape.Limit {
		return plan
	}

	var baselineOnly, candidateOnly []string
	for _, key := range baseRows.order {
		if _, ok := candRows.byKey[key]; !ok {
			baselineOnly = append(baselineOnly, key)
		}
	}
	for _, key := range candRows.order {
		if _, ok := baseRows.byKey[key]; !ok {
			candidateOnly = append(candidateOnly, key)
		}
	}
	// Rule 2.
	if len(baselineOnly) != len(candidateOnly) {
		return plan
	}
	if len(baselineOnly) == 0 {
		// Nothing to admit this run -- a valid, empty plan, the
		// Intermittent "absent" state.
		plan.valid = true
		return plan
	}

	// Rule 3.
	shared, ok := normalizeTieValue(baseRows.byKey[baselineOnly[0]].tie)
	if !ok {
		return plan
	}
	for _, key := range baselineOnly {
		tie, ok := normalizeTieValue(baseRows.byKey[key].tie)
		if !ok || tie != shared {
			return plan
		}
	}
	for _, key := range candidateOnly {
		tie, ok := normalizeTieValue(candRows.byKey[key].tie)
		if !ok || tie != shared {
			return plan
		}
	}

	plan.valid = true
	for _, key := range baselineOnly {
		plan.admittedBaselineOnly[key] = true
	}
	for _, key := range candidateOnly {
		plan.admittedCandidateOnly[key] = true
	}
	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *boundaryTiePlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	key, ok := parsePresenceDetailKey(finding.Detail)
	if !ok {
		return false
	}
	switch {
	case strings.Contains(finding.Detail, "absent in candidate"):
		return p.admittedBaselineOnly[key]
	case strings.Contains(finding.Detail, "absent in baseline"):
		return p.admittedCandidateOnly[key]
	}
	return false
}

// decodeBoundaryTieRows reads shape.ListPath into a key-indexed row set.
// ok is false when the path does not resolve to a list of objects each
// carrying every declared key field, or when two elements on the same
// side share a key -- the same vacuity discipline orderInsensitiveKey's
// own callers use elsewhere in this package. TieField is read but never
// validated here: a row this run never needs as a baseline-only or
// candidate-only entrant can carry a null/absent TieField (a genuinely
// Nullable column) with no effect on decoding the rest of the list --
// normalizeTieValue is where an entrant's own TieField is actually
// required to resolve.
func decodeBoundaryTieRows(root any, shape *BoundaryTieShape) (boundaryTieRowSet, bool) {
	listValue, ok := navigateSegments(root, citedSegments(shape.ListPath))
	if !ok {
		return boundaryTieRowSet{}, false
	}
	list, ok := listValue.([]any)
	if !ok {
		return boundaryTieRowSet{}, false
	}
	out := boundaryTieRowSet{byKey: make(map[string]boundaryTieRow, len(list))}
	for _, element := range list {
		key, ok := orderInsensitiveKey(element, shape.KeyFields)
		if !ok {
			return boundaryTieRowSet{}, false
		}
		if _, duplicate := out.byKey[key]; duplicate {
			return boundaryTieRowSet{}, false
		}
		object, ok := element.(map[string]any)
		if !ok {
			return boundaryTieRowSet{}, false
		}
		out.order = append(out.order, key)
		out.byKey[key] = boundaryTieRow{tie: object[shape.TieField]}
	}
	return out, true
}

// normalizeTieValue resolves one row's raw TieField value into a
// comparable string, or refuses (ok false) for anything that is not a
// non-empty string -- a nil (Nullable column, genuinely absent) or any
// other JSON type carries no comparable instant, and treating it as a
// valid tie key would let two UNRELATED rows that both happen to carry
// an empty/null TieField collide into a false shared value. Strips an
// optional trailing "Z" -- the ONE declared difference between the two
// planes' own datetime wire form for this route's started_at/
// completed_at columns (Python emits a naive isoformat string, Go emits
// RFC 3339 with an explicit UTC offset) -- so a genuinely shared tie
// value compares equal across planes despite that already-declared
// formatting difference, without this shape re-deciding anything the
// sibling datetime-format defect already owns.
func normalizeTieValue(raw any) (string, bool) {
	s, ok := raw.(string)
	if !ok || s == "" {
		return "", false
	}
	return strings.TrimSuffix(s, "Z"), true
}
