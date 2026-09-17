package goapiproof

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
// The verdict is PER LOGICAL ID, never a whole-comparison boolean:
// the mechanism this shape explains is a property of one edge at a time
// -- one id's physical versions disagreeing says nothing about whether a
// DIFFERENT id's versions agree -- so one id's own failure excludes only
// that id, and every other id that independently satisfies the rules
// below is still admitted. An excluded id's own findings report as
// ordinary, uncovered differences, and the report names which id: see
// uncoveredEdgeID.
//
// The shape this type verifies, over the two DECODED edge lists, per id:
//
//  1. Every physical copy of this id in the baseline list is, ignoring
//     order, equal to every other copy field for field: the SAME logical
//     edge inserted more than once, not a real per-field disagreement
//     hiding behind a shared id. A number compares by VALUE, not by
//     literal form, so a plane that always writes a whole number as "1"
//     and one that sometimes writes it as "1.0" still agree -- that is a
//     formatting choice, never the difference this rule exists to catch.
//     An id present only once in the baseline trivially satisfies this
//     rule (nothing to disagree with).
//  2. Where this id is also present in the candidate list, the
//     (deduplicated) baseline element and the candidate element are
//     equal, field for field, by the same by-value comparison as rule 1.
//     An id present in baseline only is outside this rule and stands or
//     falls on rule 1 alone -- see below.
//
// Two conditions gate whether the shape applies to this comparison AT
// ALL, evaluated once, not per id, because neither has a per-id answer:
//
//  3. The candidate list carries no repeated id at all -- the mechanism
//     this shape explains is one-sided. A repeated id on the candidate
//     side is a different, unexplained condition, and nothing here may
//     admit anything while it holds.
//  4. At least one id was actually repeated in the baseline and at least
//     one id was actually shared between the two lists -- a plan built
//     from a comparison with neither has nothing to admit and never
//     applies, the same safe default every shape in this package uses.
//
// An id present in the candidate list but never in the baseline list is
// NOT a contradiction of this shape: a page-length budget spent on
// baseline duplicate rows means the baseline simply never reached that
// edge within the page, exactly as a deduplicated candidate page reaches
// further than a raw one for the same request. Rule 2 only asks that
// where the two pages DO overlap, by id, their content agrees -- which is
// the fact that tells a genuine field-level regression on that one id
// apart from the duplicate-row mechanism.
//
// An id excluded by rule 1 or rule 2 -- its own duplicate copies
// disagree, or its shared content disagrees with the candidate -- is NOT
// covered: classifyBaselineDefects reports its own findings as ordinary
// differences outside the citation, exactly like any other uncited
// mismatch, and names the id in each finding's own Detail.
type WorkGraphEdgeDedupShape struct {
	// EdgesListPath is the dotted, index-free path to the edge LIST
	// itself (no trailing field name), e.g. "data.workGraphEdges.edges".
	EdgesListPath string
	// IDField is the object field that identifies one logical edge within
	// an element of the list at EdgesListPath, e.g. "edgeId".
	IDField string
	// TrailingCursorPath is an OPTIONAL sibling leaf -- not itself an
	// element of the list at EdgesListPath -- whose value deterministically
	// names whichever edge lands in the page's LAST slot (a GraphQL
	// pageInfo.endCursor, e.g. "data.workGraphEdges.pageInfo.endCursor").
	// A shift in the page's last slot moves this field exactly as it
	// moves every edge element, so its own admission is resolved through
	// the LAST baseline list position's id, the same per-id rules 1/2
	// every edge element itself is judged by. Leave empty when the
	// defect's own Paths name no such field -- as drilldown/prs's own
	// entry does not, having no cursor at all.
	TrailingCursorPath string
	// WriteOnceField, when set, switches this shape to a second,
	// narrower admission: it admits ONLY an id whose baseline copies
	// DISAGREE, and only when that disagreement is exactly the one a
	// write-once nullable column produces while an unmerged older
	// physical version is still visible -- see writeOnceRepresentative.
	// An id whose copies agree is left alone (neither admitted nor
	// annotated): a sibling entry without WriteOnceField judges it by
	// rules 1/2. Per id, in this mode:
	//
	//   - every copy carries the same key set, and every field other
	//     than WriteOnceField is equal across copies (rule 1's by-value
	//     comparison);
	//   - WriteOnceField is null on at least one copy and, on every
	//     other copy, one and the same timestamp string (exact text,
	//     so two populated values never collapse into one);
	//   - the id IS present in the candidate list, and the candidate
	//     element equals the populated copy field for field (rule 2's
	//     comparison), so a candidate null, a candidate carrying a
	//     different populated value, or any other candidate field
	//     difference refuses the id.
	//
	// The field is named per entry and never inferred: this is not a
	// general "null loses" rule, and an entry names a field here only
	// when the upstream writer itself refuses a null over a populated
	// value for that field. Rules 3 and 4 gate this mode exactly as they
	// gate the default one.
	WriteOnceField string
}

// workGraphEdgeDedupPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// edge lists.
type workGraphEdgeDedupPlan struct {
	// applies is whether this shape's mechanism is in play in this
	// comparison at all -- rules 3 and 4 of the type doc comment, which
	// have no per-id answer. false means nothing here is admitted and
	// admittedIDs/baselineIndexID are never consulted, the same safe
	// default every shape in this package uses.
	applies bool
	// edgesListPath is shape.EdgesListPath, kept on the plan so admits/
	// uncoveredEdgeID can resolve a finding's own list position without
	// the caller re-threading the shape through every call.
	edgesListPath string
	// trailingCursorPath is shape.TrailingCursorPath, or empty when the
	// shape names none.
	trailingCursorPath string
	// admittedIDs is the set of ids whose OWN situation satisfies rules
	// 1 and 2: every physical copy of that id in the baseline agrees
	// with the others, and where the id is also present in the
	// candidate, that content agrees too. Evaluated per id -- one id's
	// absence from this set says nothing about any other id.
	admittedIDs map[string]bool
	// baselineIndexID names, by LIST POSITION (the same index a finding's
	// own indexed path carries, e.g. the "2" in
	// "$.data.items[2].createdAt"), the id occupying that position in
	// the BASELINE list. A finding's owning id is resolved through the
	// BASELINE side specifically, because the baseline is the side that
	// spends page slots on duplicate rows, so it is the baseline's id at
	// a given position that explains why that position shifted -- the
	// candidate's element at the same numeric position is very often a
	// different logical edge entirely once any earlier duplicate has
	// consumed a slot.
	baselineIndexID []string
	// writeOnceField is shape.WriteOnceField. When set, judgedIDs names
	// the ids this plan evaluated at all (the ids whose baseline copies
	// disagree); an id outside judgedIDs belongs to a sibling entry, so
	// uncoveredEdgeID names nothing for it.
	writeOnceField string
	judgedIDs      map[string]bool
}

// edgeListIndex extracts the list-position index a finding's own path
// names into edgesListPath's own list -- e.g. path
// "$.data.items[2].createdAt" against edgesListPath "data.items" names
// index 2. ok is false when the path does not name an index into this
// exact list; every finding admits/uncoveredEdgeID is ever asked about
// already lies under EdgesListPath (defectCovers filters to the
// defect's own Paths first), so this only parses out the position that
// check does not need to keep.
func edgeListIndex(path, edgesListPath string) (int, bool) {
	prefix := "$." + edgesListPath + "["
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return 0, false
	}
	end := strings.IndexByte(rest, ']')
	if end < 0 {
		return 0, false
	}
	index, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0, false
	}
	return index, true
}

// owningID resolves the baseline id a finding's own path belongs to: an
// indexed element under EdgesListPath resolves through that index; the
// declared TrailingCursorPath (when set and matched exactly) resolves
// through the LAST baseline position instead, because a page-end cursor
// names whichever edge lands in the final slot and shifts exactly as
// that edge does. ok is false for a path that names neither -- the plan
// does not apply, the index is out of range, or the path is not one of
// ours.
func (p *workGraphEdgeDedupPlan) owningID(finding Finding) (string, bool) {
	if p == nil || !p.applies || len(p.baselineIndexID) == 0 {
		return "", false
	}
	if index, ok := edgeListIndex(finding.Path, p.edgesListPath); ok {
		if index < 0 || index >= len(p.baselineIndexID) {
			return "", false
		}
		return p.baselineIndexID[index], true
	}
	if p.trailingCursorPath != "" && finding.Path == "$."+p.trailingCursorPath {
		return p.baselineIndexID[len(p.baselineIndexID)-1], true
	}
	return "", false
}

// admits reports whether one Finding is covered by this plan: the plan
// must apply at all (rules 3/4), the finding must resolve to an owning
// id (owningID), and that id must be in admittedIDs (rules 1/2,
// evaluated for that one id).
func (p *workGraphEdgeDedupPlan) admits(finding Finding) bool {
	id, ok := p.owningID(finding)
	return ok && p.admittedIDs[id]
}

// uncoveredEdgeID resolves the id a NOT-admitted finding belongs to, so
// the caller can name it in the finding's own Detail: "which id was not
// covered" stays readable straight from the report, not only from a
// count.
func (p *workGraphEdgeDedupPlan) uncoveredEdgeID(finding Finding) (string, bool) {
	id, ok := p.owningID(finding)
	if !ok {
		return "", false
	}
	if p.writeOnceField != "" && !p.judgedIDs[id] {
		return "", false
	}
	return id, true
}

// refusalDetail is the Detail suffix naming one not-admitted id and the
// rule that refused it.
func (p *workGraphEdgeDedupPlan) refusalDetail(id string) string {
	if p.writeOnceField != "" {
		return fmt.Sprintf(" (dedup id %q not admitted by the declared write-once %s rule: its baseline copies differ in more than null versus one populated %s value, or the candidate row is absent or differs from the populated copy)", id, p.writeOnceField, p.writeOnceField)
	}
	return fmt.Sprintf(" (dedup id %q not admitted by the declared duplicate-row shape: its own baseline copies disagree, or its shared content differs from the candidate)", id)
}

// buildWorkGraphEdgeDedupPlan evaluates every rule WorkGraphEdgeDedupShape
// documents against one comparison's two decoded edge lists, producing a
// PER-ID admission set rather than a single whole-comparison verdict.
func buildWorkGraphEdgeDedupPlan(shape *WorkGraphEdgeDedupShape, baselineData, candidateData any) *workGraphEdgeDedupPlan {
	plan := &workGraphEdgeDedupPlan{edgesListPath: shape.EdgesListPath, trailingCursorPath: shape.TrailingCursorPath, writeOnceField: shape.WriteOnceField}

	baseList, ok1 := listAtDottedPath(baselineData, shape.EdgesListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.EdgesListPath)
	if !ok1 || !ok2 {
		return plan
	}

	baseGroups := map[string][]map[string]any{}
	baselineIndexID := make([]string, len(baseList))
	for i, element := range baseList {
		object, id, ok := edgeObjectAndID(element, shape.IDField)
		if !ok {
			return plan
		}
		baseGroups[id] = append(baseGroups[id], object)
		baselineIndexID[i] = id
	}

	// Rule 1, evaluated PER ID: a repeated id whose own copies disagree
	// is excluded below (agreeingIDs[id] stays false) rather than
	// invalidating every other id's own verdict.
	//
	// In WriteOnceField mode the roles flip: only a DISAGREEING id is
	// judged (judged[id]), it passes rule 1 only through
	// writeOnceRepresentative, and its populated copy stands in for it in
	// rule 2.
	baseUnique := make(map[string]map[string]any, len(baseGroups))
	agreeingIDs := make(map[string]bool, len(baseGroups))
	judged := make(map[string]bool, len(baseGroups))
	hasDuplicate := false
	for id, group := range baseGroups {
		first := group[0]
		agree := true
		for _, other := range group[1:] {
			if !jsonValuesEqual(first, other) {
				agree = false
				break
			}
		}
		if len(group) > 1 {
			hasDuplicate = true
		}
		baseUnique[id] = first
		if shape.WriteOnceField == "" {
			agreeingIDs[id] = agree
			continue
		}
		if agree {
			continue
		}
		judged[id] = true
		if representative, ok := writeOnceRepresentative(group, shape.WriteOnceField); ok {
			baseUnique[id] = representative
			agreeingIDs[id] = true
		}
	}
	if !hasDuplicate {
		return plan
	}

	// Rule 3: the candidate carries no repeated id at all. Unlike rules
	// 1/2 this stays a WHOLE-COMPARISON gate -- a repeated candidate id
	// is a different, unexplained condition this shape has no per-id
	// answer for, not something excluding just that one id fixes.
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

	// Rule 2, evaluated PER ID: a shared id whose content disagrees is
	// excluded, not a whole-plan failure -- the fact that tells a real
	// per-id regression apart from the duplicate-row mechanism now
	// narrows admission to every OTHER id, rather than invalidating the
	// whole comparison. An id present in baseline only stands on rule 1
	// alone (see the type doc comment).
	admitted := make(map[string]bool, len(baseUnique))
	shared := 0
	for id, baseObject := range baseUnique {
		candObject, ok := candByID[id]
		if !ok {
			// WriteOnceField mode needs the candidate's own row to
			// confirm the populated copy; without it nothing is admitted.
			admitted[id] = agreeingIDs[id] && shape.WriteOnceField == ""
			continue
		}
		shared++
		admitted[id] = agreeingIDs[id] && jsonValuesEqual(baseObject, candObject)
	}
	if shared == 0 {
		return plan
	}

	plan.applies = true
	plan.admittedIDs = admitted
	if shape.WriteOnceField != "" {
		plan.judgedIDs = judged
	}
	plan.baselineIndexID = baselineIndexID
	return plan
}

// writeOnceRepresentative reports the copy of one id that a
// deduplicated read of a write-once nullable field returns, when the
// group's copies differ ONLY in that field: every copy carries the same
// key set, every other field agrees by value, field is null on at least
// one copy, and every non-null copy carries the SAME timestamp string
// (compared as exact text). ok is false for anything else -- two
// different populated values, a non-timestamp value, a missing key, a
// difference in any other field, or a group with no null copy or no
// populated copy.
func writeOnceRepresentative(group []map[string]any, field string) (map[string]any, bool) {
	var populated map[string]any
	var populatedValue string
	sawNull := false
	for _, copyObject := range group {
		value, present := copyObject[field]
		if !present {
			return nil, false
		}
		switch typed := value.(type) {
		case nil:
			sawNull = true
		case string:
			if _, isTimestamp := parseTimestamp(typed); !isTimestamp {
				return nil, false
			}
			if populated == nil {
				populated, populatedValue = copyObject, typed
			} else if typed != populatedValue {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	if !sawNull || populated == nil {
		return nil, false
	}
	for _, copyObject := range group {
		if len(copyObject) != len(populated) {
			return nil, false
		}
		for key, value := range populated {
			if key == field {
				continue
			}
			other, present := copyObject[key]
			if !present || !jsonValuesEqual(value, other) {
				return nil, false
			}
		}
	}
	return populated, true
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
// value. A string that names a timestamp compares the same way, by
// parsed INSTANT through timestampsEqual, so an offset form and a naive
// form (or two different offsets) naming the same moment agree, exactly
// as the naive-vs-aware BaselineDefects declared elsewhere in this
// corpus already treat that difference as a wire
// formatting choice, not a content disagreement -- this shape's own Rule
// 3 must not read the very divergence a sibling citation already
// excuses as proof the shape does not apply. A string that fails to
// parse as a timestamp under every layout timestampsEqual tries falls
// back to plain string equality; this never guesses at what counts as a
// timestamp. reflect.DeepEqual has none of this normalization -- it
// would read either plane's own formatting convention as a content
// disagreement, which is a different plane's serializer, not a
// duplicate row whose fields actually differ. Every other JSON kind
// (object, list, bool, null) compares structurally, recursively.
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
	if aStr, ok := a.(string); ok {
		bStr, ok := b.(string)
		if !ok {
			return false
		}
		if aStr == bStr {
			return true
		}
		if equal, comparable := timestampsEqual(aStr, bStr); comparable {
			return equal
		}
		return false
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

// offsetTimestampLayouts are the wire forms this comparator parses as
// carrying their own explicit offset -- RFC3339Nano covers RFC3339 too
// (a trailing "Z" or a "+hh:mm"/"-hh:mm" offset, with or without
// fractional seconds), so listing it alone is enough; Go's time.Parse
// locates the result wherever the string itself says, never guessing.
var offsetTimestampLayouts = []string{time.RFC3339Nano}

// naiveTimestampLayout is Python's own naive isoformat -- no offset, no
// "Z", e.g. "2024-01-02T03:04:05" or "...T03:04:05.123456" -- the exact
// wire form every naive-datetime BaselineDefect in this corpus already
// names: clickhouse_connect returns the column with
// no tzinfo even though the column itself is UTC, so a naive string is
// read as UTC here, the same reading those declarations already assert
// ("Go is correct" because Go's driver attaches the UTC location the
// column declares).
const naiveTimestampLayout = "2006-01-02T15:04:05.999999999"

// parseTimestamp reports the instant s names under one of this
// comparator's known wire forms. ok is false for anything that is not
// an EXACT match to one of them -- a string that merely looks date-ish
// (a different layout, an extra suffix, a plain date with no time) is
// left alone rather than guessed at, so a coincidentally date-shaped
// field that is not actually a timestamp in this data can never be
// silently instant-compared.
func parseTimestamp(s string) (time.Time, bool) {
	for _, layout := range offsetTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	if t, err := time.ParseInLocation(naiveTimestampLayout, s, time.UTC); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// timestampsEqual reports whether a and b are the SAME instant, once
// both parse as a timestamp under parseTimestamp -- an offset form and a
// naive form (or two different offsets) naming the same moment compare
// equal, because the wire form is a formatting choice, never the
// difference this shape exists to catch (see jsonValuesEqual's own doc
// comment). comparable is false, and equal meaningless, whenever either
// side is not one of parseTimestamp's known forms: the caller then falls
// back to plain string equality rather than treating a parse failure as
// a mismatch in itself.
func timestampsEqual(a, b string) (equal, comparable bool) {
	aTime, aOK := parseTimestamp(a)
	if !aOK {
		return false, false
	}
	bTime, bOK := parseTimestamp(b)
	if !bOK {
		return false, false
	}
	return aTime.Equal(bTime), true
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
