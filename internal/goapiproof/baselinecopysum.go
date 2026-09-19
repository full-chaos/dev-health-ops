package goapiproof

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// ShapeBaselineCopies (structural): an order-insensitive list whose
// BASELINE side carries more than one element under one pairing key, and
// whose declared BaselineCopySumShape compared those copies as ONE element
// (their SumField summed). Reported once per such key, at that key's own
// element path, whatever the pair's comparison found; only
// BaselineCopySumShape's own admission can cover it.
const ShapeBaselineCopies = "baseline_copies"

// BaselineCopySumShape, set on a BaselineDefect, declares that the
// BASELINE plane writes one logical list element as several physical
// copies that share the list's pairing key and split its SumField between
// them, where the candidate plane writes the element once, valued at the
// whole. Measured instance: POST /api/v1/investment/flow/repo-team, whose
// reference builder appends one sankey link per (subcategory, repo, team)
// row, so a repo->team link repeats once per subcategory and a
// subcategory->repo link once per team; the candidate accumulates each
// (source, target) pair once.
//
// The rules:
//
//  1. Scope: the list at ListPath is declared order-insensitive (an
//     OrderInsensitiveList at the same Path) with exactly these KeyFields,
//     at least one, and ListPath is a dotted path of object keys under
//     data. Anywhere
//     else this shape changes nothing, and repeated baseline keys stay
//     refused by name.
//  2. Only the BASELINE side is ever collapsed. A candidate side with two
//     elements under one key is still refused by name
//     (RefusalOrderInsensitiveListKeyMissing), exactly as without this
//     shape.
//  3. Whole-key accounting: for each key, EVERY baseline copy is consumed
//     exactly once, into one element. The copies must agree on every
//     field except SumField, by value (jsonValuesEqual), carry the same
//     field set, and carry a finite number in SumField; copies that break
//     any of these are left as they are and the list is refused by name.
//     Nothing is paired by position, and no copy is chosen over another.
//  4. The collapsed element is the copies' shared fields with SumField set
//     to the sum of every copy's SumField, in baseline list order, placed
//     where the key first appears. The collapse runs once, at the start of
//     Compare, so the ordinary comparison AND every other declared shape
//     read the same collapsed baseline. The sum meets the candidate's
//     value under the entry's own declared float tier for that leaf
//     (FloatTierB, max(abs, rel) 1e-9), never under a tolerance of this
//     shape's own: the same addends summed in another order move only the
//     last bits of a float sum, orders of magnitude inside that tier.
//  5. Every collapsed key reports one ShapeBaselineCopies finding at its
//     element. The plan runs after every other declared defect has
//     decided, and admits it only when every other mismatch finding on
//     that key's element is covered by some other declaration -- a
//     presence finding (the key absent from the candidate) or a leaf
//     finding that nothing else explains keeps it outside. A candidate key
//     absent from the baseline stays an ordinary presence finding.
//
// Because every collapsed key leaves a mismatch finding, a comparison
// this shape applies to never reads as a match: it reads as a mismatch
// whose differences the defect covers, and a comparison with no repeated
// baseline key leaves the defect idle (it is declared Intermittent).
//
// What this shape cannot see: the raw body size. The body-size ratio
// check reads the baseline's raw bytes, so a baseline whose copies make
// it more than bodySizeRatioThreshold times the candidate is still
// refused as legs that do not overlap.
type BaselineCopySumShape struct {
	// ListPath is the dotted, index-free path to the list itself, equal
	// to the paired OrderInsensitiveList's own Path, e.g. "data.links".
	ListPath string
	// KeyFields equals the paired OrderInsensitiveList's own KeyFields,
	// in the same order.
	KeyFields []string
	// SumField is the numeric field the baseline copies split between
	// them, e.g. "value".
	SumField string
}

// baselineCopyList records one collapsed list for compareListByKey.
type baselineCopyList struct {
	// counts names every key that consumed more than one baseline copy,
	// with the number of copies.
	counts   map[string]int
	ticket   string
	sumField string
}

// collapseDeclaredBaselineCopies returns data with every declared
// BaselineCopySumShape's list collapsed (rules 1, 3 and 4), the collapsed
// lists by path, and one refusal per list whose copies are not one
// element split on SumField. data itself is never mutated: the maps on
// the path to a collapsed list are copied.
func collapseDeclaredBaselineCopies(data any, opts Options) (any, map[string]baselineCopyList, []string) {
	collapsedLists := map[string]baselineCopyList{}
	var refusals []string
	for _, defect := range opts.BaselineDefects {
		shape := defect.BaselineCopySumShape
		if shape == nil {
			continue
		}
		// An undeclared list reads as a zero declaration, whose nil
		// KeyFields never equal a shape's non-empty ones.
		decl, _ := findOrderInsensitiveList(opts, shape.ListPath)
		if len(shape.KeyFields) == 0 || !reflect.DeepEqual(decl.KeyFields, shape.KeyFields) {
			continue
		}
		segments := citedSegments(shape.ListPath)
		if segments == nil {
			continue
		}
		list, ok := listAtDottedPath(data, shape.ListPath)
		if !ok {
			continue
		}
		groups := make(map[string][]map[string]any, len(list))
		var order []string
		repeated := false
		keyed := true
		for _, element := range list {
			key, keyOK := orderInsensitiveKey(element, shape.KeyFields)
			if !keyOK {
				keyed = false
				break
			}
			if _, seen := groups[key]; seen {
				repeated = true
			} else {
				order = append(order, key)
			}
			groups[key] = append(groups[key], element.(map[string]any))
		}
		// An element without every key field is compareListByKey's own
		// refusal; a list with no repeated key needs no collapse.
		if !keyed || !repeated {
			continue
		}
		counts := map[string]int{}
		collapsed := make([]any, 0, len(order))
		failed := false
		for _, key := range order {
			group := groups[key]
			if len(group) == 1 {
				collapsed = append(collapsed, group[0])
				continue
			}
			element, collapseOK, reason := collapseBaselineCopies(group, shape.SumField)
			if !collapseOK {
				refusals = append(refusals, fmt.Sprintf(
					"order-insensitive list %q (ticket %s): the baseline side has %d elements sharing key %q that are not one element split on %q (%s) -- baseline copy sum (ticket %s) admits only copies equal in every other field",
					decl.Path, decl.Ticket, len(group), key, shape.SumField, reason, defect.Ticket))
				failed = true
				continue
			}
			counts[key] = len(group)
			collapsed = append(collapsed, element)
		}
		if failed {
			continue
		}
		data = replaceAtSegments(data, segments, collapsed)
		collapsedLists[shape.ListPath] = baselineCopyList{counts: counts, ticket: defect.Ticket, sumField: shape.SumField}
	}
	return data, collapsedLists, refusals
}

// replaceAtSegments returns root with the value at segments (object keys
// under data) replaced by value, copying every map on the way so root
// itself is unchanged. The path must already resolve (listAtDottedPath).
func replaceAtSegments(root any, segments []string, value any) any {
	if len(segments) == 0 {
		return value
	}
	object := maps.Clone(root.(map[string]any))
	object[segments[0]] = replaceAtSegments(object[segments[0]], segments[1:], value)
	return object
}

// collapseBaselineCopies applies rules 3 and 4 to the copies of one key,
// in baseline list order. ok is false, with a reason, when the copies are
// not one element split on SumField.
func collapseBaselineCopies(copies []map[string]any, sumField string) (collapsed map[string]any, ok bool, reason string) {
	shared := maps.Clone(copies[0])
	delete(shared, sumField)
	var sum float64
	useNumber := false
	for i, element := range copies {
		// An absent SumField reads as nil, which asFloat refuses.
		raw := element[sumField]
		value, numeric := asFloat(raw)
		if !numeric || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false, fmt.Sprintf("copy %d carries a non-finite or non-numeric %q (%v)", i, sumField, raw)
		}
		if _, isNumber := raw.(json.Number); isNumber {
			useNumber = true
		}
		sum += value
		rest := maps.Clone(element)
		delete(rest, sumField)
		if !jsonValuesEqual(shared, rest) {
			return nil, false, fmt.Sprintf("copy %d differs from copy 0 outside %q", i, sumField)
		}
	}
	if math.IsInf(sum, 0) {
		return nil, false, fmt.Sprintf("the copies' %q sum overflows", sumField)
	}
	collapsed = shared
	if useNumber {
		collapsed[sumField] = json.Number(strconv.FormatFloat(sum, 'g', -1, 64))
	} else {
		collapsed[sumField] = sum
	}
	return collapsed, true, ""
}

// baselineCopySumPlan is one comparison's admission decision for one
// BaselineCopySumShape (rule 5).
type baselineCopySumPlan struct {
	shape *BaselineCopySumShape
	// uncovered names the path of every mismatch finding, other than a
	// ShapeBaselineCopies one, that no declaration covers.
	uncovered []string
}

// buildBaselineCopySumPlan reads the comparison's final coverage: it runs
// after every other declared defect has decided, so a key whose collapsed
// pair differs in a way another declaration explains (a team scope's
// bounded subset, say) still has its copies accounted for, and a key
// whose pair differs in a way nothing explains does not.
func buildBaselineCopySumPlan(shape *BaselineCopySumShape, findings []Finding, findingRefs []int, covered []bool) *baselineCopySumPlan {
	plan := &baselineCopySumPlan{shape: shape}
	for i, ref := range findingRefs {
		if findings[ref].Shape != ShapeBaselineCopies && !covered[i] {
			plan.uncovered = append(plan.uncovered, findings[ref].Path)
		}
	}
	return plan
}

// admits reports whether one finding is covered by this plan: a finding
// at an element of the shape's own list (the gate in
// classifyBaselineDefects passes only ShapeBaselineCopies among
// structural findings; a leaf finding lies below its element and is never
// this plan's to admit) whose element carries no uncovered mismatch. An
// element is an object (orderInsensitiveKey), so everything below it
// continues its path with a dot.
func (p *baselineCopySumPlan) admits(finding Finding) bool {
	if p == nil || tieredPath(finding.Path) != p.shape.ListPath {
		return false
	}
	element := finding.Path
	for _, path := range p.uncovered {
		if path == element || strings.HasPrefix(path, element+".") {
			return false
		}
	}
	return true
}
