package goapiproof

import "strings"

// TeamRepoSubsetShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to a BOUNDED
// SUBSET claim over one list: the candidate's list is admitted only when
// every one of its elements is also present in the baseline's own list
// under the SAME key, with every declared leaf either byte-identical
// (EqualLeaves) or narrower on the candidate side (BoundedLeaves, and
// only for the elements BoundedLeafKeys names -- every other element
// still requires those same leaf names to be byte-identical). A
// candidate-only element -- one the baseline's own list does not carry --
// is never admitted under any circumstance: a subset can only ever be
// missing elements the wider list has, never gain one the wider list does
// not.
//
// WHY A SEPARATE SHAPE FROM KeyedDirectionShape/DictKeyDirectionShape:
// those two compare a value at a KNOWN, matched key on both sides and
// never touch the list's own membership -- a key present on only one side
// is invisible to them (KeyedDirectionShape's own admits, keyeddirection.go,
// dispatches only on the value leaf's own path). This shape's whole
// reason to exist is the opposite: the two planes here disagree on WHICH
// keys are present at all (a repo-scoped candidate list is genuinely
// shorter than an organization-wide baseline list), and that is a
// STRUCTURAL finding (ShapeLength or ShapePresence), which leafDifference
// categorically excludes from every OTHER shape in this package. See the
// one narrow exception this shape earns in classifyBaselineDefects
// (compare.go).
//
// WHY NOT LimitDisplacementShape: that shape admits a presence swap in
// BOTH directions (a row entering one list at the cost of one leaving the
// other), because its own mechanism is a rank DISPLACEMENT across a fixed
// LIMIT boundary -- the two lists stay the same length. This shape's own
// mechanism has no such boundary and no such swap: the candidate's list
// is simply narrower, one direction only, and it stays narrower for as
// long as the requested scope stays narrower. Admitting a candidate-only
// element here would mean the candidate answered for something the
// baseline never even considered, which this shape's own mechanism (a
// scope narrowing what the candidate reads) can never produce.
//
// WHAT THIS SHAPE DOES NOT, AND CANNOT, CERTIFY: that every admitted
// candidate element actually belongs to the narrower scope the request
// asked for (a team's owned repositories, for the routes this shape is
// declared against). None of those routes' wire responses carry a
// repository identifier on any element the candidate returns, so that
// premise is asserted by the citing BaselineDefect's own Reason -- as a
// property of the mechanism the candidate's own route implements and the
// seeded route-level integration tests already exercise -- never
// independently measured here. This shape only ever certifies the
// SUBSET-WITH-BOUNDED-LEAVES relationship between the two lists actually
// observed on the wire.
//
// The shape this type verifies, over the two DECODED lists at ListPath:
//
//  1. Both lists decode as JSON arrays of objects, every element on
//     either side carries every one of KeyFields, and no two elements on
//     the SAME side share a key -- the same vacuity discipline
//     orderInsensitiveKey's own callers use elsewhere in this package
//     (compareListByKey, limitdisplacement.go's decodeLimitDisplacementRows).
//     Any failure here refuses the WHOLE plan: nothing is admitted from a
//     shape that cannot even read its own two lists.
//  2. The candidate list is non-empty. An EMPTY candidate list proves
//     nothing about the claimed scope -- it is indistinguishable from the
//     scope resolving to nothing at all, which is exactly the failure
//     mode a stale or broken team-repository resolution produces (an
//     unfiltered baseline against an accidentally-empty candidate would
//     otherwise read as "a very small but valid team"). Refused outright,
//     never admitted as a length-zero subset.
//  3. Every candidate element's key exists in the baseline's own list
//     (no candidate-only key anywhere): a single candidate-only key
//     disproves the subset claim for the WHOLE comparison, not just that
//     one element, and refuses every admission this plan would otherwise
//     grant -- the same "one bad key invalidates the whole plan" caution
//     LimitDisplacementShape's own rule 1 already applies to an unequal
//     baseline/candidate-only count.
//  4. For every element the candidate and baseline both carry: each name
//     in EqualLeaves must be equal (recursively, through nested objects
//     and arrays, numeric leaves compared as numbers so an int/float
//     formatting difference is not a false mismatch); each name in
//     BoundedLeaves must satisfy baseline >= candidate (numeric only) for
//     an element whose key is named in BoundedLeafKeys, and must be
//     EQUAL for every other element -- a leaf is only ever a sum-type
//     aggregate for the specific keys BoundedLeafKeys names, established
//     from the route's own producing query, never assumed from the leaf
//     name alone. A ratio or average leaf is never named in
//     BoundedLeaves: a scope narrower in POPULATION carries no
//     guaranteed direction for a ratio over that population (the same
//     reasoning KeyedDirectionShape's own delta_pct exclusion states,
//     restcorpus.go), so it stays named in EqualLeaves (or unnamed
//     entirely, in which case it is not checked by this shape at all --
//     see below) and any real narrowing leaves it outside every
//     citation, correctly. BoundedLeavesAllKeys names element field(s)
//     that must satisfy baseline >= candidate for EVERY matched element,
//     with no BoundedLeafKeys restriction: the opt-in for a leaf that is
//     a sum of non-negative per-repo contributions, where every matched
//     key's own sum -- not merely the ones a caller happens to name -- is
//     bounded downward by construction (established from the route's own
//     producing query, same as BoundedLeaves).
//
// A leaf named in neither EqualLeaves, BoundedLeaves nor
// BoundedLeavesAllKeys is not checked by
// this shape at all: naming every wire leaf is not required, only every
// leaf whose disagreement should NOT silently block the subset admission
// needs a rule. A shape declared with both lists empty checks membership
// only (rule 3) -- the correct declaration for a list whose own
// per-element leaf differences are already covered by a SIBLING,
// unshaped BaselineDefect citing the same leaf paths (this package's own
// leaf-only blanket rule already admits those; this shape then only
// needs to add the membership/length admission the blanket rule cannot
// reach).
//
// Admission of a ShapeLength finding (the whole list is a different size,
// the comparator's own dispatch when ListPath carries no
// OrderInsensitiveList declaration -- compareList, compare.go) requires
// EVERY rule above to hold for EVERY element. Admission of a ShapePresence
// finding (one element's own key present on only one side, the
// comparator's dispatch when ListPath IS declared order-insensitive --
// compareListByKey) requires the SAME rules to hold across the whole list
// AND is granted only for a key present in baseline and absent in
// candidate -- never the reverse, per this type's own doc comment above.
//
// A BoundedLeaves leaf that stays WITHIN bound still differs numerically
// between the two sides whenever the scope genuinely narrowed it, so it
// carries its OWN ShapeValue finding (compareJSON emits one for any
// numeric difference, independent of this shape's own length/presence
// admission) -- admitted when its own bound held for that element, and
// ONLY in the KEYED (order-insensitive) manifestation, where the
// finding's own "[key=%q] ..." Detail lets this shape attribute it to
// one element with certainty (teamRepoSubsetPlan.boundedAdmits' own doc
// comment). A raw positional list carries no such attribution and this
// shape does not admit a bounded leaf's own value finding there -- the
// safe default. No route declares BoundedLeaves today (teamreposubset_test.go
// exercises this admission directly against synthetic keyed lists); the
// rule is stated in full for the first route that does.
type TeamRepoSubsetShape struct {
	// ListPath is the dotted, index-free path to the list itself (the
	// same form BaselineDefect.Paths uses), e.g. "data" for a route whose
	// whole response body is the list, or "data.items".
	ListPath string
	// KeyFields names the field(s) that together uniquely identify one
	// element for pairing, e.g. []string{"work_unit_id"}.
	KeyFields []string
	// EqualLeaves names element field(s) that must be recursively equal
	// between a matched baseline/candidate pair.
	EqualLeaves []string
	// BoundedLeaves names element field(s) that must satisfy baseline >=
	// candidate (numeric) for a matched pair whose key is named in
	// BoundedLeafKeys -- and must be equal, exactly like an EqualLeaves
	// entry, for every other matched pair.
	BoundedLeaves []string
	// BoundedLeafKeys names the element keys (in orderInsensitiveKey's
	// own joined form for this shape's own KeyFields) for which
	// BoundedLeaves' bounded rule applies, established from the route's
	// own producing query for each key named here. A key not named here
	// gets the equal rule for those same leaf names instead.
	BoundedLeafKeys []string
	// BoundedLeavesAllKeys names element field(s) that must satisfy
	// baseline >= candidate (numeric) for EVERY matched pair, with no
	// BoundedLeafKeys restriction at all -- the opt-in for a leaf that is
	// a SUM of non-negative per-repo contributions over the route's own
	// producing query, where a narrower candidate population can only
	// ever pull that sum down (or leave it unchanged) at every key that
	// still has one, never merely some of them. Confirm the sum shape
	// from the route's own SQL before naming a leaf here: a leaf that is
	// a ratio, average, or any other non-additive aggregate must never be
	// named here (the same restriction BoundedLeaves/BoundedLeafKeys
	// already carries, teamreposubset.go's own package doc comment rule
	// 4). A leaf named in BOTH BoundedLeaves and BoundedLeavesAllKeys is
	// unsupported and its own behaviour is undefined -- name a leaf in
	// exactly one of the two.
	BoundedLeavesAllKeys []string
}

// teamRepoSubsetPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// lists at ListPath.
type teamRepoSubsetPlan struct {
	shape *TeamRepoSubsetShape
	// valid reports whether rules 1-2 held. false admits NOTHING -- the
	// safe default, same as every other plan in this package.
	valid bool
	// subset reports whether rules 3-4 held across the WHOLE list -- the
	// claim a ShapeLength finding needs, and a precondition for admitting
	// any ShapePresence finding too (see this type's own doc comment: one
	// bad element refuses the whole plan, not just that element).
	subset bool
	// baselineOnlyKeys are the keys the baseline's list carries and the
	// candidate's does not -- each is a legitimate subset absence when
	// subset is true.
	baselineOnlyKeys map[string]bool
	// boundedAdmits records, per matched key named in BoundedLeafKeys and
	// per BoundedLeaves leaf name, whether that ONE leaf's own bound held
	// for that element. A BoundedLeaves leaf that is WITHIN bound still
	// differs numerically from its baseline counterpart whenever the
	// scope genuinely narrowed it, so it still carries its OWN ShapeValue
	// finding (compareJSON emits one for any numeric difference,
	// independent of this shape's own length/presence admission) -- this
	// map is what admits() reads to cover THAT finding too, rather than
	// leaving a satisfied bound's own leaf permanently outside every
	// citation on a route with no sibling blanket citation over the same
	// leaf. Absent from this map (a key not in BoundedLeafKeys, or a leaf
	// not in BoundedLeaves) means "not this shape's concern" -- admits()
	// returns false for it, the safe default.
	boundedAdmits map[string]map[string]bool
}

// buildTeamRepoSubsetPlan evaluates every rule TeamRepoSubsetShape
// documents against one comparison's decoded baseline/candidate `data`
// values.
func buildTeamRepoSubsetPlan(shape *TeamRepoSubsetShape, baselineData, candidateData any) *teamRepoSubsetPlan {
	plan := &teamRepoSubsetPlan{shape: shape, baselineOnlyKeys: map[string]bool{}, boundedAdmits: map[string]map[string]bool{}}

	baseList, ok1 := teamRepoSubsetList(baselineData, shape.ListPath)
	candList, ok2 := teamRepoSubsetList(candidateData, shape.ListPath)
	if !ok1 || !ok2 {
		return plan
	}
	// Rule 2: an empty candidate subset certifies nothing.
	if len(candList) == 0 {
		return plan
	}
	baseByKey, _, ok3 := indexBySubsetKey(baseList, shape.KeyFields)
	candByKey, candOrder, ok4 := indexBySubsetKey(candList, shape.KeyFields)
	if !ok3 || !ok4 {
		return plan
	}
	plan.valid = true

	boundedAt := make(map[string]bool, len(shape.BoundedLeafKeys))
	for _, key := range shape.BoundedLeafKeys {
		boundedAt[key] = true
	}

	subset := true
	for _, key := range candOrder {
		baseElement, present := baseByKey[key]
		if !present {
			// Rule 3.
			subset = false
			continue
		}
		baseObject, ok1 := baseElement.(map[string]any)
		candObject, ok2 := candByKey[key].(map[string]any)
		if !ok1 || !ok2 {
			subset = false
			continue
		}
		for _, leaf := range shape.EqualLeaves {
			if !subsetValueEqual(baseObject[leaf], candObject[leaf]) {
				subset = false
			}
		}
		for _, leaf := range shape.BoundedLeaves {
			baseValue, candValue := baseObject[leaf], candObject[leaf]
			if boundedAt[key] {
				held := subsetLeafBounded(baseValue, candValue)
				if !held {
					subset = false
				}
				if plan.boundedAdmits[key] == nil {
					plan.boundedAdmits[key] = map[string]bool{}
				}
				plan.boundedAdmits[key][leaf] = held
			} else if !subsetValueEqual(baseValue, candValue) {
				subset = false
			}
		}
		// BoundedLeavesAllKeys: the same bounded rule as BoundedLeaves,
		// unconditionally for EVERY matched key -- no BoundedLeafKeys
		// gate to consult, by construction (this field's own doc comment
		// on TeamRepoSubsetShape).
		for _, leaf := range shape.BoundedLeavesAllKeys {
			baseValue, candValue := baseObject[leaf], candObject[leaf]
			held := subsetLeafBounded(baseValue, candValue)
			if !held {
				subset = false
			}
			if plan.boundedAdmits[key] == nil {
				plan.boundedAdmits[key] = map[string]bool{}
			}
			plan.boundedAdmits[key][leaf] = held
		}
	}
	plan.subset = subset

	for key := range baseByKey {
		if _, present := candByKey[key]; !present {
			plan.baselineOnlyKeys[key] = true
		}
	}
	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *teamRepoSubsetPlan) admits(finding Finding) bool {
	if p == nil || !p.valid || !p.subset {
		return false
	}
	switch finding.Shape {
	case ShapeLength:
		return tieredPath(finding.Path) == p.shape.ListPath
	case ShapePresence:
		key, ok := parsePresenceDetailKey(finding.Detail)
		if !ok {
			return false
		}
		if !strings.Contains(finding.Detail, "absent in candidate") {
			// "absent in baseline": a candidate-only element. Provably
			// unreachable here while p.subset is true -- rule 3 already
			// sets subset to false the moment any candidate-only key
			// exists anywhere in this comparison (buildTeamRepoSubsetPlan
			// above), and THAT is exactly the condition that produces an
			// "absent in baseline" finding at all, so this branch's own
			// guard is redundant with the top-of-function subset check by
			// construction, not by coincidence -- kept as an explicit,
			// named refusal rather than relied upon to be unreachable.
			// TestTeamRepoSubsetShape_PresenceCandidateOnlyKeyNeverAdmitted
			// pins rule 3 itself; this branch has nothing left to add to
			// that pin.
			return false
		}
		return p.baselineOnlyKeys[key]
	case ShapeValue:
		// A BoundedLeaves leaf that is WITHIN bound still differs
		// numerically from its baseline counterpart, so it carries its
		// own ShapeValue finding independent of this shape's own
		// length/presence admission (see boundedAdmits' own doc comment).
		// Only reachable in the KEYED (order-insensitive) manifestation:
		// compareListByKey's own "[key=%q] ..." prefix is the only way
		// this plan can attribute a value finding to one element with
		// certainty; a raw positional list carries no such attribution
		// and is not admitted here.
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		leaf, ok := strings.CutPrefix(tieredPath(finding.Path), p.shape.ListPath+".")
		if !ok {
			return false
		}
		return p.boundedAdmits[key][leaf]
	}
	return false
}

// teamRepoSubsetList reads listPath (dotted, index-free, under `data`)
// into a JSON array. ok is false when the path does not resolve to one.
func teamRepoSubsetList(root any, listPath string) ([]any, bool) {
	value, ok := navigateSegments(root, citedSegments(listPath))
	if !ok {
		return nil, false
	}
	list, ok := value.([]any)
	return list, ok
}

// indexBySubsetKey reads a decoded list into a key-indexed map, in
// encounter order. ok is false when an element is not an object, is
// missing one of keyFields, or shares its key with an earlier element on
// the SAME list -- the caller refuses the whole plan rather than guessing
// a partial pairing, the same discipline orderInsensitiveKey's own
// callers use elsewhere in this package.
func indexBySubsetKey(list []any, keyFields []string) (byKey map[string]any, order []string, ok bool) {
	byKey = make(map[string]any, len(list))
	order = make([]string, 0, len(list))
	for _, element := range list {
		key, keyOK := orderInsensitiveKey(element, keyFields)
		if !keyOK {
			return nil, nil, false
		}
		if _, duplicate := byKey[key]; duplicate {
			return nil, nil, false
		}
		byKey[key] = element
		order = append(order, key)
	}
	return byKey, order, true
}

// subsetValueEqual compares two decoded JSON values recursively: numbers
// as numbers (so an int/float formatting difference is never a false
// mismatch, mirroring compareJSON's own numeric handling), everything
// else through sameScalar, and objects/arrays element-for-element.
func subsetValueEqual(a, b any) bool {
	switch typedA := a.(type) {
	case map[string]any:
		typedB, ok := b.(map[string]any)
		if !ok || len(typedA) != len(typedB) {
			return false
		}
		for key, valueA := range typedA {
			valueB, present := typedB[key]
			if !present || !subsetValueEqual(valueA, valueB) {
				return false
			}
		}
		return true
	case []any:
		typedB, ok := b.([]any)
		if !ok || len(typedA) != len(typedB) {
			return false
		}
		for i := range typedA {
			if !subsetValueEqual(typedA[i], typedB[i]) {
				return false
			}
		}
		return true
	default:
		if floatA, ok := asFloat(a); ok {
			floatB, ok := asFloat(b)
			return ok && floatA == floatB
		}
		return sameScalar(a, b)
	}
}

// subsetBoundTolerance absorbs summation-order float noise the same way
// repoFanoutRatioTolerance does for a different shape's own bound checks.
const subsetBoundTolerance = 1e-9

// subsetLeafBounded reports whether baseline >= candidate, both read as
// numbers. Either side failing to parse as a number is never admitted --
// a bounded leaf this shape cannot read as a number is not a verified
// bound.
func subsetLeafBounded(baseline, candidate any) bool {
	baseValue, ok1 := asFloat(baseline)
	candValue, ok2 := asFloat(candidate)
	if !ok1 || !ok2 {
		return false
	}
	return candValue <= baseValue+subsetBoundTolerance
}
