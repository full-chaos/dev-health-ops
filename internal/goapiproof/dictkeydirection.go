package goapiproof

import "strings"

// DictKeyDirectionShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// invariant a strictly SUBTRACTIVE exclusion mechanism can honestly prove
// per key of a JSON OBJECT: the candidate (Go) plane's own row population
// is a STRICT SUBSET of the reference (baseline/Python) plane's -- Go
// additionally excludes rows the baseline still reads -- so any COUNT or
// SUM aggregate keyed by a field of those rows can only read baseline
// GREATER THAN OR EQUAL TO candidate at that key, never below.
//
// This is DictKeyDirectionShape's own counterpart to KeyedDirectionShape
// (keyeddirection.go): the CLAIM is the same shape of argument (one plane
// reads more rows than the other, so its aggregate can only be pulled in
// ONE direction), but the DATA shape is not. KeyedDirectionShape pairs
// elements of a JSON ARRAY by declared KeyFields, via the SAME
// OrderInsensitiveList machinery compareListByKey uses, and reads the
// pairing key back out of a Finding's own "[key=...] " Detail prefix.
// theme_distribution/subcategory_distribution/evidence_quality_
// distribution/evidence_quality_stats.band_counts (GET/POST
// /api/v1/investment) and unassigned_reasons (investment/flow,
// investment/flow/repo-team) are all Go maps -- JSON OBJECTS, not arrays
// -- keyed directly by their own literal JSON key (a theme name, a
// quality band, "missing_team"/"missing_repo"). compareDict never writes
// a "[key=...] " Detail prefix (only compareListByKey does), and there is
// no KeyFields pairing to perform in the first place: the object's own
// key IS the identity, unconditionally, on both planes, and it already
// appears verbatim as the LAST segment of the finding's own tiered path
// (tieredPath keeps a dict key literal, unlike a list index, which it
// collapses to "[N]" -- see compare.go's own tieredPath doc comment).
// This shape reads that path segment directly instead.
//
// UNLIKE SupersessionSkewShape's own "typically retired because the
// earlier grouping was wrong, skews toward unassigned" reasoning (an
// empirical claim about WHICH way a ratio moves), the direction claim
// here is a PLAIN STRUCTURAL FACT with no "typically": candidate's row
// set is baseline's row set minus a subset (the work_unit_supersessions
// exclusion's own unconditional `AND work_unit_id NOT IN (...)` clause,
// analytics.LatestWorkUnitInvestmentsSource's own doc comment), so
// counting or summing rows that share a key value over a STRICT SUBSET
// of a population can only read less than or equal to the same
// count/sum over the FULL population -- true for every key,
// unconditionally, not merely "typically". Equality is expected and
// common (a key whose rows never included an excluded one), never a
// violation of the claim -- a difference only exists to admit when one
// actually differs.
//
// A CANDIDATE-ONLY KEY IS NEVER ADMITTED, and must not be: a key present
// in the candidate map but absent from the baseline's would mean Go's
// STRICT SUBSET somehow contains a key the SUPERSET lacks entirely --
// the subset claim's own contradiction, not an instance of it. For every
// declaration that leaves AdmitBaselineOnlyKeys false (every declaration
// that existed before that field did, and any new one that does not name
// a genuinely narrower candidate population), this can never even reach
// admission: compareDict reports a key present on only one side as
// ShapePresence (compare.go), and compare.go's own classifyBaselineDefects
// only ever asks such a declaration's admits() about a LEAF finding
// (leafDifference(shape) gates every shape in this package identically
// by default, see compare.go's own doc comment) -- this shape does not
// additionally special-case it in code for that case, the same
// "redundant gate" reasoning as before.
//
// A DECLARATION WITH AdmitBaselineOnlyKeys SET is the one exception:
// compare.go's own gate (classifyBaselineDefects) widens by that field,
// named explicitly, to let a ShapePresence finding under this
// declaration's own Paths reach admits() too -- for the case a strict
// subset population is missing an ENTIRE key the wider population
// carries, not merely shrinking one it still has (the JSON-OBJECT
// counterpart of TeamRepoSubsetShape's own rule 3 for a JSON ARRAY).
// admits() still refuses a candidate-only key unconditionally even then
// (see the type's own AdmitBaselineOnlyKeys doc comment) --
// TestDictKeyDirectionShape_CandidateOnlyKeyNeverAdmitted asserts that
// refusal directly, as its own rule, rather than trusting the upstream
// gate alone to keep holding for a declaration that opted in.
//
// What this shape CANNOT catch: a real Go regression that UNDER-counts
// or UNDER-sums at the SAME key (moving further below baseline, in the
// SAME direction the mechanism itself produces) is indistinguishable
// from a genuine instance from the two response bodies alone -- only the
// direction is checked, never disproved. The same known, accepted limit
// KeyedDirectionShape's own doc comment states for its per-key claim.
type DictKeyDirectionShape struct {
	// DictPath is the dotted, index-free path to the JSON OBJECT itself,
	// e.g. "data.theme_distribution" or "data.unassigned_reasons".
	DictPath string
	// ValuePath is the leaf path findings carry for a value difference at
	// one of the object's own keys -- must equal one of the defect's own
	// Paths entries, e.g. "data.theme_distribution" (the SAME as
	// DictPath: a one-level-deep object's own key IS the last path
	// segment, so there is no separate ".value" suffix the way a list
	// element's own named field needs one).
	ValuePath string
	// AdmitBaselineOnlyKeys opts this ONE declaration into a second,
	// independent claim: a key the baseline's own object carries and the
	// candidate's does not (compareDict's own "present in baseline,
	// absent in candidate" ShapePresence finding) is admitted too -- the
	// same "a strict subset can be missing an ENTIRE key, not merely
	// shrink one it still has" case TeamRepoSubsetShape's own rule 3
	// already admits for a JSON ARRAY, here for a JSON OBJECT. Default
	// false: every EXISTING DictKeyDirectionShape declaration keeps its
	// current power exactly -- this is opt-in per declaration, not a
	// change to what any declaration already in production admits. A
	// candidate-only key ("present in candidate, absent in baseline")
	// is NEVER admitted regardless of this field -- the subset claim's
	// own contradiction, exactly as a candidate-only key is refused for
	// every matched-key value admission above.
	AdmitBaselineOnlyKeys bool
}

// dictKeyDirectionPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// objects at DictPath.
type dictKeyDirectionPlan struct {
	shape *DictKeyDirectionShape
	// valid is false when the object could not even be read as a
	// map[string]any on both sides. A plan that is not valid admits
	// NOTHING -- the safe default, never a guess.
	valid bool
	// admittedKeys is the set of object keys this plan admits: present on
	// both sides, both numeric, and baseline > candidate.
	admittedKeys map[string]bool
	// baselineOnlyKeys is the set of keys the baseline's own object
	// carries and the candidate's does not -- populated only when
	// shape.AdmitBaselineOnlyKeys is true (nil, and therefore admitting
	// nothing, otherwise: the safe default for every existing
	// declaration that never opted in).
	baselineOnlyKeys map[string]bool
}

// buildDictKeyDirectionPlan evaluates DictKeyDirectionShape's rule
// against one comparison's decoded baseline/candidate `data` values.
func buildDictKeyDirectionPlan(shape *DictKeyDirectionShape, baselineData, candidateData any) *dictKeyDirectionPlan {
	plan := &dictKeyDirectionPlan{shape: shape, admittedKeys: map[string]bool{}}

	baseObject, okB := dictAtDottedPath(baselineData, shape.DictPath)
	candObject, okC := dictAtDottedPath(candidateData, shape.DictPath)
	if !okB || !okC {
		return plan
	}
	plan.valid = true

	for key, candRaw := range candObject {
		baseRaw, present := baseObject[key]
		if !present {
			// A candidate-only key: never admitted -- see the type doc
			// comment. Nothing to do; admittedKeys simply never gains
			// this key.
			continue
		}
		candValue, okCV := asFloat(candRaw)
		baseValue, okBV := asFloat(baseRaw)
		if !okCV || !okBV {
			continue
		}
		if baseValue > candValue {
			plan.admittedKeys[key] = true
		}
	}

	if shape.AdmitBaselineOnlyKeys {
		plan.baselineOnlyKeys = map[string]bool{}
		for key := range baseObject {
			if _, present := candObject[key]; !present {
				plan.baselineOnlyKeys[key] = true
			}
		}
	}

	return plan
}

// admits reports whether one Finding is covered by this plan. The
// admitted key is read directly off the finding's own tiered path (its
// last segment), never off Detail -- unlike KeyedDirectionShape, this
// shape's findings never carry a "[key=...] " prefix, because compareDict
// (not compareListByKey) produced them.
//
// A ShapePresence finding only ever reaches this function at all when
// compare.go's own gate let it through, which it does only for a
// declaration with AdmitBaselineOnlyKeys set (classifyBaselineDefects'
// own shape-field gate, compare.go) -- so the shape.AdmitBaselineOnlyKeys
// check below is a second, redundant refusal for a non-opted-in
// declaration, kept explicit rather than relied upon to be unreachable,
// the same discipline TeamRepoSubsetShape.admits() already applies to
// its own ShapePresence branch (teamreposubset.go).
func (p *dictKeyDirectionPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	key, ok := dictKeyFromPath(finding.Path, p.shape.ValuePath)
	if !ok {
		return false
	}
	if finding.Shape == ShapePresence {
		if !p.shape.AdmitBaselineOnlyKeys {
			return false
		}
		if !strings.Contains(finding.Detail, "absent in candidate") {
			// "present in candidate, absent in baseline": a
			// candidate-only key. Never admitted, in either direction --
			// the subset claim's own contradiction (this type's own doc
			// comment on AdmitBaselineOnlyKeys).
			// TestDictKeyDirectionShape_CandidateOnlyKeyNeverAdmitted
			// pins this even with AdmitBaselineOnlyKeys set.
			return false
		}
		return p.baselineOnlyKeys[key]
	}
	return p.admittedKeys[key]
}

// dictAtDottedPath reads one JSON object leaf at a dotted, index-free
// path under `data` (the same form BaselineDefect.Paths uses).
func dictAtDottedPath(root any, dottedPath string) (map[string]any, bool) {
	value, ok := navigateSegments(root, citedSegments(dottedPath))
	if !ok {
		return nil, false
	}
	object, ok := value.(map[string]any)
	return object, ok
}

// dictKeyFromPath recovers the object key a compareDict-produced
// Finding's own concrete path names, given the shape's declared
// DictPath/ValuePath (as tieredPath would reduce it -- e.g.
// "data.theme_distribution"): the concrete path is that SAME prefix plus
// ".<key>" (compareDict appends the literal JSON key as-is, never an
// ordinal the way a list index does), so the key is whatever follows the
// declared prefix plus its separating dot. ok is false when the concrete
// path does not have that prefix at all.
func dictKeyFromPath(concretePath, dottedValuePath string) (string, bool) {
	tiered := tieredPath(concretePath)
	prefix := dottedValuePath + "."
	rest, ok := strings.CutPrefix(tiered, prefix)
	if !ok || rest == "" {
		return "", false
	}
	return rest, true
}
