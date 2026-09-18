package goapiproof

import (
	"fmt"
	"sort"
)

// CHAOS-5661: compareJSON assumes the two legs describe the SAME entities
// and disagree about their VALUES. That assumption failed silently for
// flowMatrix's TEAM/REPO variants (JOB 7 step 7 run 2): Go's nodes/edges
// are keyed by name, Python's by UUID, with ZERO ids in common (TEAM: Go
// 3 nodes/6 edges vs Python 2/2; REPO: Go 11/110 vs Python 9/54) -- two
// structurally unrelated graphs, not one graph two planes disagree
// about. A comparator that pairs elements by POSITION, or that only
// visits a path present on both legs, cannot tell that apart from an
// ordinary value disagreement: cite the one field that differs (or
// exclude the identifying one as volatile) and a structurally divergent
// operation reads as MATCH, or as a fully-cited MISMATCH -- functionally
// the same false proof, since a cited mismatch with nothing outside is
// enablement-eligible.
//
// The functions below run BEFORE any value is compared (see Compare's
// own call site) and refuse outright when the two legs do not share
// enough SHAPE for a value comparison to mean anything. They never soften
// or replace compareJSON's own findings -- when they find nothing, the
// ordinary comparison runs exactly as before.

// bodySizeRatioThreshold and minBodySizeForRatioCheck give "large size
// disagreement" a concrete definition, so the rule is a number stated
// once here rather than a judgment call repeated at every call site.
const (
	// bodySizeRatioThreshold: if one leg's response body is more than
	// this many times the size of the other's (both past
	// minBodySizeForRatioCheck), the two bodies are not the same shape of
	// answer, whatever a field-by-field comparison would go on to say.
	bodySizeRatioThreshold = 3.0
	// minBodySizeForRatioCheck excuses small bodies from the ratio check:
	// a 40-byte vs 120-byte body is already a 3x ratio and is noise near
	// the envelope's own fixed overhead (`{"data":{}}` alone is 11
	// bytes), not a structural signal.
	minBodySizeForRatioCheck = 256
)

// structuralAgreementFailure walks baseline and candidate together, depth
// first, purely to REACH every list -- a JSON object's own key set is
// deliberately never judged here (see the note below) -- and returns the
// first LIST of "id"-carrying objects that is non-empty on both legs
// while sharing NOT ONE id between them. Returns ("", "") when no such
// list is found.
//
// Object (map) nodes are walked ONLY to descend into their children,
// never refused on their own key overlap. An early version of this check
// also refused when two objects at the same path shared no KEY at all --
// which is exactly right for a map dynamically keyed by entity identity,
// but a GraphQL object has a small, SCHEMA-FIXED field set, and "the
// candidate returned almost none of the fields the baseline did" is an
// ordinary, already-correctly-handled PRESENCE bug (hotspots'
// "rows key absent" cell: candidate returns {"total":0} where baseline
// returns {"rows":[...]}, sharing no key -- a real, single, already-cited
// presence finding, not a sign the two legs describe unrelated data).
// TestTheWriterRecordsShapeDifferencesOutsideTheCitation caught this by
// refusing the run outright instead of reporting the ordinary uncovered
// presence finding it expects. Lists of "id"-carrying elements do not
// have this problem: a GraphQL list's elements share the SAME schema by
// construction, so id disjointness there is never an artefact of one
// side returning a different, smaller field set -- it is genuinely two
// different sets of entities (JOB 7's measured shape).
//
// Descent into a list's ELEMENTS pairs by the list's own DECLARED
// identity when one exists, never by raw position -- the same class of
// bug this file's own top comment already names for the list itself
// (JOB 7), one level down. Measured shape: GET /api/v1/work-units
// team_scoped, where Go's team-repository resolution genuinely omits one
// PR baseline includes (already declared and admitted by
// workUnitsTeamScopeSubsetDefect's own TeamRepoSubsetShape) -- both legs'
// own `data` lists carry the SAME LENGTH (both saturate the request's
// LIMIT), so the one missing baseline unit does not show as a length
// difference, only as a one-position SHIFT for everything past it.
// `data`'s own elements key on `work_unit_id`, never literal "id", so the
// PREVIOUS code fell through this function's positional loop below,
// paired baseline's PR at its shifted index against candidate's unrelated
// next unit at the same index, and then found their nested
// evidence.textual lists shared no "id" -- true, but for the wrong
// reason: not because the same unit's evidence diverged, but because they
// were never the same unit. structuralListIdentity resolves the field(s)
// a list's elements
// are keyed by for THIS alignment purpose alone; when it finds one, every
// element sharing a key on BOTH legs is paired by that key instead of
// position, and a key present on only one side is left to the ordinary
// compareJSON/BaselineDefect pipeline (a presence difference, never this
// check's business) rather than corrupting every later index. Sharing NOT
// ONE key under a resolved identity still refuses, exactly like
// idOverlapFailure's own literal-"id" rule above -- generalized to
// whichever identity a list resolves, not a new, separate rule. A list
// with no resolvable identity keeps today's exact positional walk.
func structuralAgreementFailure(baseline, candidate any, path string, opts Options) (reason, detail string) {
	baselineMap, baselineIsMap := baseline.(map[string]any)
	candidateMap, candidateIsMap := candidate.(map[string]any)
	if baselineIsMap && candidateIsMap {
		for _, key := range unionOfMapKeys(baselineMap, candidateMap) {
			if reason, detail := structuralAgreementFailure(baselineMap[key], candidateMap[key], path+"."+key, opts); reason != "" {
				return reason, detail
			}
		}
		return "", ""
	}

	baselineList, baselineIsList := baseline.([]any)
	candidateList, candidateIsList := candidate.([]any)
	if baselineIsList && candidateIsList {
		if reason, detail := idOverlapFailure(baselineList, candidateList, path); reason != "" {
			return reason, detail
		}
		if fields, ok := structuralListIdentity(baselineList, candidateList, path, opts); ok {
			baselineByKey, ok1 := pairListElementsByKey(baselineList, fields)
			candidateByKey, ok2 := pairListElementsByKey(candidateList, fields)
			if ok1 && ok2 {
				var matched []string
				for key := range baselineByKey {
					if _, present := candidateByKey[key]; present {
						matched = append(matched, key)
					}
				}
				// Zero overlap under a NON-"id" identity is the same
				// disjoint-entities signal idOverlapFailure already reports
				// above for literal "id" -- generalized to whichever
				// identity this list resolved. Unreachable when fields is
				// exactly ["id"]: idOverlapFailure already returned above
				// the moment that case shares nothing, so matched can never
				// be empty here for it; stated as a plain length check
				// rather than special-cased on the source, since the
				// mathematics already make it a no-op there.
				if len(matched) == 0 && len(baselineList) > 0 && len(candidateList) > 0 {
					return RefusalLegsDoNotOverlap, fmt.Sprintf(
						"%s: baseline has %d element(s) keyed by %v, candidate has %d element(s) keyed by %v -- not one key is shared, so this is not two legs disagreeing about the same entities",
						path, len(baselineList), fields, len(candidateList), fields)
				}
				sort.Strings(matched)
				for i, key := range matched {
					elementPath := fmt.Sprintf("%s[%d]", path, i)
					if reason, detail := structuralAgreementFailure(baselineByKey[key], candidateByKey[key], elementPath, opts); reason != "" {
						return reason, detail
					}
				}
				return "", ""
			}
			// fields resolved but a duplicate key on one side means those
			// fields do not uniquely identify elements in THIS data --
			// fall through to the positional walk below, the same safe
			// default an unresolved identity gets.
		}
		for i := 0; i < len(baselineList) && i < len(candidateList); i++ {
			if reason, detail := structuralAgreementFailure(baselineList[i], candidateList[i], fmt.Sprintf("%s[%d]", path, i), opts); reason != "" {
				return reason, detail
			}
		}
		return "", ""
	}

	// Different kinds, or two scalars: not this check's business --
	// compareJSON already reports a kind mismatch (ShapeStructure) or a
	// scalar difference on its own, uncoverable when structural.
	return "", ""
}

// structuralListIdentity resolves the field(s) elements of the list at
// path are keyed by, for ALIGNMENT purposes only -- never for the whole-
// list overlap refusal idOverlapFailure performs above, which stays
// literal-"id"-only and unchanged. Three sources, checked in this order:
//
//  1. literal "id", when every element on BOTH legs carries it -- this
//     file's own original, longest-standing convention, unchanged in
//     meaning here (idOverlapFailure already required this for the
//     overlap check; this reuses the same field for pairing too).
//  2. the synthetic RESTDedupKeyField a request's own DedupListPath/
//     DedupKeyFields already wrote onto every element before Compare ran
//     (InjectRESTDedupKeys, restdedup.go, cmd/go-api-rest-prove/main.go's
//     own call site precedes goapiproof.Compare), when every element on
//     BOTH legs carries it. Covers every route declaring
//     WorkGraphEdgeDedupShape today (drilldown/prs), which already relies
//     on this exact synthetic field for its OWN admission -- no separate
//     handling needed here.
//  3. a BaselineDefect's own TeamRepoSubsetShape whose ListPath equals
//     this path (dotted, index-free form, tieredPath), when every element
//     on BOTH legs carries every one of its KeyFields. Covers every route
//     declaring one across this package -- see workunits_corpus.go's own
//     doc comment on workUnitsTeamScopeOrderInsensitiveLists for the full
//     list this rule now keys.
//
// Returns nil, false when none apply: the caller keeps today's raw
// positional walk, exactly as before this function existed.
func structuralListIdentity(baseline, candidate []any, path string, opts Options) ([]string, bool) {
	if _, ok := elementIDs(baseline); ok {
		if _, ok := elementIDs(candidate); ok {
			return []string{"id"}, true
		}
	}
	if listElementKeys(baseline, []string{RESTDedupKeyField}) && listElementKeys(candidate, []string{RESTDedupKeyField}) {
		return []string{RESTDedupKeyField}, true
	}
	if shape := teamRepoSubsetShapeAt(opts, path); shape != nil && len(shape.KeyFields) > 0 {
		if listElementKeys(baseline, shape.KeyFields) && listElementKeys(candidate, shape.KeyFields) {
			return shape.KeyFields, true
		}
	}
	return nil, false
}

// teamRepoSubsetShapeAt returns the declared TeamRepoSubsetShape whose own
// ListPath equals path (dotted, index-free), or nil when none of opts'
// BaselineDefects declares one there.
func teamRepoSubsetShapeAt(opts Options, path string) *TeamRepoSubsetShape {
	normalized := tieredPath(path)
	for _, defect := range opts.BaselineDefects {
		if defect.TeamRepoSubsetShape != nil && defect.TeamRepoSubsetShape.ListPath == normalized {
			return defect.TeamRepoSubsetShape
		}
	}
	return nil
}

// listElementKeys reports whether EVERY element of elements is an object
// carrying every one of fields -- the same all-or-nothing discipline
// elementIDs already applies to literal "id". A caller never keys a list
// by a field only SOME elements carry.
func listElementKeys(elements []any, fields []string) (ok bool) {
	for _, element := range elements {
		if _, keyOK := orderInsensitiveKey(element, fields); !keyOK {
			return false
		}
	}
	return true
}

// pairListElementsByKey indexes elements by fields' joined values
// (orderInsensitiveKey). ok is false when two elements on the SAME list
// share a key -- fields do not uniquely identify elements in this data,
// and the caller falls back to the positional walk rather than guessing a
// partial pairing (the same discipline compareListByKey's own "index"
// closure applies, compare.go).
func pairListElementsByKey(elements []any, fields []string) (map[string]any, bool) {
	byKey := make(map[string]any, len(elements))
	for _, element := range elements {
		key, ok := orderInsensitiveKey(element, fields)
		if !ok {
			return nil, false
		}
		if _, duplicate := byKey[key]; duplicate {
			return nil, false
		}
		byKey[key] = element
	}
	return byKey, true
}

// idOverlapFailure checks ONE list: when every element on BOTH non-empty
// legs is an object carrying "id" (the same identity convention
// OrderInsensitiveList's KeyFields already uses for the operations that
// declare one, e.g. sankey nodes), and the two legs' id sets share not
// one value, the legs describe disjoint entities under this path -- the
// measured JOB 7 shape exactly (Go name-keyed, Python UUID-keyed).
//
// A list whose elements do not ALL carry "id" is not checked at all, so
// this never reports a false positive over a business object that
// coincidentally has no such field (hotspots' rows, flowMatrix's
// {"value": N} nodes in the declared-citation shape), and it never
// requires an operation to declare anything to be protected.
func idOverlapFailure(baseline, candidate []any, path string) (reason, detail string) {
	if len(baseline) == 0 || len(candidate) == 0 {
		return "", ""
	}
	baselineIDs, ok := elementIDs(baseline)
	if !ok {
		return "", ""
	}
	candidateIDs, ok := elementIDs(candidate)
	if !ok {
		return "", ""
	}
	for id := range baselineIDs {
		if candidateIDs[id] {
			return "", ""
		}
	}
	return RefusalLegsDoNotOverlap, fmt.Sprintf(
		"%s: baseline has %d element(s) with id(s) %v, candidate has %d element(s) with id(s) %v -- not one id is shared, so this is not two legs disagreeing about the same entities",
		path, len(baseline), sortedSet(baselineIDs), len(candidate), sortedSet(candidateIDs))
}

// elementIDs collects the "id" field's value from every element, rendered
// as its %v text form so an id of any JSON scalar type can be compared.
// ok is false when ANY element is not an object or carries no "id" --
// this check never guesses at an identity a list did not carry.
func elementIDs(elements []any) (map[string]bool, bool) {
	ids := make(map[string]bool, len(elements))
	for _, element := range elements {
		object, isObject := element.(map[string]any)
		if !isObject {
			return nil, false
		}
		value, ok := object["id"]
		if !ok {
			return nil, false
		}
		ids[fmt.Sprintf("%v", value)] = true
	}
	return ids, true
}

func unionOfMapKeys(a, b map[string]any) []string {
	seen := make(map[string]bool, len(a)+len(b))
	keys := make([]string, 0, len(a)+len(b))
	for key := range a {
		seen[key] = true
		keys = append(keys, key)
	}
	for key := range b {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedSet(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// bodySizeDisagreement reports a gross size mismatch between the two raw
// response bodies -- a signal computed from the BYTES, before either is
// decoded, and independent of what the decoded structures turn out to
// share.
func bodySizeDisagreement(baselineBytes, candidateBytes int) (reason, detail string) {
	if baselineBytes < minBodySizeForRatioCheck || candidateBytes < minBodySizeForRatioCheck {
		return "", ""
	}
	larger, smaller := float64(baselineBytes), float64(candidateBytes)
	if smaller > larger {
		larger, smaller = smaller, larger
	}
	ratio := larger / smaller
	if ratio <= bodySizeRatioThreshold {
		return "", ""
	}
	return RefusalLegsDoNotOverlap, fmt.Sprintf(
		"response body sizes disagree by %.1fx (baseline %d bytes, candidate %d bytes), over the %.1fx threshold: two responses this far apart in size are not the same shape of answer",
		ratio, baselineBytes, candidateBytes, bodySizeRatioThreshold)
}

// countNonNullLeaves counts every non-null scalar reachable under value,
// with no path restriction -- the whole-tree form of compare.go's
// nonNullLeaves, which is scoped to one cited path. Used by
// vacuousEmptyLegs to judge whether a LEG carries any signal at all.
func countNonNullLeaves(value any) int {
	switch typed := value.(type) {
	case map[string]any:
		total := 0
		for _, child := range typed {
			total += countNonNullLeaves(child)
		}
		return total
	case []any:
		total := 0
		for _, element := range typed {
			total += countNonNullLeaves(element)
		}
		return total
	case nil:
		return 0
	default:
		return 1
	}
}

// vacuousEmptyLegs reports whether baseline and candidate both carry ZERO
// non-null leaves under a Parity that declares a BaselineDefect or an
// OrderInsensitiveLists entry -- CHAOS-5661 rule 2, scoped to the case
// that rule exists for.
//
// It is deliberately NOT "any two empty legs refuse": admission.go is
// explicit that an empty list is a legitimate result ("this org has no
// feature flags" is a real answer), and TestEmptyListRootIsStillAdmitted
// / TestCompareEnvelopeKeyExceptionIsTopLevelOnly pin exactly that for an
// operation that declares no relaxation -- refusing those runs would make
// this instrument unable to prove any operation over empty data, which is
// the failure admission.go was written to avoid.
//
// It is also deliberately narrower than "any declared relaxation":
// throughputForecast/capacityForecast declare VolatileFields (and
// throughputForecast FloatTierB) while their ResponseRoot is explicitly
// RootNullable -- "a null result is a TOLERATED empty" (operations.go) --
// so both legs resolving to null, and therefore to zero leaves, is a
// LEGITIMATE match for those two, not evidence of nothing having been
// compared. Scoping to BaselineDefects/OrderInsensitiveLists targets
// exactly the case that needs a NEW reason: their own "matched nothing"
// guards (StaleBaselineDefects / UnusedOrderInsensitiveLists) already
// fire on a vacuous comparison, and read as "the citation is stale, go
// delete the ticket reference" -- the SAME misleading text CHAOS-5661
// rule 3 exists to stop for the overlap case, since the real story is
// "there was nothing here to test the citation against" (workGraphEdges's
// own history, operations.go, names this exact principle one level up).
// FloatTierB/VolatileFields have their OWN accurately-worded "matched
// nothing" refusals (UnusedTierB/UnusedExclusions) already, so leaving
// them out of this gate loses no coverage.
func vacuousEmptyLegs(baseline, candidate any, opts Options) bool {
	if len(opts.BaselineDefects) == 0 && len(opts.OrderInsensitiveLists) == 0 {
		return false
	}
	return countNonNullLeaves(baseline) == 0 && countNonNullLeaves(candidate) == 0
}
