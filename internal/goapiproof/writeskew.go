package goapiproof

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

// A write skew between the two legs of one case: every read runs a live
// argMax(computed_at) over the investment tables with no snapshot shared
// between the planes, so a materializer write that lands between the
// baseline read and the candidate read leaves the two legs reading two
// generations of the same row. The difference is real on the wire and
// says nothing about either plane's code.
//
// The prover reads the baseline FIRST and the candidate second. The
// bracketed re-read reads the baseline -- the reference plane -- a second
// time, after the candidate. A write between the legs shows on the
// reference plane itself: the second baseline read carries the
// candidate's generation, a value the first baseline read did not. That
// change on the reference plane is the witness that a write happened; a
// candidate plane that answers wrong (once, or on every read) cannot
// produce it. A leaf is admitted as write skew only when:
//
//   - it is a VALUE difference at a leaf both legs carry (Shape value) --
//     a membership, presence, length or type difference is never
//     admitted, and a case with any such difference outside every
//     declaration is not re-read at all;
//   - EVERY outside leaf carries the witness itself: the second baseline
//     read carries, at that leaf, EXACTLY the candidate's decoded value
//     (computed equality, no tolerance), which the first baseline read
//     did not. One outside leaf where the reference plane did not move
//     makes the whole case stand, whatever else holds -- a declaration
//     evaluated against the second read is never a substitute for the
//     witness at a leaf;
//   - and, in addition, the second baseline read agrees with the
//     candidate on the WHOLE case: compared under the same declarations,
//     nothing is outside.
//
// The direction trap stays closed: a candidate value that is wrong (an
// undercount or overcount, stable or not) leaves the reference plane
// unchanged -- second baseline == first baseline != candidate -- and the
// difference stays outside. A leaf whose second baseline read equals
// neither the first baseline read nor the candidate is data moving under
// the case; the case is refused (RESTRefusalLeafMovedBetweenReads).

// WriteSkewCitation is the citation an admitted write skew adds to the
// case's matched declarations, so a receipt says why nothing is outside.
const WriteSkewCitation = "write-skew:bracketed-reread"

// RESTRefusalLeafMovedBetweenReads refuses a case whose second baseline
// read carries, at an outside value leaf, a value equal to neither the
// first baseline read nor the candidate, or no value at all there.
const RESTRefusalLeafMovedBetweenReads = "rest_leaf_moved_between_baseline_reads"

// RESTRefusalRereadStructural refuses a case whose second baseline read,
// compared with the candidate, is itself refused structurally
// (vacuous_empty_legs / legs_do_not_overlap): the reference changed shape
// between its two reads, so nothing is comparable.
const RESTRefusalRereadStructural = "rest_reread_structural_refusal"

// RESTRefusalRereadLeafAmbiguous refuses a case with an outside leaf
// whose display path denotes more than one location in one of the three
// reads -- a literal key such as "a.b" or "q[0]" beside the nested path
// it spells -- so the reads of one leaf cannot be told apart and nothing
// is witnessed.
const RESTRefusalRereadLeafAmbiguous = "rest_reread_leaf_path_ambiguous"

// RESTRefusalRereadDeclarationsInvalid refuses a case whose second
// comparison (second baseline read against the candidate) carries any
// Result.Acceptance entry, hard or soft: a skew admission is a new verdict
// and must never rest on a comparison whose declarations the prover
// could not stand behind.
const RESTRefusalRereadDeclarationsInvalid = "rest_reread_declarations_invalid"

// WriteSkewVerdict is ClassifyWriteSkew's decision for one case.
type WriteSkewVerdict string

const (
	// WriteSkewNotApplicable: nothing to re-read -- no outside
	// difference, a structural refusal, or an outside difference that is
	// not a value leaf.
	WriteSkewNotApplicable WriteSkewVerdict = "not_applicable"
	// WriteSkewAdmitted: every outside leaf is write skew and the second
	// baseline read agrees with the candidate on the whole case.
	WriteSkewAdmitted WriteSkewVerdict = "skew_admitted"
	// WriteSkewStands: the re-read did not account for the difference;
	// the first comparison stands unchanged.
	WriteSkewStands WriteSkewVerdict = "stands"
	// WriteSkewRefused: a leaf moved to a third value, or could not be
	// found in the second read.
	WriteSkewRefused WriteSkewVerdict = "refused"
)

// WriteSkewLeaf records one outside leaf and the three values the bracket
// read there, in read order: first baseline (B1), candidate (C1), second
// baseline (B2). Key is the element's pairing key for a leaf inside a
// declared order-insensitive list, empty otherwise.
//
// SecondBaseline is always serialised, null included;
// SecondBaselinePresent says whether the second baseline read carried the
// leaf at all, so a JSON null and a missing leaf stay distinguishable.
type WriteSkewLeaf struct {
	Path                  string `json:"path"`
	Key                   string `json:"key,omitempty"`
	FirstBaseline         any    `json:"first_baseline"`
	Candidate             any    `json:"candidate"`
	SecondBaseline        any    `json:"second_baseline"`
	SecondBaselinePresent bool   `json:"second_baseline_present"`
}

// WriteSkewRereadNeeded reports whether first -- the comparison of the
// first baseline read with the candidate -- has a difference a bracketed
// re-read can account for: at least one finding outside every
// declaration, and every one of them a value leaf. Anything else stays
// what it is without a second call. (A structural refusal carries no
// findings at all, so it never qualifies.)
func WriteSkewRereadNeeded(first Result) bool {
	if len(first.outsideFindings) == 0 {
		return false
	}
	for _, i := range first.outsideFindings {
		if first.Findings[i].Shape != ShapeValue {
			return false
		}
	}
	return true
}

// WriteSkewDecision is ClassifyWriteSkew's decision for one case.
type WriteSkewDecision struct {
	Verdict WriteSkewVerdict
	Leaves  []WriteSkewLeaf
	Detail  string
	// Refusal names why a WriteSkewRefused case is refused:
	// RESTRefusalRereadStructural, RESTRefusalLeafMovedBetweenReads or
	// RESTRefusalRereadDeclarationsInvalid.
	Refusal string
	// Second is the comparison of the second baseline read with the
	// candidate, set whenever it was run. On WriteSkewAdmitted it is the
	// clean comparison the case now stands on.
	Second Result
	// ReferenceUnmoved is set on the R5 "stands" only: some outside leaf
	// was read the same by the first two baseline reads. It is the one
	// stands the delayed re-read (gapreread.go) may take up.
	ReferenceUnmoved bool
}

// ClassifyWriteSkew decides one case from its first comparison (first
// baseline read against the candidate), the three decoded legs in read
// order and the case's own declarations, in this precedence (the first
// that applies decides):
//
//   - R3: the second comparison is refused structurally -> refused
//     RESTRefusalRereadStructural;
//   - R4: some outside leaf's second read equals neither the first read
//     nor the candidate (missing and null included) -> refused
//     RESTRefusalLeafMovedBetweenReads;
//   - R5: some outside leaf's second read equals its first read (the
//     reference did not move there) -> stands;
//   - R7: every outside leaf witnessed, the second comparison has an
//     outside difference -> stands (no admission is possible, so the case
//     is exactly the normal path's);
//   - R6: every outside leaf witnessed, the second comparison agrees with
//     the candidate on the whole case, and it carries any
//     Result.Acceptance entry, hard or soft -> refused
//     RESTRefusalRereadDeclarationsInvalid (an admission must never rest
//     on declarations the prover could not stand behind);
//   - R8: otherwise -> admitted, on the second comparison.
func ClassifyWriteSkew(first Result, firstBaseline, candidate, secondBaseline Snapshot, opts Options) WriteSkewDecision {
	if !WriteSkewRereadNeeded(first) {
		return WriteSkewDecision{Verdict: WriteSkewNotApplicable}
	}
	second := Compare(secondBaseline, candidate, opts)
	if second.StructuralRefusal != "" {
		return WriteSkewDecision{Verdict: WriteSkewRefused, Refusal: RESTRefusalRereadStructural, Second: second,
			Detail: fmt.Sprintf("the second baseline read compared with the candidate is refused structurally (%s): %s", second.StructuralRefusal, second.StructuralDetail)}
	}
	leaves := make([]WriteSkewLeaf, 0, len(first.outsideFindings))
	witnessed := true
	for _, i := range first.outsideFindings {
		finding := first.Findings[i]
		keys := findingKeys(finding.Detail)
		leaf := WriteSkewLeaf{Path: finding.Path, Key: strings.Join(keys, " / ")}
		// A leaf the locator cannot find reads as nil. The first two
		// reads carry this leaf (the comparison reported a value
		// difference there, never a null one), so a nil second read
		// equals neither and refuses below.
		firstValue, _, firstAmbiguous := leafAt(firstBaseline.Data, finding.Path, keys, opts)
		candidateValue, _, candidateAmbiguous := leafAt(candidate.Data, finding.Path, keys, opts)
		secondValue, secondPresent, secondAmbiguous := leafAt(secondBaseline.Data, finding.Path, keys, opts)
		leaf.FirstBaseline, leaf.Candidate, leaf.SecondBaseline, leaf.SecondBaselinePresent = firstValue, candidateValue, secondValue, secondPresent
		leaves = append(leaves, leaf)
		if firstAmbiguous || candidateAmbiguous || secondAmbiguous {
			return WriteSkewDecision{Verdict: WriteSkewRefused, Refusal: RESTRefusalRereadLeafAmbiguous, Leaves: leaves, Second: second,
				Detail: fmt.Sprintf("%s: the path denotes more than one location in a read (a literal key that spells a nested path), so no one leaf can witness a write", finding.Path)}
		}
		switch {
		case leafValuesEqual(secondValue, firstValue):
			// The reference plane did not move at this leaf: no write is
			// witnessed here, so the case cannot be admitted.
			witnessed = false
		case leafValuesEqual(secondValue, candidateValue):
			// Moved to the candidate's generation: the witness.
		default:
			return WriteSkewDecision{Verdict: WriteSkewRefused, Refusal: RESTRefusalLeafMovedBetweenReads, Leaves: leaves, Second: second,
				Detail: fmt.Sprintf("%s: second baseline read %v equals neither the first baseline read %v nor the candidate %v", finding.Path, secondValue, firstValue, candidateValue)}
		}
	}
	if !witnessed {
		return WriteSkewDecision{Verdict: WriteSkewStands, Leaves: leaves, Second: second, ReferenceUnmoved: true, Detail: "the reference plane did not move at every outside leaf"}
	}
	if second.DifferencesOutsideBaselineDefect != 0 {
		return WriteSkewDecision{Verdict: WriteSkewStands, Leaves: leaves, Second: second,
			Detail: fmt.Sprintf("the second baseline read does not agree with the candidate on the whole case (outside=%d)", second.DifferencesOutsideBaselineDefect)}
	}
	if acceptance := second.Acceptance(); len(acceptance) > 0 {
		details := make([]string, 0, len(acceptance))
		for _, refusal := range acceptance {
			details = append(details, refusal.Code+": "+refusal.Detail)
		}
		return WriteSkewDecision{Verdict: WriteSkewRefused, Refusal: RESTRefusalRereadDeclarationsInvalid, Leaves: leaves, Second: second,
			Detail: "the second comparison's declarations do not stand: " + strings.Join(details, "; ")}
	}
	return WriteSkewDecision{Verdict: WriteSkewAdmitted, Leaves: leaves, Second: second}
}

// leafValuesEqual is exact equality of two decoded leaf values. JSON
// numbers compare by numeric value, exactly (as rationals), so the two
// planes' spellings of one number (1 and 1.0, 1e3 and 1000) are equal
// and no two distinct numbers ever are; every other value compares as
// decoded.
func leafValuesEqual(a, b any) bool {
	ra, aNumeric := exactNumber(a)
	rb, bNumeric := exactNumber(b)
	if aNumeric || bNumeric {
		return aNumeric && bNumeric && ra.Cmp(rb) == 0
	}
	return reflect.DeepEqual(a, b)
}

func exactNumber(v any) (*big.Rat, bool) {
	switch typed := v.(type) {
	case json.Number:
		return new(big.Rat).SetString(string(typed))
	case float64:
		r := new(big.Rat)
		if r.SetFloat64(typed) == nil {
			return nil, false
		}
		return r, true
	}
	return nil, false
}

// findingKeys returns the pairing keys compareListByKey prefixed onto a
// finding's Detail ("[key=%q] "), outermost first, one per declared
// order-insensitive list the finding's path passes through.
func findingKeys(detail string) []string {
	var keys []string
	for strings.HasPrefix(detail, "[key=") {
		rest := detail[len("[key="):]
		quoted, err := strconv.QuotedPrefix(rest)
		if err != nil {
			return keys
		}
		key, err := strconv.Unquote(quoted)
		if err != nil {
			return keys
		}
		keys = append(keys, key)
		rest = rest[len(quoted):]
		if !strings.HasPrefix(rest, "] ") {
			return keys
		}
		detail = rest[len("] "):]
	}
	return keys
}

// leafAt returns the value one display path (Finding.Path) denotes in
// data. A display path is not always one location: "$.data.a.b" is both
// the field "b" of the object at "a" and the literal key "a.b", and
// "$.data.q[0]" is both element 0 of the list at "q" and the literal key
// "q[0]". leafAt resolves every location the path can denote
// (leafLocations) and answers only when there is exactly one: ambiguous
// is true when there is more than one, and the case must be refused,
// because no single leaf's reads can witness anything.
func leafAt(data any, path string, keys []string, opts Options) (value any, present, ambiguous bool) {
	locations := leafLocations(data, path, keys, opts)
	switch len(locations) {
	case 0:
		return nil, false, false
	case 1:
		return locations[0], true, false
	}
	return nil, false, true
}

// leafLocations returns every value path can denote in data, following
// each way the path's text can be split into object fields and list
// ordinals (and, inside a declared order-insensitive list, the element
// whose pairing key is the next of keys).
func leafLocations(data any, path string, keys []string, opts Options) []any {
	rest, ok := strings.CutPrefix(path, "$.data")
	if !ok {
		return nil
	}
	var out []any
	resolveLeaf(data, rest, "$.data", keys, opts, &out)
	return out
}

func resolveLeaf(value any, rest, walked string, keys []string, opts Options, out *[]any) {
	if rest == "" {
		*out = append(*out, value)
		return
	}
	switch {
	case strings.HasPrefix(rest, "["):
		end := strings.Index(rest, "]")
		if end < 0 {
			return
		}
		ordinal, err := strconv.Atoi(rest[1:end])
		if err != nil {
			return
		}
		list, isList := value.([]any)
		if !isList {
			return
		}
		next := walked + rest[:end+1]
		if decl, declared := findOrderInsensitiveList(opts, tieredPath(walked)); declared {
			if len(keys) == 0 {
				return
			}
			element, found := elementByKey(list, decl.KeyFields, keys[0])
			if !found {
				return
			}
			resolveLeaf(element, rest[end+1:], next, keys[1:], opts, out)
			return
		}
		if ordinal < 0 || ordinal >= len(list) {
			return
		}
		resolveLeaf(list[ordinal], rest[end+1:], next, keys, opts, out)
	case strings.HasPrefix(rest, "."):
		object, isObject := value.(map[string]any)
		if !isObject || len(object) == 0 {
			return
		}
		for field := range object {
			after, matched := strings.CutPrefix(rest[1:], field)
			if !matched || (after != "" && after[0] != '.' && after[0] != '[') {
				continue
			}
			resolveLeaf(object[field], after, walked+"."+field, keys, opts, out)
		}
	}
}

// elementByKey finds the one element of list whose declared key fields
// render to key, the same rendering compareListByKey pairs by.
func elementByKey(list []any, keyFields []string, key string) (any, bool) {
	var match any
	count := 0
	for _, element := range list {
		if k, ok := orderInsensitiveKey(element, keyFields); ok && k == key {
			match = element
			count++
		}
	}
	return match, count == 1
}
