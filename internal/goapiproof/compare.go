// Package goapiproof implements CHAOS-5425's `prove` verb: it executes
// every registered Go-API operation against a DEPLOYED query-api build
// through the real edge path, compares the two planes' complete
// observable responses under CHAOS-4381's signed parity rules, and
// records an immutable `deployed_executed` receipt in `go_api_proof_run`
// per operation.
//
// Why a Go port of a Python comparator that already exists. The Python
// `compare_responses` (src/dev_health_ops/api/graphql/go_api_comparator.py)
// is the in-process dual-run comparator for the Python edge's own
// harnesses. `prove` is an operator command in the Go cutover's
// no-new-Python regime, so it cannot import it. This is a deliberate
// second implementation of one signed rule set, and the risk that buys
// is drift between the two -- so every rule below cites the Python
// function it mirrors by name, and the rule number from CHAOS-4381 it
// implements. A rule change lands in both or in neither.
//
// What this file deliberately does NOT port: `tie_ordering="relaxed"` /
// `relaxed_list_path`. Parity rule 5 makes positional list ordering the
// default and every relaxation a per-operation exception carrying a
// written reason and a ticket. None of the fifteen registered operations
// has one today, so porting the relaxation machinery would ship an unused
// loosening whose first user would face no review. Absent it, an
// order-only divergence is reported as an ordinary `mismatch` finding --
// loud, recorded, and ticketable -- never silently tolerated.
package goapiproof

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Terminal states from src/dev_health_ops/models/go_api_registry.py's
// TERMINAL_STATES. Only the subset this comparator can itself produce is
// named here; the runner adds the transport-level ones (auth_rejected,
// timeout, proof_failed, ...) from its own observations.
const (
	TerminalStateMatch       = "match"
	TerminalStateMismatch    = "mismatch"
	TerminalStateUnsupported = "unsupported"
)

// Finding kinds. Only "mismatch" blocks a MATCH verdict; parity rule 1
// makes an error MESSAGE difference at the same (path, code) identity a
// recorded observation rather than a blocking one, because the message is
// prose and the code is the contract.
const (
	FindingMismatch          = "mismatch"
	FindingErrorMessageDrift = "error_message_drift"
	FindingWatermarkDrift    = "watermark_drift"
	FindingWatermarkMissing  = "watermark_missing"
)

// floatTolerance is CHAOS-4381 parity rule 3's Tier-B tolerance: 1e-9,
// applied as max(abs, rel) exactly as the Python comparator's
// _compare_number does.
const floatTolerance = 1e-9

// Snapshot is one plane's complete observable GraphQL response body --
// the Python comparator's ResponseSnapshot.
//
// DataPresent distinguishes `"data": null` (key present, value null: a
// nullable root field that errored) from the `data` key being absent
// entirely (a request that never reached execution, e.g. a validation
// error). Both collapse to a nil Data, and parity rule 2's
// null-vs-omission rule applies at the envelope's top level too, so the
// two must stay distinguishable.
//
// Watermark is parity rule 4's high-water value this side observed; an
// empty string means this side reported none.
type Snapshot struct {
	Data        any
	DataPresent bool
	Errors      []map[string]any
	Watermark   string
	// TrailingBytes reports that the body carried more bytes after its
	// first JSON value. The decoder silently drops them, so a comparison
	// over the decoded value is not a comparison over what was served --
	// the admission gate refuses rather than compare.
	TrailingBytes bool
}

// Finding is one comparator observation.
type Finding struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Detail string `json:"detail"`
}

// Result is the comparator's verdict plus every observation behind it.
type Result struct {
	TerminalState string    `json:"terminal_state"`
	Findings      []Finding `json:"findings"`
	// UnusedExclusions names every VolatileFields entry that matched no
	// path in this comparison. An exclusion that excuses nothing is
	// either stale or misspelled, and a misspelled exclusion silently
	// excuses NOTHING while reading as though it excuses something --
	// the caller fails the run on a non-empty value here. Same discipline
	// as internal/testsupport/oraclecompare: "exclusions that need a
	// written reason and must actually match something".
	UnusedExclusions []string `json:"unused_exclusions,omitempty"`

	// UnusedTierB names every FloatTierB entry that matched no compared
	// numeric field. Same rule, same reason as UnusedExclusions: a Tier-B
	// declaration that relaxes nothing reads as a relaxation that is
	// there, and the first person to trust it is trusting nothing.
	UnusedTierB []string `json:"unused_tier_b,omitempty"`

	// BaselineDefectsMatched names the tickets whose declared paths cover
	// at least one of this comparison's differences.
	BaselineDefectsMatched []string `json:"baseline_defect,omitempty"`

	// StaleBaselineDefects names declared baseline-defect entries that
	// cover NO difference here -- the defect was fixed, or the paths are
	// wrong. Either way the entry must go, so the caller fails the run.
	StaleBaselineDefects []string `json:"stale_baseline_defects,omitempty"`

	// DifferencesOutsideBaselineDefect counts the mismatch findings NOT
	// covered by any declared baseline defect. It is serialised even when
	// zero: "every difference is a known Python defect" and "there were no
	// differences" are different facts, and an omitted zero hides which
	// one happened.
	DifferencesOutsideBaselineDefect int `json:"differences_outside_baseline_defect"`
}

// IsMatch reports whether the verdict is a clean match.
func (r Result) IsMatch() bool { return r.TerminalState == TerminalStateMatch }

// Options carries the per-operation parity configuration CHAOS-4381
// requires to be declared, never inferred.
type Options struct {
	// FloatTierB names Tier-B float fields by dotted, index-free path
	// (e.g. "data.hotspots.edges.score") mapped to the WRITTEN REASON
	// that field tolerates a difference. Every field not named here is
	// Tier A (exact) by default -- parity rule 3.
	//
	// The reason this table exists at all, measured under CHAOS-5451:
	// ClickHouse merges partial aggregate states in thread-completion
	// order, so avg/sum/stddevPop over Float64 differ by 1-2 ULP run to
	// run on BOTH planes -- 20 runs of stddevPop over 5M rows produced 9
	// distinct values, and max_threads=1 produced 1. That is engine
	// nondeterminism, not a Go-vs-Python defect, and comparing such a
	// field Tier A (as the enablement harness did) manufactures a
	// mismatch. It is equally not a licence to relax any field that
	// happens to differ: an entry here names a field whose value is a
	// merged floating-point aggregate, and nothing else.
	FloatTierB map[string]string

	// BaselineDefects declares differences that are known PYTHON defects
	// with Go correct. See BaselineDefect.
	BaselineDefects []BaselineDefect

	// VolatileFields names fields excluded from comparison entirely, by
	// the same dotted, index-free path form. Reserved for values that are
	// freshly generated per request and therefore cannot agree across two
	// calls -- capacityForecast's forecastId/computedAt are the measured
	// instance (CHAOS-5425 comment, 2026-09-07: identical request, Go
	// forecastId=33fb9f32..., Python 78296c67..., timestamps ~350ms
	// apart). This is NOT a general "ignore a difference I do not like"
	// knob: each entry carries a written reason in exclusions.go, and an
	// entry matching nothing fails the run via UnusedExclusions.
	VolatileFields map[string]string

	// EnvelopeKeys excuses named keys from the null-vs-omission check at
	// the TOP LEVEL of `data` only -- parity rule 2's stated exception for
	// transport-envelope keys one framework omits when empty. Never
	// applied to a nested field of the same name.
	EnvelopeKeys map[string]bool

	// RequireWatermark marks the operation as watermark-bearing: a
	// MISSING watermark on either side is then `unsupported`, not a
	// silent comparison without one -- parity rule 4.
	RequireWatermark bool
}

// BaselineDefect declares that a named set of field paths differs
// because the BASELINE (Python) is wrong and the candidate (Go) is right.
//
// It changes what a receipt SAYS, never what it is. Measured instances:
// CHAOS-5448 (Python omits FINAL on work_item_cycle_times and so counts
// superseded row versions) and CHAOS-5450 (Python's list path emits a
// naive timestamp). Both are real divergences, and a proof run must keep
// recording them as such.
//
// So the rules are deliberately narrow:
//
//   - the terminal state stays `mismatch`, unchanged. A declared baseline
//     defect NEVER converts a mismatch into a match and never promotes an
//     operation. Anything else would let "we know why" become "it passed".
//   - the receipt additionally records which tickets covered the
//     differences, and an explicit count of the differences covered by
//     NONE of them -- so "every difference is a known Python defect" is a
//     readable, checkable claim rather than a footnote in a chat message.
//   - an entry covering no difference in this comparison FAILS the run.
//     The defect was fixed, or the paths are wrong; a stale exemption that
//     silently exempts nothing is the failure mode this whole file is
//     built against.
type BaselineDefect struct {
	// Ticket is the issue that owns the defect, e.g. "CHAOS-5448".
	Ticket string
	// Reason states, in words, what the baseline gets wrong.
	Reason string
	// Paths are dotted, index-free field paths (the same form
	// FloatTierB and VolatileFields use). A path also covers everything
	// beneath it, so naming a subtree root covers its fields.
	Paths []string
}

// tracker records which declared entries actually matched something, so
// a declaration that matched nothing can be reported rather than sitting
// in the table reading as coverage.
type tracker struct {
	volatile map[string]bool
	tierB    map[string]bool
}

// Compare compares a baseline (Python) and candidate (Go) response under
// CHAOS-4381's parity rules. It mirrors the Python comparator's
// compare_responses, including its ordering: watermark handling runs
// FIRST and short-circuits, because a watermark delta is ClickHouse
// eventual consistency, never Go-vs-Python evidence, and must be allowed
// neither to produce nor to hide a mismatch verdict.
func Compare(baseline, candidate Snapshot, opts Options) Result {
	if opts.RequireWatermark && (baseline.Watermark == "" || candidate.Watermark == "") {
		return Result{
			TerminalState: TerminalStateUnsupported,
			Findings: []Finding{{
				Kind: FindingWatermarkMissing,
				Path: "$.watermark",
				Detail: fmt.Sprintf("required watermark missing: baseline=%q candidate=%q",
					baseline.Watermark, candidate.Watermark),
			}},
		}
	}
	if baseline.Watermark != "" && candidate.Watermark != "" && baseline.Watermark != candidate.Watermark {
		return Result{
			TerminalState: TerminalStateUnsupported,
			Findings: []Finding{{
				Kind: FindingWatermarkDrift,
				Path: "$.watermark",
				Detail: fmt.Sprintf("baseline watermark %q != candidate watermark %q",
					baseline.Watermark, candidate.Watermark),
			}},
		}
	}

	track := &tracker{volatile: map[string]bool{}, tierB: map[string]bool{}}
	findings := compareErrors(baseline.Errors, candidate.Errors, "$.errors")

	switch {
	case baseline.DataPresent != candidate.DataPresent:
		detail := "present in baseline, absent in candidate"
		if !baseline.DataPresent {
			detail = "present in candidate, absent in baseline"
		}
		findings = append(findings, Finding{Kind: FindingMismatch, Path: "$.data", Detail: detail})
	case baseline.DataPresent && candidate.DataPresent:
		findings = append(findings, compareJSON(baseline.Data, candidate.Data, "$.data", opts, opts.EnvelopeKeys, track)...)
	}
	// Neither side present: two responses that never reached execution.
	// Absence on both sides is not itself a finding.

	terminal := TerminalStateMatch
	for _, f := range findings {
		if f.Kind == FindingMismatch {
			terminal = TerminalStateMismatch
			break
		}
	}

	result := Result{TerminalState: terminal, Findings: findings}
	result.UnusedExclusions = unmatched(opts.VolatileFields, track.volatile)
	result.UnusedTierB = unmatched(opts.FloatTierB, track.tierB)
	classifyBaselineDefects(&result, opts.BaselineDefects)
	return result
}

// unmatched lists declared table keys that nothing marked as used.
func unmatched(declared map[string]string, used map[string]bool) []string {
	var out []string
	for path := range declared {
		if !used[path] {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

// classifyBaselineDefects attributes each mismatch finding to a declared
// baseline defect, if any, and counts the ones nothing covers.
//
// It never touches result.TerminalState. That is the whole contract: a
// declared, understood, ticketed Python defect is still a divergence, and
// the receipt still says mismatch.
func classifyBaselineDefects(result *Result, defects []BaselineDefect) {
	var mismatches []string
	for _, finding := range result.Findings {
		if finding.Kind == FindingMismatch {
			mismatches = append(mismatches, tieredPath(finding.Path))
		}
	}

	covered := make([]bool, len(mismatches))
	var matched, stale []string
	for _, defect := range defects {
		hit := false
		for i, path := range mismatches {
			if defectCovers(defect, path) {
				covered[i] = true
				hit = true
			}
		}
		if hit {
			matched = append(matched, defect.Ticket)
		} else {
			stale = append(stale, defect.Ticket)
		}
	}
	sort.Strings(matched)
	sort.Strings(stale)

	outside := 0
	for _, isCovered := range covered {
		if !isCovered {
			outside++
		}
	}

	result.BaselineDefectsMatched = matched
	result.StaleBaselineDefects = stale
	result.DifferencesOutsideBaselineDefect = outside
}

// defectCovers reports whether a declared path covers a difference path.
// A cited path covers itself and everything beneath it, so a subtree root
// can be named once instead of every leaf under it.
func defectCovers(defect BaselineDefect, path string) bool {
	for _, cited := range defect.Paths {
		if path == cited || strings.HasPrefix(path, cited+".") {
			return true
		}
	}
	return false
}

// --- Error comparison (parity rule 1) ---------------------------------

// errorIdentity is parity rule 1's error identity: path plus
// extensions.code. The MESSAGE is deliberately not part of it -- two
// planes phrasing the same failure differently is drift to record, not a
// different error.
func errorIdentity(err map[string]any) [2]string {
	segments := []string{}
	if raw, ok := err["path"]; ok {
		if list, ok := raw.([]any); ok {
			for _, seg := range list {
				segments = append(segments, fmt.Sprintf("%v", seg))
			}
		}
	}
	code := ""
	if raw, ok := err["extensions"]; ok {
		if ext, ok := raw.(map[string]any); ok {
			if value, ok := ext["code"]; ok {
				code = fmt.Sprintf("%v", value)
			}
		}
	}
	return [2]string{strings.Join(segments, "."), code}
}

func compareErrors(baseline, candidate []map[string]any, pathPrefix string) []Finding {
	byIdentity := func(errs []map[string]any) map[[2]string]map[string]any {
		out := make(map[[2]string]map[string]any, len(errs))
		for _, err := range errs {
			out[errorIdentity(err)] = err
		}
		return out
	}
	baselineByIdentity := byIdentity(baseline)
	candidateByIdentity := byIdentity(candidate)

	findings := []Finding{}
	identityPath := func(id [2]string) string {
		return fmt.Sprintf("%s[%q,%q]", pathPrefix, id[0], id[1])
	}

	var missing, extra, shared [][2]string
	for id := range baselineByIdentity {
		if _, ok := candidateByIdentity[id]; ok {
			shared = append(shared, id)
		} else {
			missing = append(missing, id)
		}
	}
	for id := range candidateByIdentity {
		if _, ok := baselineByIdentity[id]; !ok {
			extra = append(extra, id)
		}
	}
	sortIdentities(missing)
	sortIdentities(extra)
	sortIdentities(shared)

	for _, id := range missing {
		findings = append(findings, Finding{
			Kind:   FindingMismatch,
			Path:   identityPath(id),
			Detail: "error present in baseline, missing from candidate",
		})
	}
	for _, id := range extra {
		findings = append(findings, Finding{
			Kind:   FindingMismatch,
			Path:   identityPath(id),
			Detail: "error present in candidate, missing from baseline",
		})
	}
	for _, id := range shared {
		baseMessage := fmt.Sprintf("%v", baselineByIdentity[id]["message"])
		candMessage := fmt.Sprintf("%v", candidateByIdentity[id]["message"])
		if baseMessage != candMessage {
			findings = append(findings, Finding{
				Kind:   FindingErrorMessageDrift,
				Path:   identityPath(id) + ".message",
				Detail: fmt.Sprintf("%q != %q", baseMessage, candMessage),
			})
		}
	}
	return findings
}

func sortIdentities(ids [][2]string) {
	sort.Slice(ids, func(i, j int) bool {
		if ids[i][0] != ids[j][0] {
			return ids[i][0] < ids[j][0]
		}
		return ids[i][1] < ids[j][1]
	})
}

// --- Structural JSON comparison (parity rules 2, 3, 5) -----------------

var listIndexSuffix = regexp.MustCompile(`\[\d+\]`)

// tieredPath normalises a concrete comparator path ("$.data.edges[0].score")
// to the dotted, index-free form callers declare Tier-B and volatile
// fields with ("data.edges.score").
func tieredPath(path string) string {
	segments := strings.Split(path, ".")
	if len(segments) > 0 && segments[0] == "$" {
		segments = segments[1:]
	}
	for i, segment := range segments {
		segments[i] = listIndexSuffix.ReplaceAllString(segment, "")
	}
	return strings.Join(segments, ".")
}

// excluded reports whether path is a declared volatile field, recording
// the hit so an exclusion that never matches can be reported as unused.
func excluded(path string, opts Options, track *tracker) bool {
	key := tieredPath(path)
	if _, ok := opts.VolatileFields[key]; !ok {
		return false
	}
	track.volatile[key] = true
	return true
}

func compareJSON(baseline, candidate any, path string, opts Options, envelopeKeys map[string]bool, track *tracker) []Finding {
	baselineMap, baselineIsMap := baseline.(map[string]any)
	candidateMap, candidateIsMap := candidate.(map[string]any)
	if baselineIsMap && candidateIsMap {
		return compareDict(baselineMap, candidateMap, path, opts, envelopeKeys, track)
	}

	baselineList, baselineIsList := baseline.([]any)
	candidateList, candidateIsList := candidate.([]any)
	if baselineIsList && candidateIsList {
		return compareList(baselineList, candidateList, path, opts, track)
	}

	baselineBool, baselineIsBool := baseline.(bool)
	candidateBool, candidateIsBool := candidate.(bool)
	if baselineIsBool || candidateIsBool {
		if !baselineIsBool || !candidateIsBool || baselineBool != candidateBool {
			return []Finding{{Kind: FindingMismatch, Path: path, Detail: fmt.Sprintf("%v != %v", baseline, candidate)}}
		}
		return nil
	}

	baselineNumber, baselineIsNumber := asFloat(baseline)
	candidateNumber, candidateIsNumber := asFloat(candidate)
	if baselineIsNumber && candidateIsNumber {
		// DELIBERATE DIVERGENCE from the Python comparator, in the
		// STRICTER direction. _compare_number does `float(baseline) !=
		// float(candidate)`, so two DIFFERENT int64s beyond float64's
		// 2^53 precision (9007199254740992 vs 9007199254740993) collapse
		// to the same float and compare EQUAL -- a Tier-A "exact"
		// comparison that is not exact. A proof instrument that cannot
		// distinguish two different integers cannot certify parity over
		// them, so integers are compared as integers here. Floats, and
		// any mixed int/float pair, still go through the float path, so
		// a formatting-only difference (1 vs 1.0) does not become a false
		// mismatch. Filed against the Python side as a divergence to
		// re-mirror, not silently forked.
		//
		// Tier-B fields are excluded from this path: a field declared as
		// tolerance-compared must stay tolerance-compared even when both
		// sides happen to be whole numbers, or the declaration would mean
		// different things depending on the data.
		if _, isTierB := opts.FloatTierB[tieredPath(path)]; !isTierB {
			if baselineInt, ok := asInt64(baseline); ok {
				if candidateInt, ok := asInt64(candidate); ok {
					if baselineInt != candidateInt {
						return []Finding{{
							Kind:   FindingMismatch,
							Path:   path,
							Detail: fmt.Sprintf("%d != %d (Tier A, exact integer)", baselineInt, candidateInt),
						}}
					}
					return nil
				}
			}
		}
		return compareNumber(baselineNumber, candidateNumber, path, opts, track)
	}

	if !sameScalar(baseline, candidate) {
		return []Finding{{Kind: FindingMismatch, Path: path, Detail: fmt.Sprintf("%v != %v", baseline, candidate)}}
	}
	return nil
}

// asFloat accepts every JSON number shape this package can be handed:
// json.Number (the decoder configured by DecodeSnapshot, which preserves
// the literal so an integer is never silently widened before comparison),
// and float64/int for values built in Go by a test or caller.
//
// A json.Number that does not parse as a float is NOT a number: it is
// returned as not-a-number so the scalar branch reports a mismatch rather
// than this function inventing a zero.
func asFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			// An OUT-OF-RANGE literal (1e400) is still a number, and
			// ParseFloat has already returned the correct ±Inf for it --
			// returning "not a number" here would drop it into the scalar
			// branch, where two identical overflowing literals compare
			// EQUAL and parity rule 3's "non-finite always mismatches"
			// would silently never fire. Only a SYNTAX error means this
			// value is not a number.
			if errors.Is(err, strconv.ErrRange) {
				return parsed, true
			}
			return 0, false
		}
		return parsed, true
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

// asInt64 reports whether value is an INTEGER JSON number, and its exact
// value. A float literal ("1.0", "1e3") is deliberately not an integer
// here: json.Number.Int64 rejects it, which is what keeps a
// formatting-only difference on the float path instead of this exact one.
func asInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

// sameScalar compares two non-map, non-list, non-bool, non-number values.
// nil equals only nil (parity rule 2: null is a value, never a wildcard).
func sameScalar(baseline, candidate any) bool {
	if baseline == nil || candidate == nil {
		return baseline == nil && candidate == nil
	}
	baselineString, baselineIsString := baseline.(string)
	candidateString, candidateIsString := candidate.(string)
	if baselineIsString && candidateIsString {
		return baselineString == candidateString
	}
	return fmt.Sprintf("%T:%v", baseline, baseline) == fmt.Sprintf("%T:%v", candidate, candidate)
}

func compareNumber(baseline, candidate float64, path string, opts Options, track *tracker) []Finding {
	if math.IsNaN(baseline) || math.IsNaN(candidate) || math.IsInf(baseline, 0) || math.IsInf(candidate, 0) {
		// Parity rule 3: NaN/Infinity ALWAYS mismatches, never
		// tolerance-compared -- including when both sides agree. inf==inf
		// is true in IEEE 754, but a non-finite value reaching a client at
		// all is itself the defect this rule exists to catch.
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("non-finite value: %v vs %v (NaN/Infinity always mismatches)", baseline, candidate),
		}}
	}

	key := tieredPath(path)
	if _, isTierB := opts.FloatTierB[key]; !isTierB {
		if baseline != candidate {
			return []Finding{{
				Kind:   FindingMismatch,
				Path:   path,
				Detail: fmt.Sprintf("%v != %v (Tier A, exact)", baseline, candidate),
			}}
		}
		return nil
	}
	// Marked as soon as a Tier-B field is actually COMPARED, not only
	// when its tolerance is exercised: the declaration's claim is "this
	// field exists and is a merged aggregate", and a field that compares
	// equal every run still satisfies it. Marking only on a tolerated
	// difference would report every correctly-declared, currently-stable
	// field as stale.
	track.tierB[key] = true

	tolerance := math.Max(floatTolerance, floatTolerance*math.Max(math.Abs(baseline), math.Abs(candidate)))
	if math.Abs(baseline-candidate) > tolerance {
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("%v != %v (Tier B, tolerance %v)", baseline, candidate, tolerance),
		}}
	}
	return nil
}

// compareDict applies envelopeKeys to THIS dict's own key-presence check
// only, then drops it: every nested dict, at any depth, compares with no
// envelope exclusions. A field named "extensions" nested inside `data` is
// ordinary business data, not a transport-envelope key.
func compareDict(baseline, candidate map[string]any, path string, opts Options, envelopeKeys map[string]bool, track *tracker) []Finding {
	keys := make([]string, 0, len(baseline)+len(candidate))
	seen := make(map[string]bool, len(baseline)+len(candidate))
	for key := range baseline {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	for key := range candidate {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var findings []Finding
	for _, key := range keys {
		childPath := path + "." + key
		if excluded(childPath, opts, track) {
			continue
		}
		_, inBaseline := baseline[key]
		_, inCandidate := candidate[key]
		if inBaseline != inCandidate {
			if envelopeKeys[key] {
				continue
			}
			detail := "present in baseline, absent in candidate"
			if !inBaseline {
				detail = "present in candidate, absent in baseline"
			}
			findings = append(findings, Finding{Kind: FindingMismatch, Path: childPath, Detail: detail})
			continue
		}
		findings = append(findings, compareJSON(baseline[key], candidate[key], childPath, opts, nil, track)...)
	}
	return findings
}

// compareList compares positionally -- parity rule 5's default. A length
// difference is reported alone: element-wise findings past that point
// would be a cascade of noise about an offset, not independent defects.
func compareList(baseline, candidate []any, path string, opts Options, track *tracker) []Finding {
	if len(baseline) != len(candidate) {
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("length %d != %d", len(baseline), len(candidate)),
		}}
	}
	var findings []Finding
	for i := range baseline {
		childPath := fmt.Sprintf("%s[%d]", path, i)
		if excluded(childPath, opts, track) {
			continue
		}
		findings = append(findings, compareJSON(baseline[i], candidate[i], childPath, opts, nil, track)...)
	}
	return findings
}
