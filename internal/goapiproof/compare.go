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
}

// IsMatch reports whether the verdict is a clean match.
func (r Result) IsMatch() bool { return r.TerminalState == TerminalStateMatch }

// Options carries the per-operation parity configuration CHAOS-4381
// requires to be declared, never inferred.
type Options struct {
	// FloatTierB names Tier-B float fields by dotted, index-free path
	// (e.g. "data.hotspots.edges.score"). Every field not named here is
	// Tier A (exact) by default -- parity rule 3.
	FloatTierB map[string]bool

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

	used := map[string]bool{}
	findings := compareErrors(baseline.Errors, candidate.Errors, "$.errors")

	switch {
	case baseline.DataPresent != candidate.DataPresent:
		detail := "present in baseline, absent in candidate"
		if !baseline.DataPresent {
			detail = "present in candidate, absent in baseline"
		}
		findings = append(findings, Finding{Kind: FindingMismatch, Path: "$.data", Detail: detail})
	case baseline.DataPresent && candidate.DataPresent:
		findings = append(findings, compareJSON(baseline.Data, candidate.Data, "$.data", opts, opts.EnvelopeKeys, used)...)
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

	var unused []string
	for path := range opts.VolatileFields {
		if !used[path] {
			unused = append(unused, path)
		}
	}
	sort.Strings(unused)

	return Result{TerminalState: terminal, Findings: findings, UnusedExclusions: unused}
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
func excluded(path string, opts Options, used map[string]bool) bool {
	key := tieredPath(path)
	if _, ok := opts.VolatileFields[key]; !ok {
		return false
	}
	used[key] = true
	return true
}

func compareJSON(baseline, candidate any, path string, opts Options, envelopeKeys map[string]bool, used map[string]bool) []Finding {
	baselineMap, baselineIsMap := baseline.(map[string]any)
	candidateMap, candidateIsMap := candidate.(map[string]any)
	if baselineIsMap && candidateIsMap {
		return compareDict(baselineMap, candidateMap, path, opts, envelopeKeys, used)
	}

	baselineList, baselineIsList := baseline.([]any)
	candidateList, candidateIsList := candidate.([]any)
	if baselineIsList && candidateIsList {
		return compareList(baselineList, candidateList, path, opts, used)
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
		if !opts.FloatTierB[tieredPath(path)] {
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
		return compareNumber(baselineNumber, candidateNumber, path, opts)
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

func compareNumber(baseline, candidate float64, path string, opts Options) []Finding {
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

	if !opts.FloatTierB[tieredPath(path)] {
		if baseline != candidate {
			return []Finding{{
				Kind:   FindingMismatch,
				Path:   path,
				Detail: fmt.Sprintf("%v != %v (Tier A, exact)", baseline, candidate),
			}}
		}
		return nil
	}

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
func compareDict(baseline, candidate map[string]any, path string, opts Options, envelopeKeys map[string]bool, used map[string]bool) []Finding {
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
		if excluded(childPath, opts, used) {
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
		findings = append(findings, compareJSON(baseline[key], candidate[key], childPath, opts, nil, used)...)
	}
	return findings
}

// compareList compares positionally -- parity rule 5's default. A length
// difference is reported alone: element-wise findings past that point
// would be a cascade of noise about an offset, not independent defects.
func compareList(baseline, candidate []any, path string, opts Options, used map[string]bool) []Finding {
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
		if excluded(childPath, opts, used) {
			continue
		}
		findings = append(findings, compareJSON(baseline[i], candidate[i], childPath, opts, nil, used)...)
	}
	return findings
}
