package goapiproof

import "math"

// HomeConfidenceTierShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the
// mechanical CONSEQUENCE a coverage_pct drift produces on GET/POST
// /api/v1/home's own tier/confidence leaves, never the base drift itself.
//
// The mechanism this shape's OWN citation names (in its Reason, never
// here) is fetch_coverage's raw read of work_item_cycle_times
// (ReplacingMergeTree(computed_at), key (org_id, provider, work_item_id),
// `day` NOT part of that key -- internal/queryapi/home/
// queries_freshness.go's own doc comment) on the reference plane, FINAL
// on this port. A physical stale/live version pair can straddle the
// day-window filter independently of one another, so the resulting
// coverage ratio (data.freshness.coverage.issues_with_cycle_states_pct,
// and through it data.data_confidence.coverage_pct,
// _coverage_pct_from_coverage's mean of three ratios, services/home.py)
// has NO PROVABLE DIRECTION from the two response bodies alone -- home_
// corpus.go's own package doc comment already states this for the base
// ratio and leaves it uncovered. This shape never re-derives or bounds
// that base drift; it only certifies that, GIVEN the two coverage_pct
// values already observed on the wire (whatever produced them), every
// downstream tier/confidence leaf is the documented, mechanical function
// of its OWN leg's own coverage_pct (and, for a signal, its own
// evidence_count) -- nothing more.
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. data.data_confidence.coverage_pct resolves to a number on BOTH
//     legs (Rule 1's own precondition -- a null coverage_pct is outside
//     this shape's scope entirely, refused, never guessed).
//  2. data.data_confidence.missing_sources and .connected_sources are
//     BYTE-IDENTICAL (as sets) between the two legs. These two lists were
//     never announced as differing by the ticket this shape was written
//     for, and admitting the tier leaf without pinning them first would
//     let an unrelated source-status regression hide behind this shape.
//  3. data.data_confidence.level is admitted only when RECOMPUTING it
//     from THAT leg's own coverage_pct/missing_sources/connected_sources,
//     via the documented thresholds (build_data_confidence, services/
//     home.py:444-456 / BuildDataConfidence, internal/queryapi/home/
//     signals.go:261-289), reproduces the OBSERVED level on BOTH legs
//     independently. A leg whose own observed level disagrees with its
//     own recomputed level is never admitted -- that is a real,
//     independent defect this shape must never hide.
//  4. Each data.signals[N].confidence is admitted only when its own leg's
//     evidence_count is IDENTICAL between legs (evidence_count is drawn
//     from the same admitted evidence set on both planes and is never
//     itself part of the declared mechanism; a difference there means
//     something else is wrong) AND recomputing that leg's own confidence
//     from ITS OWN evidence_count/coverage_pct, via the documented
//     thresholds (_confidence_from_evidence, home.py:359-365 /
//     signals.go:150), reproduces the OBSERVED confidence on BOTH legs
//     independently.
//  5. data.limiting_factor.confidence is admitted only when it equals
//     data.signals[0].confidence on EACH leg independently (the wiring
//     build_limiting_factor/its Go port actually use), the two legs'
//     OWN top signal (signals[0]) share the SAME id, and rule 4's own
//     admission already holds for signals[0] on this same comparison.
//
// What this shape CANNOT, and does not try to, certify: the magnitude or
// direction of the coverage_pct/issues_with_cycle_states_pct drift
// itself, or any signals[] leaf besides confidence (severity, direction,
// evidence_count, ...), which stay outside every citation exactly as
// before this shape existed.
type HomeConfidenceTierShape struct {
	// CoveragePctPath is data.data_confidence's own coverage_pct leaf,
	// e.g. "data.data_confidence.coverage_pct".
	CoveragePctPath string
	// MissingSourcesPath/ConnectedSourcesPath are data.data_confidence's
	// own missing_sources/connected_sources list leaves.
	MissingSourcesPath, ConnectedSourcesPath string
	// LevelPath is data.data_confidence's own level leaf.
	LevelPath string
	// LimitingFactorConfidencePath is data.limiting_factor's own
	// confidence leaf.
	LimitingFactorConfidencePath string
	// SignalsListPath is the dotted, index-free path to the signals LIST
	// itself, e.g. "data.signals". Each element's own "confidence"/
	// "evidence_count"/"id" fields are read directly, not declared here:
	// naming three more fields for a fixed, known wire shape buys nothing
	// a constant field name does not already state.
	SignalsListPath string

	// SupersededRatioPaths are the coverage ratios drawn from the table
	// whose superseded versions the reference plane counts (the issues and
	// PR-link ratios); OtherRatioPath is the ratio no such read feeds
	// (repos covered), which is never admitted here: a difference at it
	// stays outside every citation. Rule 6 admits a difference at
	// CoveragePctPath or a SupersededRatioPaths leaf only while each leg's
	// own coverage_pct is the mean of its own three ratios (the mean's
	// inputs are SupersededRatioPaths and OtherRatioPath).
	SupersededRatioPaths []string
	OtherRatioPath       string
}

// meanIdentityTolerance bounds the float noise of recomputing the mean of
// three ratios; a mean that differs by more than this is not that mean.
const meanIdentityTolerance = 1e-9

// homeConfidenceTierPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded response bodies.
type homeConfidenceTierPlan struct {
	shape *HomeConfidenceTierShape
	// valid is false when coverage_pct/missing_sources/connected_sources
	// could not even be read on both legs, or the two source lists
	// disagree -- a plan that is not valid admits NOTHING, the safe
	// default every other plan in this package uses.
	valid bool
	// levelAdmits/limitingFactorAdmits hold rules 3/5's own whole-
	// comparison verdicts.
	levelAdmits          bool
	limitingFactorAdmits bool
	// signalConfidenceAdmits holds rule 4's own per-index verdict, keyed
	// by the signal's own list position (identical on both legs -- see
	// buildHomeConfidenceTierPlan's own length check).
	signalConfidenceAdmits map[int]bool
	// coverageAdmits holds rule 6's whole-comparison verdict.
	coverageAdmits bool
}

// homeConfidenceLevel ports build_data_confidence's own level derivation
// (services/home.py:450-455 / BuildDataConfidence, signals.go:282-289).
func homeConfidenceLevel(coveragePct float64, missingEmpty, connectedNonEmpty bool) string {
	switch {
	case coveragePct >= 75 && missingEmpty:
		return "high"
	case coveragePct >= 40 && connectedNonEmpty:
		return "medium"
	default:
		return "low"
	}
}

// homeSignalConfidence ports _confidence_from_evidence (home.py:359-365 /
// signals.go's own port at line 150).
func homeSignalConfidence(evidenceCount, coveragePct float64) string {
	switch {
	case evidenceCount >= 7 && coveragePct >= 75:
		return "high"
	case evidenceCount >= 2 && coveragePct >= 40:
		return "medium"
	default:
		return "low"
	}
}

// stringAtDottedPath reads one scalar string leaf at a dotted, index-free
// path under `data`. ok is false when the path does not resolve to a
// string.
func stringAtDottedPath(root any, dottedPath string) (string, bool) {
	value, ok := navigateSegments(root, citedSegments(dottedPath))
	if !ok {
		return "", false
	}
	s, ok := value.(string)
	return s, ok
}

// homeStringSetEqual reports whether a and b hold the same multiset of
// strings, order-independent -- missing_sources/connected_sources are
// declared sorted on both planes, but this check does not lean on that:
// a genuine content difference is refused either way, and a difference
// that is ONLY ordering is correctly still equal.
func homeStringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}

// buildHomeConfidenceTierPlan evaluates every rule HomeConfidenceTierShape
// documents against one comparison's decoded baseline/candidate `data`
// values.
func buildHomeConfidenceTierPlan(shape *HomeConfidenceTierShape, baselineData, candidateData any) *homeConfidenceTierPlan {
	plan := &homeConfidenceTierPlan{shape: shape, signalConfidenceAdmits: map[int]bool{}}

	baseCoverage, okBC := floatAtDottedPath(baselineData, shape.CoveragePctPath)
	candCoverage, okCC := floatAtDottedPath(candidateData, shape.CoveragePctPath)
	if !okBC || !okCC {
		return plan
	}
	baseMissing, okBM := stringListAtDottedPath(baselineData, shape.MissingSourcesPath)
	candMissing, okCM := stringListAtDottedPath(candidateData, shape.MissingSourcesPath)
	baseConnected, okBCo := stringListAtDottedPath(baselineData, shape.ConnectedSourcesPath)
	candConnected, okCCo := stringListAtDottedPath(candidateData, shape.ConnectedSourcesPath)
	if !okBM || !okCM || !okBCo || !okCCo {
		return plan
	}
	// Rule 2.
	if !homeStringSetEqual(baseMissing, candMissing) || !homeStringSetEqual(baseConnected, candConnected) {
		return plan
	}
	plan.valid = true

	// Rule 3.
	baseLevel, okBL := stringAtDottedPath(baselineData, shape.LevelPath)
	candLevel, okCL := stringAtDottedPath(candidateData, shape.LevelPath)
	if okBL && okCL {
		expectedBase := homeConfidenceLevel(baseCoverage, len(baseMissing) == 0, len(baseConnected) > 0)
		expectedCand := homeConfidenceLevel(candCoverage, len(candMissing) == 0, len(candConnected) > 0)
		plan.levelAdmits = expectedBase == baseLevel && expectedCand == candLevel
	}

	// Rule 4.
	baseSignals, okBS := listAtDottedPath(baselineData, shape.SignalsListPath)
	candSignals, okCS := listAtDottedPath(candidateData, shape.SignalsListPath)
	if okBS && okCS && len(baseSignals) == len(candSignals) {
		for i := range candSignals {
			baseObj, ok1 := baseSignals[i].(map[string]any)
			candObj, ok2 := candSignals[i].(map[string]any)
			if !ok1 || !ok2 {
				continue
			}
			baseEvidence, okBE := asFloat(baseObj["evidence_count"])
			candEvidence, okCE := asFloat(candObj["evidence_count"])
			baseConfidence, okBCf := baseObj["confidence"].(string)
			candConfidence, okCCf := candObj["confidence"].(string)
			if !okBE || !okCE || !okBCf || !okCCf || baseEvidence != candEvidence {
				continue
			}
			expectedBase := homeSignalConfidence(baseEvidence, baseCoverage)
			expectedCand := homeSignalConfidence(candEvidence, candCoverage)
			if expectedBase == baseConfidence && expectedCand == candConfidence {
				plan.signalConfidenceAdmits[i] = true
			}
		}

		// Rule 5.
		if len(baseSignals) > 0 {
			baseTop, ok1 := baseSignals[0].(map[string]any)
			candTop, ok2 := candSignals[0].(map[string]any)
			baseLF, okLF1 := stringAtDottedPath(baselineData, shape.LimitingFactorConfidencePath)
			candLF, okLF2 := stringAtDottedPath(candidateData, shape.LimitingFactorConfidencePath)
			if ok1 && ok2 && okLF1 && okLF2 {
				baseTopID, _ := baseTop["id"].(string)
				candTopID, _ := candTop["id"].(string)
				baseTopConfidence, _ := baseTop["confidence"].(string)
				candTopConfidence, _ := candTop["confidence"].(string)
				if baseTopID != "" && baseTopID == candTopID &&
					baseLF == baseTopConfidence && candLF == candTopConfidence {
					plan.limitingFactorAdmits = plan.signalConfidenceAdmits[0]
				}
			}
		}
	}

	// Rule 6.
	if len(shape.SupersededRatioPaths) > 0 && shape.OtherRatioPath != "" {
		plan.coverageAdmits = homeCoverageMeanHolds(shape, baselineData, candidateData, baseCoverage, candCoverage)
	}

	return plan
}

// homeCoverageMeanHolds reports rule 6: each leg's coverage_pct is the
// mean of that leg's own three ratios, so a coverage_pct difference is
// fully explained by the ratio differences the plan also admits (a
// difference at the other ratio is itself outside every citation).
func homeCoverageMeanHolds(shape *HomeConfidenceTierShape, baselineData, candidateData any, baseCoverage, candCoverage float64) bool {
	paths := append([]string{shape.OtherRatioPath}, shape.SupersededRatioPaths...)
	for _, leg := range []struct {
		data     any
		coverage float64
	}{{baselineData, baseCoverage}, {candidateData, candCoverage}} {
		sum := 0.0
		for _, path := range paths {
			v, ok := floatAtDottedPath(leg.data, path)
			if !ok {
				return false
			}
			sum += v
		}
		if math.Abs(sum/float64(len(paths))-leg.coverage) > meanIdentityTolerance {
			return false
		}
	}
	return true
}

// admits reports whether one Finding is covered by this plan.
func (p *homeConfidenceTierPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	path := tieredPath(finding.Path)
	if path == p.shape.CoveragePctPath {
		return p.coverageAdmits
	}
	for _, ratio := range p.shape.SupersededRatioPaths {
		if path == ratio {
			return p.coverageAdmits
		}
	}
	switch path {
	case p.shape.LevelPath:
		return p.levelAdmits
	case p.shape.LimitingFactorConfidencePath:
		return p.limitingFactorAdmits
	case p.shape.SignalsListPath + ".confidence":
		idx, ok := edgeListIndex(finding.Path, p.shape.SignalsListPath)
		if !ok {
			return false
		}
		return p.signalConfidenceAdmits[idx]
	}
	return false
}
