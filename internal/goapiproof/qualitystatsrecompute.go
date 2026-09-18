package goapiproof

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// This file holds the two shapes that check GET/POST /api/v1/investment's
// evidence_quality_stats leaves against each leg's OWN response body:
//
//   - QualityDriversRecomputeShape recomputes quality_drivers from the
//     same leg's own total/band_counts/mean/stddev and admits a
//     difference only when BOTH legs report exactly the list their own
//     inputs give.
//   - BandMomentSubsetShape checks mean/stddev for consistency with the
//     same leg's own band_counts, and the baseline-minus-candidate
//     excluded population for consistency with its own band counts. It
//     bounds the two leaves from the bodies; it claims no direction.
//
// Neither shape admits a band_counts or total difference: those leaves
// stay with the declarations that already cover them. Both shapes only
// READ the counts from the bodies as inputs.

// evidenceQualityBand is one evidence-quality band and the closed value
// interval [Low, High] of every evidence_quality value stored under it.
// Both writers derive evidence_quality_band from evidence_quality with the
// same thresholds (units.EvidenceQualityBand; utils/normalization.py
// evidence_quality_band) after clamping it to [0, 1] (units.ClampUnit).
// The intervals are built from units.EvidenceQualityBandFloors -- the
// thresholds units.EvidenceQualityBand itself reads -- so the writer and
// this shape share one definition: Low is the band's floor, and High is
// the largest float64 below the next band's floor (the band excludes that
// floor), or the clamp's upper bound for the highest band.
// qualitystatsrecompute_test.go walks every edge through
// units.EvidenceQualityBand.
type evidenceQualityBand struct {
	Name      string
	Low, High float64
}

var evidenceQualityBands = evidenceQualityBandsFrom(units.EvidenceQualityBandFloors, units.ClampUnit(math.Inf(1)))

// evidenceQualityBandsFrom turns band floors, lowest first, into closed
// value intervals whose highest band ends at top.
func evidenceQualityBandsFrom(floors []units.EvidenceQualityBandFloor, top float64) []evidenceQualityBand {
	bands := make([]evidenceQualityBand, len(floors))
	for i, floor := range floors {
		high := top
		if i+1 < len(floors) {
			high = math.Nextafter(floors[i+1].Floor, math.Inf(-1))
		}
		bands[i] = evidenceQualityBand{Name: floor.Name, Low: floor.Floor, High: high}
	}
	return bands
}

// evidenceQualityUnknownBand is the band_counts key for a row with a
// NULL evidence_quality or an empty band (qualitystats.go's own
// unknown_count).
const evidenceQualityUnknownBand = "unknown"

// qualityStatsLeg is one leg's decoded evidence_quality_stats object.
type qualityStatsLeg struct {
	total      int64
	bands      map[string]int64
	mean       *float64
	stddev     *float64
	drivers    []string
	driversSet bool
}

// readQualityStatsLeg decodes the evidence_quality_stats object at
// statsPath. ok is false when any field is missing or of the wrong type:
// the caller then admits nothing.
func readQualityStatsLeg(data any, statsPath string) (qualityStatsLeg, bool) {
	var leg qualityStatsLeg
	raw, ok := navigateSegments(data, citedSegments(statsPath))
	if !ok {
		return leg, false
	}
	stats, ok := raw.(map[string]any)
	if !ok {
		return leg, false
	}
	total, ok := nonNegativeInteger(stats["total"])
	if !ok {
		return leg, false
	}
	leg.total = total
	bandsRaw, ok := stats["band_counts"].(map[string]any)
	if !ok || len(bandsRaw) != len(evidenceQualityBands)+1 {
		return leg, false
	}
	leg.bands = make(map[string]int64, len(bandsRaw))
	for _, name := range evidenceQualityBandCountKeys() {
		count, ok := nonNegativeInteger(bandsRaw[name])
		if !ok {
			return leg, false
		}
		leg.bands[name] = count
	}
	if leg.mean, ok = optionalFiniteFloat(stats["mean"]); !ok {
		return leg, false
	}
	if leg.stddev, ok = optionalFiniteFloat(stats["stddev"]); !ok {
		return leg, false
	}
	if driversRaw, present := stats["quality_drivers"]; present {
		list, ok := driversRaw.([]any)
		if !ok {
			return leg, false
		}
		leg.drivers = make([]string, 0, len(list))
		for _, item := range list {
			driver, ok := item.(string)
			if !ok {
				return leg, false
			}
			leg.drivers = append(leg.drivers, driver)
		}
		leg.driversSet = true
	}
	return leg, true
}

// evidenceQualityBandCountKeys lists the five band_counts keys: the four
// value bands, then unknown.
func evidenceQualityBandCountKeys() []string {
	keys := make([]string, 0, len(evidenceQualityBands)+1)
	for _, band := range evidenceQualityBands {
		keys = append(keys, band.Name)
	}
	return append(keys, evidenceQualityUnknownBand)
}

// nonNegativeInteger reads a JSON number that is a whole, non-negative
// integer.
func nonNegativeInteger(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || parsed < 0 {
		return 0, false
	}
	return parsed, true
}

// optionalFiniteFloat reads a JSON number or null. ok is false for any
// other type or a non-finite number.
func optionalFiniteFloat(value any) (*float64, bool) {
	if value == nil {
		return nil, true
	}
	parsed, ok := asFloat(value)
	if !ok || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil, false
	}
	return &parsed, true
}

// QualityDriversRecomputeShape, set on a BaselineDefect, admits a
// quality_drivers difference only when EACH leg's own list equals,
// element for element and in order, the list computeQualityStats
// (cmd/query-api/internal/investment/response.go) and
// _compute_quality_stats (api/services/investment.py) derive from that
// SAME leg's own total, band_counts, mean and stddev:
//
//  1. missing_evidence_metadata: unknown/T > 0.3
//  2. low_text_signal: mean present and < 0.4
//  3. high_uncertainty_spread: stddev present and > 0.25
//  4. weak_cross_links: (low + very_low)/T > 0.5
//  5. thin_component: quality_known_count/T < 0.7
//
// where T is total, or the band_counts sum when total is 0. Rule 5 reads
// quality_known_count, which the response does not carry. A NULL
// evidence_quality is always counted under unknown, so
// max(total - unknown, 0) <= quality_known_count <= total. When
// total/T < 0.7 rule 5 is determined present; when
// max(total - unknown, 0)/T >= 0.7 (always so when unknown is 0 and
// total > 0) it is determined absent; otherwise it cannot be determined
// from the body and the plan admits nothing.
//
// The shape does not explain why the INPUTS differ between the legs:
// that stays with the declarations covering total, band_counts, mean and
// stddev.
type QualityDriversRecomputeShape struct {
	// StatsPath is the dotted path to the evidence_quality_stats object,
	// e.g. "data.evidence_quality_stats".
	StatsPath string
	// DriversPath is the leaf path findings carry for the list -- must
	// equal one of the defect's own Paths entries, e.g.
	// "data.evidence_quality_stats.quality_drivers".
	DriversPath string
}

type qualityDriversRecomputePlan struct {
	shape *QualityDriversRecomputeShape
	valid bool
}

func buildQualityDriversRecomputePlan(shape *QualityDriversRecomputeShape, baselineData, candidateData any) *qualityDriversRecomputePlan {
	plan := &qualityDriversRecomputePlan{shape: shape}
	for _, data := range []any{baselineData, candidateData} {
		leg, ok := readQualityStatsLeg(data, shape.StatsPath)
		if !ok || !leg.driversSet {
			return plan
		}
		want, ok := recomputeQualityDrivers(leg)
		if !ok || !equalStringLists(want, leg.drivers) {
			return plan
		}
	}
	plan.valid = true
	return plan
}

// recomputeQualityDrivers applies the five rules in the order the type
// doc comment lists them. ok is false when rule 5 cannot be determined
// from the body.
func recomputeQualityDrivers(leg qualityStatsLeg) ([]string, bool) {
	unknown := leg.bands[evidenceQualityUnknownBand]
	t := leg.total
	if t == 0 {
		t = bandSum(leg)
	}
	drivers := []string{}
	if t > 0 && float64(unknown)/float64(t) > 0.3 {
		drivers = append(drivers, "missing_evidence_metadata")
	}
	if leg.mean != nil && *leg.mean < 0.4 {
		drivers = append(drivers, "low_text_signal")
	}
	if leg.stddev != nil && *leg.stddev > 0.25 {
		drivers = append(drivers, "high_uncertainty_spread")
	}
	lowPlus := leg.bands["low"] + leg.bands["very_low"]
	if t > 0 && float64(lowPlus)/float64(t) > 0.5 {
		drivers = append(drivers, "weak_cross_links")
	}
	if t > 0 {
		// quality_known_count lies in [max(total - unknown, 0), total].
		// With t > 0, total is either t or 0 (t falls back to the
		// band_counts sum only when total is 0), so the ceiling total/t
		// is below 0.7 exactly when total is 0.
		knownFloor := max(leg.total-unknown, 0)
		switch {
		case leg.total == 0:
			drivers = append(drivers, "thin_component")
		case float64(knownFloor)/float64(t) < 0.7:
			return nil, false
		}
	}
	return drivers, true
}

// RecomputeQualityDrivers applies QualityDriversRecomputeShape's own five
// rules to one evidence_quality_stats body: total, band_counts keyed by
// the five band names, and mean/stddev (nil for null). ok is false when
// thin_component cannot be determined from these values. It exists so a
// test beside computeQualityStats can hold the two to the same list.
func RecomputeQualityDrivers(total int64, bandCounts map[string]int64, mean, stddev *float64) (drivers []string, ok bool) {
	leg := qualityStatsLeg{total: total, bands: make(map[string]int64, len(evidenceQualityBands)+1), mean: mean, stddev: stddev}
	for _, name := range evidenceQualityBandCountKeys() {
		leg.bands[name] = bandCounts[name]
	}
	return recomputeQualityDrivers(leg)
}

func equalStringLists(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// admits covers every finding at DriversPath -- a length difference on
// the list or a value difference on one of its elements.
func (p *qualityDriversRecomputePlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	return tieredPath(finding.Path) == p.shape.DriversPath
}

// BandMomentSubsetShape, set on a BaselineDefect, admits a mean/stddev
// difference on evidence_quality_stats when the baseline population
// contains the candidate population and each population's (mean,
// E[x^2]) pair is one that values inside its own band intervals can
// have. N below is a population's row count; for a leg it is total,
// which equals quality_known_count because rule 2 requires unknown == 0.
// Every rule must hold, or nothing is admitted:
//
//  0. A candidate with total 0 is admitted only with every band count 0
//     and a null mean and stddev -- what computeQualityStats reports for
//     a population with no known quality -- and a baseline that passes
//     rules 1-3 and 6 on its own. Its null leaves reach the comparator as
//     an empty result, and only this case admits one.
//  1. Each leg's band_counts sum to its total.
//  2. unknown == 0 on both legs.
//  3. Both legs carry a finite mean and stddev, and stddev >= 0; a leg
//     of one row carries stddev exactly 0 (its variance bound is 0, see
//     reportedMomentError).
//  4. The candidate's count is <= the baseline's in every band, and
//     N_x = N_baseline - N_candidate > 0. The excluded population's band
//     counts are the per-band differences; its mean and E[x^2] are
//     (N_baseline*m_baseline - N_candidate*m_candidate)/N_x with
//     m = mean and m = stddev^2 + mean^2.
//  5. The excluded population's variance E[x^2] - mean^2 is >= 0.
//  6. For the baseline leg, the candidate leg and the excluded
//     population, (mean, E[x^2]) passes momentPairRefusal: the mean lies
//     inside [sum(n_b*Low_b), sum(n_b*High_b)]/N, and E[x^2] lies between
//     the least and a greatest E[x^2] that N values with those band
//     counts and that mean can have.
//
// Both E[x^2] bounds are exact. The least is reached with every value
// at clamp(w, Low_b, High_b) for one common level w, since x^2 is convex.
// The greatest is reached at a vertex of the feasible set: every value
// on a band edge except at most one. Moving sum from a value that can
// fall to one that can rise raises the sum of squares whenever the
// rising value is not below the falling one, so at the greatest every
// value at its High exceeds every value at its Low. With contiguous
// bands that leaves one threshold band c: bands below c all at Low,
// bands above c all at High, band c split, and one free value;
// momentGreatestSquares enumerates exactly those vertices.
//
// A stddev is admitted when it lies inside the exact range; a wrong
// stddev that stays inside it (on the real captures, stddevSamp in
// place of stddevPop at N ~ 1000) is admitted too.
//
// Every bound allows exactly the rounding the reported values and this
// shape's own arithmetic can carry (reportedMomentError derives it from
// the operations avgIf and stddevPopIf perform), so a correct pair whose
// exact value sits on a bound -- one excluded row, excluded rows that
// all share one value, every value on one band edge -- is admitted on
// every run.
//
// The baseline leg's check is implied by the candidate's and the
// excluded population's (the least is at most the sum of the parts'
// least, the greatest at least the sum of the parts' greatest); it is
// kept so a baseline body is judged by the same rule.
type BandMomentSubsetShape struct {
	// StatsPath is the dotted path to the evidence_quality_stats object.
	StatsPath string
	// MeanPath and StddevPath are the leaf paths findings carry -- each
	// must equal one of the defect's own Paths entries.
	MeanPath   string
	StddevPath string
}

type bandMomentSubsetPlan struct {
	shape *BandMomentSubsetShape
	valid bool
	// emptyCandidate records that the plan admits a candidate with no
	// rows (rule 0), whose mean/stddev are null by construction.
	emptyCandidate bool
	// refusal names the first rule that failed; empty when valid.
	refusal string
}

// BandMomentSubsetShape refusal reasons.
const (
	momentRefusalUnreadable       = "evidence_quality_stats unreadable"
	momentRefusalBandSum          = "band_counts do not sum to total"
	momentRefusalUnknown          = "unknown band non-zero"
	momentRefusalNullMoment       = "mean or stddev null"
	momentRefusalEmptyCandidate   = "candidate with no rows carries band counts or moments"
	momentRefusalNegativeStddev   = "stddev negative"
	momentRefusalSingleRowStddev  = "stddev of a single row not 0"
	momentRefusalNotSubset        = "candidate band count above baseline"
	momentRefusalNoExcluded       = "no excluded rows"
	momentRefusalExcludedVariance = "excluded variance negative"

	// momentPairRefusal's own reasons, prefixed by the population.
	momentPairEmpty       = "no rows"
	momentPairMeanBelow   = "mean below its band interval"
	momentPairMeanAbove   = "mean above its band interval"
	momentPairSecondBelow = "E[x^2] below the least its mean allows"
	momentPairSecondAbove = "E[x^2] above the greatest its mean allows"

	momentPopulationBaseline  = "baseline: "
	momentPopulationCandidate = "candidate: "
	momentPopulationExcluded  = "excluded: "
)

func buildBandMomentSubsetPlan(shape *BandMomentSubsetShape, baselineData, candidateData any) *bandMomentSubsetPlan {
	plan := &bandMomentSubsetPlan{shape: shape}
	base, okB := readQualityStatsLeg(baselineData, shape.StatsPath)
	cand, okC := readQualityStatsLeg(candidateData, shape.StatsPath)
	if !okB || !okC {
		plan.refusal = momentRefusalUnreadable
		return plan
	}
	// Rule 0: a candidate with no rows.
	if cand.total == 0 {
		if bandSum(cand) != 0 || cand.mean != nil || cand.stddev != nil {
			plan.refusal = momentRefusalEmptyCandidate
			return plan
		}
		if plan.refusal = momentLegRefusal(base); plan.refusal != "" {
			return plan
		}
		baseE2 := *base.stddev**base.stddev + *base.mean**base.mean
		if reason := momentPairRefusal(base.bands, base.total, *base.mean, baseE2, reportedMomentError(base.total)); reason != "" {
			plan.refusal = momentPopulationBaseline + reason
			return plan
		}
		plan.valid, plan.emptyCandidate = true, true
		return plan
	}
	// Rules 1, 2 and 3.
	for _, leg := range []qualityStatsLeg{base, cand} {
		if plan.refusal = momentLegRefusal(leg); plan.refusal != "" {
			return plan
		}
	}
	// Rule 4.
	excluded := make(map[string]int64, len(evidenceQualityBands))
	var nX int64
	for _, band := range evidenceQualityBands {
		diff := base.bands[band.Name] - cand.bands[band.Name]
		if diff < 0 {
			plan.refusal = momentRefusalNotSubset
			return plan
		}
		excluded[band.Name] = diff
		nX += diff
	}
	if nX <= 0 {
		plan.refusal = momentRefusalNoExcluded
		return plan
	}
	baseE2 := *base.stddev**base.stddev + *base.mean**base.mean
	candE2 := *cand.stddev**cand.stddev + *cand.mean**cand.mean
	nB, nC, n := float64(base.total), float64(cand.total), float64(nX)
	sumX := nB**base.mean - nC**cand.mean
	meanX := sumX / n
	e2X := (nB*baseE2 - nC*candE2) / n
	baseErr, candErr := reportedMomentError(base.total), reportedMomentError(cand.total)
	ownX := roundingGamma(4) * (nB + nC) / n
	excludedErr := momentError{
		mean: (nB*baseErr.mean+nC*candErr.mean)/n + ownX,
		e2:   (nB*baseErr.e2+nC*candErr.e2)/n + ownX,
	}
	// Rule 5.
	if e2X-meanX*meanX < -(excludedErr.e2 + 2*excludedErr.mean + 3*unitRoundoff) {
		plan.refusal = momentRefusalExcludedVariance
		return plan
	}
	// Rule 6.
	if reason := momentPairRefusal(base.bands, base.total, *base.mean, baseE2, baseErr); reason != "" {
		plan.refusal = momentPopulationBaseline + reason
		return plan
	}
	if reason := momentPairRefusal(cand.bands, cand.total, *cand.mean, candE2, candErr); reason != "" {
		plan.refusal = momentPopulationCandidate + reason
		return plan
	}
	if reason := momentSumPairRefusal(excluded, nX, new(big.Rat).SetFloat64(sumX), e2X, excludedErr); reason != "" {
		plan.refusal = momentPopulationExcluded + reason
		return plan
	}
	plan.valid = true
	return plan
}

// momentLegRefusal applies rules 1-3 to one leg.
func momentLegRefusal(leg qualityStatsLeg) string {
	switch {
	case bandSum(leg) != leg.total:
		return momentRefusalBandSum
	case leg.bands[evidenceQualityUnknownBand] != 0:
		return momentRefusalUnknown
	case leg.mean == nil || leg.stddev == nil:
		return momentRefusalNullMoment
	case *leg.stddev < 0:
		return momentRefusalNegativeStddev
	case leg.total == 1 && *leg.stddev != 0:
		// reportedMomentError(1).variance is 0: one row's variance is
		// computed exactly, so its stddev is exactly 0.
		return momentRefusalSingleRowStddev
	}
	return ""
}

func bandSum(leg qualityStatsLeg) int64 {
	var sum int64
	for _, name := range evidenceQualityBandCountKeys() {
		sum += leg.bands[name]
	}
	return sum
}

// momentBreaks returns, for k = 0..len(bands), the sum with every band
// below k at its High and every band from k on at its Low: breaks[0] is
// the least sum and breaks[len] the greatest.
func momentBreaks(counts map[string]int64) []float64 {
	breaks := make([]float64, len(evidenceQualityBands)+1)
	for _, band := range evidenceQualityBands {
		breaks[0] += float64(counts[band.Name]) * band.Low
	}
	for k, band := range evidenceQualityBands {
		breaks[k+1] = breaks[k] + float64(counts[band.Name])*(band.High-band.Low)
	}
	return breaks
}

// unitRoundoff is the float64 unit roundoff, 2^-53.
const unitRoundoff = 0x1p-53

// roundingGamma is the standard bound gamma_k = k*u/(1-k*u) on the
// relative error of k chained float64 roundings.
func roundingGamma(k float64) float64 {
	return k * unitRoundoff / (1 - k*unitRoundoff)
}

// momentError bounds the absolute rounding error of a population's
// mean, of its reported variance stddev^2, and of its E[x^2] = stddev^2 +
// mean^2 as this shape reads them.
type momentError struct {
	mean, variance, e2 float64
}

// reportedMomentError bounds the rounding in a reported mean and stddev
// of n values in [0, 1], computed by avgIf and stddevPopIf in float64 --
// the sum and sum-of-squares form, over any merge order of partial
// sums. The bound is stated in the variance domain, where it is derived:
//
//   - avg: a sum of n values in [0, 1] carries at most gamma_(n-1)*n,
//     and the division one more rounding, so the mean is within
//     gamma_(n+1). For n = 1 the sum and the division by 1 are exact: 0.
//   - varPop = sumsq/n - (sum/n)^2: the n squares and their sum carry at
//     most gamma_n*n, so sumsq/n is within gamma_(n+1); (sum/n)^2 is
//     within 2*gamma_(n+1) + u; the subtraction adds u; the square root
//     and this shape's squaring of the stddev add two more roundings of a
//     value at most 1. All of it is within 4*gamma_(n+3). For n = 1 both
//     terms are the same rounded product x*x, so the variance is exactly
//     0: the bound is 0 and the stddev must be exactly 0.
//   - E[x^2] read here as stddev*stddev + mean*mean: the variance error,
//     2*|mean|*(mean error) with |mean| <= 1, and three roundings.
//
// In the stddev domain a variance bound V admits, near variance 0, a
// stddev up to sqrt(V): 0 at n = 1, 6.8e-7 at n = 1029 (V = 4*gamma_1032
// = 4.6e-13), 8.8e-7 at n = 1732 (7.7e-13) -- the size the planes' own
// cancellation reaches (a population of 516 equal values reports stddev
// 5.65e-8). momentPairRefusal's E[x^2] allowance adds the mean's error
// and its own arithmetic to V, so for n >= 2 the stddev it admits near
// variance 0 is up to about 1.3e-6 at n = 1029 and 1.6e-6 at n = 1732;
// for n = 1 rule 3 requires exactly 0.
func reportedMomentError(n int64) momentError {
	if n == 1 {
		return momentError{e2: 3 * unitRoundoff}
	}
	total := float64(n)
	meanErr := roundingGamma(total + 1)
	variance := 4 * roundingGamma(total+3)
	return momentError{mean: meanErr, variance: variance, e2: variance + 2*meanErr + 3*unitRoundoff}
}

// momentPairRefusal returns "" when n values with these per-band counts
// can have this mean and a second moment e2 between the least and the
// greatest the type doc comment of BandMomentSubsetShape states, and the
// failing rule otherwise. evidenceQualityBands is ordered by Low and
// contiguous (qualitystatsrecompute_test.go pins both), which the level
// search and the vertex enumeration below rely on.
func momentPairRefusal(counts map[string]int64, n int64, mean, e2 float64, err momentError) string {
	sum := new(big.Rat).SetFloat64(mean)
	sum.Mul(sum, new(big.Rat).SetInt64(n))
	return momentSumPairRefusal(counts, n, sum, e2, err)
}

// momentSumPairRefusal is momentPairRefusal given the population's sum
// rather than its mean. The mean interval is checked in exact arithmetic
// against the band edges, with only the sum's own error err.mean*n
// allowed, so a one-row mean on a band's excluded edge is refused.
func momentSumPairRefusal(counts map[string]int64, n int64, exactSum *big.Rat, e2 float64, err momentError) string {
	if n <= 0 {
		return momentPairEmpty
	}
	total := float64(n)
	low, high := new(big.Rat), new(big.Rat)
	for _, band := range evidenceQualityBands {
		c := new(big.Rat).SetInt64(counts[band.Name])
		low.Add(low, new(big.Rat).Mul(c, new(big.Rat).SetFloat64(band.Low)))
		high.Add(high, new(big.Rat).Mul(c, new(big.Rat).SetFloat64(band.High)))
	}
	allow := new(big.Rat).Mul(new(big.Rat).SetFloat64(err.mean), new(big.Rat).SetInt64(n))
	if exactSum.Cmp(new(big.Rat).Sub(low, allow)) < 0 {
		return momentPairMeanBelow
	}
	if exactSum.Cmp(new(big.Rat).Add(high, allow)) > 0 {
		return momentPairMeanAbove
	}
	sum, _ := exactSum.Float64()
	// own bounds this function's own rounding on a quantity normalised by
	// n: every sum below adds at most 2n+16 rounded terms of magnitude at
	// most 1 per row.
	own := roundingGamma(2*total + 16)
	sumSlack := total * (err.mean + own)
	// The least and greatest E[x^2] move by at most 2 per unit of mean
	// (their derivative in the mean is twice a value in [0, 1]).
	e2Slack := err.e2 + 2*(err.mean+own) + own
	breaks := momentBreaks(counts)

	// Least second moment: the common level lies in the first non-empty
	// band k whose own break reaches the sum. A sum past the last break
	// (by no more than sumSlack) puts every value at its High.
	level := evidenceQualityBands[len(evidenceQualityBands)-1].High
	for k, band := range evidenceQualityBands {
		c := float64(counts[band.Name])
		if c == 0 || sum > breaks[k+1] {
			continue
		}
		// Clamped per band below, so a level just outside band k from
		// rounding lands on its edge.
		level = band.Low + (sum-breaks[k])/c
		break
	}
	var leastSquares float64
	for _, band := range evidenceQualityBands {
		v := math.Min(math.Max(level, band.Low), band.High)
		leastSquares += float64(counts[band.Name]) * v * v
	}
	if e2 < leastSquares/total-e2Slack {
		return momentPairSecondBelow
	}
	if e2 > momentGreatestSquares(counts, sum, sumSlack)/total+e2Slack {
		return momentPairSecondAbove
	}
	return ""
}

// momentGreatestSquares returns the greatest sum of squares of values
// with these per-band counts and this sum, enumerating the vertices the
// type doc comment of BandMomentSubsetShape derives: for each threshold
// band c and each band f holding the free value, bands below c at Low,
// bands above c at High, k values of band c at High and the rest of it
// at Low, and the free value x = sum - (everything else), which must lie
// inside band f. The sum of squares is convex in k, so only the two
// extreme k that keep x inside band f (and k inside [0, n_c]) are
// evaluated. x may lie up to sumSlack outside band f, the sum's own
// rounding, which the caller's allowance already covers. It returns -Inf
// when no vertex matches the sum.
func momentGreatestSquares(counts map[string]int64, sum, sumSlack float64) float64 {
	// Add this function's own rounding: fixed, rest and the k range each
	// carry at most 2n+16 roundings of terms at most 1 per row, and the
	// planes differ in how they fuse a multiply-add, so an exact vertex
	// can land just outside its band on one architecture.
	var rows float64
	for _, band := range evidenceQualityBands {
		rows += float64(counts[band.Name])
	}
	sumSlack += roundingGamma(2*rows+16) * rows
	best := math.Inf(-1)
	for c, threshold := range evidenceQualityBands {
		for f, free := range evidenceQualityBands {
			n := make([]float64, len(evidenceQualityBands))
			for b, band := range evidenceQualityBands {
				n[b] = float64(counts[band.Name])
			}
			if n[f] == 0 {
				continue
			}
			n[f]--
			var fixed, fixedSquares float64
			for b, band := range evidenceQualityBands {
				edge := band.Low
				if b > c {
					edge = band.High
				}
				fixed += n[b] * edge
				fixedSquares += n[b] * edge * edge
			}
			width := threshold.High - threshold.Low
			rest := sum - fixed
			lo := math.Max(math.Ceil((rest-free.High-sumSlack)/width), 0)
			hi := math.Min(math.Floor((rest-free.Low+sumSlack)/width), n[c])
			for _, k := range []float64{lo, hi} {
				if k < 0 || k > n[c] {
					continue
				}
				x := rest - k*width
				if x < free.Low-sumSlack || x > free.High+sumSlack {
					continue
				}
				squares := fixedSquares + k*(threshold.High*threshold.High-threshold.Low*threshold.Low) + x*x
				best = math.Max(best, squares)
			}
		}
	}
	return best
}

func (p *bandMomentSubsetPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	// An empty-result finding on mean/stddev reaches a valid plan only in
	// the empty-candidate case: any other candidate with a null moment is
	// refused by rule 3.
	path := tieredPath(finding.Path)
	return path == p.shape.MeanPath || path == p.shape.StddevPath
}
