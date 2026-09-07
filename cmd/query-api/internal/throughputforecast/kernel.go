// Package throughputforecast is the Go port of
// dev_health_ops.api.graphql.resolvers.forecast.resolve_throughput_forecast
// and the model it calls, dev_health_ops.metrics.forecast
// (CHAOS-5349, the query-api half of the capacity/forecast cutover).
//
// This file is the KERNEL -- the arithmetic, ported from metrics/forecast.py.
// clickhouse.go holds the seven reads, and resolve.go maps the result onto the
// GraphQL model. The split matters: the kernel is pure and is pinned
// bit-for-bit against live Python by tests/fixtures/throughput_forecast_golden.json,
// captured BEFORE the Python is deleted, while the reads can only be checked
// against a real ClickHouse.
//
// # How this differs from the capacity kernel next door
//
// internal/jobs/metrics/numerical's capacity forecast is a Monte Carlo and
// needs an exact CPython MT19937 port to be comparable at all. This one is
// DETERMINISTIC: no RNG, no numpy, no scipy -- math.floor/ceil,
// statistics.fmean and stdlib sorted, and nothing else. A plain input->output
// table is therefore a complete oracle.
//
// # Behaviours reproduced deliberately
//
// Each of these looks like a defect and is not; a tidier port diverges.
//
//  1. A PARTIAL WINDOW EMITS NO ESTIMATE, NOT A SCALED ONE.
//     rolling_weekly_throughput returns (mean 0.0, no samples, insufficient)
//     when it holds fewer than window_weeks*7 daily rows. The predecessor
//     `total / max(weeks, 1.0)` ignored window_weeks and produced the SAME
//     number for 4w/8w/12w, three identical figures masquerading as a
//     confident forecast (CHAOS-2574). Averaging whatever is on hand
//     reintroduces exactly that.
//
//  2. THE PERCENTILE BANDS ARE INVERTED, ON PURPOSE. p50 reads the 0.50
//     quantile, p75 reads 0.25 and p90 reads 0.10 -- a HIGHER confidence band
//     reads a LOWER throughput, because "finish within N weeks with 90%
//     confidence" means assuming throughput no better than the slowest tenth.
//     Reading 0.75/0.90 there is the natural mistake and silently inverts
//     every p75Weeks and p90Weeks the UI shows.
//
//  3. THIS PERCENTILE IS NOT compute_capacity's. Here it is
//     `lo + (hi - lo) * fraction`, float-valued; the capacity kernel's is
//     `int(lo*(1-f) + hi*f)`, truncating. The two round differently, and both
//     are FMA-fusable on arm64 -- see percentile's own comment.
//
//  4. insufficient_history IS PROVENANCE, NOT VOLUME. It reports "the estimate
//     did not come from the window you asked for", so a history_weeks outside
//     (4, 8, 12) sets it even with years of data behind it, and 84 days of
//     dailies sets it for a 12-week request because 12 weeks of dailies yields
//     exactly ONE rolling sample. It is computed from the REQUESTED window,
//     never from the one selection fell back to.
//
//  5. THE PRIMARY RISK IS max() OVER NORMALISED SCORES, FIRST WINNER ON A TIE.
//     Overlays measure ratios, hours and counts; only value/threshold makes
//     them comparable. Python's max() keeps the FIRST maximum, so the
//     iteration order (wip, review, incident) is part of the contract.
package throughputforecast

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// Thresholds and window sizes, mirroring metrics/forecast.py's module
// constants one for one.
const (
	minDaysPerWeek            = 7
	wipCongestionThreshold    = 1.25
	reviewBottleneckThreshold = 48.0
	incidentLoadThreshold     = 10.0

	// minSamplesForEstimate is the floor below which a rolling distribution is
	// treated as unusable. One sample is not a distribution: every percentile
	// of it is that same value, which would render as a confident forecast
	// derived from a single week.
	minSamplesForEstimate = 2
)

// rollingWindowsWeeks mirrors ROLLING_WINDOWS_WEEKS. The ORDER is load-bearing
// twice over: selection falls back to `windows[-1]` (the last, longest entry)
// when nothing matches, and the GraphQL rollingWindows list is rendered in this
// order.
var rollingWindowsWeeks = [...]int{4, 8, 12}

// riskKind values mirror the RiskKind str-enum. They are the exact strings the
// wire carries, so they are declared as constants rather than derived.
const (
	riskKindWIP      = "wip"
	riskKindReview   = "review"
	riskKindIncident = "incident_load"
	riskKindNone     = "none"
)

// Labels, verbatim from _risk_overlay's call sites and the neutral fallback.
const (
	labelWIP      = "WIP congestion"
	labelReview   = "Review bottleneck"
	labelIncident = "Incident load"
	labelNone     = "No elevated risk"
)

// rollingWindow mirrors RollingWindowThroughput.
type rollingWindow struct {
	windowWeeks int
	// meanWeeklyThroughput is fmean(samples), which is fsum(samples)/n -- an
	// EXACTLY rounded sum, not a running one. See fsum.
	meanWeeklyThroughput float64
	samples              []float64
	insufficientHistory  bool
}

// riskOverlay mirrors RiskOverlay.
type riskOverlay struct {
	kind      string
	score     float64
	label     string
	value     float64
	threshold float64
	active    bool
}

// forecastResult mirrors ThroughputForecastResult minus forecast_id and
// computed_at, which are volatile per call on both sides (uuid4 and the wall
// clock) and belong to the resolver rather than the kernel -- the same split
// numerical.ForecastResult makes for the capacity family.
type forecastResult struct {
	teamID              *string
	workScopeID         *string
	backlogSize         int
	historyWeeks        int
	p50Weeks            *int
	p75Weeks            *int
	p90Weeks            *int
	rollingWindows      []rollingWindow
	primaryRisk         riskOverlay
	wipCongestion       riskOverlay
	reviewBottleneck    riskOverlay
	incidentLoad        riskOverlay
	insufficientHistory bool
}

// errNonMonotonicWeeks mirrors _assert_monotonic_weeks's ValueError.
//
// Reproduced rather than softened: it is a LIVE 500 path today, so a port that
// clamped or reordered instead would turn an operator-visible failure into a
// silently different answer -- and the invariant it guards (a higher confidence
// band never finishes sooner) is the one thing a reader trusts about this
// output.
var errNonMonotonicWeeks = errors.New("forecast weeks must be monotonic")

// fsum is math.fsum: an exactly-rounded sum, via Shewchuk's partial-sum
// algorithm -- the same one CPython's implementation uses.
//
// Not a nicety. statistics.fmean is `fsum(data) / n`, so the mean of a rolling
// distribution is computed from a sum with ONE rounding at the end, while a
// running `total += value` loop rounds at every step. On the sawtooth case in
// the golden corpus the two disagree in the last ulp, and ceil() downstream can
// turn that into a whole week. (CPython's builtin sum() has been
// Neumaier-compensated since 3.12, but fmean does not call it -- it calls fsum,
// which is exact, not merely compensated.)
func fsum(values []float64) float64 {
	// partials holds a set of non-overlapping partial sums whose exact total is
	// the exact total of everything consumed so far. Each new value is folded
	// into them by repeated two-sum, which is error-free for IEEE-754 doubles.
	partials := make([]float64, 0, 8)
	for _, value := range values {
		index := 0
		for _, partial := range partials {
			current, other := value, partial
			if math.Abs(current) < math.Abs(other) {
				current, other = other, current
			}
			total := current + other
			// The exact residual of the addition above. Representable whenever
			// the addition itself did not overflow, which is what makes the
			// whole scheme exact.
			residual := other - (total - current)
			if residual != 0 {
				partials = partials[:index+1]
				partials[index] = residual
				index++
			}
			value = total
		}
		partials = append(partials[:index], value)
	}
	total := 0.0
	for _, partial := range partials {
		total += partial
	}
	return total
}

// fmean ports statistics.fmean for the non-weighted sequence case.
func fmean(values []float64) float64 {
	if len(values) == 0 {
		// Python raises StatisticsError here, but no call site can reach it:
		// rolling_weekly_throughput only calls fmean once it has established
		// that at least one rolling sample exists. Returning 0 rather than
		// panicking keeps that guarantee from becoming a crash if it ever
		// stops holding.
		return 0
	}
	return fsum(values) / float64(len(values))
}

// percentile ports metrics/forecast.py's _percentile.
//
// The interpolation is written with the products wrapped in explicit float64
// conversions, which is load-bearing on arm64 (CHAOS-4818, the same discipline
// numerical.IntegerPercentiles documents). Go's spec permits fusing `x*y + z`
// into a single fused-multiply-add -- one rounding -- where CPython always
// rounds the multiply and the add separately, and the spec allows that fusion
// to reach ACROSS statements, so `rank` needs the same guard as the
// interpolation itself.
//
// The consequence here is not cosmetic: every percentile feeds
// weeksToComplete, which applies math.Ceil, so a last-ulp difference is either
// invisible or a whole week -- never anything in between.
//
// NOTE the argument scale. This percentile takes a FRACTION in [0, 1];
// compute_capacity's takes 0..100. Handing this one 50 asks for the maximum.
func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered) == 1 {
		return ordered[0]
	}
	rank := float64(float64(len(ordered)-1) * fraction)
	lower := math.Floor(rank)
	upper := math.Ceil(rank)
	if lower == upper {
		return ordered[int(lower)]
	}
	remainder := rank - lower
	low, high := ordered[int(lower)], ordered[int(upper)]
	return low + float64((high-low)*remainder)
}

// rollingWeeklyThroughput ports rolling_weekly_throughput.
func rollingWeeklyThroughput(dailyThroughputs []int, windowWeeks int) (rollingWindow, error) {
	if windowWeeks <= 0 {
		return rollingWindow{}, fmt.Errorf("window_weeks must be positive")
	}

	// max(0, int(sample.items_completed)) per row. A negative daily value --
	// which a corrected ClickHouse aggregate can produce -- is clamped, not
	// subtracted, so one bad row cannot pull a window's total below the work
	// that genuinely completed in it.
	throughputs := make([]int, 0, len(dailyThroughputs))
	for _, value := range dailyThroughputs {
		throughputs = append(throughputs, max(0, value))
	}

	windowDays := windowWeeks * minDaysPerWeek
	if len(throughputs) == 0 || len(throughputs) < windowDays {
		// The no-estimate contract -- see this package's doc comment, item 1.
		// Both branches are Python's, kept separate there and merged here only
		// because they return the identical value.
		return rollingWindow{
			windowWeeks:         windowWeeks,
			samples:             nil,
			insufficientHistory: true,
		}, nil
	}

	samples := make([]float64, 0, len(throughputs)-windowDays+1)
	for index := 0; index+windowDays <= len(throughputs); index++ {
		// Python sums the INTEGER slice and only then divides, so the numerator
		// is exact regardless of how large the window is. Accumulating in
		// float64 instead would round every partial sum.
		total := 0
		for _, value := range throughputs[index : index+windowDays] {
			total += value
		}
		samples = append(samples, float64(total)/float64(windowWeeks))
	}

	return rollingWindow{
		windowWeeks:          windowWeeks,
		meanWeeklyThroughput: fmean(samples),
		samples:              samples,
		insufficientHistory:  len(samples) < minSamplesForEstimate,
	}, nil
}

// computeRollingWindows ports compute_rolling_windows over the standard sizes.
func computeRollingWindows(dailyThroughputs []int) ([]rollingWindow, error) {
	windows := make([]rollingWindow, 0, len(rollingWindowsWeeks))
	for _, weeks := range rollingWindowsWeeks {
		window, err := rollingWeeklyThroughput(dailyThroughputs, weeks)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, nil
}

// selectPercentileDistribution ports _select_percentile_distribution.
//
// Returns nil -- not an empty non-nil slice, though nothing downstream can tell
// them apart -- when no window has enough samples, which is what makes every
// percentile 0.0 and therefore every weeks value nil for a positive backlog.
func selectPercentileDistribution(windows []rollingWindow, historyWeeks int) []float64 {
	if len(windows) == 0 {
		return nil
	}

	selected := windows[len(windows)-1]
	for _, window := range windows {
		if window.windowWeeks == historyWeeks {
			selected = window
			break
		}
	}
	if len(selected.samples) >= minSamplesForEstimate {
		return selected.samples
	}

	// Fall back to the LONGEST strictly-shorter window that has enough samples.
	// Longest, because a longer window smooths more; strictly shorter, because
	// a longer one would have at least as few samples by construction.
	best := -1
	var bestSamples []float64
	for _, window := range windows {
		if window.windowWeeks >= selected.windowWeeks {
			continue
		}
		if len(window.samples) < minSamplesForEstimate {
			continue
		}
		if window.windowWeeks > best {
			best, bestSamples = window.windowWeeks, window.samples
		}
	}
	return bestSamples
}

// weeksToComplete ports _weeks_to_complete.
//
// Three distinct zero-ish answers, and they mean different things: 0 for an
// empty backlog (nothing to do), nil for a non-positive throughput (cannot
// say), and a floor of 1 for anything that would finish inside a week (a week
// is the unit; there is no "zero weeks" that means "soon").
func weeksToComplete(backlogSize int, weeklyThroughput float64) *int {
	if backlogSize <= 0 {
		zero := 0
		return &zero
	}
	if weeklyThroughput <= 0 {
		return nil
	}
	weeks := max(1, int(math.Ceil(float64(backlogSize)/weeklyThroughput)))
	return &weeks
}

// assertMonotonicWeeks ports _assert_monotonic_weeks, including its skip when
// any band is nil.
func assertMonotonicWeeks(p50, p75, p90 *int) error {
	if p50 == nil || p75 == nil || p90 == nil {
		return nil
	}
	if *p50 <= *p75 && *p75 <= *p90 {
		return nil
	}
	return fmt.Errorf("%w: P50=%d, P75=%d, P90=%d", errNonMonotonicWeeks, *p50, *p75, *p90)
}

// newRiskOverlay ports _risk_overlay.
func newRiskOverlay(kind string, value, threshold float64, label string) riskOverlay {
	score := 0.0
	if threshold > 0 {
		score = value / threshold
	}
	return riskOverlay{
		kind:      kind,
		score:     score,
		label:     label,
		value:     value,
		threshold: threshold,
		// `>=`, not `>`. A value sitting exactly on its threshold is ACTIVE
		// with a score of exactly 1.0 -- the boundary the operator configured
		// is included in the alert, not excluded from it.
		active: value >= threshold,
	}
}

// computeRiskOverlays ports compute_risk_overlays, returning
// (primary, wip, review, incident) in Python's own tuple order.
func computeRiskOverlays(currentWIP, averageWIP, reviewLatencyHours, incidentCount float64) (
	riskOverlay, riskOverlay, riskOverlay, riskOverlay,
) {
	// Guarded division: an average of zero yields a ratio of 0.0, not +Inf.
	// Without the guard an org with no WIP history has an infinite score and
	// is therefore ALWAYS the primary risk -- the loudest possible signal from
	// the least possible data.
	wipRatio := 0.0
	if averageWIP > 0 {
		wipRatio = currentWIP / averageWIP
	}

	wip := newRiskOverlay(riskKindWIP, wipRatio, wipCongestionThreshold, labelWIP)
	review := newRiskOverlay(riskKindReview, reviewLatencyHours, reviewBottleneckThreshold, labelReview)
	incident := newRiskOverlay(riskKindIncident, incidentCount, incidentLoadThreshold, labelIncident)

	// Python's max() over the active overlays keeps the FIRST maximum, so the
	// (wip, review, incident) order decides ties. `>` rather than `>=` below is
	// what reproduces that: a later overlay with an equal score does not
	// displace an earlier one.
	primary := riskOverlay{kind: riskKindNone, label: labelNone}
	found := false
	for _, overlay := range []riskOverlay{wip, review, incident} {
		if !overlay.active {
			continue
		}
		if !found || overlay.score > primary.score {
			primary, found = overlay, true
		}
	}
	return primary, wip, review, incident
}

// forecastThroughputCapacity ports forecast_throughput_capacity.
func forecastThroughputCapacity(
	dailyThroughputs []int,
	backlogSize int,
	teamID, workScopeID *string,
	historyWeeks int,
	currentWIP, averageWIP, reviewLatencyHours, incidentCount float64,
) (forecastResult, error) {
	if backlogSize < 0 {
		return forecastResult{}, fmt.Errorf("backlog_size must be non-negative")
	}
	if historyWeeks <= 0 {
		return forecastResult{}, fmt.Errorf("history_weeks must be positive")
	}

	windows, err := computeRollingWindows(dailyThroughputs)
	if err != nil {
		return forecastResult{}, err
	}
	distribution := selectPercentileDistribution(windows, historyWeeks)

	// Provenance, computed from the REQUESTED window rather than the selected
	// one -- see this package's doc comment, item 4. Two independent reasons
	// set it: the request named a window size that does not exist, or the
	// window it named has too few rolling samples and selection fell back.
	windowMatched := false
	requested := windows[len(windows)-1]
	for _, window := range windows {
		if window.windowWeeks == historyWeeks {
			windowMatched, requested = true, window
			break
		}
	}
	insufficientHistory := !windowMatched || len(requested.samples) < minSamplesForEstimate

	// 0.50 / 0.25 / 0.10 -- the inversion is deliberate, see item 2.
	p50Weeks := weeksToComplete(backlogSize, percentile(distribution, 0.50))
	p75Weeks := weeksToComplete(backlogSize, percentile(distribution, 0.25))
	p90Weeks := weeksToComplete(backlogSize, percentile(distribution, 0.10))
	if err := assertMonotonicWeeks(p50Weeks, p75Weeks, p90Weeks); err != nil {
		return forecastResult{}, err
	}

	primary, wip, review, incident := computeRiskOverlays(
		currentWIP, averageWIP, reviewLatencyHours, incidentCount,
	)
	return forecastResult{
		teamID:              teamID,
		workScopeID:         workScopeID,
		backlogSize:         backlogSize,
		historyWeeks:        historyWeeks,
		p50Weeks:            p50Weeks,
		p75Weeks:            p75Weeks,
		p90Weeks:            p90Weeks,
		rollingWindows:      windows,
		primaryRisk:         primary,
		wipCongestion:       wip,
		reviewBottleneck:    review,
		incidentLoad:        incident,
		insufficientHistory: insufficientHistory,
	}, nil
}
