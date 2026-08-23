package numerical

import (
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical/cpyrandom"
)

// Capacity's Monte Carlo forecast, ported from compute_capacity.py.
//
// Unlike the other kernels in this package, this one is STOCHASTIC: its output
// is a function of the generation seed as well as the input rows. It is
// nevertheless exactly comparable against Python, because cpyrandom reproduces
// CPython's stream rather than substituting Go's own generator. Without that,
// no fixture could make a row-for-row comparison pass.
//
// # Behaviours reproduced deliberately
//
// Three of these look like defects and are not; a tidier port would diverge.
//
//  1. EACH SIMULATION RESEEDS. monte_carlo_forecast_days and
//     monte_carlo_forecast_items each call random.seed(seed) themselves
//     (compute_capacity.py:166,216), so when a forecast runs BOTH modes the
//     second simulation restarts the identical stream rather than continuing
//     it. Sharing one generator across both -- the obvious Go structure --
//     silently produces different numbers for the second mode.
//
//  2. THE ITEMS PERCENTILES ARE FLIPPED ON PURPOSE. Python asks for
//     [50, 15, 5] and assigns them to p50/p85/p95 (compute_capacity.py:342),
//     because for ITEMS a low percentile is the conservative answer while for
//     DAYS a high one is. Reading [50, 85, 95] there is the natural mistake
//     and inverts the meaning of every p85_items and p95_items row.
//
//  3. THE TWO MODES ARE INDEPENDENT, NOT EXCLUSIVE. Python uses two separate
//     `if` statements, so a call carrying both target_items and target_date
//     runs both simulations and fills both sets of columns. An `else` here
//     would silently drop half the output for such a call.
//
// The early returns also matter: target_items <= 0 and days_available <= 0
// return before seeding, so the generator is neither seeded nor consumed on
// those paths.
type Throughput struct {
	DailyThroughputs []int
	// DaysOfHistory is the SAMPLE count, which Python takes from the sample
	// list rather than from the throughput list. They are equal in practice,
	// but the distinction is the producer's, not this port's, to collapse.
	DaysOfHistory int
}

// ForecastRequest mirrors forecast_capacity's arguments.
type ForecastRequest struct {
	History     Throughput
	TargetItems *int
	TargetDate  *time.Time
	BacklogSize int
	Simulations int
	Seed        int64
	// Today mirrors Python's `today` parameter, which exists so a caller can
	// pin the forecast horizon (compute_capacity.py:282, citing CHAOS-2400's
	// UTC-midnight flip). The WORKER path does not use it -- Python's job
	// leaves it None and takes the wall clock, so the Go worker does too, and
	// injecting here would be the divergence. The seam is kept so the API
	// surfaces match and so tests can pin the horizon.
	Today *time.Time
}

// ForecastResult mirrors the ForecastResult dataclass's computed fields.
// forecast_id and computed_at are NOT here: both are volatile per run on both
// sides, and belong to the executor rather than the kernel.
type ForecastResult struct {
	P50Days, P85Days, P95Days    *int
	P50Date, P85Date, P95Date    *time.Time
	P50Items, P85Items, P95Items *int
	ThroughputMean               float64
	ThroughputStddev             float64
	HistoryDays                  int
	SimulationCount              int
	InsufficientHistory          bool
	HighVariance                 bool
}

// ErrNoForecastTarget and ErrEmptyHistory mirror Python's two ValueErrors.
var (
	ErrNoForecastTarget = errors.New("must provide either target items or a target date")
	ErrEmptyHistory     = errors.New("cannot forecast with empty throughput history")
)

const (
	sufficientHistoryDays   = 14
	highVarianceCVThreshold = 1.5
)

// ForecastCapacity ports forecast_capacity.
func ForecastCapacity(request ForecastRequest, today time.Time) (ForecastResult, error) {
	if request.TargetItems == nil && request.TargetDate == nil {
		return ForecastResult{}, ErrNoForecastTarget
	}
	if len(request.History.DailyThroughputs) == 0 {
		return ForecastResult{}, ErrEmptyHistory
	}
	if request.Today != nil {
		today = *request.Today
	}
	// Python's `today` is a DATE (now.date(), compute_capacity.py:304), and
	// every use of it below is calendar arithmetic against another date. The
	// worker hands this function a TIMESTAMP, so it is truncated here exactly
	// once.
	//
	// Skipping this is not a rounding nicety, it silently loses a day:
	// days_available was computed as (target - now).Hours()/24, so a job
	// running at 14:00 UTC with a target of TOMORROW got 10/24 -> 0 and
	// forecast zero items. Every fixed-date run outside midnight was affected.
	today = startOfUTCDay(today)

	statistics := ThroughputStatistics(request.History.DailyThroughputs)
	coefficientOfVariation := 0.0
	if statistics.Mean != 0 {
		coefficientOfVariation = statistics.Stddev / statistics.Mean
	}

	result := ForecastResult{
		ThroughputMean:      statistics.Mean,
		ThroughputStddev:    statistics.Stddev,
		HistoryDays:         request.History.DaysOfHistory,
		SimulationCount:     request.Simulations,
		InsufficientHistory: request.History.DaysOfHistory < sufficientHistoryDays,
		HighVariance:        coefficientOfVariation > highVarianceCVThreshold,
	}

	// Fixed-scope: "when will we finish N items?"
	if request.TargetItems != nil {
		days, err := MonteCarloForecastDays(
			request.History.DailyThroughputs, *request.TargetItems,
			request.Simulations, request.Seed,
		)
		if err != nil {
			return ForecastResult{}, err
		}
		quantiles := IntegerPercentiles(days, []float64{50, 85, 95})
		result.P50Days, result.P85Days, result.P95Days =
			&quantiles[0], &quantiles[1], &quantiles[2]

		p50Date := AddDays(today, quantiles[0])
		p85Date := AddDays(today, quantiles[1])
		p95Date := AddDays(today, quantiles[2])
		result.P50Date, result.P85Date, result.P95Date = &p50Date, &p85Date, &p95Date
	}

	// Fixed-date: "how many items by date X?" -- a separate `if`, not an else.
	if request.TargetDate != nil {
		// Both operands are start-of-day UTC, so this is the exact calendar-day
		// count Python's date subtraction yields.
		daysAvailable := int(startOfUTCDay(*request.TargetDate).Sub(today).Hours() / 24)
		zero := 0
		if daysAvailable > 0 {
			items, err := MonteCarloForecastItems(
				request.History.DailyThroughputs, daysAvailable,
				request.Simulations, request.Seed,
			)
			if err != nil {
				return ForecastResult{}, err
			}
			// [50, 15, 5] -- see (2) above.
			quantiles := IntegerPercentiles(items, []float64{50, 15, 5})
			result.P50Items, result.P85Items, result.P95Items =
				&quantiles[0], &quantiles[1], &quantiles[2]
		} else {
			result.P50Items, result.P85Items, result.P95Items = &zero, &zero, &zero
		}
	}
	return result, nil
}

// MonteCarloForecastDays ports monte_carlo_forecast_days.
func MonteCarloForecastDays(
	throughputHistory []int, targetItems, simulations int, seed int64,
) ([]int, error) {
	if len(throughputHistory) == 0 {
		return nil, ErrEmptyHistory
	}
	// Returns BEFORE seeding, exactly as Python does, so the generator is
	// untouched on this path.
	if targetItems <= 0 {
		return make([]int, simulations), nil
	}
	const maxDays = 365
	source := cpyrandom.New(seed)
	completionDays := make([]int, 0, simulations)
	for simulation := 0; simulation < simulations; simulation++ {
		remaining := targetItems
		days := 0
		for remaining > 0 && days < maxDays {
			index, err := source.Choice(len(throughputHistory))
			if err != nil {
				return nil, err
			}
			remaining -= throughputHistory[index]
			days++
		}
		completionDays = append(completionDays, days)
	}
	return completionDays, nil
}

// MonteCarloForecastItems ports monte_carlo_forecast_items.
func MonteCarloForecastItems(
	throughputHistory []int, daysAvailable, simulations int, seed int64,
) ([]int, error) {
	if len(throughputHistory) == 0 {
		return nil, ErrEmptyHistory
	}
	if daysAvailable <= 0 {
		return make([]int, simulations), nil
	}
	source := cpyrandom.New(seed)
	itemsCompleted := make([]int, 0, simulations)
	for simulation := 0; simulation < simulations; simulation++ {
		total := 0
		for day := 0; day < daysAvailable; day++ {
			index, err := source.Choice(len(throughputHistory))
			if err != nil {
				return nil, err
			}
			total += throughputHistory[index]
		}
		itemsCompleted = append(itemsCompleted, total)
	}
	return itemsCompleted, nil
}

// AddDays mirrors `today + timedelta(days=n)` on a calendar date.
//
// AddDate rather than adding 24-hour durations: a duration walks absolute time
// and lands an hour early or late across a DST transition, while Python's
// timedelta on a date object walks CALENDAR days. The parity fixture cannot
// reach that difference -- these are UTC dates, where the two agree -- so it is
// a case the comparison would not catch and the unit vectors must.
func AddDays(day time.Time, days int) time.Time {
	return startOfUTCDay(day).AddDate(0, 0, days)
}

// startOfUTCDay truncates a timestamp to the calendar day Python's
// datetime.now(UTC).date() would have produced.
func startOfUTCDay(moment time.Time) time.Time {
	utc := moment.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}
