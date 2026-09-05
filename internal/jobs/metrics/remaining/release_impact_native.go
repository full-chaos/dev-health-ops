// Package remaining: native release_impact compute (CHAOS-4296).
//
// Pure port of src/dev_health_ops/metrics/release_impact.py's arithmetic, with
// no ClickHouse dependency -- callers own loading rows and writing records, the
// same split capacity_native.go and dora_native.go use.
//
// A NOTE ON THE PYTHON MODULE DOCSTRING, deliberately NOT reproduced: it claims
// "Append-only: new rows with newer computed_at win via argMax". No reader
// anywhere implements that -- every reference to release_impact_daily was
// checked (query-api analytics, ff_validation, GraphQL flag metrics,
// workerctl) and none argMaxes or FINALs on computed_at. The claim has been
// aspirational since it was written, which is consistent with the table never
// having had a generation column. Migration 088 makes the intent structural by
// moving the table to ReplacingMergeTree(computed_at); this doc records that
// the ORIGINAL claim was never true, so a future reader does not cite it as
// evidence that dedup already worked.
package remaining

import "math"

// Thresholds and weights, ported verbatim from release_impact.py:24-32.
const (
	MinSessionsFriction = 300
	MinEventsError      = 1000
	BaselineWindowDays  = 7
	PostWindowHours     = 24
	SpikeDetectionHours = 72

	wCoverage = 0.35 // PRD §release_impact_confidence_score weights
	wSample   = 0.35
	wConfound = 0.30
)

// SignalRate is one (rate, sessions) reading over a window, mirroring
// _signal_rate's tuple return. Rate is nil where Python returns None.
type SignalRate struct {
	Rate     *float64
	Sessions int
}

// NewSignalRate applies _signal_rate's exact null semantics (release_impact.py:200-253):
// no rows -> (None, 0); zero sessions -> (None, 0); otherwise signals/sessions.
//
// The zero-sessions branch returns sessions=0 even though totalSessions was
// read as 0 anyway -- kept explicit because the caller SUMS sessions across
// windows, so "no rows" and "rows summing to zero sessions" must both
// contribute 0 rather than one of them contributing a garbage count.
func NewSignalRate(hadRows bool, totalSignals, totalSessions int) SignalRate {
	if !hadRows || totalSessions == 0 {
		return SignalRate{Rate: nil, Sessions: 0}
	}
	r := float64(totalSignals) / float64(totalSessions)
	return SignalRate{Rate: &r, Sessions: totalSessions}
}

// ComputeDelta ports _compute_delta (release_impact.py:254-299).
//
// Returns (delta, postRate). Both are nil where Python returns None. Note the
// ORDER of the two guards is load-bearing and matches Python: the
// sample-size guard is checked BEFORE the pre-rate guard, so a partition with
// too few sessions returns postRate even when preRate is also unusable. Swapping
// them would still return (nil, postRate) here, but would diverge the moment a
// future edit made the branches differ.
func ComputeDelta(pre, post SignalRate, minSessions int) (delta *float64, postRate *float64) {
	postRate = post.Rate

	if pre.Sessions+post.Sessions < minSessions {
		return nil, postRate
	}
	if pre.Rate == nil || *pre.Rate == 0.0 {
		return nil, postRate
	}
	if post.Rate == nil {
		return nil, postRate
	}
	d := (*post.Rate - *pre.Rate) / *pre.Rate
	return &d, postRate
}

// ComputeConfidence ports _compute_confidence (release_impact.py:436-456).
//
// FMA BARRIER, load-bearing (CHAOS-4818 class). The Python is a plain
// three-term weighted sum. On arm64 Go's compiler FUSES `a*b + c*d` into FMA
// instructions, which round ONCE instead of twice, so the Go result can differ
// from CPython's in the last ULP -- a bit-exact parity oracle then fails on a
// difference that is not a porting error. Each product is forced through a
// float64() conversion barrier so the multiply rounds before the add, matching
// CPython's evaluation exactly. Do not "simplify" these away.
func ComputeConfidence(coverageRatio float64, totalSessions, concurrentDeploys, minSessions int) float64 {
	sampleScore := 1.0
	if minSessions > 0 {
		sampleScore = math.Min(float64(totalSessions)/float64(minSessions), 1.0)
	}
	confoundScore := 1.0 / (1.0 + float64(concurrentDeploys))

	a := float64(wCoverage * coverageRatio)
	b := float64(wSample * sampleScore)
	c := float64(wConfound * confoundScore)
	score := float64(float64(a+b) + c)

	return math.Max(0.0, math.Min(1.0, score))
}

// DataCompleteness ports _data_completeness's scaling (release_impact.py:433-434):
// distinct hourly buckets over the expected 24, clamped at 1.0.
func DataCompleteness(bucketHours int) float64 {
	return math.Min(float64(bucketHours)/24.0, 1.0)
}

// CoverageRatio ports _compute_day's ratio (release_impact.py:98-102):
// releases WITH telemetry over total releases deployed that day; 0.0 when the
// denominator is zero, matching Python's explicit guard rather than producing NaN.
func CoverageRatio(releasesWithTelemetry, totalReleasesOnDay int) float64 {
	if totalReleasesOnDay <= 0 {
		return 0.0
	}
	return float64(releasesWithTelemetry) / float64(totalReleasesOnDay)
}

// MissingRequiredFields ports the `missing` count (release_impact.py:538-543):
// how many of the four delta/rate fields came back None.
func MissingRequiredFields(frictionDelta, postFrictionRate, errorDelta, postErrorRate *float64) uint32 {
	var n uint32
	for _, v := range []*float64{frictionDelta, postFrictionRate, errorDelta, postErrorRate} {
		if v == nil {
			n++
		}
	}
	return n
}
