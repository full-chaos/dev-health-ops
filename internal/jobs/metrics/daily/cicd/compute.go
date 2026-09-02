package cicd

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// pipelineBucket mirrors compute_cicd.py's PipelineBucket TypedDict.
type pipelineBucket struct {
	pipelines int
	success   int
	durations []float64
	queues    []float64
}

func utcDayWindow(day time.Time) (time.Time, time.Time) {
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 0, 1)
}

// percentile mirrors compute_cicd.py's module-level _percentile: linear
// interpolation between closest ranks. This is the SAME formula
// internal/jobs/metrics/daily/repouser/compute.go's percentile implements
// (Python duplicates the identical function across compute.py and
// compute_cicd.py rather than sharing it; this package follows the same
// convention rather than importing repouser's unexported helper).
func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch {
	case pct <= 0:
		return minFloat(values)
	case pct >= 100:
		return maxFloat(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	// float64(...) on rank is load-bearing (CHAOS-4818): sort.Float64s above
	// can push the compiler to rematerialize rank after the call instead of
	// reusing the already-rounded value, fusing that recomputation with the
	// next statement's subtraction (frac := rank - float64(lo)) into one
	// FNMSUBD on arm64 -- fusion "across statements", not just within one.
	rank := float64(float64(len(sorted)-1) * (pct / 100.0))
	lo := int(rank)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	// float64(...) around each product prevents Go from fusing this into one
	// FMA on arm64, which would round differently than CPython's
	// compute_cicd._percentile (CHAOS-4818).
	return float64(sorted[lo]*(1-frac)) + float64(sorted[hi]*frac)
}

func minFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}

func maxFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func ptr(value float64) *float64 { return &value }

// successStatuses mirrors compute_cicd.py's literal set:
// {"success", "succeeded", "passed"}.
var successStatuses = map[string]bool{"success": true, "succeeded": true, "passed": true}

// ComputeCICDMetricsDaily mirrors compute_cicd_metrics_daily (compute_cicd.py:42).
//
// day is the target UTC calendar day (time-of-day/location ignored, matching
// Python's date). pipelineRuns is expected to already be filtered by
// LoadPipelineRuns' finished_at window (see the package doc comment on the
// DOUBLE WINDOW FILTER) -- this function re-applies the started_at filter
// in-process, exactly as compute_cicd_metrics_daily does, regardless of
// whether the caller pre-filtered by started_at too.
func ComputeCICDMetricsDaily(day time.Time, pipelineRuns []PipelineRunRow, computedAt time.Time) []CICDMetric {
	start, end := utcDayWindow(day)
	computedAtUTC := computedAt.UTC()

	byRepo := map[string]*pipelineBucket{}
	order := make([]string, 0)
	for _, row := range pipelineRuns {
		startedAt := row.StartedAt.UTC()
		if startedAt.Before(start) || !startedAt.Before(end) {
			continue
		}
		repoKey := row.RepoID.String()
		bucket, ok := byRepo[repoKey]
		if !ok {
			bucket = &pipelineBucket{}
			byRepo[repoKey] = bucket
			order = append(order, repoKey)
		}

		bucket.pipelines++
		status := strings.ToLower(strings.TrimSpace(row.Status))
		if successStatuses[status] {
			bucket.success++
		}

		if row.FinishedAt != nil {
			durationMin := row.FinishedAt.UTC().Sub(startedAt).Minutes()
			if durationMin >= 0 {
				bucket.durations = append(bucket.durations, durationMin)
			}
		}

		if row.QueuedAt != nil {
			queueMin := startedAt.Sub(row.QueuedAt.UTC()).Minutes()
			if queueMin >= 0 {
				bucket.queues = append(bucket.queues, queueMin)
			}
		}
	}

	// sorted(by_repo.items(), key=lambda kv: kv[0]) -- ascending by repo_id
	// string.
	sort.Strings(order)

	records := make([]CICDMetric, 0, len(order))
	for _, repoKey := range order {
		bucket := byRepo[repoKey]
		repoID := uuid.MustParse(repoKey)
		pipelines := bucket.pipelines
		success := bucket.success
		successRate := 0.0
		if pipelines > 0 {
			successRate = float64(success) / float64(pipelines)
		}

		var avgDuration, p90Duration, avgQueue *float64
		if len(bucket.durations) > 0 {
			avgDuration = ptr(mean(bucket.durations))
			p90Duration = ptr(percentile(bucket.durations, 90.0))
		}
		if len(bucket.queues) > 0 {
			avgQueue = ptr(mean(bucket.queues))
		}

		records = append(records, CICDMetric{
			RepoID:             repoID,
			Day:                day,
			PipelinesCount:     pipelines,
			SuccessRate:        successRate,
			AvgDurationMinutes: avgDuration,
			P90DurationMinutes: p90Duration,
			AvgQueueMinutes:    avgQueue,
			ComputedAt:         computedAtUTC,
		})
	}
	return records
}
