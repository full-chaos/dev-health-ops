// Package loadharness drives a fixed-rate HTTP load against a dev stack and
// reports per-path latency percentiles and error rate.
package loadharness

import (
	"math"
	"sort"
	"time"
)

// Summary is the latency and error summary of one path.
type Summary struct {
	Count     int
	Errors    int
	Dropped   int
	ErrorRate float64
	P50       time.Duration
	P95       time.Duration
	P99       time.Duration
	Max       time.Duration
}

// Summarize computes nearest-rank percentiles over latencies (one entry per
// completed request, failed ones included). Errors is the number of completed
// requests that failed; dropped requests never ran and count as failures in
// the rate. The input slice is not modified.
func Summarize(latencies []time.Duration, errors, dropped int) Summary {
	s := Summary{Count: len(latencies), Errors: errors, Dropped: dropped}
	if total := s.Count + dropped; total > 0 {
		s.ErrorRate = float64(errors+dropped) / float64(total)
	}
	if s.Count == 0 {
		return s
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	s.P50 = nearestRank(sorted, 0.50)
	s.P95 = nearestRank(sorted, 0.95)
	s.P99 = nearestRank(sorted, 0.99)
	s.Max = sorted[len(sorted)-1]
	return s
}

func nearestRank(sorted []time.Duration, p float64) time.Duration {
	rank := int(math.Ceil(p * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}
