package daily

import (
	"context"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// pooled_samples_stored of a daily row (migration 161): 0 is a row written without samples (before 161), 1 a row with
// its samples, 2 a row over the cap. The pooled readers read the samples of a row marked 1 and the raw rows of any other.
const (
	pooledSamplesStored  uint8 = 1
	pooledSamplesOverCap uint8 = 2
)

// pooledSamplesCap is the most elements one pooled array of a daily row holds, inclusive.
const pooledSamplesCap = 100_000

// pooledSamplesMaxPerRow is the cap the writers apply. Only a test lowers it (pooled_samples_cap_census_test.go proves
// no non-test file assigns it).
var pooledSamplesMaxPerRow = pooledSamplesCap

// pooledSamplesFit reports whether samples samples fit one row. A row over the cap stores none (never a cut or sampled
// set: its stored values stay over all samples), and the writer logs it.
func pooledSamplesFit(ctx context.Context, family, organizationID, repoID string, day time.Time, samples int) bool {
	if samples <= pooledSamplesMaxPerRow {
		return true
	}
	slog.WarnContext(ctx, "metrics daily: pooled samples over the cap; the row stores none and the pooled readers read its raw rows",
		"family", family, "org_id", organizationID, "repo_id", repoID, "day", day.Format(time.DateOnly),
		"samples", samples, "cap", pooledSamplesMaxPerRow)
	return false
}

// pooledMarker is the pooled_samples_stored value of a row whose samples fit or not.
func pooledMarker(fit bool) uint8 {
	if fit {
		return pooledSamplesStored
	}
	return pooledSamplesOverCap
}

// pooledSampleColumns splits the samples into the parallel pooled_* arrays of one daily row. The three arrays always
// have one element per sample, so the readers' ARRAY JOIN pairs each service with its own samples.
func pooledSampleColumns(samples []testops.PooledSample) (services []*string, durations, queues []*float64) {
	services = make([]*string, 0, len(samples))
	durations = make([]*float64, 0, len(samples))
	queues = make([]*float64, 0, len(samples))
	for _, sample := range samples {
		services = append(services, sample.ServiceID)
		durations = append(durations, sample.Duration)
		queues = append(queues, sample.Queue)
	}
	return services, durations, queues
}

// float64Samples is a non-nil sample array: a row with no sample stores an empty array, never NULL.
func float64Samples(samples []float64) []float64 {
	if samples == nil {
		return []float64{}
	}
	return samples
}
