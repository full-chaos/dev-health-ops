package daily

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The readers ARRAY JOIN the three pooled arrays of a TestOps row, so each sample keeps one element in every array, a
// missing duration, queue or service included.
func TestPooledSampleColumnsHaveEqualLengths(t *testing.T) {
	service, duration, queue := "svc", 10.0, 20.0
	for name, samples := range map[string][]testops.PooledSample{
		"none":  nil,
		"mixed": {{ServiceID: &service, Duration: &duration}, {Queue: &queue}, {ServiceID: &service, Duration: &duration, Queue: &queue}},
	} {
		services, durations, queues := pooledSampleColumns(samples)
		if services == nil || durations == nil || queues == nil {
			t.Fatalf("%s: a nil array (want an empty one)", name)
		}
		if len(services) != len(samples) || len(durations) != len(samples) || len(queues) != len(samples) {
			t.Fatalf("%s: lengths %d/%d/%d, want %d each", name, len(services), len(durations), len(queues), len(samples))
		}
		for i, sample := range samples {
			if services[i] != sample.ServiceID || durations[i] != sample.Duration || queues[i] != sample.Queue {
				t.Fatalf("%s: element %d is not sample %d", name, i, i)
			}
		}
	}
}
