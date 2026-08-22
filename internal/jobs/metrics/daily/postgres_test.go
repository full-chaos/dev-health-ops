package daily

import "testing"

// The Python recommendations readiness gate selects the authoritative run with
// starts_with(generation, 'fixed-schedule:daily_metrics_fanout:')
// (src/dev_health_ops/workers/recommendations_tasks.py). This constant is the
// store-side half of that contract: isScheduledFanoutGeneration gates which
// runs the scheduled-fanout paths accept at all, so if it and the Python
// prefix ever diverge the gate matches zero rows and silently stops gating
// (CHAOS-4066).
func TestScheduledFanoutPrefixIsThePythonGatePrefix(t *testing.T) {
	const pythonGatePrefix = "fixed-schedule:daily_metrics_fanout:"

	if scheduledFanoutGenerationPrefix != pythonGatePrefix {
		t.Fatalf(
			"scheduledFanoutGenerationPrefix = %q, but the Python readiness gate "+
				"matches %q; the gate would match no fan-out run (CHAOS-4066)",
			scheduledFanoutGenerationPrefix, pythonGatePrefix,
		)
	}
	if !isScheduledFanoutGeneration(pythonGatePrefix + "2026-07-24T01:00:00Z") {
		t.Fatal("the store rejects a generation the Python gate treats as authoritative")
	}
}
