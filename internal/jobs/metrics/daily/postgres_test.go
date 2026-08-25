package daily

import "testing"

// TestDailyRepositoryPartitionSizeEnvKeyIsTheCoordinatedCHAOS4264Contract
// pins the exact env key and default CHAOS-4263 and CHAOS-4264 (bridge-runner
// OOM + ambiguity reaper) agreed on, so both PRs bound job_daily.py's
// per-partition repository count the same way without waiting on each other
// to land first (chris's ruling 2026-08-25). If this key or default drift
// from what CHAOS-4264 actually reads, the two PRs silently stop agreeing.
func TestDailyRepositoryPartitionSizeEnvKeyIsTheCoordinatedCHAOS4264Contract(t *testing.T) {
	const wantKey = "DEV_HEALTH_DAILY_PARTITION_MAX_REPOS"
	if dailyRepositoryPartitionSizeEnvKey != wantKey {
		t.Fatalf("dailyRepositoryPartitionSizeEnvKey = %q, want %q (CHAOS-4264 coordination)",
			dailyRepositoryPartitionSizeEnvKey, wantKey)
	}
	if defaultDailyRepositoryPartitionSize != 3 {
		t.Fatalf("defaultDailyRepositoryPartitionSize = %d, want 3 (chris's ruling 2026-08-25)",
			defaultDailyRepositoryPartitionSize)
	}
}

func TestLoadDailyRepositoryPartitionSizeFallsBackOnUnsetOrInvalid(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "not-a-number"} {
		t.Setenv(dailyRepositoryPartitionSizeEnvKey, value)
		if got := loadDailyRepositoryPartitionSize(); got != defaultDailyRepositoryPartitionSize {
			t.Fatalf("env=%q: loadDailyRepositoryPartitionSize()=%d, want default %d",
				value, got, defaultDailyRepositoryPartitionSize)
		}
	}
	t.Setenv(dailyRepositoryPartitionSizeEnvKey, "7")
	if got := loadDailyRepositoryPartitionSize(); got != 7 {
		t.Fatalf("env=7: loadDailyRepositoryPartitionSize()=%d, want 7", got)
	}
}

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

// TestPostSyncAndScheduledFanoutGenerationPrefixesAreDisjoint pins that the two
// deferred-discovery generation families (CHAOS-4263) never overlap. If they
// ever did, isScheduledFanoutGeneration -- whose exact prefix is the
// CHAOS-4066 Python contract asserted above -- would start matching post-sync
// runs the Python readiness gate was never told about.
func TestPostSyncAndScheduledFanoutGenerationPrefixesAreDisjoint(t *testing.T) {
	postSync := postSyncGenerationPrefix + "00000000-0000-4000-8000-000000000001"
	scheduled := scheduledFanoutGenerationPrefix + "2026-08-25T01:00:00Z"

	if !isPostSyncGeneration(postSync) || isScheduledFanoutGeneration(postSync) {
		t.Fatalf("post-sync generation %q: isPostSyncGeneration=%t isScheduledFanoutGeneration=%t, want true/false",
			postSync, isPostSyncGeneration(postSync), isScheduledFanoutGeneration(postSync))
	}
	if !isScheduledFanoutGeneration(scheduled) || isPostSyncGeneration(scheduled) {
		t.Fatalf("scheduled-fanout generation %q: isScheduledFanoutGeneration=%t isPostSyncGeneration=%t, want true/false",
			scheduled, isScheduledFanoutGeneration(scheduled), isPostSyncGeneration(scheduled))
	}
}
