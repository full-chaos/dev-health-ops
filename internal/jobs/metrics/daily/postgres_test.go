package daily

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestPreBridgeReasonIsAcceptedByPostgresStore is CHAOS-5078 codex round 3's
// direct proof that "pre_bridge_family_incomplete" is actually wired into
// dailyMetricsPartitionFailureReasons, the closed vocabulary
// ReleasePartitionWithReason/FailPartitionPermanently check BEFORE touching
// the database. Mirrors lane-ci-required-to-arc's
// TestPostBridgeReasonIsAcceptedByPostgresStore (#2276), added there after
// codex r1 caught the same class as a P2: a reason string used by Work but
// missing from this map is silently rejected with ErrInvalidState on every
// real production call, invisible to any fakeStore-based test, because
// fakeStore does not enforce this vocabulary at all.
//
// A zero-value &PostgresStore{} has no pool, so store.valid() is false and
// ReleasePartitionWithReason's OWN vocabulary check runs first -- if the
// reason were missing from the map, this would return ErrInvalidState. It
// instead reaches transitionPartition, which fails on the absent pool with
// ErrUnavailable. That specific error (not ErrInvalidState) is the proof the
// vocabulary check passed.
func TestPreBridgeReasonIsAcceptedByPostgresStore(t *testing.T) {
	store := &PostgresStore{}
	err := store.ReleasePartitionWithReason(context.Background(), PartitionClaim{}, "pre_bridge_family_incomplete")
	if errors.Is(err, ErrInvalidState) {
		t.Fatalf("err = %v, want NOT ErrInvalidState -- \"pre_bridge_family_incomplete\" must be registered in dailyMetricsPartitionFailureReasons", err)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable (the zero-value store's own guard, proving the vocabulary check itself passed)", err)
	}
}

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

	if ScheduledFanoutGenerationPrefix != pythonGatePrefix {
		t.Fatalf(
			"ScheduledFanoutGenerationPrefix = %q, but the Python readiness gate "+
				"matches %q; the gate would match no fan-out run (CHAOS-4066)",
			ScheduledFanoutGenerationPrefix, pythonGatePrefix,
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
	scheduled := ScheduledFanoutGenerationPrefix + "2026-08-25T01:00:00Z"

	if !isPostSyncGeneration(postSync) || isScheduledFanoutGeneration(postSync) {
		t.Fatalf("post-sync generation %q: isPostSyncGeneration=%t isScheduledFanoutGeneration=%t, want true/false",
			postSync, isPostSyncGeneration(postSync), isScheduledFanoutGeneration(postSync))
	}
	if !isScheduledFanoutGeneration(scheduled) || isPostSyncGeneration(scheduled) {
		t.Fatalf("scheduled-fanout generation %q: isScheduledFanoutGeneration=%t isPostSyncGeneration=%t, want true/false",
			scheduled, isScheduledFanoutGeneration(scheduled), isPostSyncGeneration(scheduled))
	}
}

// TestManualDailyGenerationPrefixIsDisjointFromScheduledFanoutAndPostSync
// pins that ManualDailyGenerationPrefix (CHAOS-5055) never overlaps the other
// two deferred-discovery generation families. MaterializeScheduledFanout
// accepts all three prefixes for a deferred-discovery run (codex adversarial
// review round 1, P1: a manual `metrics daily-start` with no --repo-id used
// to permanently fail materialization because its generation matched
// neither of the other two) -- if ManualDailyGenerationPrefix ever collided
// with ScheduledFanoutGenerationPrefix or postSyncGenerationPrefix, a manual
// run could be misclassified as one of the other two triggers.
func TestManualDailyGenerationPrefixIsDisjointFromScheduledFanoutAndPostSync(t *testing.T) {
	manual := ManualDailyGenerationPrefix + "8f7004e018318490"
	postSync := postSyncGenerationPrefix + "00000000-0000-4000-8000-000000000001"
	scheduled := ScheduledFanoutGenerationPrefix + "2026-08-25T01:00:00Z"

	if !isManualDailyGeneration(manual) || isScheduledFanoutGeneration(manual) || isPostSyncGeneration(manual) {
		t.Fatalf("manual-daily generation %q: isManualDailyGeneration=%t isScheduledFanoutGeneration=%t isPostSyncGeneration=%t, want true/false/false",
			manual, isManualDailyGeneration(manual), isScheduledFanoutGeneration(manual), isPostSyncGeneration(manual))
	}
	if isManualDailyGeneration(postSync) {
		t.Fatalf("post-sync generation %q must not read as manual-daily", postSync)
	}
	if isManualDailyGeneration(scheduled) {
		t.Fatalf("scheduled-fanout generation %q must not read as manual-daily", scheduled)
	}
}

// TestManualDailyRunGenerationOutputSatisfiesIsManualDailyGeneration proves
// the two halves of the CHAOS-5055 fix agree: ManualDailyRunGeneration (which
// StartManualDailyRun's caller uses to build a real run's generation) must
// always produce a value isManualDailyGeneration -- and therefore
// MaterializeScheduledFanout -- accepts, or a real deferred-discovery manual
// run would still permanently fail at materialization despite this file's
// own predicate tests passing.
func TestManualDailyRunGenerationOutputSatisfiesIsManualDailyGeneration(t *testing.T) {
	generation := ManualDailyRunGeneration("00000000-0000-4000-8000-000000000001", "2026-08-26", nil)
	if !isManualDailyGeneration(generation) {
		t.Fatalf("ManualDailyRunGeneration(...) = %q, which isManualDailyGeneration rejects", generation)
	}
}

// TestEscapeLikePrefixEscapesActualUnderscoresInTheScheduledFanoutPrefix is
// the regression test for codex adversarial review round 3, P1:
// HasSucceededRunForDay's coverage query restricts matches to
// ScheduledFanoutGenerationPrefix/postSyncGenerationPrefix via LIKE, and
// ScheduledFanoutGenerationPrefix itself contains literal underscores
// ("fixed-schedule:daily_metrics_fanout:") -- LIKE's own wildcard for
// "any single character" -- so an unescaped prefix would still match every
// real fixed-schedule generation, but would ALSO silently accept an
// impostor that substituted a different character in either underscore's
// position. This proves the escaped form only matches the literal text.
func TestEscapeLikePrefixEscapesActualUnderscoresInTheScheduledFanoutPrefix(t *testing.T) {
	escaped := escapeLikePrefix(ScheduledFanoutGenerationPrefix)
	if escaped == ScheduledFanoutGenerationPrefix {
		t.Fatalf("escapeLikePrefix did not change a prefix containing literal underscores: %q", escaped)
	}
	if !strings.Contains(escaped, `\_`) {
		t.Fatalf("escapeLikePrefix(%q) = %q, want the literal underscores escaped as \\_", ScheduledFanoutGenerationPrefix, escaped)
	}
	// postSyncGenerationPrefix ("post-sync:") has no wildcard characters, so
	// escaping it is a genuine no-op -- pinning that the function does not
	// spuriously alter a prefix that needs no escaping.
	if got := escapeLikePrefix(postSyncGenerationPrefix); got != postSyncGenerationPrefix {
		t.Fatalf("escapeLikePrefix(%q) = %q, want unchanged (no wildcard characters)", postSyncGenerationPrefix, got)
	}
}
