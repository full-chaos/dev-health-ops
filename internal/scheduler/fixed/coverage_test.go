package fixed

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for depth := 0; depth < 8; depth++ {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	t.Fatal("could not locate the repository root from the test working directory")
	return ""
}

func TestLegacyInventoryReplacementsAreNotSilentlyDropped(t *testing.T) {
	// Every non-schedule owner must name a concrete replacement component. A
	// legacy entry whose owner is "removed" is the only case where no runtime
	// component takes over, and it still has to say what makes the removal
	// safe.
	for _, entry := range LegacyBeatInventory() {
		switch entry.Owner {
		case OwnerFixedSchedule:
			continue
		case OwnerRemoved, OwnerProductScheduler, OwnerReconciler,
			OwnerStreamRunner, OwnerRuntimeTelemetry:
			if strings.TrimSpace(entry.Note) == "" {
				t.Errorf("legacy entry %s is replaced by %s without a reason", entry.Name, entry.Owner)
			}
			if strings.TrimSpace(entry.OwnerRef) == "" {
				t.Errorf("legacy entry %s names no replacement component", entry.Name)
			}
		default:
			t.Errorf("legacy entry %s has unknown owner %q", entry.Name, entry.Owner)
		}
	}
}

// Retiring a Beat row is distinct from merely omitting it from the current
// inventory. The current inventory mirrors the schedules the Go engine owns; this
// ledger records the reviewed absence of each retired Beat entry.
func TestRetiredBeatEntriesAreExplained(t *testing.T) {
	for _, retired := range RetiredBeatInventory() {
		// CHAOS-3093 (PR2b, r1 codex review finding): a non-empty check alone
		// lets a reason be mutated to a short placeholder (e.g. "MUTATED:")
		// without failing. Every real reviewed reason/evidence in this file
		// is at minimum a short cited-ticket sentence (the shortest today,
		// "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).", is 57
		// characters) -- a minimum length comfortably below that catches a
		// placeholder without pinning exact wording, which would make this
		// test brittle against routine rewording.
		const minReviewedTextLength = 40
		if len(strings.TrimSpace(retired.Reason)) < minReviewedTextLength {
			t.Errorf("retired beat entry %q has no reviewed reason (or one too short to be a real one)", retired.Name)
		}
		if len(strings.TrimSpace(retired.Evidence)) < minReviewedTextLength {
			t.Errorf("retired beat entry %q has no reviewed evidence (or one too short to be real)", retired.Name)
		}
	}
}

// CHAOS-3093 (PR2b, r1 codex review finding): TestRetiredBeatEntriesAreExplained
// only checks that each listed retired entry carries a reason and evidence -- a
// mutation that RENAMES a retired entry's Name (e.g. "prune-rate-limit-
// observations" -> a typo) still passes it trivially. This pins the exact,
// reviewed name set so a rename, an accidental add, or an accidental removal
// all fail here.
func TestRetiredBeatEntriesNamesArePinnedExactly(t *testing.T) {
	expected := []string{
		"ask-dev-retention-sweep",
		"consume-pending-scheduled-sync-occurrences",
		"dispatch-go-external-ingest-recompute-bridge",
		"dispatch-scheduled-metrics",
		"dispatch-scheduled-reports",
		"dispatch-scheduled-syncs",
		"external-ingest-stream-health",
		"monitor-queue-depths",
		"phone-home-heartbeat",
		"process-external-ingest-streams",
		"process-ingest-streams",
		"process-product-telemetry-streams",
		"prune-external-ingest-batches",
		"prune-rate-limit-observations",
		"reconcile-sync-dispatch",
		"run-capacity-forecast",
		"run-complexity-daily",
		"run-daily-metrics",
		"run-membership-backfill-daily",
		"run-recommendations",
		"run-release-impact-daily",
	}
	sort.Strings(expected)

	actual := make([]string, 0, len(RetiredBeatInventory()))
	seen := make(map[string]struct{}, len(RetiredBeatInventory()))
	for _, retired := range RetiredBeatInventory() {
		if _, duplicate := seen[retired.Name]; duplicate {
			t.Fatalf("retired beat entry %q is listed more than once", retired.Name)
		}
		seen[retired.Name] = struct{}{}
		actual = append(actual, retired.Name)
	}
	sort.Strings(actual)

	if len(actual) != len(expected) {
		t.Fatalf("RetiredBeatInventory() has %d entries, want %d -- a retired "+
			"beat entry was added, removed, or renamed without updating this "+
			"pin\ngot:  %v\nwant: %v",
			len(actual), len(expected), actual, expected)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("RetiredBeatInventory() name mismatch at sorted index %d: "+
				"got %q, want %q -- a retired beat entry was renamed without "+
				"updating this pin",
				i, actual[i], expected[i])
		}
	}
}

// Sync coverage is durable product state, not runtime telemetry. Its Python
// Beat entry and task are deleted only after the native schedule and producer
// are constructed, so a Go-only stack can create cold projections and rebuild
// invalidated ones without restoring a second writer.
func TestSyncCoverageRefreshHasAConstructedFixedScheduleOwner(t *testing.T) {
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range schedules {
		if schedule.ID == "sync_coverage_refresh" && schedule.Native &&
			schedule.LegacyBeatEntry == "" &&
			schedule.Cadence.Fingerprint() == EveryInterval(300*time.Second).Fingerprint() {
			producers, producerErr := NewProducerSet(NewSyncCoverageRefreshProducer())
			if producerErr != nil {
				t.Fatal(producerErr)
			}
			if _, ok := producers.Producer(schedule.ProducerID); !ok {
				t.Fatalf("sync coverage producer %q is not constructed", schedule.ProducerID)
			}
			return
		}
	}
	t.Fatal("native sync coverage schedule is not constructed")
}

// The missed-run policy is a reviewed decision per schedule, not a default.
// Pinning it here means flipping a safety net to skip, or making telemetry
// catch up, has to be an explicit edit to this table.
func TestScheduleCoveragePinsTheMissedRunPolicy(t *testing.T) {
	want := map[string]CatchUpPolicy{
		"scheduled_reports_dispatch":         CatchUpSkip,
		"phone_home_heartbeat":               CatchUpSkip,
		"prune_rate_limit_observations":      CatchUpSkip,
		"prune_external_ingest_batches":      CatchUpSkip,
		"prune_ask_dev_conversations":        CatchUpSkip,
		"prune_worker_job_terminal":          CatchUpSkip,
		"daily_metrics_fanout":               CatchUpBounded,
		"complexity_daily_fanout":            CatchUpBounded,
		"release_impact_daily_fanout":        CatchUpBounded,
		"recommendations_daily_fanout":       CatchUpBounded,
		"dora_daily_fanout":                  CatchUpBounded,
		"membership_backfill_daily_fanout":   CatchUpBounded,
		"capacity_forecast_weekly_fanout":    CatchUpBounded,
		"sync_coverage_refresh":              CatchUpSkip,
		"dimension_fold":                     CatchUpSkip,
		"llm_budget_stale_reservations":      CatchUpSkip,
		"work_item_attribution_daily_fanout": CatchUpBounded,
	}
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	if len(schedules) != len(want) {
		t.Fatalf("%d schedules declared but %d policies pinned", len(schedules), len(want))
	}
	for _, schedule := range schedules {
		expected, ok := want[schedule.ID]
		if !ok {
			t.Errorf("schedule %s has no pinned missed-run policy", schedule.ID)
			continue
		}
		if schedule.CatchUp != expected {
			t.Errorf(
				"schedule %s missed-run policy changed to %q (pinned %q)",
				schedule.ID, schedule.CatchUp, expected,
			)
		}
	}
}
