//go:build integration

package synchandoff

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// awaitCounts is the process-wide _count of sync_manual_trigger_await_latency_seconds and the
// outcome counter, per outcome, read from the exposition the api registry serves.
func awaitCounts(t *testing.T) (outcome map[string]string, latency map[string]string) {
	t.Helper()
	var b strings.Builder
	if err := processAwaitMetrics.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	outcome, latency = map[string]string{}, map[string]string{}
	for _, line := range strings.Split(b.String(), "\n") {
		switch {
		case strings.HasPrefix(line, `sync_manual_trigger_await_outcome_total{outcome="`):
			rest := strings.TrimPrefix(line, `sync_manual_trigger_await_outcome_total{outcome="`)
			name, value, _ := strings.Cut(rest, `"} `)
			outcome[name] = value
		case strings.HasPrefix(line, `sync_manual_trigger_await_latency_seconds_count{outcome="`):
			rest := strings.TrimPrefix(line, `sync_manual_trigger_await_latency_seconds_count{outcome="`)
			name, value, _ := strings.Cut(rest, `"} `)
			latency[name] = value
		}
	}
	return outcome, latency
}

// CHAOS-8268: Wait is the one place the Python await counters are taken, for every caller (the manual
// Sync Now and Backfill routes among them). Each terminal state of a REAL occurrence row moves the
// counter of ITS outcome label by exactly one, in both the outcome counter and the latency histogram,
// and no other label moves: a quarantined occurrence is not "pending", a never-materialized one is not
// "materialized" (the three outcomes Python records, execution_trigger.py _record).
func TestWaitCountsEachTerminalOutcomeUnderItsOwnLabel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, config := startHandoffDatabase(ctx, t, 4)

	var jobID string
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var err error
		jobID, _, err = ensureScheduledJob(ctx, tx, config, time.Now().UTC())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	const runID = "00000000-0000-4000-8000-0000000000c1"
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: "org-1", Status: "success", TotalUnits: 3, CompletedUnits: 3})
	const jobRunID = "00000000-0000-4000-8000-0000000000c2"
	if _, err := pool.Exec(ctx, `
INSERT INTO public.job_runs (id, job_id, status, triggered_by, created_at)
VALUES ($1::uuid, $2::uuid, 0, 'scheduler', now())`, jobRunID, jobID); err != nil {
		t.Fatal(err)
	}
	seed := func(occurrenceID, status, errorCode string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
    (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at,
     reconcile_status, reconcile_error_code, reconcile_error_at, sync_run_id, job_run_id)
VALUES ($1, 1, 'org-1', $2::uuid, $3::uuid, now(), now(), $4::text, NULLIF($5::text, ''), CASE WHEN $5::text <> '' THEN now() END,
        CASE WHEN $4::text = 'completed' THEN $6::uuid END, CASE WHEN $4::text = 'completed' THEN $7::uuid END)`,
			occurrenceID, config.ID, jobID, status, errorCode, runID, jobRunID); err != nil {
			t.Fatal(err)
		}
	}
	seed("occ-materialized", "completed", "")
	seed("occ-quarantined", "quarantined", "planner_error")
	seed("occ-pending", "pending", "")

	cases := []struct {
		occurrence string
		wantState  State
		wantLabel  string
	}{
		{"occ-materialized", StateMaterialized, "materialized"},
		{"occ-quarantined", StateQuarantined, "quarantined"},
		{"occ-pending", StatePending, "pending"},
	}
	for _, c := range cases {
		beforeOutcome, beforeLatency := awaitCounts(t)
		got, err := Wait(ctx, pool, c.occurrence, 300*time.Millisecond, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("%s: %v", c.occurrence, err)
		}
		if got.State != c.wantState {
			t.Fatalf("%s: state = %d, want %d", c.occurrence, got.State, c.wantState)
		}
		afterOutcome, afterLatency := awaitCounts(t)
		for _, label := range awaitOutcomeNames {
			wantMoved := label == c.wantLabel
			if moved := afterOutcome[label] != beforeOutcome[label]; moved != wantMoved {
				t.Errorf("%s: outcome counter %q moved=%v (%q -> %q), want moved=%v", c.occurrence, label, moved, beforeOutcome[label], afterOutcome[label], wantMoved)
			}
			if moved := afterLatency[label] != beforeLatency[label]; moved != wantMoved {
				t.Errorf("%s: latency count %q moved=%v (%q -> %q), want moved=%v", c.occurrence, label, moved, beforeLatency[label], afterLatency[label], wantMoved)
			}
		}
		if afterOutcome[c.wantLabel] != afterLatency[c.wantLabel] {
			t.Errorf("%s: outcome counter %q != latency count %q for label %s", c.occurrence, afterOutcome[c.wantLabel], afterLatency[c.wantLabel], c.wantLabel)
		}
	}

	// An error return records nothing (Python records none on its error paths either).
	beforeOutcome, _ := awaitCounts(t)
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := Wait(cancelled, pool, "occ-pending", time.Minute, time.Minute); err == nil {
		t.Fatal("a cancelled Wait returned no error")
	}
	if afterOutcome, _ := awaitCounts(t); afterOutcome["pending"] != beforeOutcome["pending"] {
		t.Errorf("an error return moved the pending counter: %q -> %q", beforeOutcome["pending"], afterOutcome["pending"])
	}
}
