//go:build integration

package synchandoff

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestMintRefusesATriggerThatNamesNoCaller: a Mint with no Caller is refused
// before anything is written (no scheduled job, occurrence or trigger row),
// with one WARN line naming the configuration and one count on
// dev_health_sync_manual_trigger_refused_total{reason="unnamed_caller"}.
func TestMintRefusesATriggerThatNamesNoCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	pool, config := startHandoffDatabase(ctx, t, 4)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	rows := func() (counts [3]int) {
		t.Helper()
		for i, table := range []string{"scheduled_jobs", "scheduled_sync_occurrences", "sync_manual_triggers"} {
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.`+table).Scan(&counts[i]); err != nil {
				t.Fatal(err)
			}
		}
		return counts
	}
	before := rows()
	counted := processRefusalMetrics.value(refusalUnnamedCaller)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, mintErr := Mint(ctx, tx, config, MintInput{Mode: "incremental", TriggeredBy: "manual"}, now)
	if mintErr == nil {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	} else {
		_ = tx.Rollback(ctx)
	}
	if !errors.Is(mintErr, ErrUnnamedCaller) {
		t.Errorf("Mint with no Caller: err %v, want a refusal naming the missing caller", mintErr)
	}
	if after := rows(); after != before {
		t.Errorf("rows (scheduled_jobs, occurrences, triggers) went %v -> %v; a refused mint writes nothing", before, after)
	}
	if got := strings.Count(logs.String(), `"msg":"sync_trigger.refused_unnamed_caller"`); got != 1 ||
		!strings.Contains(logs.String(), `"level":"WARN"`) || !strings.Contains(logs.String(), `"sync_config_id":"`+config.ID+`"`) {
		t.Errorf("refusal log lines = %d, want 1 WARN line naming the configuration:\n%s", got, logs.String())
	}
	if got := processRefusalMetrics.value(refusalUnnamedCaller) - counted; got != 1 {
		t.Errorf("unnamed_caller refusals counted %d, want 1", got)
	}
}
