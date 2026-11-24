//go:build integration

package pgmigrate_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// holdShareLock opens a transaction that holds SHARE on sync_run_units: it conflicts with the SHARE UPDATE EXCLUSIVE of
// the table-lock probe, so every attempt fails at stage "probe" after LockTimeout and the retry path runs.
func holdShareLock(t *testing.T, uri string) (release func()) {
	t.Helper()
	conn := connect(t, uri)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.sync_run_units IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(release)
	return release
}

type retryWindowVerb struct {
	revision string
	build    func(context.Context, *pgx.Conn, pgmigrate.IndexBuildOptions, *slog.Logger) (pgmigrate.IndexBuildResult, error)
}

func retryWindowVerbs() map[string]retryWindowVerb {
	return map[string]retryWindowVerb{
		"failure-streak": {revision: "0159", build: pgmigrate.BuildFailureStreakIndex},
		"open-units":     {revision: "0161", build: pgmigrate.BuildOpenUnitsIndex},
	}
}

func retryLines(lines []map[string]any) []map[string]any {
	var out []map[string]any
	for _, l := range linesWith(lines, "build-index: existing index") {
		if l["when"] == "retry" {
			out = append(out, l)
		}
	}
	return out
}

// A retry pause longer than the rest of the build window ends with the window: the run stops after attempt 1, with no
// further state read, probe or attempt after the wake, and says the window ended.
func TestBuildIndexRetryPauseEndsWithTheBuildWindow(t *testing.T) {
	for name, verb := range retryWindowVerbs() {
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.withoutIndex(t, verb.revision)
			release := holdShareLock(t, uri)
			conn := connect(t, uri)
			logs := &syncBuffer{}
			// A failure-only guard: the window is 3 s and the cleanup budget 3 s; a pause of 30 min that does not end with
			// the window never returns.
			result, err := runBuildGuarded(t, 60*time.Second, "the retry pause must end with the build window", release,
				func(ctx context.Context) (pgmigrate.IndexBuildResult, error) {
					return verb.build(ctx, conn, pgmigrate.IndexBuildOptions{
						LockTimeout: time.Second, Attempts: 3, RetryDelay: 30 * time.Minute, Deadline: 3 * time.Second, CleanupBudget: 3 * time.Second,
					}, slog.New(slog.NewJSONHandler(logs, nil)))
				})
			if err == nil || !strings.Contains(err.Error(), "the build window ended") {
				t.Fatalf("err = %v, want the window end; log %s", err, logs.String())
			}
			if result.Attempts != 1 {
				t.Fatalf("attempts = %d, want 1; log %s", result.Attempts, logs.String())
			}
			lines := logLines(t, logs.String())
			failed := linesWith(lines, "build-index: attempt failed")
			if len(failed) != 1 || failed[0]["stage"] != "probe" {
				t.Fatalf("failed attempts %v, want one at stage probe; log %s", failed, logs.String())
			}
			if got := retryLines(lines); len(got) != 0 {
				t.Fatalf("state read after the wake: %v", got)
			}
			ended := linesWith(lines, "build-index: the build window ended; no further attempt")
			if len(ended) != 1 || ended[0]["attempts"] != float64(1) {
				t.Fatalf("window-end lines %v, want one with attempts 1; log %s", ended, logs.String())
			}
		})
	}
}

// RetryDelay 0 still retries: every attempt runs within the window, each after a state read.
func TestBuildIndexRetriesWithZeroRetryDelay(t *testing.T) {
	for name, verb := range retryWindowVerbs() {
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.withoutIndex(t, verb.revision)
			release := holdShareLock(t, uri)
			conn := connect(t, uri)
			logs := &syncBuffer{}
			result, err := runBuildGuarded(t, 90*time.Second, "three probe failures must end", release,
				func(ctx context.Context) (pgmigrate.IndexBuildResult, error) {
					return verb.build(ctx, conn, pgmigrate.IndexBuildOptions{
						LockTimeout: time.Second, Attempts: 3, RetryDelay: 0, Deadline: 60 * time.Second, CleanupBudget: 3 * time.Second,
					}, slog.New(slog.NewJSONHandler(logs, nil)))
				})
			if err == nil || strings.Contains(err.Error(), "the build window ended") {
				t.Fatalf("err = %v, want the probe failure of the last attempt; log %s", err, logs.String())
			}
			if result.Attempts != 3 {
				t.Fatalf("attempts = %d, want 3; log %s", result.Attempts, logs.String())
			}
			lines := logLines(t, logs.String())
			failed := linesWith(lines, "build-index: attempt failed")
			if len(failed) != 3 {
				t.Fatalf("%d failed attempts, want 3; log %s", len(failed), logs.String())
			}
			for _, l := range failed {
				if l["stage"] != "probe" {
					t.Fatalf("failed attempt %v, want stage probe", l)
				}
			}
			retries := retryLines(lines)
			if len(retries) != 2 || retries[0]["attempt"] != float64(2) || retries[1]["attempt"] != float64(3) {
				t.Fatalf("retry lines %v, want attempts 2 and 3; log %s", retries, logs.String())
			}
			if got := linesWith(lines, "build-index: the build window ended; no further attempt"); len(got) != 0 {
				t.Fatalf("window-end line inside the window: %v", got)
			}
		})
	}
}

// RetryDelay 0 retries at once while the window has time left: with the blocker gone after attempt 1, attempt 2 builds
// a valid index.
func TestBuildIndexZeroRetryDelayBuildsOnTheNextAttempt(t *testing.T) {
	for name, verb := range retryWindowVerbs() {
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.withoutIndex(t, verb.revision)
			release := holdShareLock(t, uri)
			conn := connect(t, uri)
			logs := &syncBuffer{}
			go func() {
				for !strings.Contains(logs.String(), "build-index: attempt failed") {
					time.Sleep(10 * time.Millisecond)
				}
				release()
			}()
			result, err := runBuildGuarded(t, 90*time.Second, "the second attempt must build the index", release,
				func(ctx context.Context) (pgmigrate.IndexBuildResult, error) {
					return verb.build(ctx, conn, pgmigrate.IndexBuildOptions{
						LockTimeout: 5 * time.Second, Attempts: 2, RetryDelay: 0, Deadline: time.Minute, CleanupBudget: 10 * time.Second,
					}, slog.New(slog.NewJSONHandler(logs, nil)))
				})
			if err != nil || !result.Valid || result.Attempts != 2 {
				t.Fatalf("result %+v err %v, want a valid index on attempt 2; log %s", result, err, logs.String())
			}
		})
	}
}
