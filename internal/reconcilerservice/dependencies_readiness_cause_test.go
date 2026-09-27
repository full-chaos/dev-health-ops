package reconcilerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// syncLogBuffer guards a bytes.Buffer with a mutex: registry.go's reportRefusals writes the
// refusal-log record from its own background goroutine while this harness polls and reads it from
// the test goroutine, so a bare bytes.Buffer is a real, -race-detected data race (caught by CI's
// go-quality-leg race leg, not a review finding).
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// CHAOS-6949. health.Registry logs a bounded "readiness check refused" line (CHAOS-6883,
// registry.go's reportRefusals) naming each failing check's Cause and TimedOut -- wired for every
// service through internal/platform/shell.go's shared bootstrap (SetRefusalLogger). The reconciler's
// dependency check wrappers (domainReady, queueReady, coordinatorReady, riverSchemaReady,
// postureManifestLockstepReady, domainTransactionReady) used to log the real underlying error via
// logDependencyCheckFailure, then discard it and return the flat errReconcilerDependencyUnavailable
// sentinel -- so the registry, which classifies a check by what the check FUNCTION returns, never
// saw the real cause. A dependency that is merely slow (its own query ran into a busy pool's
// deadline) was therefore indistinguishable, in this wired, already-observable log line, from one
// that is genuinely broken (a real posture mismatch): both always reported cause=error,
// timed_out=false. 6934's r1 named this as a separate finding; this pins the fix (dependencyCheckFailed).
//
// reconcilerReadinessCauseHarness builds the real production wiring (configureReconcilerDependencies...)
// against a fakeReconcilerDatabase, with a real registry and refusal logger, and reads back one
// check's refusal-log record.
func reconcilerReadinessCauseHarness(t *testing.T, database *fakeReconcilerDatabase, checkName string) (cause string, timedOut bool) {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(readinessTestCheckTimeout)
	logs := &syncLogBuffer{}
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(logs, nil)))

	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	if _, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(), config.Config{RiverDatabaseSchema: "river"}, registry,
		reconcilerTestLogger(), sources,
	); err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	registry.CheckRequired(context.Background()) // triggers the first-refusal log line
	// The refusal log is written by a bounded background goroutine
	// (registry.go's reportRefusals); poll briefly for it rather than
	// reaching for its unexported flush (health-package-only, a test tool).
	deadline := time.Now().Add(2 * time.Second)
	for logs.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	var record struct {
		Msg      string `json:"msg"`
		Check    string `json:"check"`
		Cause    string `json:"cause"`
		TimedOut bool   `json:"timed_out"`
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if record.Msg == "readiness check refused" && record.Check == checkName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no 'readiness check refused' line for %s: %s", checkName, logs.String())
	}
	return record.Cause, record.TimedOut
}

func TestReconcilerReadinessRefusalKeepsTheDeadlineCause(t *testing.T) {
	cause, timedOut := reconcilerReadinessCauseHarness(t, &fakeReconcilerDatabase{domainErr: context.DeadlineExceeded}, "domain_postgres")
	if cause != "timeout" || !timedOut {
		t.Fatalf("domain_postgres refused with the underlying check returning context.DeadlineExceeded, got cause=%q timed_out=%v, want cause=timeout timed_out=true", cause, timedOut)
	}
}

// A genuine, non-timeout failure (a real posture mismatch, never merely slow) must still report
// cause=error, timed_out=false: the fix must not over-classify every failure as retryable.
func TestReconcilerReadinessRefusalKeepsAGenuineErrorAsNonTimeout(t *testing.T) {
	cause, timedOut := reconcilerReadinessCauseHarness(t, &fakeReconcilerDatabase{queueErr: errors.New("role posture refused")}, "queue_postgres")
	if cause != "error" || timedOut {
		t.Fatalf("queue_postgres refused with a genuine error, got cause=%q timed_out=%v, want cause=error timed_out=false", cause, timedOut)
	}
}

func TestReconcilerReadinessRefusalKeepsTheDeadlineCauseAcrossEveryCheck(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() (*fakeReconcilerDatabase, string)
	}{
		{"coordinator_postgres", func() (*fakeReconcilerDatabase, string) {
			return &fakeReconcilerDatabase{coordinatorErr: context.DeadlineExceeded}, "coordinator_postgres"
		}},
		{"river_schema", func() (*fakeReconcilerDatabase, string) {
			return &fakeReconcilerDatabase{schemaErr: context.DeadlineExceeded}, "river_schema"
		}},
		{"posture_manifest_lockstep", func() (*fakeReconcilerDatabase, string) {
			return &fakeReconcilerDatabase{postureLockstepErr: context.DeadlineExceeded}, "posture_manifest_lockstep"
		}},
		{"queue_postgres", func() (*fakeReconcilerDatabase, string) {
			return &fakeReconcilerDatabase{queueErr: context.DeadlineExceeded}, "queue_postgres"
		}},
		{"domain_transaction", func() (*fakeReconcilerDatabase, string) {
			return &fakeReconcilerDatabase{domainTransactionErr: context.DeadlineExceeded}, "domain_transaction"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, checkName := tc.build()
			cause, timedOut := reconcilerReadinessCauseHarness(t, database, checkName)
			if cause != "timeout" || !timedOut {
				t.Fatalf("%s refused with context.DeadlineExceeded, got cause=%q timed_out=%v, want cause=timeout timed_out=true", checkName, cause, timedOut)
			}
		})
	}
}

// dependencyCheckFailed must keep errors.Is(_, errReconcilerDependencyUnavailable) true: every
// existing caller (e.g. TestReconcilerRegistryReadinessIsExplicitAndValueFree) still branches on it.
func TestDependencyCheckFailedIsStillTheReconcilerDependencySentinel(t *testing.T) {
	err := dependencyCheckFailed(context.DeadlineExceeded)
	if !errors.Is(err, errReconcilerDependencyUnavailable) {
		t.Fatalf("dependencyCheckFailed(...) = %v, want errors.Is(_, errReconcilerDependencyUnavailable)", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dependencyCheckFailed(...) = %v, want errors.Is(_, context.DeadlineExceeded)", err)
	}
}
