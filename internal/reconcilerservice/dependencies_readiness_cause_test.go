package reconcilerservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
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

// refusalRecord is one "readiness check refused" line of health.Registry's refusal log.
type refusalRecord struct {
	Msg      string `json:"msg"`
	Check    string `json:"check"`
	Cause    string `json:"cause"`
	TimedOut bool   `json:"timed_out"`
}

// refusalSink is the log sink the harness installs on the registry. registry.go's reportRefusals
// writes the refusal line from its own background goroutine after CheckRequired has returned, so
// each completed Write is the event the harness waits for; it needs no timer. A Write stores the
// decoded line and wakes every waiter.
type refusalSink struct {
	mu      sync.Mutex
	records []refusalRecord
	wake    chan struct{}
}

func (s *refusalSink) Write(p []byte) (int, error) {
	var decoded []refusalRecord
	for _, line := range strings.Split(string(p), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record refusalRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			record = refusalRecord{Msg: "undecodable line: " + err.Error()}
		}
		decoded = append(decoded, record)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, decoded...)
	if s.wake != nil {
		close(s.wake)
		s.wake = nil
	}
	return len(p), nil
}

// first returns the first refusal record of check, and the channel a later Write closes.
func (s *refusalSink) first(check string) (refusalRecord, bool, <-chan struct{}, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.records {
		if record.Msg == "readiness check refused" && record.Check == check {
			return record, true, nil, len(s.records)
		}
	}
	if s.wake == nil {
		s.wake = make(chan struct{})
	}
	return refusalRecord{}, false, s.wake, len(s.records)
}

// waitForRefusalRecord returns the first refusal record of check, waiting for a Write that
// brings it. It ends with an error only when ctx ends first; a record that arrived before the end
// is never lost, because the lines are read again after ctx is done.
func waitForRefusalRecord(ctx context.Context, sink *refusalSink, check string) (refusalRecord, error) {
	for {
		record, found, wake, seen := sink.first(check)
		if found {
			return record, nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			if record, found, _, seen = sink.first(check); found {
				return record, nil
			}
			return refusalRecord{}, fmt.Errorf("refusal record never arrived for %s (%d lines seen): %w", check, seen, ctx.Err())
		}
	}
}

// refusalExpected reports whether a refusal line will be written for check: only a check that
// failed is logged.
func refusalExpected(readiness health.Readiness, check string) bool {
	for _, status := range readiness.Checks {
		if status.Name == check {
			return status.Failed
		}
	}
	return false
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
	sink := &refusalSink{}
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(sink, nil)))

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
	readiness := registry.CheckRequired(context.Background())
	if !refusalExpected(readiness, checkName) {
		t.Fatalf("%s did not refuse readiness, so no refusal line is written: %+v", checkName, readiness.Checks)
	}
	// The refusal line is written by the registry's own goroutine (registry.go's reportRefusals)
	// after CheckRequired returned, so wait for that write. The 60 s context only ends the wait of
	// a line that is never written (dropped); no passing run depends on it.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	record, err := waitForRefusalRecord(ctx, sink, checkName)
	if err != nil {
		t.Fatal(err)
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

type ioTimeoutFake struct{}

func (ioTimeoutFake) Error() string   { return "dial tcp 10.0.0.9:5432: i/o timeout secret-host" }
func (ioTimeoutFake) Timeout() bool   { return true }
func (ioTimeoutFake) Temporary() bool { return false }

// CHAOS-11617: an OS socket deadline or net timeout reads cause=timeout, timed_out=true on every check that
// goes through dependencyCheckFailed -- the form a context deadline has. (A raw refused dial is CHAOS-11644.)
func TestReconcilerReadinessRefusalClassifiesIOTimeoutsAsTimeoutAcrossChecks(t *testing.T) {
	for failureName, failure := range map[string]error{
		"os_deadline":    os.ErrDeadlineExceeded,
		"net_op_error":   &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake": ioTimeoutFake{},
	} {
		for checkName, build := range map[string]func(error) *fakeReconcilerDatabase{
			"domain_postgres": func(err error) *fakeReconcilerDatabase { return &fakeReconcilerDatabase{domainErr: err} },
			"queue_postgres":  func(err error) *fakeReconcilerDatabase { return &fakeReconcilerDatabase{queueErr: err} },
			"river_schema":    func(err error) *fakeReconcilerDatabase { return &fakeReconcilerDatabase{schemaErr: err} },
		} {
			t.Run(checkName+"/"+failureName, func(t *testing.T) {
				cause, timedOut := reconcilerReadinessCauseHarness(t, build(failure), checkName)
				if cause != "timeout" || !timedOut {
					t.Fatalf("%s refused with an I/O timeout, got cause=%q timed_out=%v, want cause=timeout timed_out=true", checkName, cause, timedOut)
				}
			})
		}
	}
}
