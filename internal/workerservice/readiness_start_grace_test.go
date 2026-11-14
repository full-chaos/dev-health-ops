package workerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// dependencyLineLevel returns the level of the one dependency line in logs.
func dependencyLineLevel(t *testing.T, logs *bytes.Buffer, message string) string {
	t.Helper()
	var levels []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &record) == nil && record.Msg == message {
			levels = append(levels, record.Level)
		}
	}
	if len(levels) != 1 {
		t.Fatalf("%d %q lines, want 1: %s", len(levels), message, logs.String())
	}
	logs.Reset()
	return levels[0]
}

// A worker pod's queue pool is not up for its first seconds; the preclaim
// path (CheckRequired before the gate opens) refuses meanwhile. Before the
// first Ready answer and inside health.ReadinessStartGrace that refusal is a
// WARN; after the first Ready it is the ERROR it always was.
func TestWorkerReadinessRefusalBeforeFirstReadyIsWarn(t *testing.T) {
	const message = "worker readiness dependency check failed"
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{"refused_dial", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
		{"error_text", errors.New("queue role posture refused")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			database := &fakeWorkerDatabase{queueErr: testCase.err}
			dependencies := &workerDependencies{database: database, logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			registry := health.NewRegistry(readinessTestCheckTimeout)
			if err := registry.RegisterRequired("queue_postgres", dependencies.queueReady); err != nil {
				t.Fatal(err)
			}

			if registry.CheckRequired(context.Background()).Ready {
				t.Fatal("preclaim answered ready with the queue pool down")
			}
			if got := dependencyLineLevel(t, &logs, message); got != "WARN" {
				t.Errorf("preclaim refusal at start, never Ready: level %s, want WARN", got)
			}

			database.queueErr = nil
			registry.SetReady(true)
			if !registry.Readiness(context.Background()).Ready {
				t.Fatal("readiness refused with the queue pool up")
			}
			database.queueErr = testCase.err
			if registry.Readiness(context.Background()).Ready {
				t.Fatal("readiness answered ready with the queue pool down")
			}
			if got := dependencyLineLevel(t, &logs, message); got != "ERROR" {
				t.Errorf("refusal after first Ready: level %s, want ERROR", got)
			}
		})
	}
}
