package schedulerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
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

// The scheduler's pools are not up for its first seconds while /readyz is
// already asked. Before the first Ready answer and inside
// health.ReadinessStartGrace that refusal is a WARN; after the first Ready it
// is the ERROR it always was.
func TestSchedulerReadinessRefusalBeforeFirstReadyIsWarn(t *testing.T) {
	const message = "scheduler readiness dependency check failed"
	var logs bytes.Buffer
	down := errors.New("queue pool not open yet")
	queueErr := down
	registry := health.NewRegistry(readinessTestCheckTimeout)
	if err := registry.RegisterRequired("queue_postgres", wrapSchedulerReadinessCheckWithLogging(
		slog.New(slog.NewJSONHandler(&logs, nil)), "queue_postgres",
		func(context.Context) error { return queueErr },
	)); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	if registry.Readiness(context.Background()).Ready {
		t.Fatal("readiness answered ready with the queue pool down")
	}
	if got := dependencyLineLevel(t, &logs, message); got != "WARN" {
		t.Errorf("refusal at start, never Ready: level %s, want WARN", got)
	}

	queueErr = nil
	if !registry.Readiness(context.Background()).Ready {
		t.Fatal("readiness refused with the queue pool up")
	}
	queueErr = down
	if registry.Readiness(context.Background()).Ready {
		t.Fatal("readiness answered ready with the queue pool down")
	}
	if got := dependencyLineLevel(t, &logs, message); got != "ERROR" {
		t.Errorf("refusal after first Ready: level %s, want ERROR", got)
	}
}
