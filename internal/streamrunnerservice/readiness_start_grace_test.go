package streamrunnerservice

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

// A stream runner's dependencies are not up for its first seconds while
// /readyz is already asked. Before the first Ready answer and inside
// health.ReadinessStartGrace both of its dependency lines are a WARN; after
// the first Ready they are the ERROR they always were.
func TestStreamRunnerReadinessRefusalBeforeFirstReadyIsWarn(t *testing.T) {
	down := errors.New("dependency not up yet")
	for _, testCase := range []struct {
		name    string
		message string
		check   func(logger *slog.Logger, dependencyErr *error) health.CheckFunc
	}{
		{
			name:    "wrapped_check",
			message: "stream-runner readiness dependency check failed",
			check: func(logger *slog.Logger, dependencyErr *error) health.CheckFunc {
				return wrapStreamRunnerReadinessCheckWithLogging(logger, "posture_manifest_lockstep", func(context.Context) error {
					return *dependencyErr
				})
			},
		},
		{
			name:    "storage_check",
			message: "stream runner readiness dependency check failed",
			check: func(logger *slog.Logger, dependencyErr *error) health.CheckFunc {
				conn := &failingClickHouseConn{}
				storage := &productionStreamStorage{clickHouse: conn, logger: logger}
				return func(ctx context.Context) error {
					conn.err = *dependencyErr
					return storage.ClickHouseReady(ctx)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			dependencyErr := down
			registry := health.NewRegistry(readinessTestCheckTimeout)
			check := testCase.check(slog.New(slog.NewJSONHandler(&logs, nil)), &dependencyErr)
			if err := registry.RegisterRequired("dependency", check); err != nil {
				t.Fatal(err)
			}
			registry.SetReady(true)

			if registry.Readiness(context.Background()).Ready {
				t.Fatal("readiness answered ready with the dependency down")
			}
			if got := dependencyLineLevel(t, &logs, testCase.message); got != "WARN" {
				t.Errorf("refusal at start, never Ready: level %s, want WARN", got)
			}

			dependencyErr = nil
			if !registry.Readiness(context.Background()).Ready {
				t.Fatal("readiness refused with the dependency up")
			}
			dependencyErr = down
			if registry.Readiness(context.Background()).Ready {
				t.Fatal("readiness answered ready with the dependency down")
			}
			if got := dependencyLineLevel(t, &logs, testCase.message); got != "ERROR" {
				t.Errorf("refusal after first Ready: level %s, want ERROR", got)
			}
		})
	}
}
