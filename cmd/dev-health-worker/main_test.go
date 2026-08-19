package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func TestWorkerCommandUsesExplicitQueuesInsteadOfProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		{
			name:     "comma separated queues",
			args:     []string{"--queues", "heartbeat,webhooks", "--version"},
			wantCode: 0,
		},
		{
			name:     "repeatable queues",
			args:     []string{"--queues", "heartbeat", "--queues", "webhooks", "--version"},
			wantCode: 0,
		},
		{
			name:     "profile flag is not a worker option",
			args:     []string{"--profile", "ops", "--version"},
			wantCode: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			code := shell.Execute(
				context.Background(),
				workerSpec,
				test.args,
				func(string) (string, bool) { return "", false },
				shell.IO{Stdout: &stdout, Stderr: &stderr},
			)
			if code != test.wantCode {
				t.Fatalf(
					"exit code = %d, want %d; stdout=%s stderr=%s",
					code,
					test.wantCode,
					stdout.String(),
					stderr.String(),
				)
			}
		})
	}
}

func TestWorkerCommandPassesProcessArgumentsToDependencyConstruction(t *testing.T) {
	t.Parallel()

	spec := workerSpec
	var received config.Config
	spec.ConfigureDependenciesWithLogger = func(
		_ context.Context,
		cfg config.Config,
		_ *health.Registry,
		_ *slog.Logger,
	) ([]lifecycle.Component, error) {
		received = cfg
		return nil, errors.New("stop after observing queue selection")
	}

	var stdout, stderr bytes.Buffer
	code := shell.Execute(
		context.Background(),
		spec,
		[]string{
			"--queues", "webhooks,heartbeat",
			"--queues", "retention",
			"--queue-concurrency", "heartbeat=1,retention=2",
			"--queue-concurrency", "webhooks=4",
			"--worker-group", "api-workers",
			"--shutdown-timeout", "17m",
		},
		func(string) (string, bool) { return "", false },
		shell.IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 1 {
		t.Fatalf("exit code = %d, want dependency failure; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	want := []string{"heartbeat", "retention", "webhooks"}
	if !slices.Equal(received.Queues, want) {
		t.Fatalf("dependency queues = %v, want %v", received.Queues, want)
	}
	wantConcurrency := map[string]int{"heartbeat": 1, "retention": 2, "webhooks": 4}
	if !maps.Equal(received.WorkerQueueConcurrency, wantConcurrency) {
		t.Fatalf("queue concurrency = %v, want %v", received.WorkerQueueConcurrency, wantConcurrency)
	}
	if received.WorkerGroup != "api-workers" {
		t.Fatalf("worker group = %q, want api-workers", received.WorkerGroup)
	}
	if received.ShutdownTimeout != 17*time.Minute {
		t.Fatalf("shutdown timeout = %s, want 17m", received.ShutdownTimeout)
	}
}
