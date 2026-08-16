package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"

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
			name:     "named profile is not a worker option",
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

func TestWorkerCommandPassesCanonicalQueuesToDependencyConstruction(t *testing.T) {
	t.Parallel()

	spec := workerSpec
	var received []string
	spec.ConfigureDependenciesWithLogger = func(
		_ context.Context,
		cfg config.Config,
		_ *health.Registry,
		_ *slog.Logger,
	) ([]lifecycle.Component, error) {
		received = append([]string(nil), cfg.Queues...)
		return nil, errors.New("stop after observing queue selection")
	}

	var stdout, stderr bytes.Buffer
	code := shell.Execute(
		context.Background(),
		spec,
		[]string{"--queues", "webhooks,heartbeat", "--queues", "retention"},
		func(string) (string, bool) { return "", false },
		shell.IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 1 {
		t.Fatalf("exit code = %d, want dependency failure; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	want := []string{"heartbeat", "retention", "webhooks"}
	if !slices.Equal(received, want) {
		t.Fatalf("dependency queues = %v, want %v", received, want)
	}
}
