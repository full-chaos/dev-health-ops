package main

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
)

var streamRunnerSpec = shell.Spec{
	Service:               "dev-health-stream-runner",
	Profiles:              []string{"ingest", "external"},
	DefaultProfile:        "ingest",
	ConfigureDependencies: configureStreamRunnerDependencies,
}

func main() {
	shell.Main(streamRunnerSpec)
}

func configureStreamRunnerDependencies(
	_ context.Context,
	_ config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	// Phase 1 has no stream consumer or composed storage clients. Keep every
	// dependency required by both stream profiles explicitly closed.
	err := processreadiness.RegisterUnavailable(
		registry,
		"clickhouse",
		"domain_postgres",
		"stream_consumer",
		"valkey",
	)
	return nil, err
}
