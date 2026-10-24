package streamrunnerservice

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate/chgate"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type chRecordedStreamStorage struct {
	*streamCommandStorage
	reads       atomic.Int64
	behindReads int64
	closes      atomic.Int64
}

func (storage *chRecordedStreamStorage) AppliedVersions(context.Context) ([]string, error) {
	build, _ := chgate.LoadBuild()
	all := build.Versions()
	if storage.reads.Add(1) <= storage.behindReads {
		return all[:len(all)-1], nil
	}
	return all, nil
}

func (storage *chRecordedStreamStorage) Close() { storage.closes.Add(1) }

func TestStreamRunnerDoesNotConsumeUntilClickHouseReachesTheBuild(t *testing.T) {
	storage := &chRecordedStreamStorage{streamCommandStorage: &streamCommandStorage{}, behindReads: 2}
	var opens atomic.Int64
	var logs syncBuffer
	registry := health.NewRegistry(readinessTestCheckTimeout)
	components, err := configureStreamRunnerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ingest", StreamConfiguredReplicas: 1},
		registry,
		streamDependencySources{
			openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
				opens.Add(1)
				return storage, nil
			},
		},
		slog.New(slog.NewJSONHandler(&logs, nil)),
	)
	if err != nil {
		t.Fatalf("a ClickHouse schema behind the build must not stop the process: %v", err)
	}
	runStreamComponents(t, registry, components)
	waitForLogLine(t, &logs, "stream consumers started", 15*time.Second)
	output := logs.String()
	for _, want := range []string{"clickhouse schema gate: refusing to start", `"reason":"stream_clickhouse_schema_behind"`} {
		if !strings.Contains(output, want) {
			t.Fatalf("log is missing %q:\n%s", want, output)
		}
	}
	if got := opens.Load(); got != 3 {
		t.Fatalf("storage opened %d times, want 3 (two refusals, then one start)", got)
	}
	if got := storage.closes.Load(); got != 2 {
		t.Fatalf("refused storage closed %d times, want 2", got)
	}
}
