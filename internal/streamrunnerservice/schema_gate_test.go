package streamrunnerservice

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type migratingStreamStorage struct {
	*streamCommandStorage
	behind      schemagatetest.Database
	atHead      schemagatetest.Database
	behindReads int64
	reads       atomic.Int64
	closes      atomic.Int64
}

func (storage *migratingStreamStorage) RecordedRevisions(ctx context.Context) ([]string, error) {
	if storage.reads.Add(1) <= storage.behindReads {
		return storage.behind.RecordedRevisions(ctx)
	}
	return storage.atHead.RecordedRevisions(ctx)
}

func (storage *migratingStreamStorage) Close() { storage.closes.Add(1) }

func TestStreamRunnerDoesNotConsumeUntilTheDatabaseReachesTheBuild(t *testing.T) {
	storage := &migratingStreamStorage{
		streamCommandStorage: &streamCommandStorage{},
		behind:               schemagatetest.OneBehind(t),
		atHead:               schemagatetest.AtHead(t),
		behindReads:          2,
	}
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
		t.Fatalf("a schema behind the build must not stop the process: %v", err)
	}
	runStreamComponents(t, registry, components)
	waitForLogLine(t, &logs, "stream consumers started", 15*time.Second)
	output := logs.String()
	for _, want := range []string{"schema gate: refusing to start", `"reason":"stream_schema_behind"`} {
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
	if strings.Index(output, "stream consumers started") < strings.LastIndex(output, "stream_schema_behind") {
		t.Fatalf("consumers started before the schema reached the build:\n%s", output)
	}
}
