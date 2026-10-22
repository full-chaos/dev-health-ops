package apiservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func buildWithRecordedDatabase(t *testing.T, database schemagatetest.Database) (string, error) {
	t.Helper()
	saved := apiSchemaRecorder
	apiSchemaRecorder = func(*pgxpool.Pool) schemagate.Recorder { return database }
	t.Cleanup(func() { apiSchemaRecorder = saved })
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, components, err := buildDeps(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}),
		health.NewRegistry(time.Second), slog.New(slog.NewJSONHandler(&logs, nil)), AuthProxyConfig{})
	closeComponents(components)
	return logs.String(), err
}

func TestAPIRefusesToStartOnADatabaseBehindTheBuild(t *testing.T) {
	logs, err := buildWithRecordedDatabase(t, schemagatetest.OneBehind(t))
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "api_schema_behind" {
		t.Fatalf("a database one migration behind: error = %v, want api_schema_behind", err)
	}
	if !strings.Contains(logs, "schema gate: refusing to start") || strings.Contains(logs, testCredential) {
		t.Fatalf("the refusal line is missing or leaks a credential:\n%s", logs)
	}
}

func TestAPIStartsOnADatabaseAtOrAheadOfTheBuild(t *testing.T) {
	for name, database := range map[string]schemagatetest.Database{
		"equal": schemagatetest.AtHead(t),
		"ahead": schemagatetest.Ahead(t),
	} {
		_, err := buildWithRecordedDatabase(t, database)
		var coded interface{ DependencyReason() string }
		if errors.As(err, &coded) && coded.DependencyReason() == "api_schema_behind" {
			t.Fatalf("%s: the gate refused a database that is not behind", name)
		}
	}
}
