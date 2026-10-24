package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
)

func runQueryAPISchemaGate(t *testing.T, database schemagatetest.Database) (string, error) {
	t.Helper()
	saved := queryAPISchemaRecorder
	queryAPISchemaRecorder = func(*pgxpool.Pool) schemagate.Recorder { return database }
	t.Cleanup(func() { queryAPISchemaRecorder = saved })
	var logs bytes.Buffer
	err := enforceQueryAPISchemaGate(context.Background(), slog.New(slog.NewJSONHandler(&logs, nil)), nil)
	return logs.String(), err
}

func TestQueryAPIRefusesADatabaseBehindTheBuild(t *testing.T) {
	logs, err := runQueryAPISchemaGate(t, schemagatetest.OneBehind(t))
	var behind *schemagate.BehindError
	if !errors.As(err, &behind) {
		t.Fatalf("a database one migration behind: error = %v, want a BehindError", err)
	}
	if !strings.Contains(logs, "schema gate: refusing to start") || !strings.Contains(logs, `"process":"query-api"`) {
		t.Fatalf("the refusal line is missing:\n%s", logs)
	}
}

func TestQueryAPIStartsOnADatabaseAtOrAheadOfTheBuild(t *testing.T) {
	for name, database := range map[string]schemagatetest.Database{
		"equal": schemagatetest.AtHead(t),
		"ahead": schemagatetest.Ahead(t),
	} {
		logs, err := runQueryAPISchemaGate(t, database)
		if err != nil {
			t.Fatalf("%s: the gate refused a database that is not behind: %v", name, err)
		}
		if name == "ahead" && !strings.Contains(logs, "ahead of this build") {
			t.Fatalf("ahead: no WARN line:\n%s", logs)
		}
	}
}

func TestQueryAPISkipsTheGateOnAReadError(t *testing.T) {
	logs, err := runQueryAPISchemaGate(t, schemagatetest.Database{Err: &schemagate.ReadError{Class: "permission_denied"}})
	if err != nil {
		t.Fatalf("a read error must not refuse start: %v", err)
	}
	if !strings.Contains(logs, "schema gate skipped") || !strings.Contains(logs, "permission_denied") {
		t.Fatalf("no skip WARN:\n%s", logs)
	}
}

// buildQueryRoute needs a live ClickHouse, so the order is pinned at the call site: the gate runs
// after the registry pool opens and before the verifier, the posture check and readiness exist.
func TestBuildQueryRouteRunsTheSchemaGateAfterThePoolAndBeforeReadiness(t *testing.T) {
	src, err := os.ReadFile("query_route.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	calls := []string{
		"postgresstore.NewTracedPool(context.Background(), cfg.RegistryPostgresURI)",
		"enforceQueryAPISchemaGate(context.Background(), slog.Default(), pgPool)",
		"principal.NewVerifier(cfg.EnvelopeJWKSPath",
		"posture := queryAPIPostureCheck(getenv, pgPool)",
		"ready := readinessCheck(chClient, pgPool, verifier, posture)",
	}
	last := -1
	for _, call := range calls {
		if strings.Count(text, call) != 1 {
			t.Fatalf("query_route.go must contain %q exactly once", call)
		}
		at := strings.Index(text, call)
		if at < last {
			t.Fatalf("%q is out of order: the gate must follow NewTracedPool and precede NewVerifier, the posture check and readiness", call)
		}
		last = at
	}
}
