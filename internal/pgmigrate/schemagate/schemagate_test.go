package schemagate_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
)

func run(t *testing.T, database schemagatetest.Database) (string, error) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	err := schemagate.Enforce(context.Background(), logger, "test", database)
	return logs.String(), err
}

func TestEnforceRefusesADatabaseBehindTheBuildWithOneLine(t *testing.T) {
	logs, err := run(t, schemagatetest.OneBehind(t))
	var behind *schemagate.BehindError
	if !errors.As(err, &behind) {
		t.Fatalf("error = %v, want *BehindError", err)
	}
	build, loadErr := pgmigrate.LoadBuildHeads()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	for _, head := range build.Heads() {
		if !strings.Contains(logs, head) {
			t.Errorf("the log line lacks build head %s: %s", head, logs)
		}
	}
	if lines := strings.Count(strings.TrimSpace(logs), "\n") + 1; lines != 1 {
		t.Fatalf("%d log lines, want one: %s", lines, logs)
	}
	if !strings.Contains(logs, "database_heads") {
		t.Fatalf("the log line lacks the database heads: %s", logs)
	}
}

func TestEnforceStartsAtEqualAndWarnsWhenAhead(t *testing.T) {
	if logs, err := run(t, schemagatetest.AtHead(t)); err != nil || strings.Contains(logs, "WARN") {
		t.Fatalf("equal: error = %v, log = %q; want a clean start", err, logs)
	}
	logs, err := run(t, schemagatetest.Ahead(t))
	if err != nil || !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "ahead of this build") {
		t.Fatalf("ahead: error = %v, log = %q; want a start with a WARN", err, logs)
	}
}

func TestEnforceSkipsWhenTheRevisionsCannotBeRead(t *testing.T) {
	for name, database := range map[string]schemagatetest.Database{
		"denied":  {Err: &schemagate.ReadError{Class: "permission_denied"}},
		"down":    {Err: errors.New("dial tcp 10.0.0.1:5432: connection refused")},
		"no rows": {},
	} {
		logs, err := run(t, database)
		if err != nil || !strings.Contains(logs, "schema gate skipped") {
			t.Errorf("%s: error = %v, log = %q; want a skip with a WARN", name, err, logs)
		}
		if strings.Contains(logs, "10.0.0.1") {
			t.Errorf("%s: the skip line leaked driver text: %s", name, logs)
		}
	}
}

func TestEnforceIgnoresADatabaseThatCannotReadRevisions(t *testing.T) {
	if err := schemagate.Enforce(context.Background(), slog.Default(), "test", struct{}{}); err != nil {
		t.Fatal(err)
	}
}
