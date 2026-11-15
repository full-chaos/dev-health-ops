package benchmarking

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// recordingConn records each statement and its named arguments; a teams read
// gets no rows, and the series read stops the fetch.
type recordingConn struct {
	statements []string
	args       [][]any
}

var errSeriesRecorded = errors.New("series statement recorded")

func (conn *recordingConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM teams") && !strings.Contains(query, "team_repo_ownership") &&
		!strings.Contains(query, "testops_") && !strings.Contains(query, "work_item_metrics_daily") {
		return noRows{}, nil
	}
	conn.statements = append(conn.statements, query)
	conn.args = append(conn.args, args)
	return nil, errSeriesRecorded
}

func (conn *recordingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("unused")
}

type noRows struct{ driver.Rows }

func (noRows) Next() bool        { return false }
func (noRows) Err() error        { return nil }
func (noRows) Close() error      { return nil }
func (noRows) Scan(...any) error { return errors.New("no row") }

func recordTeamStatement(t *testing.T, metric string, asOf time.Time) (string, []any) {
	t.Helper()
	conn := &recordingConn{}
	loader, err := NewClickHouseLoader(conn, "org-13857")
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(asOf)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if _, err := loader.FetchMetricSeriesByScope(context.Background(), metric, day, day, ScopeTeam); !errors.Is(err, errSeriesRecorded) {
		t.Fatalf("%s: fetch err = %v, want the recorded series statement", metric, err)
	}
	if len(conn.statements) != 1 {
		t.Fatalf("%s: %d series statements, want 1", metric, len(conn.statements))
	}
	return conn.statements[0], conn.args[0]
}

// A team series of a TestOps table groups by the owners of the repository at
// the run (the ownership rule of every team scope, teamscope), never by the
// team stored on the row; the instant is the run's, bound as a DateTime64(3)
// literal. A work-item team series keeps its own team key.
func TestTestopsTeamSeriesReadTheOwnershipJoin(t *testing.T) {
	asOf := time.Date(2026, 8, 10, 12, 30, 15, 250_000_000, time.UTC)
	for name, definition := range metricDefinitions {
		if !definition.ScopeSupport[ScopeTeam] {
			continue
		}
		statement, args := recordTeamStatement(t, name, asOf)
		if !testopsDailyTables[definition.Table] {
			if strings.Contains(statement, "team_repo_ownership") || strings.Contains(statement, "owner_team_id") {
				t.Errorf("%s (%s): a work-item team series reads ownership:\n%s", name, definition.Table, statement)
			}
			continue
		}
		for _, want := range []string{teamscope.Marker, "o.team_id AS owner_team_id", "own.owner_team_id AS scope_key", "INNER JOIN"} {
			if !strings.Contains(statement, want) {
				t.Errorf("%s (%s): the team statement lacks %q:\n%s", name, definition.Table, want, statement)
			}
		}
		if strings.Contains(statement, "argMax(tuple(team_id)") || strings.Contains(statement, "owner_share") {
			t.Errorf("%s (%s): the team statement reads the stored team or an owner share:\n%s", name, definition.Table, statement)
		}
		var asOfArg any
		for _, arg := range args {
			if named, ok := arg.(driver.NamedValue); ok && named.Name == teamscope.BindingAsOf {
				asOfArg = named.Value
			}
		}
		if asOfArg != "2026-08-10 12:30:15.250" {
			t.Errorf("%s: as-of argument = %#v, want the run instant as the literal 2026-08-10 12:30:15.250", name, asOfArg)
		}
	}
}

// A TestOps team read with no run instant is refused, never read at another
// instant.
func TestTestopsTeamSeriesNeedTheRunInstant(t *testing.T) {
	conn := &recordingConn{}
	loader, err := NewClickHouseLoader(conn, "org-13857")
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if _, err := loader.FetchMetricSeriesByScope(context.Background(), "flake_rate", day, day, ScopeTeam); err == nil || errors.Is(err, errSeriesRecorded) {
		t.Fatalf("fetch err = %v, want a refusal before any series statement", err)
	}
	if len(conn.statements) != 0 {
		t.Fatalf("%d series statements ran, want none", len(conn.statements))
	}
}

func TestOwnershipBindingsOfAnUnprovenTypeAreRefused(t *testing.T) {
	_, err := namedArguments([]dhclickhouse.Binding{{Name: "owners_org", Value: "org"}, {Name: "owners_n", Value: 3.5}})
	if err == nil || !strings.Contains(err.Error(), "owners_n") || !strings.Contains(err.Error(), "float64") {
		t.Fatalf("an unproven binding type must be an error naming the binding and its type, got %v", err)
	}
	args, err := namedArguments([]dhclickhouse.Binding{
		{Name: "a", Value: "x"}, {Name: "b", Value: []string{"y"}},
		{Name: "c", Value: time.Date(2026, 11, 15, 1, 2, 3, 4_000_000, time.FixedZone("x", 3600))},
	})
	if err != nil || len(args) != 3 {
		t.Fatalf("string, []string and time.Time are bound: %v %d", err, len(args))
	}
	if got := args[2].(driver.NamedValue).Value; got != "2026-11-15 00:02:03.004" {
		t.Fatalf("a time is bound as its UTC literal, got %v", got)
	}
}
