package home

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// CHAOS-8186: a failed read of the recommendation or the compounding-risk
// signals was answered as "no signals of that kind" with HTTP 200 (a port of
// Python's `except Exception: return []`). A caller could not tell it from a
// window that has none. These tests pin the two states apart: a failed read is
// an error, an empty read is an answer.

// brokenRowsScanner is a result set whose iteration fails after zero rows:
// the failure a ClickHouse stream gives when the server ends it mid-read.
type brokenRowsScanner struct{ err error }

func (s *brokenRowsScanner) Next() bool        { return false }
func (s *brokenRowsScanner) Scan(...any) error { return nil }
func (s *brokenRowsScanner) Err() error        { return s.err }
func (s *brokenRowsScanner) Close() error      { return nil }

type signalReadHandler = func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)

// overrideRead answers every query through base, except the one read of
// table, which answers with (scanner, err).
func overrideRead(base signalReadHandler, table string, scanner dhclickhouse.RowScanner, err error) signalReadHandler {
	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM "+table) {
			return scanner, err
		}
		return base(t, query, bindings)
	}
}

func signalReadFilters(level string, ids ...string) (Filters, time.Time) {
	return Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: level, IDs: ids},
	}, time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC)
}

func signalIDsWithPrefix(signals []Signal, prefix string) []string {
	var ids []string
	for _, signal := range signals {
		if strings.HasPrefix(signal.ID, prefix) {
			ids = append(ids, signal.ID)
		}
	}
	return ids
}

func TestBuildResponse_AFailedSignalReadIsAnErrorNotAnEmptyPanel(t *testing.T) {
	boom := errors.New("clickhouse: read failed")
	cases := []struct {
		name    string
		level   string
		ids     []string
		base    func(t *testing.T) signalReadHandler
		table   string
		scanner dhclickhouse.RowScanner
		err     error
	}{
		{
			name: "risk signals: the query fails", level: "org",
			base:  func(t *testing.T) signalReadHandler { return orgGoldenHandler(t) },
			table: "compounding_risk_daily", err: boom,
		},
		{
			name: "risk signals: the row stream fails", level: "org",
			base:  func(t *testing.T) signalReadHandler { return orgGoldenHandler(t) },
			table: "compounding_risk_daily", scanner: &brokenRowsScanner{err: boom},
		},
		{
			name: "recommendation signals: the query fails", level: "team", ids: []string{"team-1"},
			base:  func(t *testing.T) signalReadHandler { return teamGoldenHandler(t) },
			table: "recommendations_daily", err: boom,
		},
		{
			name: "recommendation signals: the row stream fails", level: "team", ids: []string{"team-1"},
			base:  func(t *testing.T) signalReadHandler { return teamGoldenHandler(t) },
			table: "recommendations_daily", scanner: &brokenRowsScanner{err: boom},
		},
		{
			name: "signal attribution: the query fails", level: "org",
			base:  func(t *testing.T) signalReadHandler { return orgGoldenHandler(t) },
			table: "work_item_team_attributions", err: boom,
		},
		{
			name: "signal attribution: the row stream fails", level: "org",
			base:  func(t *testing.T) signalReadHandler { return orgGoldenHandler(t) },
			table: "work_item_team_attributions", scanner: &brokenRowsScanner{err: boom},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, now := signalReadFilters(tc.level, tc.ids...)
			client := fakeQueryClient{t: t, handler: overrideRead(tc.base(t), tc.table, tc.scanner, tc.err)}

			got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
			if err == nil {
				t.Fatalf("BuildResponse answered %d signals and no error: a failed read of %s was served as an empty panel",
					len(got.Signals), tc.table)
			}
			if !errors.Is(err, boom) {
				t.Fatalf("BuildResponse error = %v, want it to wrap the read failure", err)
			}
			if got != nil {
				t.Fatalf("BuildResponse returned a response beside the error: %+v", got)
			}
		})
	}
}

// The other half of the pair: a read that succeeds with no rows is an answer.
// Without this case, "always fail" would pass the test above.
func TestBuildResponse_AnEmptySignalReadIsAnAnswer(t *testing.T) {
	t.Run("no risk row in the window", func(t *testing.T) {
		f, now := signalReadFilters("org")
		client := fakeQueryClient{t: t, handler: overrideRead(orgGoldenHandler(t), "compounding_risk_daily", &fixtureRowScanner{}, nil)}

		got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if ids := signalIDsWithPrefix(got.Signals, "risk:"); len(ids) != 0 {
			t.Fatalf("risk signals = %v, want none for an empty read", ids)
		}
		if len(got.Signals) == 0 {
			t.Fatal("the metric signals are gone too: an empty risk read must not empty the answer")
		}
	})

	t.Run("no recommendation row for the team", func(t *testing.T) {
		f, now := signalReadFilters("team", "team-1")
		client := fakeQueryClient{t: t, handler: overrideRead(teamGoldenHandler(t), "recommendations_daily", &fixtureRowScanner{}, nil)}

		got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if ids := signalIDsWithPrefix(got.Signals, "recommendation:"); len(ids) != 0 {
			t.Fatalf("recommendation signals = %v, want none for an empty read", ids)
		}
		if len(got.Signals) == 0 {
			t.Fatal("the metric signals are gone too: an empty recommendation read must not empty the answer")
		}
	})
}

// The fixture must HAVE the signals the empty-read cases remove. Without this,
// a prefix that matches nothing would make those cases pass for no reason.
func TestBuildResponse_TheSignalReadFixturesCarryBothKinds(t *testing.T) {
	f, now := signalReadFilters("org")
	got, err := BuildResponse(context.Background(), fakeQueryClient{t: t, handler: orgGoldenHandler(t)}, nil, "org-1", f, now)
	if err != nil {
		t.Fatalf("BuildResponse (org): %v", err)
	}
	if ids := signalIDsWithPrefix(got.Signals, "risk:"); len(ids) == 0 {
		t.Fatal("the org fixture has no risk signal: the empty-read case measures nothing")
	}

	f, now = signalReadFilters("team", "team-1")
	got, err = BuildResponse(context.Background(), fakeQueryClient{t: t, handler: teamGoldenHandler(t)}, nil, "org-1", f, now)
	if err != nil {
		t.Fatalf("BuildResponse (team): %v", err)
	}
	if ids := signalIDsWithPrefix(got.Signals, "recommendation:"); len(ids) == 0 {
		t.Fatal("the team fixture has no recommendation signal: the empty-read case measures nothing")
	}
}
