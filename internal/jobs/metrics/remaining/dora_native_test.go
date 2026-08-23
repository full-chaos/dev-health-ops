package remaining

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewDORAExecutorFailsClosedWithoutAConnection(t *testing.T) {
	// The two-plane rule is that the native implementation REPLACES the
	// executor for a kind wholesale. A worker that cannot build it must refuse
	// to serve the family, never quietly revert to the Python bridge -- a
	// silent fallback would make "the kind is native" unfalsifiable.
	if _, err := NewDORAExecutor(context.Background(), nil, nil); err == nil {
		t.Fatal("a nil connection must refuse, not degrade to the bridge")
	}
}

func TestDayRangeMirrorsPythonDateRange(t *testing.T) {
	end := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		backfillDays int
		want         []string
	}{
		{name: "one day is just the end day", backfillDays: 1, want: []string{"2026-08-22"}},
		{
			name:         "a window ENDS at the day, it does not start there",
			backfillDays: 3,
			want:         []string{"2026-08-20", "2026-08-21", "2026-08-22"},
		},
		{
			name:         "zero or negative degrades to the end day, as _date_range does",
			backfillDays: 0,
			want:         []string{"2026-08-22"},
		},
		{
			name:         "a window crossing a month boundary walks calendar days",
			backfillDays: 3,
			want:         []string{"2026-08-20", "2026-08-21", "2026-08-22"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			days := dayRange(end, test.backfillDays)
			if len(days) != len(test.want) {
				t.Fatalf("got %d days, want %d: %v", len(days), len(test.want), days)
			}
			for index, day := range days {
				if got := day.Format("2006-01-02"); got != test.want[index] {
					t.Errorf("day %d = %s, want %s", index, got, test.want[index])
				}
			}
		})
	}
}

func TestDayRangeCrossesAMonthBoundary(t *testing.T) {
	days := dayRange(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), 3)
	want := []string{"2026-08-30", "2026-08-31", "2026-09-01"}
	for index, day := range days {
		if got := day.Format("2006-01-02"); got != want[index] {
			t.Fatalf("day %d = %s, want %s", index, got, want[index])
		}
	}
}

func TestMetricFilterMirrorsParseMetrics(t *testing.T) {
	all := map[string]bool{
		"deployment_frequency": true, "lead_time_for_changes": true,
		"time_to_restore_service": true, "change_failure_rate": true,
	}
	names := func(raw *string) map[string]bool {
		got := map[string]bool{}
		for name := range metricFilter(raw) {
			got[name] = true
		}
		return got
	}
	assertSet := func(t *testing.T, got, want map[string]bool) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for name := range want {
			if !got[name] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}

	t.Run("absent means the full default set", func(t *testing.T) {
		assertSet(t, names(nil), all)
	})
	t.Run("empty string means the full default set", func(t *testing.T) {
		empty := ""
		assertSet(t, names(&empty), all)
	})
	t.Run("a list of separators only also falls back", func(t *testing.T) {
		// _parse_metrics returns the default set when the parse yields nothing,
		// rather than computing zero metrics.
		junk := " , , "
		assertSet(t, names(&junk), all)
	})
	t.Run("a real list selects exactly those", func(t *testing.T) {
		subset := "deployment_frequency, change_failure_rate"
		assertSet(t, names(&subset), map[string]bool{
			"deployment_frequency": true, "change_failure_rate": true,
		})
	})
}

func TestOrderingContractSelectsDifferentSQL(t *testing.T) {
	// These two branches differ ONLY for a row with multiple versions, which
	// is why the parity fixture has to carry a superseded version: over
	// single-version data both shapes return the same row and a whole-table
	// comparison would report EQUAL for a port using the wrong one.
	legacy := currentOperationalRowsSQL("operational_incidents", nil, OperationalOrderingLegacy)
	revision := currentOperationalRowsSQL("operational_incidents", nil, OperationalOrderingRevision)

	if !strings.Contains(legacy, "FROM operational_incidents FINAL") {
		t.Errorf("legacy contract must resolve via FINAL:\n%s", legacy)
	}
	if strings.Contains(legacy, "LIMIT 1 BY") {
		t.Errorf("legacy contract must not use revision ordering:\n%s", legacy)
	}
	if !strings.Contains(revision, "LIMIT 1 BY org_id, id") ||
		!strings.Contains(revision, "source_revision DESC") {
		t.Errorf("revision contract must resolve by explicit ordering:\n%s", revision)
	}
	if strings.Contains(revision, "FINAL") {
		t.Errorf("revision contract must not use FINAL:\n%s", revision)
	}
	if legacy == revision {
		t.Fatal("the two contracts must produce different SQL")
	}
}

func TestPostSelectionFiltersLandOutsideTheCurrentRowSelection(t *testing.T) {
	// Python applies these AFTER the current-row selection on purpose: a
	// filter pushed inside would pick the newest row THAT MATCHES rather than
	// filtering the newest row, which silently resurrects superseded data.
	query := currentOperationalRowsSQL(
		"operational_incidents", []string{"is_deleted = 0"}, OperationalOrderingRevision,
	)
	inner := strings.Index(query, "LIMIT 1 BY")
	outer := strings.Index(query, "WHERE is_deleted = 0")
	if inner < 0 || outer < 0 {
		t.Fatalf("query missing expected clauses:\n%s", query)
	}
	if outer < inner {
		t.Fatalf("the post-selection filter must come AFTER the row selection:\n%s", query)
	}
}

func TestResolvedIncidentsQueryKeepsPythonsShape(t *testing.T) {
	query := resolvedIncidentsQuery("", OperationalOrderingLegacy)
	for _, fragment := range []string{
		"resolved_at IS NOT NULL",
		"AND resolved_at >= {start:DateTime64(3, 'UTC')}",
		"AND resolved_at < {end:DateTime64(3, 'UTC')}",
		"valid_from <= {as_of:DateTime64(6, 'UTC')}",
		"(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})",
		"INNER JOIN repos AS repo FINAL",
		"LIMIT 1 BY mapping.repo_id, incident.id",
		"ORDER BY repo_id, incident_id",
	} {
		if !strings.Contains(query, fragment) {
			t.Errorf("resolved-incident query omitted %q:\n%s", fragment, query)
		}
	}
}

func TestRepoFilterPrefersIDAndStaysOrgScoped(t *testing.T) {
	repoID := "00000000-0000-4000-8000-00000000000a"
	repoName := "acme/widgets"

	t.Run("repo_id wins when both are set", func(t *testing.T) {
		arguments := map[string]any{}
		clause := repoFilterClause(doraScope{RepoID: &repoID, RepoName: &repoName}, arguments)
		if !strings.Contains(clause, "repo_id = {repo_id:UUID}") {
			t.Fatalf("clause = %q", clause)
		}
		if _, bound := arguments["repo_name"]; bound {
			t.Error("repo_name must not be bound when repo_id decided the filter")
		}
	})

	t.Run("a name lookup stays scoped to the org", func(t *testing.T) {
		// Without the org scope a repo name colliding across tenants would
		// pull another organization's repository into this org's metrics.
		arguments := map[string]any{}
		clause := repoFilterClause(doraScope{RepoName: &repoName}, arguments)
		if !strings.Contains(clause, "AND org_id = {org_id:String}") {
			t.Fatalf("name lookup must be org-scoped: %q", clause)
		}
	})

	t.Run("no repo scope means no filter", func(t *testing.T) {
		if clause := repoFilterClause(doraScope{}, map[string]any{}); clause != "" {
			t.Fatalf("expected no clause, got %q", clause)
		}
	})
}

func TestDeploymentWindowUsesTheFourWayCoalesce(t *testing.T) {
	// The loading window and the counting window are deliberately different
	// (see the DORAExecutor doc comment). Narrowing this query to match the
	// kernel's `deployed_at or started_at` would start counting rows Python
	// never fetches.
	executor := &DORAExecutor{}
	_ = executor
	arguments := map[string]any{"org_id": "org"}
	filter := repoFilterClause(doraScope{}, arguments)
	query := deploymentWindowQuery(filter)
	if !strings.Contains(query, "coalesce(deployed_at, finished_at, started_at, last_synced)") {
		t.Fatalf("deployment window must use Python's four-way coalesce:\n%s", query)
	}
	if !strings.Contains(query, "FROM deployments FINAL") {
		t.Fatalf("deployment read must resolve current rows:\n%s", query)
	}
}

func TestUTCDayWindowIsHalfOpen(t *testing.T) {
	start, end := utcDayWindow(time.Date(2026, 8, 22, 13, 45, 0, 0, time.UTC))
	if start.Format(time.RFC3339) != "2026-08-22T00:00:00Z" {
		t.Errorf("start = %s", start.Format(time.RFC3339))
	}
	if end.Format(time.RFC3339) != "2026-08-23T00:00:00Z" {
		t.Errorf("end = %s", end.Format(time.RFC3339))
	}
	if !end.After(start) || end.Sub(start) != 24*time.Hour {
		t.Errorf("window must be exactly one day: %s..%s", start, end)
	}
}

func TestDateTime64ArgumentsAreLiteralsNotExpressions(t *testing.T) {
	// ClickHouse PARSES a query parameter rather than evaluating it, so a
	// value rendered as toDateTime('...') is rejected with
	// BAD_QUERY_PARAMETER. The driver renders time.Time exactly that way, and
	// the failure only appears once a query actually runs -- which is why this
	// asserts the rendered SHAPE here as well.
	moment := time.Date(2026, 8, 9, 13, 45, 30, 123456000, time.UTC)

	if got := dateTime64Argument(moment, millisecondPrecision); got != "2026-08-09 13:45:30.123" {
		t.Errorf("millisecond rendering = %q", got)
	}
	if got := dateTime64Argument(moment, microsecondPrecision); got != "2026-08-09 13:45:30.123456" {
		t.Errorf("microsecond rendering = %q", got)
	}
	for _, rendered := range []string{
		dateTime64Argument(moment, millisecondPrecision),
		dateTime64Argument(moment, microsecondPrecision),
	} {
		if strings.ContainsAny(rendered, "(')") {
			t.Errorf(
				"%q looks like an expression; a query parameter must be a bare "+
					"literal or the server refuses it", rendered,
			)
		}
	}

	// A non-UTC input must still cross the wire as UTC: the columns are
	// DateTime64(_, 'UTC') and carry no offset, so a local-zone rendering
	// would shift the day window silently.
	newYork := time.FixedZone("EDT", -4*60*60)
	shifted := dateTime64Argument(moment.In(newYork), millisecondPrecision)
	if shifted != "2026-08-09 13:45:30.123" {
		t.Errorf("a zoned timestamp must render as UTC, got %q", shifted)
	}
}

func TestOrderingContractParsingMirrorsPythonsStrictness(t *testing.T) {
	// parse_operational_ordering_contract (operational_ordering_guard.py:62)
	// returns LEGACY for None and RAISES for anything that is not exactly "1"
	// or "2". Matching the strictness is the point: a value the Python runtime
	// refuses to start on must not be one the Go runtime quietly accepts, or
	// the two disagree about whether the same deployment is configured at all.
	tests := []struct {
		name    string
		raw     string
		present bool
		want    OperationalOrderingContract
		wantErr bool
	}{
		{name: "unset is legacy, as None is", present: false, want: OperationalOrderingLegacy},
		{name: "explicit 1", raw: "1", present: true, want: OperationalOrderingLegacy},
		{name: "explicit 2", raw: "2", present: true, want: OperationalOrderingRevision},
		{
			// Exported-but-empty is NOT unset. Python sees "" rather than None
			// and raises; reading through Getenv would flatten the two.
			name: "empty but present is an error, not legacy",
			raw:  "", present: true, wantErr: true,
		},
		{
			// Trimming first would ACCEPT this, which is the dangerous
			// direction: Go would proceed on a value Python refuses.
			name: "a trailing space is not the number two",
			raw:  "2 ", present: true, wantErr: true,
		},
		{name: "out of range", raw: "3", present: true, wantErr: true},
		{name: "not a number", raw: "legacy", present: true, wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := parseOperationalOrderingContract(test.raw, test.present)
			if test.wantErr {
				if err == nil {
					t.Fatalf(
						"%q must be refused, not silently resolved to %d -- a "+
							"typo would otherwise select the branch that counts "+
							"one incident as several", test.raw, got,
					)
				}
				if !errors.Is(err, ErrOrderingContractUnparseable) {
					t.Errorf("wrong error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != test.want {
				t.Errorf("got contract %d, want %d", got, test.want)
			}
		})
	}
}

func TestEveryTableTheProjectionReadsIsGuarded(t *testing.T) {
	// The guard must cover every table resolvedIncidentsQuery wraps in
	// currentOperationalRowsSQL. Migration 067 rebuilds operational tables in a
	// loop with non-transactional DDL, so a partial migration leaving one on v2
	// and another on legacy is reachable -- and guarding only the first would
	// let construction succeed and the query then reference v2 columns that do
	// not exist on the other table.
	query := resolvedIncidentsQuery("", OperationalOrderingRevision)
	for _, table := range doraContractTables {
		if !strings.Contains(query, table) {
			t.Errorf(
				"doraContractTables lists %q but the projection does not read "+
					"it -- guarding a table this code never queries is "+
					"over-refusal", table,
			)
		}
	}
	// And the converse: nothing the projection reads through the contract may
	// be missing from the guarded set.
	for _, table := range []string{
		"operational_incidents",
		"operational_service_repository_mappings",
	} {
		found := false
		for _, guarded := range doraContractTables {
			if guarded == table {
				found = true
			}
		}
		if !found {
			t.Errorf(
				"the projection reads %q through the ordering contract but the "+
					"guard does not check it", table,
			)
		}
	}
}
