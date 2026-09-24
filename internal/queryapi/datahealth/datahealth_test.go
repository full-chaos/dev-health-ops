package datahealth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeScanner hands scripted rows to typed scan destinations.
type fakeScanner struct {
	rows   [][]any
	cursor int
}

func (f *fakeScanner) Next() bool { return f.cursor < len(f.rows) }
func (f *fakeScanner) Err() error { return nil }
func (f *fakeScanner) Close() error {
	return nil
}
func (f *fakeScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return fmt.Errorf("scan arity: %d destinations, %d values", len(dest), len(row))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case **string:
			if v, ok := row[i].(string); ok {
				*p = &v
			} else {
				*p = nil
			}
		case *uint64:
			*p = row[i].(uint64)
		case **time.Time:
			if v, ok := row[i].(time.Time); ok {
				*p = &v
			} else {
				*p = nil
			}
		case *[]string:
			*p = row[i].([]string)
		default:
			return fmt.Errorf("unsupported destination %T", d)
		}
	}
	return nil
}

// routingCH answers by a substring of the statement.
type routingCH struct {
	rules []chRule
	seen  []string
}

type chRule struct {
	match string
	rows  [][]any
	err   error
}

func (c *routingCH) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.seen = append(c.seen, statement)
	for _, r := range c.rules {
		if strings.Contains(statement, r.match) {
			if r.err != nil {
				return nil, r.err
			}
			return &fakeScanner{rows: r.rows}, nil
		}
	}
	return &fakeScanner{}, nil
}

func sp(s string) *string { return &s }

func TestRequireOperator(t *testing.T) {
	cases := []struct {
		name string
		p    Principal
		want string
	}{
		{"no principal", Principal{}, "Authentication required"},
		{"empty role", Principal{Present: true}, "Data health requires operator access"},
		{"member", Principal{Present: true, Role: "member"}, "Data health requires operator access"},
		{"viewer", Principal{Present: true, Role: "viewer"}, "Data health requires operator access"},
		{"operator", Principal{Present: true, Role: "operator"}, ""},
		{"admin", Principal{Present: true, Role: "admin"}, ""},
		{"owner", Principal{Present: true, Role: "owner"}, ""},
		{"role compared lowercased", Principal{Present: true, Role: "ADMIN"}, ""},
		{"role with spaces is not a role", Principal{Present: true, Role: " admin "}, "Data health requires operator access"},
		{"raw superuser flag", Principal{Present: true, IsSuperuser: true}, ""},
		{"superuser with member role", Principal{Present: true, Role: "member", IsSuperuser: true}, ""},
		{"superuser flag without a principal", Principal{IsSuperuser: true}, "Authentication required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireOperator(tc.p)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != tc.want {
				t.Fatalf("RequireOperator(%+v) = %q, want %q", tc.p, got, tc.want)
			}
		})
	}
}

func TestPyIntMatchesPythonInt(t *testing.T) {
	num := func(s string) any { return jsonMapping([]byte(`{"v":` + s + `}`))["v"] }
	cases := []struct {
		value any
		want  int
	}{
		{nil, 0}, {num("0"), 0}, {num("12"), 12}, {num("-3"), -3}, {num("12.9"), 12}, {num("-12.9"), -12},
		{num("true"), 1}, {num("false"), 0}, {num(`"12"`), 12}, {num(`" 12 "`), 12}, {num(`"+7"`), 7},
		{num(`"1_0"`), 10}, {num(`"1__0"`), 0}, {num(`"_1"`), 0}, {num(`"1_"`), 0}, {num(`"abc"`), 0},
		{num(`"1e3"`), 0}, {num(`""`), 0}, {num("[1]"), 0}, {num("[]"), 0}, {num("{}"), 0}, {num(`{"a":1}`), 0},
	}
	for _, tc := range cases {
		if got := pyInt(tc.value); got != tc.want {
			t.Errorf("pyInt(%#v) = %d, want %d", tc.value, got, tc.want)
		}
	}
}

func TestRowsIngestedFirstPresentKeyDecides(t *testing.T) {
	m := func(s string) map[string]any { return jsonMapping([]byte(s)) }
	cases := []struct {
		stats map[string]any
		want  int
	}{
		{nil, 0},
		{m(`{}`), 0},
		{m(`{"rows_ingested": 5, "rows": 9}`), 5},
		{m(`{"rows": 9, "items": 1}`), 9},
		{m(`{"items_synced": 4}`), 4},
		{m(`{"count": 3, "items": 2}`), 2},
		{m(`{"rows_ingested": "abc", "rows": 9}`), 0},
		{m(`{"rows_ingested": null, "rows": 9}`), 0},
		{m(`{"other": 7}`), 0},
	}
	for _, tc := range cases {
		if got := rowsIngested(tc.stats); got != tc.want {
			t.Errorf("rowsIngested(%v) = %d, want %d", tc.stats, got, tc.want)
		}
	}
}

func TestConnectorScope(t *testing.T) {
	cases := []struct {
		targets string
		name    string
		want    string
	}{
		{`[]`, "cfg", "cfg"},
		{``, "cfg", "cfg"},
		{`null`, "cfg", "cfg"},
		{`["a"]`, "cfg", "a"},
		{`["a","b","c","d"]`, "cfg", "a, b, c"},
		{`[1, true]`, "cfg", "1, True"},
	}
	for _, tc := range cases {
		got := connectorScope(connectorRow{syncTargets: []byte(tc.targets), name: tc.name})
		if got != tc.want {
			t.Errorf("targets %s: scope %q, want %q", tc.targets, got, tc.want)
		}
	}
}

func TestConnectorFailureDecisionTable(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t0.Add(2 * time.Hour)
	t3 := t0.Add(3 * time.Hour)
	now := t0.Add(24 * time.Hour)
	status := func(v int32) *int32 { return &v }
	no, yes := false, true

	cases := []struct {
		name      string
		row       connectorRow
		wantNil   bool
		wantMsg   string
		wantAt    time.Time
		wantStage string
	}{
		{name: "healthy", row: connectorRow{}, wantNil: true},
		{name: "success recorded", row: connectorRow{lastSyncSuccess: &yes}, wantNil: true},
		{name: "run ok, no error", row: connectorRow{hasRun: true, runStatus: status(2)}, wantNil: true},
		{name: "config error wins", row: connectorRow{lastSyncError: sp("cfg boom"), hasRun: true, runStatus: status(2), runError: sp("run boom"), runComplete: &t1, lastSyncAt: &t2},
			wantMsg: "cfg boom", wantAt: t1},
		{name: "run error when config has none", row: connectorRow{lastSyncError: sp(""), hasRun: true, runStatus: status(2), runError: sp("run boom"), runStarted: &t2},
			wantMsg: "run boom", wantAt: t2},
		{name: "last_sync_success false", row: connectorRow{lastSyncSuccess: &no, lastSyncAt: &t2}, wantMsg: "Last sync failed", wantAt: t2},
		{name: "failed run", row: connectorRow{hasRun: true, runStatus: status(3), updatedAt: &t3}, wantMsg: "Last sync failed", wantAt: t3},
		{name: "cancelled run", row: connectorRow{hasRun: true, runStatus: status(4), lastSyncAt: &t2}, wantMsg: "Last sync failed", wantAt: t2},
		{name: "running run is not a failure", row: connectorRow{hasRun: true, runStatus: status(1)}, wantNil: true},
		{name: "no timestamps uses now", row: connectorRow{lastSyncError: sp("x")}, wantMsg: "x", wantAt: now},
		{name: "stage from run result", row: connectorRow{lastSyncError: sp("x"), hasRun: true, runStatus: status(3), runResult: []byte(`{"stage":"load"}`)},
			wantMsg: "x", wantAt: now, wantStage: "load"},
		{name: "falsy stage is none", row: connectorRow{lastSyncError: sp("x"), hasRun: true, runStatus: status(3), runResult: []byte(`{"stage":""}`)},
			wantMsg: "x", wantAt: now},
		{name: "stage ignored without a run", row: connectorRow{lastSyncError: sp("x"), runResult: []byte(`{"stage":"load"}`)},
			wantMsg: "x", wantAt: now},
	}
	r := &Reader{Now: func() time.Time { return now }}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := r.connectorFailure(tc.row)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("want no failure, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("want a failure, got none")
			}
			if got.Message != tc.wantMsg || !got.OccurredAt.Equal(tc.wantAt) {
				t.Fatalf("failure = %+v, want message %q at %v", got, tc.wantMsg, tc.wantAt)
			}
			if tc.wantStage == "" && got.Stage != nil {
				t.Fatalf("stage = %q, want none", *got.Stage)
			}
			if tc.wantStage != "" && (got.Stage == nil || *got.Stage != tc.wantStage) {
				t.Fatalf("stage = %v, want %q", got.Stage, tc.wantStage)
			}
		})
	}
}

func TestConnectorStatusStatsAndLastSyncFallbacks(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	status := int32(2)
	r := &Reader{}

	got := r.connectorStatus(connectorRow{provider: "github", name: "n", lastSyncStats: []byte(`{}`), hasRun: true, runStatus: &status, runResult: []byte(`{"rows":8}`), runComplete: &t1})
	if got.RowsIngested != 8 || got.LastSyncAt == nil || !got.LastSyncAt.Equal(t1) || got.Scope != "n" || got.Provider != "github" {
		t.Fatalf("empty config stats fall back to the run result and completion time: %+v", got)
	}
	got = r.connectorStatus(connectorRow{lastSyncStats: []byte(`{"count":2}`), hasRun: true, runStatus: &status, runResult: []byte(`{"rows":8}`)})
	if got.RowsIngested != 2 {
		t.Fatalf("non-empty config stats win: %+v", got)
	}
	got = r.connectorStatus(connectorRow{})
	if got.RowsIngested != 0 || got.LastSyncAt != nil || got.LastFailure != nil {
		t.Fatalf("bare row: %+v", got)
	}
}

func TestCoverageStat(t *testing.T) {
	stat := coverageStat([]coverageRow{{"a", 3}, {"b", 0}, {"", 0}, {"d", 1}}, "why")
	if stat.TotalRepos != 4 || stat.CoveredRepos != 2 || stat.CoveragePct != 50.0 {
		t.Fatalf("stat = %+v", stat)
	}
	if len(stat.Missing) != 2 || stat.Missing[0].RepoName != "b" || stat.Missing[1].RepoName != "unknown" || stat.Missing[0].Reason != "why" {
		t.Fatalf("missing = %+v", stat.Missing)
	}
	empty := coverageStat(nil, "why")
	if empty.TotalRepos != 0 || empty.CoveragePct != 100.0 || empty.Missing == nil || len(empty.Missing) != 0 {
		t.Fatalf("empty = %+v", empty)
	}
	third := coverageStat([]coverageRow{{"a", 1}, {"b", 0}, {"c", 0}}, "why")
	if third.CoveragePct != float64(1)/float64(3)*100.0 {
		t.Fatalf("pct = %v", third.CoveragePct)
	}
}

func identityRows() *routingCH {
	return &routingCH{rules: []chRule{
		{match: "FROM git_commits", rows: [][]any{
			{"git", "Ann@Example.com", "Ann", uint64(10)},
			{"git", "bob@example.com", "Bob", uint64(7)},
			{"git", "carol@example.com", "Carol", uint64(7)},
			{"jira", "dave", "dave", uint64(3)},
			{"git", "known@example.com", "Known", uint64(2)},
			{"", "erin@other.io", "", uint64(1)},
			{"git", "zed@example.com", "Known Person", uint64(1)},
		}},
		{match: "FROM identities FINAL", rows: [][]any{
			{"known-canonical", "known@example.com", "Known Person", `{"jira":["dave-alias"]}`, []string{}},
			{"bob-c", "bob@corp.example", "Bob B", `{}`, []string{"team-a"}},
			{"erin-c", "erin@corp.example", "", `not json`, []string{"team-b"}},
		}},
	}}
}

func TestIdentityMapping(t *testing.T) {
	r := &Reader{ClickHouse: identityRows()}
	got := r.IdentityMapping(context.Background(), "org-1", "")
	if got.UnmappedCount != 5 {
		t.Fatalf("unmapped count = %d, want 5 (known is mapped): %+v", got.UnmappedCount, got.UnmappedIdentities)
	}
	// ordered by count desc, ties in observed order
	wantOrder := []string{"Ann@Example.com", "bob@example.com", "carol@example.com", "dave", "erin@other.io"}
	for i, want := range wantOrder {
		id := got.UnmappedIdentities[i]
		have := ""
		if id.Email != nil {
			have = *id.Email
		} else if id.DisplayName != nil {
			have = *id.DisplayName
		}
		if have != want {
			t.Fatalf("unmapped[%d] = %q, want %q (%+v)", i, have, want, got.UnmappedIdentities)
		}
	}
	if got.UnmappedIdentities[3].Email != nil || *got.UnmappedIdentities[3].DisplayName != "dave" {
		t.Fatalf("an identity without @ has no email: %+v", got.UnmappedIdentities[3])
	}
	if got.UnmappedIdentities[4].Provider != "unknown" || *got.UnmappedIdentities[4].DisplayName != "erin@other.io" {
		t.Fatalf("empty provider is unknown and empty display name falls back to the identity: %+v", got.UnmappedIdentities[4])
	}
	// bob@example.com matches bob@corp.example by local part; erin likewise
	var suggested []string
	for _, s := range got.SuggestedAliases {
		suggested = append(suggested, *s.UnmappedIdentity.Email+"=>"+s.SuggestedCanonicalID)
		if s.Confidence != 0.82 {
			t.Fatalf("confidence = %v", s.Confidence)
		}
	}
	if fmt.Sprint(suggested) != "[bob@example.com=>bob-c erin@other.io=>erin-c]" {
		t.Fatalf("suggestions = %v", suggested)
	}
}

func TestIdentityMappingTeamScopesTheMappedSet(t *testing.T) {
	// known has no team scope (always counts); bob is on team-a only.
	inTeam := (&Reader{ClickHouse: identityRows()}).IdentityMapping(context.Background(), "org-1", "team-a")
	other := (&Reader{ClickHouse: identityRows()}).IdentityMapping(context.Background(), "org-1", "team-z")
	if len(inTeam.SuggestedAliases) != 1 || inTeam.SuggestedAliases[0].SuggestedCanonicalID != "bob-c" {
		t.Fatalf("team-a suggestions = %+v", inTeam.SuggestedAliases)
	}
	if len(other.SuggestedAliases) != 0 {
		t.Fatalf("an identity scoped to another team must not be suggested: %+v", other.SuggestedAliases)
	}
	if inTeam.UnmappedCount != other.UnmappedCount {
		t.Fatalf("mapped keys come from e-mail/name/provider ids, so counts match here: %d vs %d", inTeam.UnmappedCount, other.UnmappedCount)
	}
}

func TestIdentityMappingCaps(t *testing.T) {
	var observed [][]any
	var mapped [][]any
	for i := 0; i < 40; i++ {
		observed = append(observed, []any{"git", fmt.Sprintf("u%d@example.com", i), "n", uint64(100 - i)})
		mapped = append(mapped, []any{fmt.Sprintf("c%d", i), fmt.Sprintf("u%d@corp.example", i), "", `{}`, []string{}})
	}
	ch := &routingCH{rules: []chRule{{match: "FROM git_commits", rows: observed}, {match: "FROM identities FINAL", rows: mapped}}}
	got := (&Reader{ClickHouse: ch}).IdentityMapping(context.Background(), "org-1", "")
	if got.UnmappedCount != 40 || len(got.UnmappedIdentities) != 25 || len(got.SuggestedAliases) != 25 {
		t.Fatalf("count=%d shown=%d suggestions=%d, want 40/25/25", got.UnmappedCount, len(got.UnmappedIdentities), len(got.SuggestedAliases))
	}
}

func TestClickHouseFailureAnswersEmptyLogsAndMarksDegraded(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	r := &Reader{ClickHouse: &routingCH{rules: []chRule{{match: "FROM deployments", err: errors.New("boom")}}}}
	got := r.MappingCoverage(context.Background(), "org-1")
	if !r.Degraded() {
		t.Fatal("a failed read must mark the reader degraded")
	}
	if got.Deployments.TotalRepos != 0 || got.Deployments.CoveragePct != 100.0 || got.WorkItems.TotalRepos != 0 {
		t.Fatalf("failed reads answer an empty section: %+v", got)
	}
	logged := buf.String()
	for _, want := range []string{"level=WARN", "operation=dataHealth", "section=coverage_deployments"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log missing %q: %s", want, logged)
		}
	}
	if (&Reader{ClickHouse: &routingCH{}}).MappingCoverage(context.Background(), "o") == nil {
		t.Fatal("nil coverage")
	}
}

func TestMetricLineage(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 5, 0, 0, 0, time.UTC)
	ch := &routingCH{rules: []chRule{{match: "FROM work_item_metrics_daily", rows: [][]any{{t1, uint64(42)}}}}}
	r := &Reader{ClickHouse: ch}
	got := r.MetricLineage(context.Background(), "org-1", "throughput")
	if got == nil || got.MetricID != "throughput" || !got.ComputedAt.Equal(t1) || *got.RowCount != 42 ||
		got.ComputeWindow.Kind != "daily" || got.ComputeWindow.DurationDays != nil || fmt.Sprint(got.SourceTables) != "[work_item_metrics_daily]" {
		t.Fatalf("lineage = %+v", got)
	}
	inv := (&Reader{ClickHouse: &routingCH{rules: []chRule{{match: "FROM work_unit_investments", rows: [][]any{{t1, uint64(1)}}}}}}).MetricLineage(context.Background(), "org-1", "investment_mix")
	if inv == nil || inv.ComputeWindow.Kind != "rolling" || *inv.ComputeWindow.DurationDays != 30 {
		t.Fatalf("investment_mix lineage = %+v", inv)
	}
	unknown := &routingCH{}
	if (&Reader{ClickHouse: unknown}).MetricLineage(context.Background(), "org-1", "nope") != nil || len(unknown.seen) != 0 {
		t.Fatal("an unknown metric has no lineage and issues no query")
	}
	failed := &Reader{ClickHouse: &routingCH{rules: []chRule{{match: "FROM work_item_metrics_daily", err: errors.New("boom")}}}}
	if failed.MetricLineage(context.Background(), "org-1", "throughput") != nil || !failed.Degraded() {
		t.Fatal("a failed table read leaves no lineage and marks the reader degraded")
	}
	nullTime := &Reader{ClickHouse: &routingCH{rules: []chRule{{match: "FROM work_item_metrics_daily", rows: [][]any{{nil, uint64(3)}}}}}}
	if nullTime.MetricLineage(context.Background(), "org-1", "throughput") != nil {
		t.Fatal("no computed_at at all means no lineage")
	}
	if len(lineageRegistry) == 0 {
		t.Fatal("empty lineage registry")
	}
	for name := range lineageRegistry {
		for _, table := range lineageRegistry[name].tables {
			if strings.ReplaceAll(table, "_", "") == "" || strings.ContainsAny(table, " ;'\"-") {
				t.Fatalf("registry table %q is not a plain identifier", table)
			}
		}
	}
}

// fakePG scripts the connector read.
type fakePG struct {
	rows [][]any
	err  error
	sql  string
	args []any
}

func (f *fakePG) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.sql, f.args = sql, args
	if f.err != nil {
		return nil, f.err
	}
	return &fakePGRows{rows: f.rows}, nil
}

type fakePGRows struct {
	pgx.Rows
	rows   [][]any
	cursor int
}

func (f *fakePGRows) Next() bool { return f.cursor < len(f.rows) }
func (f *fakePGRows) Err() error { return nil }
func (f *fakePGRows) Close()     {}
func (f *fakePGRows) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return fmt.Errorf("scan arity: %d destinations, %d values", len(dest), len(row))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *[]byte:
			if v, ok := row[i].(string); ok {
				*p = []byte(v)
			}
		case **string:
			if v, ok := row[i].(string); ok {
				*p = &v
			}
		case **bool:
			if v, ok := row[i].(bool); ok {
				*p = &v
			}
		case **int32:
			if v, ok := row[i].(int32); ok {
				*p = &v
			}
		case **time.Time:
			if v, ok := row[i].(time.Time); ok {
				*p = &v
			}
		default:
			return fmt.Errorf("unsupported destination %T", d)
		}
	}
	return nil
}

func TestConnectorsReadsTheAuthorizedOrgAndMapsRows(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	pg := &fakePG{rows: [][]any{
		{"github", "GH", `["org/a","org/b"]`, t1, true, nil, `{"rows_ingested":11}`, t1, nil, nil, nil, nil, nil},
		{"jira", "JR", `[]`, nil, false, "denied", `{}`, t1, int32(3), t1, t1.Add(time.Hour), `{"stage":"fetch"}`, "run failed"},
	}}
	got, err := (&Reader{Postgres: pg}).Connectors(context.Background(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(pg.args) != "[org-1]" || !strings.Contains(pg.sql, "c.org_id = $1") || !strings.Contains(pg.sql, "is_active IS TRUE") ||
		!strings.Contains(pg.sql, "ORDER BY c.provider, c.name") || !strings.Contains(pg.sql, "ORDER BY jr.created_at DESC") {
		t.Fatalf("sql/args = %q %v", pg.sql, pg.args)
	}
	if len(got) != 2 || got[0].Provider != "github" || got[0].Scope != "org/a, org/b" || got[0].RowsIngested != 11 || got[0].LastFailure != nil {
		t.Fatalf("connector 0 = %+v", got[0])
	}
	if got[1].LastFailure == nil || got[1].LastFailure.Message != "denied" || *got[1].LastFailure.Stage != "fetch" ||
		!got[1].LastFailure.OccurredAt.Equal(t1.Add(time.Hour)) || got[1].LastSyncAt == nil || !got[1].LastSyncAt.Equal(t1.Add(time.Hour)) {
		t.Fatalf("connector 1 = %+v", got[1])
	}
}

func TestConnectorsPostgresFailureFailsTheFieldAndNamesTheCause(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	cases := map[string]struct {
		err   error
		cause string
	}{
		"missing grant":   {&pgconn.PgError{Code: "42501", Message: "permission denied for table sync_configurations"}, "cause=permission_denied"},
		"missing table":   {&pgconn.PgError{Code: "42P01"}, "cause=undefined_table"},
		"other pg error":  {&pgconn.PgError{Code: "57014"}, "cause=pg_error"},
		"connection lost": {errors.New("connection refused"), "cause=connection_or_context"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			buf.Reset()
			got, err := (&Reader{Postgres: &fakePG{err: tc.err}}).Connectors(context.Background(), "org-1")
			if err == nil || got != nil {
				t.Fatalf("a failed Postgres read must fail the field, got %v %v", got, err)
			}
			logged := buf.String()
			if !strings.Contains(logged, "level=ERROR") || !strings.Contains(logged, tc.cause) || !strings.Contains(logged, "operation=dataHealth") {
				t.Fatalf("log = %s", logged)
			}
		})
	}
}

func TestConnectorsWithoutPostgresAnswerEmptyAndLog(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	got, err := (&Reader{}).Connectors(context.Background(), "org-1")
	if err != nil || got == nil || len(got) != 0 || !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("got %v err %v log %s", got, err, buf.String())
	}
}

func TestResolveReadsAllSectionsAndCarriesTheTeam(t *testing.T) {
	ch := identityRows()
	pg := &fakePG{}
	got, err := (&Reader{ClickHouse: ch, Postgres: pg}).Resolve(context.Background(), "org-1", "team-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Team != "team-a" || got.Connectors == nil || got.IdentityMapping == nil || got.MappingCoverage == nil {
		t.Fatalf("result = %+v", got)
	}
	if pg.sql == "" {
		t.Fatal("connectors are read on every request")
	}
	_, err = (&Reader{ClickHouse: ch, Postgres: &fakePG{err: errors.New("x")}}).Resolve(context.Background(), "org-1", "team-a")
	if err == nil {
		t.Fatal("a failed connector read fails the whole field")
	}
}
