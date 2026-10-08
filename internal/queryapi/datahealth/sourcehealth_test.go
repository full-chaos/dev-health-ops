package datahealth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type sourceHealthRows struct {
	pgx.Rows
	rows   [][]any
	cursor int
}

func (f *sourceHealthRows) Next() bool { return f.cursor < len(f.rows) }
func (f *sourceHealthRows) Err() error { return nil }
func (f *sourceHealthRows) Close()     {}
func (f *sourceHealthRows) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *[]byte:
			if v, ok := row[i].(string); ok {
				*p = []byte(v)
			}
		case *bool:
			*p = row[i].(bool)
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
		}
	}
	return nil
}

type sourceHealthPG struct {
	rows [][]any
	err  error
	args []any
}

func (f *sourceHealthPG) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	f.args = args
	if f.err != nil {
		return nil, f.err
	}
	return &sourceHealthRows{rows: f.rows}, nil
}

func TestSourceHealthWithoutAReaderFailsInsteadOfAnsweringEmpty(t *testing.T) {
	rows, err := (&Reader{}).SourceHealth(context.Background(), "org-1")
	if !errors.Is(err, ErrSourceHealthUnavailable) || rows != nil {
		t.Fatalf("rows=%v err=%v, want the unavailable error and no rows", rows, err)
	}
}

func TestSourceHealthFailedReadFailsAndCarriesNoRows(t *testing.T) {
	pg := &sourceHealthPG{err: errors.New("connection refused")}
	rows, err := (&Reader{Postgres: pg}).SourceHealth(context.Background(), "org-1")
	if !errors.Is(err, ErrSourceHealthUnavailable) || rows != nil {
		t.Fatalf("rows=%v err=%v, want the unavailable error and no rows", rows, err)
	}
}

func TestSourceHealthReadsOnlyTheGivenOrg(t *testing.T) {
	pg := &sourceHealthPG{}
	if _, err := (&Reader{Postgres: pg}).SourceHealth(context.Background(), "org-1"); err != nil {
		t.Fatal(err)
	}
	if len(pg.args) != 1 || pg.args[0] != "org-1" {
		t.Fatalf("args = %v, want the org only", pg.args)
	}
}

func TestSourceHealthStageIsAClosedSet(t *testing.T) {
	named, free := "provider_rate_limited", "token=SECRET-PROBE"
	for _, tc := range []struct {
		name            string
		stage, category *string
		want            string
	}{
		{"neither", nil, nil, SourceHealthStageOther},
		{"named stage", &named, nil, "provider_rate_limited"},
		{"named category", nil, &named, "provider_rate_limited"},
		{"free-text stage, named category", &free, &named, "provider_rate_limited"},
		{"free-text stage only", &free, nil, SourceHealthStageOther},
		{"free-text category only", nil, &free, SourceHealthStageOther},
		{"empty strings", ptr(""), ptr(""), SourceHealthStageOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceHealthStage(tc.stage, tc.category); got != tc.want {
				t.Fatalf("stage = %q, want %q", got, tc.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// The derivation over every shape of one configuration: active or inactive,
// canonical or a child of a listed integration, config stamp none / success /
// failed, newest run none / ok / failed, run newer or older than the stamp.
// The expected columns are written from the rules, cell by cell:
// lastSyncAt = newest success of any source; lastFailure = newest failure of
// any source when newer than lastSyncAt; listed = (active or lastFailure) and
// (canonical or an own signal). "stamp" / "run" name the source whose time is
// served, "-" none.
func TestDeriveSourceHealthMatrix(t *testing.T) {
	stampAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	runAt := map[string]time.Time{
		"":      time.Date(2026, 3, 1, 0, 5, 0, 0, time.UTC),
		"newer": time.Date(2026, 2, 2, 0, 5, 0, 0, time.UTC),
		"older": time.Date(2026, 1, 31, 0, 5, 0, 0, time.UTC),
	}
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	tr, fa := true, false
	runCategory, statsCategory := "provider_rate_limited", "pagerduty_sync_disabled"

	type cell struct {
		active                  bool
		role                    string // canonical, child
		stamp, run, order       string
		listed                  bool
		lastSyncAt, lastFailure string
	}
	const A, I = true, false
	var cells []cell
	// (stamp, run, order) -> (lastSyncAt, lastFailure); the same for every
	// active/role combination.
	values := []struct{ stamp, run, order, sync, failure string }{
		{"none", "none", "", "-", "-"},
		{"none", "ok", "", "run", "-"},
		{"none", "failed", "", "-", "run"},
		{"success", "none", "", "stamp", "-"},
		{"success", "ok", "newer", "run", "-"},
		{"success", "ok", "older", "stamp", "-"},
		{"success", "failed", "newer", "stamp", "run"},
		{"success", "failed", "older", "stamp", "-"},
		{"failed", "none", "", "-", "stamp"},
		{"failed", "ok", "newer", "run", "-"},
		{"failed", "ok", "older", "run", "stamp"},
		{"failed", "failed", "newer", "-", "run"},
		{"failed", "failed", "older", "-", "stamp"},
	}
	// listed, per (active, role), in the order of values above.
	listedBy := map[bool]map[string][13]bool{
		A: {
			"canonical": {true, true, true, true, true, true, true, true, true, true, true, true, true},
			"child":     {false, true, true, true, true, true, true, true, true, true, true, true, true},
		},
		I: {
			"canonical": {false, false, true, false, false, false, true, false, true, false, true, true, true},
			"child":     {false, false, true, false, false, false, true, false, true, false, true, true, true},
		},
	}
	for _, active := range []bool{A, I} {
		for _, role := range []string{"canonical", "child"} {
			for k, v := range values {
				cells = append(cells, cell{active, role, v.stamp, v.run, v.order, listedBy[active][role][k], v.sync, v.failure})
			}
		}
	}
	if len(cells) != 52 {
		t.Fatalf("cells = %d, want 52", len(cells))
	}

	for _, tc := range cells {
		name := fmt.Sprintf("active=%v/%s/stamp=%s/run=%s/%s", tc.active, tc.role, tc.stamp, tc.run, tc.order)
		t.Run(name, func(t *testing.T) {
			created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			subject := sourceSignals{id: "subject", integrationID: "integ", active: tc.active, provider: "github", createdAt: &created}
			configs := []sourceSignals{}
			if tc.role == "child" {
				// The listed canonical config of the integration, stamped earlier
				// than every subject time.
				parentAt, early := created.Add(-time.Hour), time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
				configs = append(configs, sourceSignals{id: "parent", integrationID: "integ", active: true, provider: "github",
					createdAt: &parentAt, lastSyncAt: &early, lastSyncSuccess: &tr})
				subject.isChild = true
			}
			switch tc.stamp {
			case "success":
				subject.lastSyncAt, subject.lastSyncSuccess = &stampAt, &tr
			case "failed":
				subject.lastSyncAt, subject.lastSyncSuccess, subject.hasSyncError = &stampAt, &fa, true
				subject.statsCategory = &statsCategory
			}
			at := runAt[tc.order]
			switch tc.run {
			case "ok":
				subject.hasRun, subject.okRun = true, &runSignal{at: at}
			case "failed":
				subject.hasRun, subject.failedRun = true, &runSignal{at: at, category: &runCategory}
			}
			configs = append(configs, subject)

			var got *listedSource
			for _, l := range deriveSourceHealth(configs, now) {
				if l.id == "subject" {
					l := l
					got = &l
				}
			}
			if (got != nil) != tc.listed {
				t.Fatalf("listed = %v, want %v", got != nil, tc.listed)
			}
			if got == nil {
				return
			}
			want := map[string]*time.Time{"-": nil, "stamp": &stampAt, "run": &at}
			wantStage := map[string]string{"stamp": statsCategory, "run": runCategory}
			if w := want[tc.lastSyncAt]; (got.row.LastSyncAt == nil) != (w == nil) || (w != nil && !got.row.LastSyncAt.Equal(*w)) {
				t.Fatalf("lastSyncAt = %v, want %s (%v)", got.row.LastSyncAt, tc.lastSyncAt, w)
			}
			w := want[tc.lastFailure]
			if (got.row.LastFailure == nil) != (w == nil) {
				t.Fatalf("lastFailure = %+v, want %s", got.row.LastFailure, tc.lastFailure)
			}
			if w != nil && (!got.row.LastFailure.OccurredAt.Equal(*w) || got.row.LastFailure.Stage != wantStage[tc.lastFailure]) {
				t.Fatalf("lastFailure = %+v, want %s at %v stage %q", got.row.LastFailure, tc.lastFailure, w, wantStage[tc.lastFailure])
			}
		})
	}
}

// Each own signal alone lets a non-canonical configuration speak, each failure
// signal alone fails a stamp, and the time fallbacks of a failure hold.
func TestDeriveSourceHealthSignalsAlone(t *testing.T) {
	at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	tr, fa := true, false
	early := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := created.Add(time.Hour)
	canonical := sourceSignals{id: "canonical", integrationID: "integ", active: true, createdAt: &created, lastSyncAt: &early, lastSyncSuccess: &tr}

	for _, tc := range []struct {
		name                 string
		subject              sourceSignals
		listed, sync, failed bool
		failureAt            time.Time
	}{
		{"silent", sourceSignals{}, false, false, false, time.Time{}},
		{"time only (a missing flag is a success)", sourceSignals{lastSyncAt: &at}, true, true, false, time.Time{}},
		{"flag only", sourceSignals{lastSyncSuccess: &tr}, true, false, false, time.Time{}},
		{"failed flag only, no time: updated_at", sourceSignals{lastSyncSuccess: &fa, updatedAt: &updated}, true, false, true, updated},
		{"error only, no time, no updated_at: now", sourceSignals{hasSyncError: true}, true, false, true, now},
		{"error with a time: the stamp is a failure", sourceSignals{lastSyncAt: &at, hasSyncError: true}, true, false, true, at},
		{"a run of any status only", sourceSignals{hasRun: true}, true, false, false, time.Time{}},
		{"a failed run only", sourceSignals{hasRun: true, failedRun: &runSignal{at: at}}, true, false, true, at},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := tc.subject
			subject.id, subject.integrationID, subject.active, subject.createdAt = "subject", "integ", true, &later
			var got *listedSource
			for _, l := range deriveSourceHealth([]sourceSignals{canonical, subject}, now) {
				if l.id == "subject" {
					l := l
					got = &l
				}
			}
			if (got != nil) != tc.listed {
				t.Fatalf("listed = %v, want %v", got != nil, tc.listed)
			}
			if got == nil {
				return
			}
			if (got.row.LastSyncAt != nil) != tc.sync || (got.row.LastFailure != nil) != tc.failed {
				t.Fatalf("row = %+v, want time=%v failure=%v", got.row, tc.sync, tc.failed)
			}
			if tc.failed && !got.row.LastFailure.OccurredAt.Equal(tc.failureAt) {
				t.Fatalf("occurredAt = %v, want %v", got.row.LastFailure.OccurredAt, tc.failureAt)
			}
		})
	}
}

// A successful run and a failed run of one configuration, with no stamp: the
// newer one decides. An inactive configuration is listed exactly when its
// failure is the newer one.
func TestDeriveSourceHealthRunPairs(t *testing.T) {
	early := time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
	code := "worker_lost"
	for _, tc := range []struct {
		name               string
		active             bool
		okAt, failAt       time.Time
		listed, hasFailure bool
	}{
		{"active, failure newer", true, early, late, true, true},
		{"active, success newer", true, late, early, true, false},
		{"inactive, failure newer", false, early, late, true, true},
		{"inactive, success newer", false, late, early, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := sourceSignals{id: "x", active: tc.active, hasRun: true,
				okRun: &runSignal{at: tc.okAt}, failedRun: &runSignal{at: tc.failAt, category: &code}}
			got := deriveSourceHealth([]sourceSignals{c}, late)
			if (len(got) == 1) != tc.listed {
				t.Fatalf("rows = %+v, want listed=%v", got, tc.listed)
			}
			if !tc.listed {
				return
			}
			row := got[0].row
			if row.LastSyncAt == nil || !row.LastSyncAt.Equal(tc.okAt) || (row.LastFailure != nil) != tc.hasFailure {
				t.Fatalf("row = %+v, want lastSyncAt %v and failure=%v", row, tc.okAt, tc.hasFailure)
			}
			if tc.hasFailure && (!row.LastFailure.OccurredAt.Equal(tc.failAt) || row.LastFailure.Stage != code) {
				t.Fatalf("failure = %+v, want %v %s", row.LastFailure, tc.failAt, code)
			}
		})
	}
}

// The served provider is a provider of the platform's registry or "other",
// never the stored text.
func TestSourceHealthProviderIsClosed(t *testing.T) {
	for _, tc := range []struct{ stored, want string }{
		{"github", "github"},
		{"GitLab", "gitlab"},
		{" jira ", "jira"},
		{"linear", "linear"},
		{"launchdarkly", "launchdarkly"},
		{"pagerduty", "pagerduty"},
		{"https://internal.example/probe?token=SECRET-PROBE", SourceHealthProviderOther},
		{"bitbucket", SourceHealthProviderOther},
		{"", SourceHealthProviderOther},
	} {
		if got := sourceHealthProvider(tc.stored); got != tc.want {
			t.Fatalf("provider(%q) = %q, want %q", tc.stored, got, tc.want)
		}
	}
	rows := deriveSourceHealth([]sourceSignals{{id: "x", active: true, provider: "https://internal.example/probe?token=SECRET-PROBE"}}, time.Now())
	if len(rows) != 1 || rows[0].row.Provider != SourceHealthProviderOther || rows[0].row.Scope != SourceHealthScopeAll {
		t.Fatalf("rows = %+v, want one row with provider other", rows)
	}
}

// Which configuration of an integration is canonical, and the rule that an
// integration with an active configuration always shows a row.
func TestDeriveSourceHealthIntegrations(t *testing.T) {
	day := func(d int) *time.Time { v := time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC); return &v }
	tr, fa := true, false
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	cfg := func(id string, active, child bool, created *time.Time) sourceSignals {
		return sourceSignals{id: id, integrationID: "integ", active: active, isChild: child, createdAt: created}
	}
	stamped := func(c sourceSignals, ok bool) sourceSignals {
		at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		c.lastSyncAt = &at
		if ok {
			c.lastSyncSuccess = &tr
		} else {
			c.lastSyncSuccess = &fa
		}
		return c
	}
	for _, tc := range []struct {
		name    string
		configs []sourceSignals
		want    []string
	}{
		{"the oldest top-level config is canonical, an older child is not",
			[]sourceSignals{cfg("child", true, true, day(1)), cfg("top-old", true, false, day(2)), cfg("top-new", true, false, day(3))},
			[]string{"top-old"}},
		{"equal creation: the lower id is canonical",
			[]sourceSignals{cfg("b", true, false, day(2)), cfg("a", true, false, day(2))},
			[]string{"a"}},
		{"a config with no creation time is canonical only alone",
			[]sourceSignals{cfg("undated", true, false, nil), cfg("dated", true, false, day(5))},
			[]string{"dated"}},
		{"B: canonical inactive and healthy, silent actives: the oldest active is listed",
			[]sourceSignals{stamped(cfg("canonical", false, false, day(1)), true), cfg("silent-old", true, false, day(2)), cfg("silent-new", true, false, day(3))},
			[]string{"silent-old"}},
		{"C: inactive parent, active child only: the child is listed",
			[]sourceSignals{stamped(cfg("parent", false, false, day(1)), true), cfg("child", true, true, day(2))},
			[]string{"child"}},
		{"D: an active top-level config before an older active child",
			[]sourceSignals{stamped(cfg("canonical", false, false, day(1)), true), cfg("child", true, true, day(2)), cfg("top", true, false, day(3))},
			[]string{"top"}},
		{"E: a listed stamped config keeps a silent older one out",
			[]sourceSignals{stamped(cfg("canonical", false, false, day(1)), true), cfg("silent", true, false, day(2)), stamped(cfg("stamped", true, false, day(3)), true)},
			[]string{"stamped"}},
		{"a silent canonical config is listed beside a stamped sibling",
			[]sourceSignals{cfg("canonical", true, false, day(1)), stamped(cfg("stamped", true, false, day(2)), true)},
			[]string{"canonical", "stamped"}},
		{"an integration listed only by an inactive failed config adds no fallback",
			[]sourceSignals{stamped(cfg("canonical", false, false, day(1)), false), cfg("silent", true, false, day(2))},
			[]string{"canonical"}},
		{"an integration with no active config and no failure is absent",
			[]sourceSignals{stamped(cfg("canonical", false, false, day(1)), true), cfg("other", false, false, day(2))},
			nil},
		{"configs with no integration speak for themselves",
			[]sourceSignals{{id: "solo-a", active: true}, {id: "solo-b", active: true, isChild: true}, {id: "solo-off", active: false}},
			[]string{"solo-a", "solo-b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, l := range deriveSourceHealth(tc.configs, now) {
				got = append(got, l.id)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("listed = %v, want %v", got, tc.want)
			}
		})
	}
}

// The stage of the served failure: the newest current failure first, a run
// before a stamp at the same time, then the older current failure.
func TestDeriveSourceHealthStageOrder(t *testing.T) {
	at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	fa := false
	runCode, statsCode, free := "provider_rate_limited", "pagerduty_sync_disabled", "token=SECRET-PROBE"
	for _, tc := range []struct {
		name     string
		runAt    time.Time
		runCodes [2]*string
		want     string
		wantAt   time.Time
	}{
		{"equal times: the run is read first", at, [2]*string{nil, &runCode}, runCode, at},
		{"stamp newer: its stats first", before, [2]*string{nil, &runCode}, statsCode, at},
		{"run newer: its codes first", at.Add(time.Hour), [2]*string{nil, &runCode}, runCode, at.Add(time.Hour)},
		{"run newer without a named code: the stamp's", at.Add(time.Hour), [2]*string{&free, &free}, statsCode, at.Add(time.Hour)},
		{"run stage before run category", at, [2]*string{ptr("worker_lost"), &runCode}, "worker_lost", at},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runAt := tc.runAt
			c := sourceSignals{id: "x", active: true, lastSyncAt: &at, lastSyncSuccess: &fa, statsCategory: &statsCode,
				hasRun: true, failedRun: &runSignal{at: runAt, stage: tc.runCodes[0], category: tc.runCodes[1]}}
			got := deriveSourceHealth([]sourceSignals{c}, at)
			if len(got) != 1 || got[0].row.LastFailure == nil || got[0].row.LastFailure.Stage != tc.want || !got[0].row.LastFailure.OccurredAt.Equal(tc.wantAt) {
				t.Fatalf("rows = %+v, want stage %q at %v", got, tc.want, tc.wantAt)
			}
		})
	}
	// A failure older than the last success gives no stage at all, and a
	// success stamp's stats category is not a stage.
	ok := true
	newer := at.Add(time.Hour)
	c := sourceSignals{id: "x", active: true, lastSyncAt: &newer, lastSyncSuccess: &ok, statsCategory: &statsCode,
		hasRun: true, failedRun: &runSignal{at: at, category: &runCode}}
	if got := deriveSourceHealth([]sourceSignals{c}, at); len(got) != 1 || got[0].row.LastFailure != nil {
		t.Fatalf("rows = %+v, want no failure", got)
	}
}

func TestSourceHealthStageReadsTheConfigStatsLast(t *testing.T) {
	free, named := "token=SECRET-PROBE", "pagerduty_sync_disabled"
	if got := sourceHealthStage(nil, nil, &named); got != named {
		t.Fatalf("stage = %q, want the stats category", got)
	}
	if got := sourceHealthStage(&free, nil, &named); got != named {
		t.Fatalf("stage = %q, want the stats category after a free-text stage", got)
	}
	if got := sourceHealthStage(nil, nil, &free); got != SourceHealthStageOther {
		t.Fatalf("stage = %q, want other", got)
	}
}

func TestSourceHealthScopeIsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, provider, targets, want string
	}{
		{"no targets", "github", `[]`, SourceHealthScopeAll},
		{"null", "github", `null`, SourceHealthScopeAll},
		{"empty column", "github", ``, SourceHealthScopeAll},
		{"named datasets in the fixed order", "github", `["prs","git"]`, "git, prs"},
		{"free text is dropped", "github", `["prs","acme/secret-repo"]`, "prs"},
		{"free text only", "github", `["acme/secret-repo"]`, SourceHealthScopeOther},
		{"non-string entries", "github", `[1,{"a":"b"}]`, SourceHealthScopeOther},
		{"object", "github", `{"git":true}`, SourceHealthScopeOther},
		{"string", "github", `"git"`, SourceHealthScopeOther},
		{"number", "github", `5`, SourceHealthScopeOther},
		{"malformed", "github", `[`, SourceHealthScopeOther},
		{"provider case", "GitHub", `["git"]`, "git"},
		{"target the provider has no dataset for", "pagerduty", `["git"]`, SourceHealthScopeOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceHealthScope(tc.provider, []byte(tc.targets)); got != tc.want {
				t.Fatalf("scope = %q, want %q", got, tc.want)
			}
		})
	}
}
