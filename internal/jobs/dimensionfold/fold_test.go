package dimensionfold

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// fakeConn answers the parts query from a per-table script and records every
// OPTIMIZE. A table in block never answers until its context ends.
type fakeConn struct {
	parts     map[string][]partsState // successive readParts answers per table
	block     map[string]bool
	execErr   map[string]error
	optimized []string
}

type fakeRow struct {
	state partsState
	err   error
}

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	*dest[0].(*uint64) = row.state.partitions
	*dest[1].(*uint64) = row.state.parts
	*dest[2].(*uint64) = row.state.rows
	*dest[3].(*string) = row.state.names
	return nil
}

func (conn *fakeConn) QueryRow(ctx context.Context, _ string, args ...any) Row {
	table := args[0].(string)
	if conn.block[table] {
		<-ctx.Done()
		return fakeRow{err: ctx.Err()}
	}
	answers := conn.parts[table]
	if len(answers) == 0 {
		return fakeRow{err: errors.New("no scripted parts")}
	}
	conn.parts[table] = answers[1:]
	return fakeRow{state: answers[0]}
}

func (conn *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	table := strings.Fields(query)[2]
	conn.optimized = append(conn.optimized, table)
	return conn.execErr[table]
}

func newTestFolder(t *testing.T, conn Conn, cfg Config) (*Folder, *bytes.Buffer) {
	t.Helper()
	var output bytes.Buffer
	folder, err := NewFolder(conn, cfg, slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	return folder, &output
}

func decodeEvents(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

// TestFoldTableTimeoutReleasesTheRun pins the per-table bound: a table whose
// reads never return is abandoned at TableTimeout with its cause logged, and
// the next table still folds, all well inside the job's own budget.
func TestFoldTableTimeoutReleasesTheRun(t *testing.T) {
	conn := &fakeConn{
		block: map[string]bool{"repos": true},
		parts: map[string][]partsState{"teams": {
			{partitions: 1, parts: 3, rows: 10, names: "a,b,c"},
			{partitions: 1, parts: 1, rows: 7, names: "d"},
		}},
	}
	cfg := DefaultConfig()
	cfg.TableTimeout = 50 * time.Millisecond
	folder, output := newTestFolder(t, conn, cfg)
	started := time.Now()
	reports, err := folder.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("run took %s with a blocked table", elapsed)
	}
	if reports[0].Outcome != OutcomeFailed || !errors.Is(reports[0].Err, context.DeadlineExceeded) {
		t.Fatalf("blocked table report=%+v", reports[0])
	}
	if reports[1].Outcome != OutcomeFolded || reports[1].PartsBefore != 3 || reports[1].PartsAfter != 1 {
		t.Fatalf("next table report=%+v", reports[1])
	}
	events := decodeEvents(t, output)
	if events[0]["table"] != "repos" || events[0]["level"] != "WARN" ||
		!strings.Contains(events[0]["error"].(string), "deadline exceeded") {
		t.Fatalf("timeout event=%v", events[0])
	}
}

// TestFoldDecisionTable runs every outcome the fold can reach for one table.
func TestFoldDecisionTable(t *testing.T) {
	optimizeErr := errors.New("merge refused")
	for _, tc := range []struct {
		name        string
		answers     []partsState
		execErr     error
		maxRows     uint64
		want        string
		optimize    bool
		wantCrossed bool
	}{
		{"empty", []partsState{{}}, nil, 100, OutcomeSkippedEmpty, false, false},
		{"partitioned", []partsState{{partitions: 2, parts: 3, rows: 5}}, nil, 100, OutcomeSkippedPartitioned, false, false},
		{"rows at bound", []partsState{{partitions: 1, parts: 2, rows: 100, names: "a,b"}, {partitions: 1, parts: 1, rows: 99, names: "c"}}, nil, 100, OutcomeFolded, true, false},
		{"rows past bound", []partsState{{partitions: 1, parts: 2, rows: 101, names: "a,b"}}, nil, 100, OutcomeSkippedOversize, false, false},
		{"single merged part", []partsState{{partitions: 1, parts: 1, rows: 5, names: "a"}, {partitions: 1, parts: 1, rows: 5, names: "a"}}, nil, 100, OutcomeSkippedAsMerged, true, false},
		{"single unmerged part rewritten", []partsState{{partitions: 1, parts: 1, rows: 5, names: "a"}, {partitions: 1, parts: 1, rows: 4, names: "b"}}, nil, 100, OutcomeFolded, true, false},
		{"optimize fails", []partsState{{partitions: 1, parts: 2, rows: 5, names: "a,b"}, {partitions: 1, parts: 1, rows: 4, names: "c"}}, optimizeErr, 100, OutcomeFailed, true, false},
		{"parts unreadable", nil, nil, 100, OutcomeFailed, false, false},
		{"parts unreadable after optimize", []partsState{{partitions: 1, parts: 2, rows: 5, names: "a,b"}}, nil, 100, OutcomeFailed, true, false},
		{"ends exactly at the bound", []partsState{{partitions: 1, parts: 2, rows: 99, names: "a,b"}, {partitions: 1, parts: 1, rows: 100, names: "c"}}, nil, 100, OutcomeFolded, true, false},
		{"grew past the bound during the run", []partsState{{partitions: 1, parts: 2, rows: 100, names: "a,b"}, {partitions: 1, parts: 1, rows: 101, names: "c"}}, nil, 100, OutcomeFolded, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeConn{
				parts:   map[string][]partsState{"repos": append([]partsState(nil), tc.answers...)},
				execErr: map[string]error{"repos": tc.execErr},
			}
			cfg := DefaultConfig()
			cfg.Tables, cfg.MaxRows = []string{"repos"}, tc.maxRows
			folder, output := newTestFolder(t, conn, cfg)
			reports, err := folder.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			// Every outcome reaches the default Info level: a refused or
			// failed table at WARN, every other outcome at INFO.
			wantLevel := "INFO"
			switch tc.want {
			case OutcomeFailed, OutcomeSkippedPartitioned, OutcomeSkippedOversize:
				wantLevel = "WARN"
			}
			if tc.wantCrossed {
				wantLevel = "WARN"
			}
			if events := decodeEvents(t, output); len(events) != 1 || events[0]["level"] != wantLevel ||
				events[0]["outcome"] != tc.want || events[0]["crossed_row_bound"] != tc.wantCrossed {
				t.Fatalf("events=%v, want one %s %s event (crossed=%v)", events, wantLevel, tc.want, tc.wantCrossed)
			}
			if reports[0].CrossedBound != tc.wantCrossed {
				t.Fatalf("crossed=%v want %v (%+v)", reports[0].CrossedBound, tc.wantCrossed, reports[0])
			}
			if reports[0].Outcome != tc.want {
				t.Fatalf("outcome=%s want %s (%+v)", reports[0].Outcome, tc.want, reports[0])
			}
			if tc.execErr != nil && !errors.Is(reports[0].Err, tc.execErr) {
				t.Fatalf("err=%v, want the OPTIMIZE cause %v", reports[0].Err, tc.execErr)
			}
			if (len(conn.optimized) == 1) != tc.optimize {
				t.Fatalf("optimized=%v want optimize=%v", conn.optimized, tc.optimize)
			}
		})
	}
}

// TestFoldRunStopsWhenTheJobIsCancelled pins that a cancelled job stops
// between tables and reports why.
func TestFoldRunStopsWhenTheJobIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	folder, _ := newTestFolder(t, &fakeConn{parts: map[string][]partsState{}}, DefaultConfig())
	reports, err := folder.Run(ctx)
	if !errors.Is(err, context.Canceled) || len(reports) != 0 {
		t.Fatalf("reports=%v err=%v", reports, err)
	}
}

func TestFolderConfigDomain(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Config)
		ok   bool
	}{
		{"default", func(*Config) {}, true},
		{"no tables", func(cfg *Config) { cfg.Tables = nil }, false},
		{"unsafe name", func(cfg *Config) { cfg.Tables = []string{"repos; DROP TABLE teams"} }, false},
		{"upper case", func(cfg *Config) { cfg.Tables = []string{"Repos"} }, false},
		{"duplicate", func(cfg *Config) { cfg.Tables = []string{"repos", "repos"} }, false},
		{"zero rows", func(cfg *Config) { cfg.MaxRows = 0 }, false},
		{"zero timeout", func(cfg *Config) { cfg.TableTimeout = 0 }, false},
		{"timeout at bound", func(cfg *Config) { cfg.TableTimeout = 10 * time.Minute }, true},
		{"timeout past bound", func(cfg *Config) { cfg.TableTimeout = 10*time.Minute + time.Nanosecond }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.edit(&cfg)
			_, err := NewFolder(&fakeConn{}, cfg, slog.Default())
			if tc.ok != (err == nil) {
				t.Fatalf("NewFolder err=%v", err)
			}
		})
	}
	if _, err := NewFolder(nil, DefaultConfig(), slog.Default()); err == nil {
		t.Fatal("nil connection accepted")
	}
	if _, err := NewFolder(&fakeConn{}, DefaultConfig(), nil); err == nil {
		t.Fatal("nil logger accepted")
	}
}

type fakeRunner struct {
	calls int
	err   error
}

func (runner *fakeRunner) Run(context.Context) ([]TableReport, error) {
	runner.calls++
	return nil, runner.err
}

func execution(scheduledFor string) *jobruntime.Execution[jobruntime.DimensionFoldArgs] {
	var args jobruntime.DimensionFoldArgs
	args.Payload = jobcontract.DimensionFoldPayload{ScheduledFor: scheduledFor}
	return &jobruntime.Execution[jobruntime.DimensionFoldArgs]{Args: args}
}

// TestHandlerDecisions pins the job boundary: a malformed request is
// permanent, a stale occurrence is dropped without folding, a run the job's
// context ended is retried, and a completed run succeeds.
func TestHandlerDecisions(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		due       string
		runErr    error
		wantErr   string
		wantCalls int
	}{
		{"current", "2026-01-01T11:59:00Z", nil, "", 1},
		{"at stale bound", now.Add(-StaleAfter).Format(time.RFC3339), nil, "", 1},
		{"past stale bound", now.Add(-StaleAfter - time.Second).Format(time.RFC3339), nil, "", 0},
		{"future", "2026-01-01T12:05:00Z", nil, "", 1},
		{"run cancelled", "2026-01-01T11:59:00Z", context.Canceled, string(jobruntime.CategoryRetryable), 1},
		{"empty", "", nil, string(jobruntime.CategoryPermanent), 0},
		{"not a time", "noon", nil, string(jobruntime.CategoryPermanent), 0},
		{"offset zone", "2026-01-01T11:59:00+01:00", nil, string(jobruntime.CategoryPermanent), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeRunner{err: tc.runErr}
			handler, err := NewHandler(runner, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
			if err != nil {
				t.Fatal(err)
			}
			handler.now = func() time.Time { return now }
			err = handler.Work(context.Background(), execution(tc.due))
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err=%v want %q", err, tc.wantErr)
			}
			if runner.calls != tc.wantCalls {
				t.Fatalf("runs=%d want %d", runner.calls, tc.wantCalls)
			}
		})
	}
	var nilHandler *Handler
	if err := nilHandler.Work(context.Background(), execution("2026-01-01T11:59:00Z")); err == nil {
		t.Fatal("nil handler worked")
	}
	if _, err := NewHandler(nil, slog.Default()); err == nil {
		t.Fatal("nil runner accepted")
	}
}

// TestFoldGuardInvariantByEnumeration proves the guard's invariant over every
// combination of a small alphabet: OPTIMIZE runs on a table only when the
// fold last read it as non-empty, single-partition and inside the row bound;
// a table that ends a run above the bound is reported crossed and logged at
// WARN; and every outcome the fold can produce is reached and declared.
func TestFoldGuardInvariantByEnumeration(t *testing.T) {
	const bound = 100
	declared := map[string]bool{
		OutcomeFolded: true, OutcomeSkippedAsMerged: true, OutcomeSkippedEmpty: true,
		OutcomeSkippedPartitioned: true, OutcomeSkippedOversize: true, OutcomeFailed: true,
	}
	reached := map[string]bool{}
	cells := 0
	for _, partitions := range []uint64{0, 1, 2} {
		for _, parts := range []uint64{0, 1, 2} {
			for _, rowsBefore := range []uint64{0, bound - 1, bound, bound + 1} {
				for _, rowsAfter := range []uint64{bound - 1, bound, bound + 1} {
					for _, sameNames := range []bool{true, false} {
						for _, execFails := range []bool{false, true} {
							cells++
							afterNames := "a"
							if !sameNames {
								afterNames = "b"
							}
							conn := &fakeConn{
								parts: map[string][]partsState{"repos": {
									{partitions: partitions, parts: parts, rows: rowsBefore, names: "a"},
									{partitions: 1, parts: 1, rows: rowsAfter, names: afterNames},
								}},
								execErr: map[string]error{},
							}
							if execFails {
								conn.execErr["repos"] = errors.New("merge refused")
							}
							cfg := DefaultConfig()
							cfg.Tables, cfg.MaxRows = []string{"repos"}, bound
							folder, output := newTestFolder(t, conn, cfg)
							reports, err := folder.Run(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							report := reports[0]
							reached[report.Outcome] = true
							eligible := parts > 0 && partitions <= 1 && rowsBefore <= bound
							if optimized := len(conn.optimized) == 1; optimized != eligible {
								t.Fatalf("partitions=%d parts=%d rows=%d: optimized=%v, eligible=%v",
									partitions, parts, rowsBefore, optimized, eligible)
							}
							wantCrossed := eligible && !execFails && rowsAfter > bound
							if report.CrossedBound != wantCrossed {
								t.Fatalf("cell %+v: crossed=%v want %v", report, report.CrossedBound, wantCrossed)
							}
							warn := report.Outcome == OutcomeFailed || report.Outcome == OutcomeSkippedPartitioned ||
								report.Outcome == OutcomeSkippedOversize || report.CrossedBound
							wantLevel := "INFO"
							if warn {
								wantLevel = "WARN"
							}
							if events := decodeEvents(t, output); len(events) != 1 || events[0]["level"] != wantLevel {
								t.Fatalf("cell %+v: events=%v want one %s event", report, events, wantLevel)
							}
						}
					}
				}
			}
		}
	}
	for outcome := range reached {
		if !declared[outcome] {
			t.Fatalf("undeclared outcome %q", outcome)
		}
	}
	for outcome := range declared {
		if !reached[outcome] && outcome != OutcomeFailed {
			t.Fatalf("declared outcome %q never reached by the enumeration", outcome)
		}
	}
	if cells != 3*3*4*3*2*2 {
		t.Fatalf("enumerated %d cells", cells)
	}
}
