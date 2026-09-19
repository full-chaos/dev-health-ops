//go:build integration

package dimensionfold

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// foldHarness is a ClickHouse migrated through the real chain, so `repos`
// and `teams` carry their production engine, key and partitioning.
type foldHarness struct {
	conn driver.Conn
}

func startFoldHarness(t *testing.T, ctx context.Context) *foldHarness {
	t.Helper()
	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clickhouse.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, clickhouse)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouse.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &foldHarness{conn: conn}
}

type capturedLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (log *capturedLog) Write(p []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.buffer.Write(p)
}

func (log *capturedLog) events(t *testing.T, msg string) []map[string]any {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	var events []map[string]any
	for _, line := range strings.Split(log.buffer.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if event["msg"] == msg {
			events = append(events, event)
		}
	}
	return events
}

func (harness *foldHarness) folder(t *testing.T, cfg Config, conn driver.Conn) (*Folder, *capturedLog) {
	t.Helper()
	log := &capturedLog{}
	logger := slog.New(slog.NewJSONHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if conn == nil {
		conn = harness.conn
	}
	folder, err := NewFolder(ClickHouseConn{Conn: conn}, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	return folder, log
}

var trackedKeys = []string{
	"11111111-1111-4111-8111-111111111111",
	"22222222-2222-4222-8222-222222222222",
	"33333333-3333-4333-8333-333333333333",
}

// dimension describes how to write one production dimension table.
type dimension struct {
	table   string
	version string
	bulk    string // %[1]s id expression, %[2]s org expression
	row     string // binds id, org, version, version
}

var dimensions = []dimension{
	{
		table: "repos", version: "last_synced",
		bulk: `INSERT INTO repos (id, org_id, repo, created_at, provider, last_synced)
SELECT %[1]s, %[2]s, concat('Acme/repo-', toString(number)), ?, 'github', ?
FROM numbers(%[3]d)`,
		row: `INSERT INTO repos (id, org_id, repo, created_at, provider, last_synced)
VALUES (?, ?, 'Acme/API', ?, 'github', ?)`,
	},
	{
		table: "teams", version: "updated_at",
		bulk: `INSERT INTO teams (id, org_id, name, updated_at, last_synced)
SELECT %[1]s, %[2]s, concat('Team ', toString(number)), ?, ?
FROM numbers(%[3]d)`,
		row: `INSERT INTO teams (id, org_id, name, updated_at, last_synced)
VALUES (?, ?, 'Platform', ?, ?)`,
	},
}

// seedSettled writes the production shape: one large settled part holding
// every organisation's rows, the tracked keys' first versions among them,
// then one new version of each tracked key in its own part.
func seedSettled(t *testing.T, ctx context.Context, conn driver.Conn, dim dimension, rows int) time.Time {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bulk := fmt.Sprintf(dim.bulk,
		fmt.Sprintf("if(number < 3, ['%s', '%s', '%s'][number + 1], toString(generateUUIDv4()))",
			trackedKeys[0], trackedKeys[1], trackedKeys[2]),
		"if(number < 3, 'org-acme', 'org-bulk')", rows)
	if err := conn.Exec(ctx, bulk, base, base); err != nil {
		t.Fatal(err)
	}
	newer := base.Add(time.Hour)
	for _, key := range trackedKeys {
		if err := conn.Exec(ctx, dim.row, key, "org-acme", newer, newer); err != nil {
			t.Fatal(err)
		}
	}
	return newer
}

// trackedDuplicates is the raw-reader view: physical rows beyond one per
// tracked key, read without FINAL.
func trackedDuplicates(t *testing.T, ctx context.Context, conn driver.Conn, table string) uint64 {
	t.Helper()
	var physical uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+
		" WHERE org_id = 'org-acme'").Scan(&physical); err != nil {
		t.Fatal(err)
	}
	return physical - uint64(len(trackedKeys))
}

func mergeCount(t *testing.T, ctx context.Context, conn driver.Conn, table string) uint64 {
	t.Helper()
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	var merges uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM system.part_log
WHERE database = currentDatabase() AND table = ? AND event_type = 'MergeParts'`,
		table).Scan(&merges); err != nil {
		t.Fatal(err)
	}
	return merges
}

func reportFor(t *testing.T, report []TableReport, table string) TableReport {
	t.Helper()
	for _, result := range report {
		if result.Table == table {
			return result
		}
	}
	t.Fatalf("run report has no %s: %+v", table, report)
	return TableReport{}
}

// TestFoldClearsDuplicatesUnderContinuousWrites pins the reader contract: one
// run leaves one physical row per key in every declared table while writes
// keep arriving, which an age-forced merge cannot do. A second run with no new
// writes merges nothing.
func TestFoldClearsDuplicatesUnderContinuousWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness := startFoldHarness(t, ctx)
	folder, log := harness.folder(t, DefaultConfig(), nil)

	for _, dim := range dimensions {
		seedSettled(t, ctx, harness.conn, dim, 100_000)
		if got := trackedDuplicates(t, ctx, harness.conn, dim.table); got != 3 {
			t.Fatalf("%s seeded with %d duplicates, want 3", dim.table, got)
		}
	}

	// Another organisation keeps syncing throughout the run.
	trickleCtx, stopTrickle := context.WithCancel(ctx)
	var trickle sync.WaitGroup
	trickleErr := make(chan error, 1)
	trickle.Add(1)
	go func() {
		defer trickle.Done()
		// Stopping is a flag, not a cancelled context: every insert the
		// writer starts completes before Wait returns, so no part lands after
		// the writer is reported stopped.
		for i := 0; trickleCtx.Err() == nil; i++ {
			for _, dim := range dimensions {
				at := time.Now().UTC()
				if err := harness.conn.Exec(ctx, dim.row,
					fmt.Sprintf("44444444-4444-4444-8444-%012d", i), "org-other", at, at); err != nil {
					trickleErr <- err
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	report := runFold(t, ctx, folder)
	for _, dim := range dimensions {
		if got := trackedDuplicates(t, ctx, harness.conn, dim.table); got != 0 {
			t.Fatalf("%s holds %d duplicates without FINAL after one run under writes", dim.table, got)
		}
		result := reportFor(t, report, dim.table)
		if result.Outcome != OutcomeFolded || result.Err != nil ||
			result.PartsBefore < 2 || result.Partitions != 1 {
			t.Fatalf("%s report=%+v", dim.table, result)
		}
	}
	stopTrickle()
	trickle.Wait()
	select {
	case err := <-trickleErr:
		t.Fatalf("trickle write: %v", err)
	default:
	}

	// Idle: the settled table is one merged part and must not be rewritten.
	runFold(t, ctx, folder)
	before := map[string]uint64{}
	for _, dim := range dimensions {
		before[dim.table] = mergeCount(t, ctx, harness.conn, dim.table)
	}
	idle := runFold(t, ctx, folder)
	for _, dim := range dimensions {
		result := reportFor(t, idle, dim.table)
		if result.Outcome != OutcomeSkippedAsMerged || result.PartsBefore != 1 || result.PartsAfter != 1 {
			t.Fatalf("idle %s report=%+v", dim.table, result)
		}
		if after := mergeCount(t, ctx, harness.conn, dim.table); after != before[dim.table] {
			t.Fatalf("idle %s merged: part_log MergeParts %d -> %d", dim.table, before[dim.table], after)
		}
	}

	events := log.events(t, TableEvent)
	if len(events) == 0 {
		t.Fatal("no per-table telemetry")
	}
	for _, event := range events[:len(dimensions)] {
		for _, key := range []string{"table", "outcome", "parts_before", "parts_after", "duration_ms", "skipped_as_merged", "rows", "partitions"} {
			if _, ok := event[key]; !ok {
				t.Fatalf("event %v has no %s", event, key)
			}
		}
		if event["outcome"] != OutcomeFolded || event["skipped_as_merged"] != false {
			t.Fatalf("first-run event=%v", event)
		}
	}
	last := events[len(events)-1]
	if last["outcome"] != OutcomeSkippedAsMerged || last["skipped_as_merged"] != true {
		t.Fatalf("idle event=%v", last)
	}
}

// failingOptimizeConn is a real connection whose OPTIMIZE on one table fails.
type failingOptimizeConn struct {
	driver.Conn
	table string
}

var errOptimizeRefused = errors.New("optimize refused by test connection")

func (conn failingOptimizeConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "OPTIMIZE TABLE "+conn.table+" ") {
		return errOptimizeRefused
	}
	return conn.Conn.Exec(ctx, query, args...)
}

// TestFoldFailureOnOneTableDoesNotStopTheNext pins per-table isolation: a
// failing statement is logged with its table and cause, and the next table
// is still folded.
func TestFoldFailureOnOneTableDoesNotStopTheNext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness := startFoldHarness(t, ctx)
	folder, log := harness.folder(t, DefaultConfig(),
		failingOptimizeConn{Conn: harness.conn, table: "repos"})
	for _, dim := range dimensions {
		seedSettled(t, ctx, harness.conn, dim, 1_000)
	}
	report := runFold(t, ctx, folder)
	failed := reportFor(t, report, "repos")
	if failed.Outcome != OutcomeFailed || !errors.Is(failed.Err, errOptimizeRefused) {
		t.Fatalf("repos report=%+v", failed)
	}
	if got := trackedDuplicates(t, ctx, harness.conn, "repos"); got != 3 {
		t.Fatalf("repos duplicates=%d, want the untouched 3", got)
	}
	if teams := reportFor(t, report, "teams"); teams.Outcome != OutcomeFolded {
		t.Fatalf("teams report=%+v", teams)
	}
	if got := trackedDuplicates(t, ctx, harness.conn, "teams"); got != 0 {
		t.Fatalf("teams duplicates=%d after the failing table", got)
	}
	events := log.events(t, TableEvent)
	if len(events) != 2 || events[0]["table"] != "repos" || events[0]["level"] != "WARN" ||
		!strings.Contains(fmt.Sprint(events[0]["error"]), errOptimizeRefused.Error()) {
		t.Fatalf("events=%v", events)
	}
}

// TestFoldRefusesTablesOutsideItsBound pins the guard that keeps OPTIMIZE
// FINAL off anything that is not a small single-partition table: a table
// above the row bound and a partitioned table are skipped with a warning and
// keep their parts.
func TestFoldRefusesTablesOutsideItsBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness := startFoldHarness(t, ctx)
	if err := harness.conn.Exec(ctx, `
CREATE TABLE fold_partitioned (id String, org_id String, updated_at DateTime64(3, 'UTC'))
ENGINE = ReplacingMergeTree(updated_at) PARTITION BY org_id ORDER BY (org_id, id)`); err != nil {
		t.Fatal(err)
	}
	for _, org := range []string{"org-a", "org-b", "org-a"} {
		if err := harness.conn.Exec(ctx,
			"INSERT INTO fold_partitioned VALUES ('ABC-123', ?, now64(3))", org); err != nil {
			t.Fatal(err)
		}
	}
	if err := harness.conn.Exec(ctx, `
CREATE TABLE fold_empty (id String, updated_at DateTime64(3, 'UTC'))
ENGINE = ReplacingMergeTree(updated_at) ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	seedSettled(t, ctx, harness.conn, dimensions[0], 5_000)
	cfg := DefaultConfig()
	cfg.MaxRows = 1_000
	cfg.Tables = []string{"repos", "fold_partitioned", "fold_empty"}
	folder, log := harness.folder(t, cfg, nil)
	partsBefore := map[string]uint64{}
	for _, table := range cfg.Tables {
		state, err := folder.readParts(ctx, table)
		if err != nil {
			t.Fatal(err)
		}
		partsBefore[table] = state.parts
	}
	report := runFold(t, ctx, folder)
	want := map[string]string{
		"repos":            OutcomeSkippedOversize,
		"fold_partitioned": OutcomeSkippedPartitioned,
		"fold_empty":       OutcomeSkippedEmpty,
	}
	for table, outcome := range want {
		result := reportFor(t, report, table)
		if result.Outcome != outcome {
			t.Fatalf("%s report=%+v want %s", table, result, outcome)
		}
		state, err := folder.readParts(ctx, table)
		if err != nil {
			t.Fatal(err)
		}
		if state.parts != partsBefore[table] {
			t.Fatalf("%s parts %d -> %d despite the guard", table, partsBefore[table], state.parts)
		}
	}
	if got := trackedDuplicates(t, ctx, harness.conn, "repos"); got != 3 {
		t.Fatalf("oversize repos was folded: duplicates=%d", got)
	}
	for _, event := range log.events(t, TableEvent) {
		wantLevel := "WARN"
		if event["table"] == "fold_empty" {
			wantLevel = "INFO"
		}
		if event["level"] != wantLevel {
			t.Fatalf("skip logged at the wrong level: %v", event)
		}
	}
}

func runFold(t *testing.T, ctx context.Context, folder *Folder) []TableReport {
	t.Helper()
	reports, err := folder.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return reports
}

// TestConcurrentFoldsConverge pins what two overlapping runs do, as happens
// when a retried job meets the next occurrence: both finish without a failed
// table and the tables end at one physical row per key.
func TestConcurrentFoldsConverge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness := startFoldHarness(t, ctx)
	for _, dim := range dimensions {
		seedSettled(t, ctx, harness.conn, dim, 100_000)
	}
	first, _ := harness.folder(t, DefaultConfig(), nil)
	second, _ := harness.folder(t, DefaultConfig(), nil)
	results := make(chan []TableReport, 2)
	errs := make(chan error, 2)
	for _, folder := range []*Folder{first, second} {
		go func(folder *Folder) {
			reports, err := folder.Run(ctx)
			results <- reports
			errs <- err
		}(folder)
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		for _, report := range <-results {
			if report.Outcome == OutcomeFailed {
				t.Fatalf("concurrent run failed a table: %+v", report)
			}
		}
	}
	for _, dim := range dimensions {
		if got := trackedDuplicates(t, ctx, harness.conn, dim.table); got != 0 {
			t.Fatalf("%s duplicates=%d after concurrent runs", dim.table, got)
		}
	}
}
