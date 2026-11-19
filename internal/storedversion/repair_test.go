package storedversion

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type countRows struct {
	driver.Rows
	n    uint64
	done bool
}

func (r *countRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}
func (r *countRows) Scan(dest ...any) error { *(dest[0].(*uint64)) = r.n; return nil }
func (r *countRows) Close() error           { return nil }
func (r *countRows) Err() error             { return nil }

// repairConn answers each count from counts in order (the last one repeats) and records the inserts.
type repairConn struct {
	counts  []uint64
	readErr error
	queries []string
	execs   []string
	args    [][]any
}

func (c *repairConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)
	c.args = append(c.args, args)
	if c.readErr != nil {
		return nil, c.readErr
	}
	n := c.counts[len(c.counts)-1]
	if len(c.queries) <= len(c.counts) {
		n = c.counts[len(c.queries)-1]
	}
	return &countRows{n: n}, nil
}

func (c *repairConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

const repairNumber uint32 = 7

var repairKeys = [][]any{{"5e1f2b1a-7c2d-4e8f-9a0b-1c2d3e4f5a6b", repairNumber}}

func TestRepairWithoutKeysReadsNothing(t *testing.T) {
	conn := &repairConn{counts: []uint64{1}}
	if err := PullRequestKept.Repair(context.Background(), conn, "w", "org-1", nil, nil); err != nil || len(conn.queries) != 0 {
		t.Fatalf("err = %v, reads = %d; want no read for an empty batch", err, len(conn.queries))
	}
}

func TestRepairWritesNothingWhenNoKeyHoldsANullLeadOverAStatedGroup(t *testing.T) {
	conn := &repairConn{counts: []uint64{0}}
	guarded := 0
	guard := func(context.Context) error { guarded++; return nil }
	if err := PullRequestKept.Repair(context.Background(), conn, "w", "org-1", repairKeys, guard); err != nil {
		t.Fatal(err)
	}
	if len(conn.queries) != 1 || len(conn.execs) != 0 || guarded != 0 {
		t.Fatalf("reads %d, inserts %d, guards %d; want 1, 0, 0", len(conn.queries), len(conn.execs), guarded)
	}
}

func TestRepairInsertsUntilTheCountIsZero(t *testing.T) {
	conn := &repairConn{counts: []uint64{2, 1, 0}}
	guarded := 0
	guard := func(context.Context) error { guarded++; return nil }
	if err := PullRequestKept.Repair(context.Background(), conn, "w", "org-1", repairKeys, guard); err != nil {
		t.Fatal(err)
	}
	if len(conn.queries) != 3 || len(conn.execs) != 2 || guarded != 2 {
		t.Fatalf("reads %d, inserts %d, guards %d; want 3, 2, 2", len(conn.queries), len(conn.execs), guarded)
	}
	// The filter binds the org and one value list per key column, once for the FINAL read and once for the kept table.
	if args := conn.args[0]; len(args) != 6 || args[0] != "org-1" || args[3] != "org-1" {
		t.Fatalf("args = %v", args)
	}
}

func TestRepairStopsAfterTheBoundWarnsAndCountsAndReturnsNil(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("t").Int64Counter(RepairUnconvergedMetric)
	if err != nil {
		t.Fatal(err)
	}
	previous := repairUnconverged
	repairUnconverged = counter
	t.Cleanup(func() { repairUnconverged = previous })
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	conn := &repairConn{counts: []uint64{2}}
	if err := PullRequestKept.Repair(context.Background(), conn, "provider_sync", "org-1", repairKeys, nil); err != nil {
		t.Fatalf("Repair = %v; an unconverged repair must not fail the write", err)
	}
	if len(conn.execs) != RepairRounds || len(conn.queries) != RepairRounds+1 {
		t.Fatalf("inserts %d, reads %d; want %d, %d", len(conn.execs), len(conn.queries), RepairRounds, RepairRounds+1)
	}
	if got := strings.Count(logs.String(), RepairUnconvergedEvent); got != 1 {
		t.Fatalf("%d %s lines, want 1:\n%s", got, RepairUnconvergedEvent, logs.String())
	}
	line := logs.String()
	for _, want := range []string{"writer=provider_sync", "table=git_pull_requests", "keys=2"} {
		if !strings.Contains(line, want) {
			t.Fatalf("WARN line lacks %s:\n%s", want, line)
		}
	}
	if strings.Contains(line, "org-1") || strings.Contains(line, "5e1f2b1a") {
		t.Fatalf("WARN line names an id:\n%s", line)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	sum := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 || sum.DataPoints[0].Attributes.Len() != 2 {
		t.Fatalf("counter = %+v; want one series (writer, table) of 2", sum.DataPoints)
	}
}

// A schema without migration 165 repairs nothing, loudly: each write logs one WARN that names the kept table and the
// org, and adds 1 to the counter.
func TestRepairWithoutTheKeptTableWarnsCountsAndRepairsNothing(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("t").Int64Counter(KeptTableMissingMetric)
	if err != nil {
		t.Fatal(err)
	}
	previous := keptTableMissing
	keptTableMissing = counter
	t.Cleanup(func() { keptTableMissing = previous })
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	conn := &repairConn{readErr: &clickhouse.Exception{Code: unknownTable, Message: "Unknown table"}}
	for write := 0; write < 2; write++ {
		if err := DeploymentKept.Repair(context.Background(), conn, "provider_sync", "org-1", [][]any{{uuid.Nil, "d1"}}, nil); err != nil {
			t.Fatalf("Repair = %v; want nil on a schema without migration 165", err)
		}
	}
	if len(conn.queries) != 2 || len(conn.execs) != 0 {
		t.Fatalf("reads %d, inserts %d; want 2, 0", len(conn.queries), len(conn.execs))
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d log lines, want one per write:\n%s", len(lines), logs.String())
	}
	for _, line := range lines {
		for _, want := range []string{"level=WARN", KeptTableMissingEvent, "kept_table=deployment_kept", "table=deployments",
			"org_id=org-1", "writer=provider_sync", "keys=1"} {
			if !strings.Contains(line, want) {
				t.Fatalf("WARN line lacks %s:\n%s", want, line)
			}
		}
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	sum := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 || sum.DataPoints[0].Attributes.Len() != 2 {
		t.Fatalf("counter = %+v; want one series (writer, table) of 2", sum.DataPoints)
	}
}

// Any other exception on the first read fails the write: only code 60 means the kept table is missing.
func TestRepairFailsOnAnotherExceptionCode(t *testing.T) {
	conn := &repairConn{readErr: &clickhouse.Exception{Code: 81, Message: "Database does not exist"}}
	if err := DeploymentKept.Repair(context.Background(), conn, "w", "org-1", [][]any{{uuid.Nil, "d1"}}, nil); err == nil {
		t.Fatal("Repair succeeded over exception code 81")
	}
}

func TestRepairFailsOnAnyOtherReadError(t *testing.T) {
	conn := &repairConn{readErr: errors.New("clickhouse unavailable")}
	if err := DeploymentKept.Repair(context.Background(), conn, "w", "org-1", [][]any{{uuid.Nil, "d1"}}, nil); err == nil {
		t.Fatal("Repair succeeded over a failed read")
	}
}

func TestRepairInsertsNothingWhenTheGuardFails(t *testing.T) {
	conn := &repairConn{counts: []uint64{1}}
	lost := errors.New("lease lost")
	err := PullRequestKept.Repair(context.Background(), conn, "w", "org-1", repairKeys, func(context.Context) error { return lost })
	if !errors.Is(err, lost) || len(conn.execs) != 0 {
		t.Fatalf("err = %v, inserts %d; want the guard error and no insert", err, len(conn.execs))
	}
}

// The repair fills only a NULL lead (fill-null-only): each group's flag requires FINAL's lead IS NULL and the kept lead
// IS NOT NULL, and a column takes the kept value only under its own group's flag.
func TestRepairInsertFillsOnlyANullLead(t *testing.T) {
	statement := PullRequestKept.RepairInsert("org_id = ?")
	for _, want := range []string{
		"base.merged_at IS NULL AND kept.kept_merged.1 IS NOT NULL AS kept_fill_merged",
		"base.first_review_at IS NULL AND kept.kept_review.1 IS NOT NULL AS kept_fill_review",
		"if(kept_fill_merged, kept_merged.1, merged_at) AS merged_at",
		"if(kept_fill_review, kept_review.1, first_review_at) AS first_review_at",
		"if(kept_fill_review, kept_review.2, changes_requested_count) AS changes_requested_count",
		"if(kept_fill_review, kept_review.3, reviews_count) AS reviews_count",
		"last_synced + toIntervalMillisecond(1) AS last_synced",
		"WHERE kept_fill_merged OR kept_fill_review",
		"FROM git_pull_requests FINAL WHERE org_id = ?",
		"FROM git_pull_request_kept WHERE org_id = ? GROUP BY",
	} {
		if !strings.Contains(statement, want) {
			t.Fatalf("repair statement lacks %q:\n%s", want, statement)
		}
	}
}

// Migration 165 ends with the repair of every key: the same statement Repair runs for a batch, without the filter.
func TestMigration165RepairsEveryKeyWithTheWritersStatement(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	for _, file := range chain {
		if file.Version == "165_stored_version_kept_groups.sql" {
			statements = chmigrate.SplitStatements(file.SQL)
		}
	}
	if len(statements) == 0 {
		t.Fatal("the chain holds no 165_stored_version_kept_groups.sql")
	}
	space := regexp.MustCompile(`\s+`)
	normal := func(sql string) string {
		sql = space.ReplaceAllString(strings.TrimSpace(sql), " ")
		return strings.NewReplacer("( ", "(", " )", ")").Replace(sql)
	}
	for _, kept := range []Kept{PullRequestKept, DeploymentKept} {
		want := normal(kept.RepairInsert(""))
		found := false
		for _, statement := range statements {
			found = found || normal(statement) == want
		}
		if !found {
			t.Errorf("migration 165 has no statement equal to the %s repair:\n%s", kept.Base, want)
		}
	}
}

func TestKeptInsertNamesItsBaseTable(t *testing.T) {
	for _, kept := range []Kept{PullRequestKept, DeploymentKept} {
		if kept.insert != "INSERT INTO "+kept.Base || !strings.HasPrefix(kept.RepairInsert(""), kept.insert+" SELECT") {
			t.Errorf("%s: insert %q does not name the base table", kept.Base, kept.insert)
		}
	}
}
