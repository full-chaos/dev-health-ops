package chwrite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

type shadowBatch struct {
	driver.Batch
	rows [][]any
	sent bool
}

func (b *shadowBatch) Append(values ...any) error { b.rows = append(b.rows, values); return nil }
func (b *shadowBatch) Send() error                { b.sent = true; return nil }

type shadowConn struct {
	statements []string
	batches    []*shadowBatch
	prepareErr error
}

func (c *shadowConn) PrepareBatch(ctx context.Context, statement string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	c.statements = append(c.statements, statement)
	batch := &shadowBatch{}
	c.batches = append(c.batches, batch)
	return batch, nil
}

// columnCount counts the columns of an INSERT ... ( col, col ) statement.
func columnCount(statement string) int {
	open := strings.Index(statement, "(")
	closing := strings.LastIndex(statement, ")")
	return len(strings.Split(statement[open+1:closing], ","))
}

func newShadowWriter(t *testing.T, conn conn) *Writer {
	t.Helper()
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

// recentTime is a ComputedAt that is inside the retention of both tables. A
// fixed date would age out of the TTL, and the forced merges of the integration
// tests would then delete the rows.
func recentTime() time.Time {
	return time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
}

func attemptRow(unit string) AttemptRecord {
	return AttemptRecord{
		RunID: "run-1", WorkUnitID: unit, Role: RoleShadow, Provider: "typesafe", APIMode: "systemone",
		Attempt: 1, Kind: "first", InputTokens: 10, OutputTokens: 1, LatencyMS: 116,
		ComputedAt: recentTime(),
	}
}

func TestShadowThemeDistributionIsTheDeterministicRollup(t *testing.T) {
	mix := map[string]float64{units.SortedSubcategories[0]: 0.75, units.SortedSubcategories[1]: 0.25}
	got := ShadowThemeDistribution(mix)
	if want := units.RollupSubcategoriesToThemes(mix); !reflect.DeepEqual(got, want) {
		t.Fatalf("theme map = %v, want the roll-up %v", got, want)
	}
	for _, empty := range []map[string]float64{nil, {}} {
		if got := ShadowThemeDistribution(empty); got == nil || len(got) != 0 {
			t.Fatalf("an empty mix must give an empty, non-nil theme map, got %#v", got)
		}
	}
}

func TestWriteShadowInvestmentsWritesOneBatchWithMatchingColumns(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	mix := map[string]float64{units.SortedSubcategories[0]: 1}
	records := []ShadowRecord{
		{WorkUnitID: "a", SubcategoryDistribution: mix, ComputedAt: recentTime()},
		{WorkUnitID: "b", State: "evidence_none", SufficiencyLevel: -1, ComputedAt: recentTime()},
	}
	written, err := writer.WriteShadowInvestments(t.Context(), "org-1", records)
	if err != nil || written != 2 {
		t.Fatalf("written %d, err %v", written, err)
	}
	if len(conn.batches) != 1 || !conn.batches[0].sent {
		t.Fatalf("want one sent batch, got %d", len(conn.batches))
	}
	columns := columnCount(conn.statements[0])
	for i, row := range conn.batches[0].rows {
		if len(row) != columns {
			t.Fatalf("row %d appends %d values for %d columns", i, len(row), columns)
		}
	}
	// Column 9 (index) is subcategory_distribution_json, 10 theme_distribution_json.
	rowA, rowB := conn.batches[0].rows[0], conn.batches[0].rows[1]
	if !reflect.DeepEqual(rowA[10], units.RollupSubcategoriesToThemes(mix)) {
		t.Fatalf("row a theme map %v is not the roll-up of its mix", rowA[10])
	}
	if m, ok := rowB[10].(map[string]float64); !ok || len(m) != 0 {
		t.Fatalf("a failure row must have an empty theme map, got %#v", rowB[10])
	}
	if rowA[0] != "org-1" {
		t.Fatalf("org id not stamped first: %v", rowA[0])
	}
}

func TestWriteShadowInvestmentsRequiresAnOrganization(t *testing.T) {
	writer := newShadowWriter(t, &shadowConn{})
	if _, err := writer.WriteShadowInvestments(t.Context(), " ", []ShadowRecord{{WorkUnitID: "a"}}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err = %v, want ErrInvalidState", err)
	}
	if _, err := writer.WriteAttempts(t.Context(), "", []AttemptRecord{attemptRow("a")}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err = %v, want ErrInvalidState", err)
	}
}

func TestAttemptBufferCapsAndCounts(t *testing.T) {
	buffer := NewAttemptBuffer(3)
	for i := 0; i < 5; i++ {
		stored := buffer.Add(attemptRow("u"))
		if stored != (i < 3) {
			t.Fatalf("Add #%d stored = %v", i, stored)
		}
	}
	if buffer.Len() != 3 || buffer.Dropped() != 2 {
		t.Fatalf("len %d dropped %d, want 3 and 2", buffer.Len(), buffer.Dropped())
	}
	if NewAttemptBuffer(0).limit != MaxAttemptRows || MaxAttemptRows != 50_000 {
		t.Fatal("the default cap must be 50,000 rows")
	}
}

func TestAttemptBufferIsSafeForConcurrentUse(t *testing.T) {
	buffer := NewAttemptBuffer(100)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				buffer.Add(attemptRow("u"))
			}
		}()
	}
	wg.Wait()
	if buffer.Len() != 100 || buffer.Dropped() != 300 {
		t.Fatalf("len %d dropped %d, want 100 and 300", buffer.Len(), buffer.Dropped())
	}
}

// The guard of the batched insert: N rows are ONE PrepareBatch and ONE Send,
// the cap holds, and a second flush writes nothing.
func TestFlushAttemptsIsOneBatchWithACapAndDrains(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	buffer := NewAttemptBuffer(4)
	for i := 0; i < 7; i++ {
		buffer.Add(attemptRow("u"))
	}

	result := writer.FlushAttempts(t.Context(), "org-1", buffer)
	if result != (AttemptFlushResult{Written: 4, Dropped: 3}) {
		t.Fatalf("result = %+v", result)
	}
	if len(conn.batches) != 1 || len(conn.batches[0].rows) != 4 || !conn.batches[0].sent {
		t.Fatalf("want one sent batch of 4 rows, got %d batches", len(conn.batches))
	}
	columns := columnCount(conn.statements[0])
	if len(conn.batches[0].rows[0]) != columns {
		t.Fatalf("row appends %d values for %d columns", len(conn.batches[0].rows[0]), columns)
	}

	again := writer.FlushAttempts(t.Context(), "org-1", buffer)
	if again != (AttemptFlushResult{}) || len(conn.batches) != 1 {
		t.Fatalf("a second flush must write nothing, got %+v with %d batches", again, len(conn.batches))
	}
}

func TestFlushAttemptsSwallowsAndCountsAWriteError(t *testing.T) {
	missing := &clickhouse.Exception{Code: 60, Name: "DB::Exception", Message: "Table default.llm_categorization_attempts does not exist"}
	for name, tc := range map[string]struct {
		err         error
		wantMissing bool
	}{
		"missing table": {missing, true},
		"other error":   {errors.New("connection reset"), false},
	} {
		t.Run(name, func(t *testing.T) {
			writer := newShadowWriter(t, &shadowConn{prepareErr: tc.err})
			buffer := NewAttemptBuffer(10)
			buffer.Add(attemptRow("u"))
			buffer.Add(attemptRow("v"))
			result := writer.FlushAttempts(t.Context(), "org-1", buffer)
			if !result.Failed || result.TableMissing != tc.wantMissing || result.Written != 0 {
				t.Fatalf("result = %+v", result)
			}
			if result.Err == nil || !strings.Contains(result.Err.Error(), tc.err.Error()) {
				t.Fatalf("the cause must be in the result for the caller's log line, got %v", result.Err)
			}
		})
	}
}

func TestShadowTableErrorWrapsTheSentinelOnlyForAnUnknownTable(t *testing.T) {
	unknown := shadowTableError("send", &clickhouse.Exception{Code: 60})
	if !errors.Is(unknown, ErrShadowTableMissing) {
		t.Fatalf("code 60 must wrap ErrShadowTableMissing: %v", unknown)
	}
	other := shadowTableError("send", &clickhouse.Exception{Code: 241})
	if errors.Is(other, ErrShadowTableMissing) {
		t.Fatalf("code 241 must not wrap ErrShadowTableMissing: %v", other)
	}
}

// A lost lease means "stop writing": a cancelled run context writes nothing.
func TestFlushAttemptsHonoursACancelledContext(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	buffer := NewAttemptBuffer(10)
	buffer.Add(attemptRow("u"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := writer.FlushAttempts(ctx, "org-1", buffer)
	if !result.Failed || result.Written != 0 || len(conn.batches) != 0 {
		t.Fatalf("result = %+v, batches = %d", result, len(conn.batches))
	}
}

func TestFlushAttemptsOfNothingWritesNothing(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	if got := writer.FlushAttempts(t.Context(), "org-1", nil); got != (AttemptFlushResult{}) {
		t.Fatalf("nil buffer: %+v", got)
	}
	if got := writer.FlushAttempts(t.Context(), "org-1", NewAttemptBuffer(5)); got != (AttemptFlushResult{}) || len(conn.batches) != 0 {
		t.Fatalf("empty buffer: %+v, %d batches", got, len(conn.batches))
	}
}

// A row with no ComputedAt is 1970 in ClickHouse, and the TTL removes it at the
// next merge: the sinks refuse it, as they refuse an empty org id.
func TestSinksRefuseARowWithNoComputedAt(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	good := attemptRow("a")
	bad := attemptRow("b")
	bad.ComputedAt = time.Time{}
	if _, err := writer.WriteAttempts(t.Context(), "org-1", []AttemptRecord{good, bad}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("WriteAttempts err = %v, want ErrInvalidState", err)
	}
	if _, err := writer.WriteShadowInvestments(t.Context(), "org-1", []ShadowRecord{{WorkUnitID: "a", ComputedAt: recentTime()}, {WorkUnitID: "b"}}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("WriteShadowInvestments err = %v, want ErrInvalidState", err)
	}
	if len(conn.batches) != 0 {
		t.Fatalf("a refused write must prepare no batch, got %d", len(conn.batches))
	}
}

func TestAttemptBufferRefusesAZeroComputedAtAndCountsIt(t *testing.T) {
	buffer := NewAttemptBuffer(10)
	bad := attemptRow("b")
	bad.ComputedAt = time.Time{}
	if buffer.Add(bad) || !buffer.Add(attemptRow("a")) {
		t.Fatal("a zero ComputedAt must be refused and a good row kept")
	}
	if buffer.Len() != 1 || buffer.Dropped() != 1 {
		t.Fatalf("len %d dropped %d, want 1 and 1", buffer.Len(), buffer.Dropped())
	}
}

// time.Unix(0, 0) is not IsZero, but it is 1970: the row is written and the TTL
// deletes it at the next merge. A ComputedAt older than the table's retention
// is refused, in both sinks and in the buffer. The shadow table keeps rows for
// 90 days and the attempt table for 400 days, so 100 days old is too old for
// the first and fine for the second.
func TestSinksRefuseAComputedAtOlderThanTheTableRetention(t *testing.T) {
	old := func(days int) time.Time { return time.Now().UTC().AddDate(0, 0, -days) }
	cases := []struct {
		name         string
		at           time.Time
		shadowOK, ok bool
	}{
		{"unix epoch", time.Unix(0, 0), false, false},
		{"year 1900", time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), false, false},
		{"401 days", old(401), false, false},
		{"100 days", old(100), false, true},
		{"89 days", old(89), true, true},
		{"now", time.Now(), true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := &shadowConn{}
			writer := newShadowWriter(t, conn)
			attempt := attemptRow("a")
			attempt.ComputedAt = c.at
			_, err := writer.WriteAttempts(t.Context(), "org-1", []AttemptRecord{attempt})
			if c.ok != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidState)) {
				t.Fatalf("WriteAttempts(%s) err = %v, want accepted=%v", c.name, err, c.ok)
			}
			_, err = writer.WriteShadowInvestments(t.Context(), "org-1", []ShadowRecord{{WorkUnitID: "a", ComputedAt: c.at}})
			if c.shadowOK != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidState)) {
				t.Fatalf("WriteShadowInvestments(%s) err = %v, want accepted=%v", c.name, err, c.shadowOK)
			}
			buffer := NewAttemptBuffer(5)
			if stored := buffer.Add(attempt); stored != c.ok || buffer.Dropped() != map[bool]int{true: 0, false: 1}[c.ok] {
				t.Fatalf("Add(%s) stored=%v dropped=%d, want stored=%v", c.name, stored, buffer.Dropped(), c.ok)
			}
		})
	}
}
