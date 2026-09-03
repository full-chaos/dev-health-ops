package remaining

import (
	"context"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// recordingBatch captures what was appended, in order, so the test can assert
// on the VALUES WRITTEN rather than on the arguments the caller believes it
// passed.
type recordingBatch struct {
	chdriver.Batch
	rows [][]any
	sent bool
}

func (batch *recordingBatch) Append(values ...any) error {
	batch.rows = append(batch.rows, values)
	return nil
}

func (batch *recordingBatch) Send() error {
	batch.sent = true
	return nil
}

// batchingConn returns the one recording batch, and panics on anything else --
// a test that reaches another method has left the path it meant to exercise.
type batchingConn struct {
	driverConnStub
	batch *recordingBatch
	query string
}

func (conn *batchingConn) PrepareBatch(
	_ context.Context, query string, _ ...chdriver.PrepareBatchOption,
) (chdriver.Batch, error) {
	conn.query = query
	return conn.batch, nil
}

// computedAtColumn is the index of computed_at in the sink's column list. It is
// last, and the assertions below read the value positionally for the same
// reason ClickHouse does.
const computedAtColumn = 12

func TestTheWriteStampReplacesTheEngineInstant(t *testing.T) {
	// The as_of path's defining property: `now` is a pure function of the
	// finalized day, so two runs of the same day carry the SAME engine
	// instant. That is what makes an unstamped write undecidable.
	engineInstant := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	writeTime := time.Date(2026, 9, 2, 11, 17, 43, 0, time.UTC)

	batch := &recordingBatch{}
	executor := &RecommendationsExecutor{conn: &batchingConn{batch: batch}}

	records := []RecommendationRecord{
		{TeamID: "team-a", RuleID: "review-concentration", ComputedAt: engineInstant},
		{TeamID: "team-a", RuleID: "sustainability-risk", ComputedAt: engineInstant},
		{TeamID: "team-b", RuleID: "review-concentration", ComputedAt: engineInstant},
	}

	written, err := executor.writeRecommendations(context.Background(), records, writeTime)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if written != len(records) {
		t.Fatalf("reported %d rows written, want %d", written, len(records))
	}
	if !batch.sent {
		t.Error("the batch was never sent; rows appended to an unsent batch are lost")
	}

	for index, row := range batch.rows {
		got, ok := row[computedAtColumn].(time.Time)
		if !ok {
			t.Fatalf("row %d: computed_at is %T, not a time.Time", index, row[computedAtColumn])
		}
		if got.Equal(engineInstant) {
			t.Errorf("row %d wrote the ENGINE instant %s. Two runs of the same "+
				"finalized day would then write an identical computed_at, and "+
				"neither argMax(fired, computed_at) nor "+
				"ReplacingMergeTree(computed_at) could pick a winner — a "+
				"recovered signal might never clear (CHAOS-2398)",
				index, engineInstant)
		}
		if !got.Equal(writeTime) {
			t.Errorf("row %d wrote computed_at %s, want the write stamp %s",
				index, got, writeTime)
		}
	}
}

// TestOneStampCoversTheWholeBatch pins the property that makes the replacement
// atomic to a reader.
//
// Stamping per row would still beat the engine instant on every comparison
// above and would still look correct in isolation, so the test that catches it
// has to compare rows to EACH OTHER. Split across generations, a reader doing
// argMax per rule could observe one rule's new state beside another's old one.
func TestOneStampCoversTheWholeBatch(t *testing.T) {
	batch := &recordingBatch{}
	executor := &RecommendationsExecutor{conn: &batchingConn{batch: batch}}

	records := []RecommendationRecord{
		{TeamID: "team-a", RuleID: "one"},
		{TeamID: "team-a", RuleID: "two"},
		{TeamID: "team-b", RuleID: "one"},
		{TeamID: "team-b", RuleID: "two"},
	}
	if _, err := executor.writeRecommendations(
		context.Background(), records, time.Now().UTC()); err != nil {
		t.Fatalf("write: %v", err)
	}

	first, ok := batch.rows[0][computedAtColumn].(time.Time)
	if !ok {
		t.Fatal("computed_at is not a time.Time")
	}
	for index, row := range batch.rows[1:] {
		got := row[computedAtColumn].(time.Time)
		if !got.Equal(first) {
			t.Fatalf("row %d carries computed_at %s but row 0 carries %s — one "+
				"scheduled run must replace the team's whole rule state as a "+
				"single generation, or a reader can tear across rules",
				index+1, got, first)
		}
	}
}

// TestAnEmptyBatchIsNotPrepared pins the reference's early return.
//
// Preparing and sending an empty batch is not merely wasteful: it would make
// "wrote nothing" and "wrote a batch of zero rows" indistinguishable in the
// query log, and the row counter would report a healthy write either way.
func TestAnEmptyBatchIsNotPrepared(t *testing.T) {
	// A bare stub: every method panics, so reaching PrepareBatch fails loudly
	// rather than passing on a nil batch.
	executor := &RecommendationsExecutor{conn: driverConnStub{}}

	written, err := executor.writeRecommendations(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatalf("an empty write must succeed trivially: %v", err)
	}
	if written != 0 {
		t.Errorf("reported %d rows written for an empty batch", written)
	}
}

// TestTheInsertNamesEveryColumnTheSinkDoes guards the position binding.
//
// ClickHouse binds by POSITION, not by name. title, rationale, severity and
// success_criterion are four adjacent strings: swapping any two would not
// error, it would write silently crossed values that look like a rule with the
// wrong text. Pinning the statement's column list is what makes a reordering a
// test failure rather than a data corruption.
func TestTheInsertNamesEveryColumnTheSinkDoes(t *testing.T) {
	batch := &recordingBatch{}
	conn := &batchingConn{batch: batch}
	executor := &RecommendationsExecutor{conn: conn}

	if _, err := executor.writeRecommendations(context.Background(),
		[]RecommendationRecord{{TeamID: "team-a"}}, time.Now()); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The order is the Python sink's list verbatim
	// (metrics/sinks/clickhouse/recommendations.py:45-58).
	wantOrder := []string{
		"team_id", "org_id", "rule_id", "rule_version",
		"window_start", "window_end", "fired", "severity",
		"title", "rationale", "success_criterion", "evidence_json", "computed_at",
	}
	position := 0
	for _, column := range wantOrder {
		found := indexAfter(conn.query, column, position)
		if found < 0 {
			t.Fatalf("the INSERT does not name %q, or names it out of order; the "+
				"column list must match the sink's exactly", column)
		}
		position = found
	}

	if got := len(batch.rows[0]); got != len(wantOrder) {
		t.Errorf("appended %d values for %d columns — a mismatch shifts every "+
			"later column by one and writes crossed values without erroring",
			got, len(wantOrder))
	}
}

func indexAfter(haystack, needle string, from int) int {
	for index := from; index+len(needle) <= len(haystack); index++ {
		if haystack[index:index+len(needle)] == needle {
			return index
		}
	}
	return -1
}
