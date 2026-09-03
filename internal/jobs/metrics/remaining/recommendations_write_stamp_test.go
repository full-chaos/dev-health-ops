package remaining

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// Query errors rather than panicking, so a test can drive a path that loads
// before it writes. The embedded stub panics on Query, which is right for tests
// that must never reach it and wrong for tests whose subject is what happens
// AFTER a load fails.
func (conn *batchingConn) Query(
	_ context.Context, _ string, _ ...any,
) (chdriver.Rows, error) {
	return nil, errRecommendationsUnavailable
}

func (conn *batchingConn) PrepareBatch(
	ctx context.Context, query string, _ ...chdriver.PrepareBatchOption,
) (chdriver.Batch, error) {
	// HONOURS CANCELLATION, because the real driver does.
	//
	// This stub used to ignore ctx entirely, which made every assertion about
	// rows surviving a cancellation VACUOUS: the write "succeeded" whether or
	// not the detached context was in place, so removing the detach failed
	// only the error-reporting assertion and never the persisted-rows one.
	// A stub that cannot refuse cannot prove that something got past a refusal.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// TestAppendedGoTypesMatchTheColumnTypes closes a gap the other tests in this
// file structurally cannot see.
//
// recordingBatch accepts any value, so every assertion above passes regardless
// of the Go type appended. The real driver does not: recommendations_daily
// declares `fired Bool` (migration 039), and appending a uint8 there fails at
// runtime with "converting uint8 to Bool is unsupported" -- which is exactly
// what happened here, by copying capacity's boolToUInt8 helper along with the
// shape of its append, where that sibling's flags genuinely are UInt8.
//
// A stub that accepts everything makes the unit tests agree with each other and
// with nothing else. Pinning the TYPE is what lets this be caught before a
// container run.
func TestAppendedGoTypesMatchTheColumnTypes(t *testing.T) {
	batch := &recordingBatch{}
	executor := &RecommendationsExecutor{conn: &batchingConn{batch: batch}}

	if _, err := executor.writeRecommendations(context.Background(),
		[]RecommendationRecord{{TeamID: "team-a", Fired: true}}, time.Now()); err != nil {
		t.Fatalf("write: %v", err)
	}

	const firedColumn = 6
	if _, ok := batch.rows[0][firedColumn].(bool); !ok {
		t.Errorf("fired was appended as %T; the column is declared Bool and the "+
			"driver refuses any narrowing to it",
			batch.rows[0][firedColumn])
	}
	for index, column := range []struct {
		position int
		name     string
	}{
		{0, "team_id"}, {1, "org_id"}, {2, "rule_id"}, {3, "rule_version"},
		{7, "severity"}, {8, "title"}, {9, "rationale"},
		{10, "success_criterion"}, {11, "evidence_json"},
	} {
		if _, ok := batch.rows[0][column.position].(string); !ok {
			t.Errorf("case %d: %s was appended as %T, want string",
				index, column.name, batch.rows[0][column.position])
		}
	}
	for _, column := range []struct {
		position int
		name     string
	}{{4, "window_start"}, {5, "window_end"}, {12, "computed_at"}} {
		if _, ok := batch.rows[0][column.position].(time.Time); !ok {
			t.Errorf("%s was appended as %T, want time.Time",
				column.name, batch.rows[0][column.position])
		}
	}
}

// TestEveryAppendedValueComesFromItsOwnField is the assertion the other tests
// in this file only APPEAR to make.
//
// TestTheInsertNamesEveryColumnTheSinkDoes pins the SQL text and the value
// COUNT; TestAppendedGoTypesMatchTheColumnTypes pins each value's Go TYPE.
// Neither pins IDENTITY, and five of the thirteen columns are adjacent strings
// -- so swapping record.Title with record.Rationale in the append changes
// nothing either can see, and persists crossed user-facing text under a
// perfectly green suite. Verified: that exact swap passes the whole package.
//
// Value, then type, then identity. Each of the first two reads like it covers
// the third, and neither does. Distinct sentinels are what close it.
func TestEveryAppendedValueComesFromItsOwnField(t *testing.T) {
	// BOTH boolean values. A single Fired:true record compares true against
	// true, so a writer that appended a literal `true` instead of record.Fired
	// would satisfy it -- the sentinel technique closes identity for the
	// STRINGS and silently reopens it for the bool, which has only two
	// inhabitants and therefore cannot be given a unique sentinel.
	for _, fired := range []bool{true, false} {
		t.Run(fmt.Sprintf("fired=%v", fired), func(t *testing.T) {
			assertAppendedValuesMatchTheirFields(t, fired)
		})
	}
}

func assertAppendedValuesMatchTheirFields(t *testing.T, fired bool) {
	t.Helper()
	batch := &recordingBatch{}
	executor := &RecommendationsExecutor{conn: &batchingConn{batch: batch}}

	// Every field gets a value that could only have come from that field.
	record := RecommendationRecord{
		TeamID:           "sentinel-team-id",
		OrgID:            "sentinel-org-id",
		RuleID:           "sentinel-rule-id",
		RuleVersion:      "sentinel-rule-version",
		WindowStart:      time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		WindowEnd:        time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC),
		Fired:            fired,
		Severity:         "sentinel-severity",
		Title:            "sentinel-title",
		Rationale:        "sentinel-rationale",
		SuccessCriterion: "sentinel-success-criterion",
		EvidenceJSON:     "sentinel-evidence-json",
	}
	writeTime := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	if _, err := executor.writeRecommendations(
		context.Background(), []RecommendationRecord{record}, writeTime); err != nil {
		t.Fatalf("write: %v", err)
	}

	want := []any{
		record.TeamID, record.OrgID, record.RuleID, record.RuleVersion,
		record.WindowStart, record.WindowEnd, record.Fired, record.Severity,
		record.Title, record.Rationale, record.SuccessCriterion,
		record.EvidenceJSON,
		// computed_at is the WRITE stamp, never the record's own field.
		writeTime,
	}
	columns := []string{
		"team_id", "org_id", "rule_id", "rule_version",
		"window_start", "window_end", "fired", "severity",
		"title", "rationale", "success_criterion", "evidence_json", "computed_at",
	}

	got := batch.rows[0]
	if len(got) != len(want) {
		t.Fatalf("appended %d values, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("column %d (%s) was appended as %v, want %v — ClickHouse binds "+
				"by position, so a value from the wrong field is written silently",
				index, columns[index], got[index], want[index])
		}
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

// queryRecordingConn records every query issued, so a test can assert on the
// path TAKEN rather than only on the value returned.
type queryRecordingConn struct {
	driverConnStub
	queries []string
}

func (conn *queryRecordingConn) Query(
	_ context.Context, query string, _ ...any,
) (chdriver.Rows, error) {
	conn.queries = append(conn.queries, query)
	return nil, errRecommendationsUnavailable
}

// TestAWhitespaceTeamIDStaysScopedToThatTeam pins a parity divergence found in
// review, and the reason it is dangerous is the direction it fails in.
//
// Python branches on ordinary truthiness (`[team_id] if team_id else discover`),
// so " " is an EXPLICIT team there. Go had `strings.TrimSpace(teamID) == ""`,
// which sends the same payload down the DISCOVERY path -- so a malformed
// team-scoped request would silently persist recommendations for every team in
// the org instead of the one asked for. A normalisation that looks defensive,
// widening a scope.
func TestAWhitespaceTeamIDStaysScopedToThatTeam(t *testing.T) {
	for _, teamID := range []string{" ", "\t", "  "} {
		t.Run(fmt.Sprintf("teamID=%q", teamID), func(t *testing.T) {
			conn := &queryRecordingConn{}
			// A real loader, so the explicit-team route actually reaches a
			// query. Without one it panics before issuing anything, and the
			// "no discovery query" assertion would pass because NO query
			// happened -- which is the exact false pass round 3 found.
			loader, err := NewRecommendationsLoader(conn, "org-1")
			if err != nil {
				t.Fatalf("new loader: %v", err)
			}
			executor := &RecommendationsExecutor{conn: conn, loader: loader}

			// Both routes fail downstream on this stub -- the explicit route
			// reaches an unimplemented method and PANICS. That is recovered on
			// purpose: the assertion is on the query issued BEFORE the failure,
			// not on how the call ends. Recovering rather than implementing the
			// whole loader keeps the test aimed at the branch it is about.
			func() {
				defer func() { _ = recover() }()
				_, _ = executor.ComputeOrg(
					context.Background(), "org-1", time.Now().UTC(), 30, "1.0.0", teamID)
			}()

			for _, query := range conn.queries {
				if strings.Contains(query, "SELECT DISTINCT team_id") {
					t.Fatalf("a whitespace-only team id reached team DISCOVERY; the "+
						"reference treats it as an explicit team, so this widens a "+
						"malformed team-scoped request to the whole org. query: %s",
						query)
				}
			}

			// AND IT MUST ACTUALLY EVALUATE THAT TEAM. Round 3 found this
			// assertion was satisfied by an early rejection deeper in
			// ComputeTeam: no discovery query was issued because NO query was
			// issued at all. "Did not take the wrong path" is not "took the
			// right one", and the negative alone cannot tell them apart.
			if len(conn.queries) == 0 {
				t.Error("no query was issued at all — the explicit-team path was " +
					"rejected before reaching the loader, so this test's " +
					"no-discovery assertion passed for the wrong reason")
			}
		})
	}
}

// TestAPerTeamFailureIsReportedRatherThanSwallowed pins the distinction that a
// round-2 fix erased and round 3 caught.
//
// That fix replaced `failedTeams = append(...); continue` with
// `cancelled = ctx.Err(); break`. On a LIVE context ctx.Err() is nil, so an
// ordinary loader or rule failure recorded no failed team, skipped every
// remaining team, and returned SUCCESS. A silent failure introduced while
// fixing a different silent failure.
//
// The test uses a LIVE context on purpose: the cancelled case was the one the
// author was thinking about, and is exactly the case that hides this.
func TestAPerTeamFailureIsReportedRatherThanSwallowed(t *testing.T) {
	conn := &queryRecordingConn{}
	// A REAL loader over the recording conn: its query returns an error, which
	// is what makes the team fail. A nil loader would panic instead, and a
	// panic is not the failure this test is about.
	loader, err := NewRecommendationsLoader(conn, "org-1")
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	executor := &RecommendationsExecutor{conn: conn, loader: loader}

	// Explicit team, so discovery is skipped and the single team's evaluation
	// is what fails -- against a live, uncancelled context.
	outcome, computeErr := executor.ComputeOrg(
		context.Background(), "org-1", time.Now().UTC(), 30, "1.0.0", "team-a")
	err = computeErr

	if err == nil {
		t.Fatal("a failing team produced no error; the run reported success while " +
			"writing no tombstone for that team, so its stale fired guidance " +
			"would persist with nothing to alert on")
	}
	var failure *TeamEvaluationFailure
	if !errors.As(err, &failure) {
		t.Fatalf("failed with %v, want a TeamEvaluationFailure naming the team", err)
	}
	if len(failure.FailedTeams) != 1 || failure.FailedTeams[0] != "team-a" {
		t.Errorf("failure names %v, want exactly [team-a]", failure.FailedTeams)
	}
	if outcome.FailedTeams != 1 {
		t.Errorf("outcome reports %d failed teams, want 1", outcome.FailedTeams)
	}
}

// TestCancellationReportsTheInterruptionRatherThanATeamFailure covers the part
// of round 2's cancellation fix this level CAN reach.
//
// # WHAT THIS TEST DOES NOT ESTABLISH, STATED BECAUSE IT LOOKS LIKE IT DOES
//
// The fix has two halves: report the cancellation (covered here), and STILL
// WRITE what clean teams already produced (not covered here). Removing the
// detached-write context does not fail this test — verified by mutation, it
// SURVIVED.
//
// The reason is the fixture, not the assertion: the only team fails, so there
// are no records, so writeRecommendations returns early in both the fixed and
// the mutated world. The fixture never enters the state that distinguishes
// them. Reaching it needs a team that SUCCEEDS and a cancellation after it,
// which needs a real loader against real data — so the write half belongs to
// the container suite, and is listed as an open gap rather than left to look
// covered.
//
// Naming this is the point. A test called "still persists" that cannot observe
// persistence is worse than no test: it occupies the slot where the real check
// would go.
func TestCancellationReportsTheInterruptionRatherThanATeamFailure(t *testing.T) {
	batch := &recordingBatch{}
	conn := &batchingConn{batch: batch}
	loader, err := NewRecommendationsLoader(conn, "org-1")
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	executor := &RecommendationsExecutor{conn: conn, loader: loader}

	// Already cancelled before the first team is evaluated: the harshest case,
	// and the one where a write is most likely to be skipped as pointless.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcome, err := executor.ComputeOrg(ctx, "org-1", time.Now().UTC(), 30, "1.0.0", "team-a")

	if !errors.Is(err, context.Canceled) {
		t.Errorf("returned %v, want the cancellation — an interrupted run must "+
			"report cancellation rather than a per-team failure, whose team list "+
			"would understate what went unevaluated", err)
	}
	// The outcome is still populated on the error path, so a caller can record
	// what the interrupted run did reach.
	if outcome.Teams != 1 {
		t.Errorf("outcome reports %d teams, want 1", outcome.Teams)
	}
}

// emptyRows is a valid, empty result set: no rows, no error.
//
// That is enough for the loader to build a zero-valued snapshot, which is
// enough for the engine to emit a tombstone per rule -- so ComputeTeam SUCCEEDS
// and the run carries records without needing a test seam to fabricate them.
// The seam that used to do this was removed rather than guarded, because
// guarding it meant importing `testing` into a package shipped in the worker
// binary.
type emptyRows struct{}

func (emptyRows) Next() bool                         { return false }
func (emptyRows) Scan(...any) error                  { return nil }
func (emptyRows) ScanStruct(any) error               { return nil }
func (emptyRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (emptyRows) Totals(...any) error                { return nil }
func (emptyRows) Columns() []string                  { return nil }
func (emptyRows) Close() error                       { return nil }
func (emptyRows) Err() error                         { return nil }
func (emptyRows) HasData() bool                      { return false }

// loadingConn answers every loader query with an empty result and records the
// context the WRITE was attempted on.
type loadingConn struct {
	driverConnStub
	batch    *recordingBatch
	prepared bool
	// errAtCall is ctx.Err() sampled AT THE MOMENT of the call, not the
	// context itself. ComputeOrg cancels its own bounded write context via
	// stopWrite() as soon as the write returns, so a context stored here and
	// inspected afterwards is cancelled either way -- and the test would fail
	// against correct code. Sampling at the call is the only reading that
	// answers "was the write executable when it was attempted".
	errAtCall error
}

func (conn *loadingConn) Query(
	_ context.Context, _ string, _ ...any,
) (chdriver.Rows, error) {
	return emptyRows{}, nil
}

func (conn *loadingConn) PrepareBatch(
	ctx context.Context, query string, _ ...chdriver.PrepareBatchOption,
) (chdriver.Batch, error) {
	// Sampled at the call, not stored -- see errAtCall.
	conn.errAtCall = ctx.Err()
	conn.prepared = true
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return conn.batch, nil
}

// TestTheWriteContextIsDetachedFromCancellation pins the WithoutCancel decision
// itself, rather than the rows it protects.
//
// # WHY THIS EXISTS AT UNIT LEVEL AT ALL
//
// The container test proves the end-to-end property: rows computed before an
// interruption still land. This one is narrower and cheaper -- it asserts only
// that the write is ATTEMPTED on a live context when the run was cancelled --
// and it exists because the alternative was a production seam. Removing that
// seam left every PERSISTED ROWS assertion behind `//go:build integration`, so
// `go test ./...` stayed green with the detach removed. CI runs the integration
// tag, so that was lost LOCAL feedback rather than a CI hole; this restores it
// without putting `testing` into shipped code.
//
// No seam is needed because a valid EMPTY result set is enough: the loader
// builds a zero snapshot, the engine emits a tombstone per rule, and the run
// therefore carries records into the write with every team having succeeded.
func TestTheWriteContextIsDetachedFromCancellation(t *testing.T) {
	batch := &recordingBatch{}
	conn := &loadingConn{batch: batch}
	loader, err := NewRecommendationsLoader(conn, "org-1")
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	executor := &RecommendationsExecutor{conn: conn, loader: loader}

	// Cancelled BEFORE the run, so the write is guaranteed to be reached with
	// the run in a cancelled state -- no timing, no hook, no successful-team
	// choreography.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcome, err := executor.ComputeOrg(ctx, "org-1", time.Now().UTC(), 30, "1.0.0", "team-a")

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("returned %v, want context.Canceled", err)
	}
	if !conn.prepared {
		t.Fatal("the write was never ATTEMPTED — a cancelled run must still try " +
			"to persist what it computed, on a context detached from the " +
			"cancellation")
	}
	if conn.errAtCall != nil {
		t.Errorf("the write was attempted on an ALREADY-CANCELLED context (%v); "+
			"WithoutCancel is what makes the insert executable at all, so "+
			"without it the rows are lost exactly when the run is torn down",
			conn.errAtCall)
	}
	if outcome.RowsWritten == 0 {
		t.Error("no rows written despite the team completing")
	}
}

// TestTheWriteContextClosesTheGapAfterTheCancellationSample pins round 5's fix
// (CHAOS-4935 gate review): the write context must be detached and bounded
// UNCONDITIONALLY, not only when the post-loop `cancelled` sample happened to
// observe a cancellation.
//
// TestTheWriteContextIsDetachedFromCancellation above cancels BEFORE the run
// starts, so the sample reads non-nil and the (then-conditional) detach fired
// -- it cannot reach this gap. This test cancels from beforeWriteHook, which
// runs strictly AFTER that sample has already read ctx.Err() == nil and
// strictly BEFORE the write context is chosen: exactly the interval a real
// shutdown can land in between one team succeeding and the batch actually
// being prepared, which round 4's fix left open. Reproduced first against the
// pre-fix code (conditional detach, gated on `cancelled != nil`): this test
// failed both on the returned error and on RowsWritten before the fix landed.
func TestTheWriteContextClosesTheGapAfterTheCancellationSample(t *testing.T) {
	batch := &recordingBatch{}
	conn := &loadingConn{batch: batch}
	loader, err := NewRecommendationsLoader(conn, "org-1")
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	executor := &RecommendationsExecutor{
		conn:   conn,
		loader: loader,
		beforeWriteHook: func() {
			// Fires after the post-loop sample already read ctx.Err() == nil
			// (the team loop had exactly one team, which succeeded) and
			// before the write context is chosen -- the TOCTOU gap itself.
			cancel()
		},
	}

	outcome, err := executor.ComputeOrg(ctx, "org-1", time.Now().UTC(), 30, "1.0.0", "team-a")

	if err != nil {
		t.Fatalf("returned %v, want nil — a cancellation landing strictly "+
			"AFTER the post-loop sample must not be allowed to fail the "+
			"write of rows a team already completed", err)
	}
	if !conn.prepared {
		t.Fatal("the write was never ATTEMPTED")
	}
	if conn.errAtCall != nil {
		t.Errorf("the write was attempted on a context already cancelled at "+
			"call time (%v) — the write context must be detached "+
			"UNCONDITIONALLY, not only when the earlier sample happened to "+
			"observe cancellation", conn.errAtCall)
	}
	if outcome.RowsWritten == 0 {
		t.Error("no rows written despite the team completing before the " +
			"gap cancellation")
	}
}
