//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Real Postgres and ClickHouse cells for the 500-row TestOps policy (synthetic rows only).

const rowsRepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

type rowsPolicyHandler struct {
	jobs, cases, failingPercent int
	// bigMessageBytes, when set, gives the case in the middle of the emission a failure message of that many bytes.
	bigMessageBytes int
}

func (handler rowsPolicyHandler) Collect(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler rowsPolicyHandler) CollectChunks(_ context.Context, claim Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	at time.Time, resumeCursor string, emit func(ChunkRouteEmission) error) error {
	stamp := at.UTC().Truncate(time.Millisecond)
	if resumeCursor == "" {
		jobs := make([]githubTestsJobRow, 0, handler.jobs)
		for index := 0; index < handler.jobs; index++ {
			jobs = append(jobs, githubTestsJobRow{OrgID: claim.OrgID, RepoID: rowsRepoID, RunID: "r" + strconv.Itoa(index), JobID: "j" + strconv.Itoa(index), JobName: "synthetic job", LastSynced: stamp})
		}
		cases := make([]testCaseResultRow, 0, handler.cases)
		suite := hashTestIdentifier("suite", "run-1")
		for index := 0; index < handler.cases; index++ {
			name := fmt.Sprintf("TestSyntheticBehaviour%05d", index)
			class := "pkg/synthetic.Suite"
			row := testCaseResultRow{OrgID: claim.OrgID, RepoID: rowsRepoID, RunID: "run-1", SuiteID: suite, CaseID: hashTestIdentifier(suite, name),
				CaseName: name, ClassName: &class, Status: "passed", LastSynced: stamp}
			if handler.bigMessageBytes > 0 && index == handler.cases/2 {
				message := strings.Repeat("m", handler.bigMessageBytes)
				row.Status, row.FailureMessage = "failed", &message
			}
			if index%100 < handler.failingPercent {
				message, kind, trace := "expected 42 got 41 "+strings.Repeat("m", 120), "AssertionError", strings.Repeat("at synthetic.frame(file.go:123)\n", 200)[:4096]
				row.Status, row.FailureMessage, row.FailureType, row.StackTrace = "failed", &message, &kind, &trace
			}
			cases = append(cases, row)
		}
		effects, err := testOpsEffects(nil, jobs, nil, nil, cases, nil)
		if err != nil {
			return err
		}
		if err := emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: effects}, CursorAfter: `{"page":1}`}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: `{"page":1}`, CursorAfter: `{"page":1}`, Final: true})
}

// S1: a unit prepared under one row bound is resumed under the other, both directions (the rollback), with a crash that leaves an effect
// `writing` (the readback path) and with the policy flipping every attempt: each row is written once, no prepared chunk is re-split.
func TestTestOpsRouteMixedRowBoundResumeBothDirections(t *testing.T) {
	type step struct {
		rows, bound, failJobWrite int
	}
	scenarios := map[string][]step{
		"100-count-stop-then-500":     {{100, 3, 0}, {500, 64, 0}},
		"100-crash-in-group-then-500": {{100, 64, 4}, {500, 64, 0}},
		"500-count-stop-then-100":     {{500, 1, 0}, {100, 64, 0}},
		"500-crash-in-group-then-100": {{500, 64, 2}, {100, 64, 0}},
		"alternating":                 {{100, 2, 0}, {500, 2, 0}, {100, 2, 0}, {500, 64, 0}},
	}
	for name, steps := range scenarios {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			repository, session, pool := runsGroupPostgres(t, ctx)
			sink := newRunsGroupSink()
			digests := func() map[int]string {
				rows, err := pool.Query(ctx, `SELECT ordinal, manifest_digest, payload_bytes FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid ORDER BY ordinal`, firstUnitID)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				out := map[int]string{}
				for rows.Next() {
					var ordinal, bytes int
					var digest string
					if err := rows.Scan(&ordinal, &digest, &bytes); err != nil {
						t.Fatal(err)
					}
					out[ordinal] = digest
				}
				return out
			}
			var first map[int]string
			run := func(index int, one step) error {
				descriptor := cicdDescriptor(t)
				descriptor.ChunkPolicy.MaxSourceItems, descriptor.ChunkPolicy.MaxChunksPerAttempt = one.rows, one.bound
				sink.failJobWrite, sink.jobWrites = one.failJobWrite, 0
				_, err := runsGroupExecutor(time.Now().UTC(), rowsPolicyHandler{jobs: 1000}, repository, sink, newRunsGroupDoer(t, 1, 1), nil).Execute(ctx, session, descriptor)
				if index == 0 {
					first = digests()
				}
				return err
			}
			for index, one := range steps {
				if err := run(index, one); err != nil && !errors.As(err, new(ChunkContinuationError)) && one.failJobWrite == 0 {
					t.Fatalf("step %d: %v", index, err)
				}
			}
			for attempt := 0; attempt < 30; attempt++ {
				err := run(1, steps[len(steps)-1])
				if err == nil {
					break
				}
				if !errors.As(err, new(ChunkContinuationError)) {
					t.Fatalf("finish: %v", err)
				}
			}
			duplicates := 0
			for _, count := range sink.jobRows {
				if count > 1 {
					duplicates++
				}
			}
			if len(sink.jobRows) != 1000 || duplicates != 0 {
				t.Fatalf("distinct job rows=%d written twice=%d, want 1000 and 0", len(sink.jobRows), duplicates)
			}
			final := digests()
			for ordinal, digest := range first {
				if final[ordinal] != digest {
					t.Fatalf("prepared chunk %d changed: %s -> %s (a prepared group is replayed as stored)", ordinal, digest, final[ordinal])
				}
			}
		})
	}
}

func startRowsClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	chschema.Apply(ctx, t, instance)
	return conn
}

// S2: a failing-heavy emission (every case with a 4 KiB trace) through the real executor, store and ClickHouse sink: 500 rows exceed 2 MiB,
// the split packs fewer rows per chunk, no chunk is refused, every stored chunk holds payload + ledger under 2 MiB, and
// ClickHouse holds each case once.
func TestTestOpsRouteFailingHeavyEmissionPacksUnderTwoMiBOnRealStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := startRowsClickHouse(t, ctx)
	repository, session, pool := runsGroupPostgres(t, ctx)
	sink := TestOpsClickHouseEffects{Conn: conn, Lease: session}
	executor := runsGroupExecutor(time.Now().UTC(), rowsPolicyHandler{cases: 2000, failingPercent: 100}, repository, newRunsGroupSink(), newRunsGroupDoer(t, 1, 1), nil)
	executor.Committer.Sink, executor.Committer.Readback = sink, sink
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 1000
	if _, err := executor.Execute(ctx, session, descriptor); err != nil {
		t.Fatalf("execute: %v", err)
	}
	rows, err := pool.Query(ctx, `SELECT payload_bytes, payload_bytes + octet_length(ledger::text), status FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid ORDER BY ordinal`, firstUnitID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	chunks, largest := 0, 0
	for rows.Next() {
		var payload, withLedger int
		var status string
		if err := rows.Scan(&payload, &withLedger, &status); err != nil {
			t.Fatal(err)
		}
		chunks++
		largest = max(largest, withLedger)
		if withLedger > maxChunkPayloadBytes || status != "committed" {
			t.Fatalf("stored chunk payload+ledger=%d status=%s, want <= %d and committed", withLedger, status, maxChunkPayloadBytes)
		}
	}
	if chunks < 5 || chunks > 8 {
		t.Fatalf("stored chunks=%d, want 2,000 failing rows (~5 KB each) packed about 400 to a chunk (5) plus the final chunk", chunks)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM test_case_results FINAL WHERE org_id = ?`, session.Claim.OrgID).Scan(&stored); err != nil || stored != 2000 {
		t.Fatalf("clickhouse holds %d cases (err=%v), want 2000", stored, err)
	}
	t.Logf("stored chunks=%d largest payload+ledger=%d", chunks, largest)
}

// S4: a crash after the ClickHouse write of a 500-row test_case_results effect leaves it `writing`; the resumed attempt reads it back with
// one point read per row (500 statements), marks it committed and never writes it again. A statement count, not a time.
func TestTestOpsRouteReadbackOfFiveHundredRowsRunsInsideTheAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := startRowsClickHouse(t, ctx)
	repository, session, _ := runsGroupPostgres(t, ctx)
	real := TestOpsClickHouseEffects{Conn: conn, Lease: session}
	crash := &crashAfterWriteSink{TestOpsClickHouseEffects: real, destination: "test_case_results"}
	descriptor := cicdDescriptor(t)
	build := func(sink EffectSink) CompleteRouteExecutor {
		executor := runsGroupExecutor(time.Now().UTC(), rowsPolicyHandler{cases: 500}, repository, newRunsGroupSink(), newRunsGroupDoer(t, 1, 1), nil)
		executor.Committer.Sink, executor.Committer.Readback = sink, real
		return executor
	}
	if _, err := build(crash).Execute(ctx, session, descriptor); err == nil || !strings.Contains(err.Error(), "crash after write") {
		t.Fatalf("first attempt err=%v, want the simulated crash", err)
	}
	if err := conn.Exec(ctx, `SYSTEM FLUSH LOGS`); err != nil {
		t.Fatal(err)
	}
	var before uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.query_log WHERE type='QueryFinish' AND query_kind='Select' AND query LIKE '%FROM test_case_results FINAL WHERE%' AND query NOT LIKE '%system.query_log%'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	crash.disarm()
	if _, err := build(crash).Execute(ctx, session, descriptor); err != nil {
		t.Fatalf("resumed attempt: %v", err)
	}
	if err := conn.Exec(ctx, `SYSTEM FLUSH LOGS`); err != nil {
		t.Fatal(err)
	}
	var after, stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.query_log WHERE type='QueryFinish' AND query_kind='Select' AND query LIKE '%FROM test_case_results FINAL WHERE%' AND query NOT LIKE '%system.query_log%'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM test_case_results FINAL WHERE org_id = ?`, session.Claim.OrgID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if after-before != 500 || stored != 500 || crash.writes != 1 {
		t.Fatalf("readback statements=%d (want 500), cases stored=%d (want 500), test_case_results writes=%d (want 1: the readback settled it)", after-before, stored, crash.writes)
	}
}

type crashAfterWriteSink struct {
	TestOpsClickHouseEffects
	destination string
	writes      int
	disarmed    bool
}

func (sink *crashAfterWriteSink) disarm() { sink.disarmed = true }

func (sink *crashAfterWriteSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	err := sink.TestOpsClickHouseEffects.WriteEffect(ctx, claim, effect)
	if err == nil && effect.Destination == sink.destination && len(effect.Rows) > 0 {
		sink.writes++
		if !sink.disarmed {
			return errors.New("crash after write")
		}
	}
	return err
}

// The window cell: one case with a failure message that puts its single-row chunk 40 KB over the split's reserved limit sits among 500
// small cases, so main (100-row chunks, 2 MiB limit) accepts the chunk that holds it. The greedy split closes the chunk before it, gives it a
// chunk of its own and goes on with the rest (3 chunks plus the final one at most; the chunk count is in the log line); every row is
// written once to ClickHouse and the store refuses nothing.
func TestTestOpsRouteBigRowInTheReserveWindowIsAllWrittenOnceWithNoRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	conn := startRowsClickHouse(t, ctx)
	repository, session, pool := runsGroupPostgres(t, ctx)
	sink := TestOpsClickHouseEffects{Conn: conn, Lease: session}
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 1000
	handler := rowsPolicyHandler{cases: 500, bigMessageBytes: splitPayloadLimit(TestOpsRouteChunkPolicy()) + 40_000 - 1_450}
	var lastErr error
	for attempt := 0; attempt < 60; attempt++ {
		executor := runsGroupExecutor(time.Now().UTC(), handler, repository, newRunsGroupSink(), newRunsGroupDoer(t, 1, 1), nil)
		executor.Committer.Sink, executor.Committer.Readback = sink, sink
		_, lastErr = executor.Execute(ctx, session, descriptor)
		if lastErr == nil || !errors.As(lastErr, new(ChunkContinuationError)) {
			break
		}
	}
	if lastErr != nil {
		t.Fatalf("the emission did not complete without a store refusal: %v", lastErr)
	}
	var chunks, largest int
	if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(max(payload_bytes), 0) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid`, firstUnitID).Scan(&chunks, &largest); err != nil {
		t.Fatal(err)
	}
	var rows, distinct uint64
	if err := conn.QueryRow(ctx, `SELECT count(), uniqExact(case_id) FROM test_case_results WHERE org_id = ?`, session.Claim.OrgID).Scan(&rows, &distinct); err != nil {
		t.Fatal(err)
	}
	t.Logf("window cell: 500 cases, one big row: %d stored chunks (incl. the final one), largest payload %d bytes, clickhouse rows=%d distinct=%d", chunks, largest, rows, distinct)
	if chunks > 4 {
		t.Fatalf("stored chunks=%d, want at most 4 (rows before the big row, the big row alone, the rest, the final chunk)", chunks)
	}
	if rows != 500 || distinct != 500 {
		t.Fatalf("clickhouse holds %d rows (%d distinct), want every one of the 500 cases written once", rows, distinct)
	}
}
