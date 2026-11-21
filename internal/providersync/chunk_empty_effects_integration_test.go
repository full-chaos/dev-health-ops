//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const emptyEffectsRepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

var errEmptyEffectsFault = errors.New("injected fault")

type statementTracer struct {
	mu   sync.Mutex
	sqls []string
}

func (tracer *statementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.sqls = append(tracer.sqls, strings.Join(strings.Fields(data.SQL), " "))
	return ctx
}

func (tracer *statementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *statementTracer) since(from int) []string {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return append([]string(nil), tracer.sqls[from:]...)
}

func (tracer *statementTracer) mark() int {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return len(tracer.sqls)
}

type emptyEffectsEnv struct {
	pool       *pgxpool.Pool
	repository *PostgresRepository
	claim      Claim
	sink       TestOpsClickHouseEffects
	tracer     *statementTracer
}

func startEmptyEffectsClickHouse(t *testing.T, ctx context.Context) driver.Conn {
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

func startEmptyEffectsPostgres(t *testing.T, ctx context.Context, conn driver.Conn) emptyEffectsEnv {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	tracer := &statementTracer{}
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb,
		since_at='2026-07-22T12:00:00Z', before_at='2026-07-22T20:00:00Z' WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: 10 * time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{Repository: repository, Claim: claim, LeaseDuration: 10 * time.Minute, Deadline: now.Add(time.Hour)}
	return emptyEffectsEnv{
		pool: pool, repository: repository, claim: claim, tracer: tracer,
		sink: TestOpsClickHouseEffects{Conn: conn, Lease: session},
	}
}

// emptyEffectsEmission is one TestOps emission split into sub-chunks of 100 rows: `cases` test case rows and, when
// withSuites, one suite row per sub-chunk's worth of cases, so a sub-chunk has one or two effects with rows.
func emptyEffectsEmission(t *testing.T, run string, cases int, withSuites bool) []PreparedProviderChunk {
	t.Helper()
	stamp := time.Now().UTC().Truncate(time.Millisecond)
	suite := hashTestIdentifier("suite", run)
	caseRows := make([]testCaseResultRow, 0, cases)
	for i := 0; i < cases; i++ {
		name := fmt.Sprintf("TestSynthetic%05d", i)
		duration := 0.01
		caseRows = append(caseRows, testCaseResultRow{
			OrgID: "org-acme", RepoID: emptyEffectsRepoID, RunID: run, SuiteID: suite,
			CaseID: hashTestIdentifier(suite, name), CaseName: name, Status: "passed", DurationSeconds: &duration, LastSynced: stamp,
		})
	}
	var suiteRows []testSuiteResultRow
	if withSuites {
		for i := 0; i < (cases+99)/100; i++ {
			suiteRows = append(suiteRows, testSuiteResultRow{
				OrgID: "org-acme", RepoID: emptyEffectsRepoID, RunID: run, SuiteID: hashTestIdentifier("suite", run, fmt.Sprint(i)),
				SuiteName: fmt.Sprintf("synthetic-suite-%d", i), LastSynced: stamp,
			})
		}
	}
	effects, err := testOpsEffects(nil, nil, nil, suiteRows, caseRows, nil)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	resetEmissionChunks(chunks, ChunkRouteEmission{CursorBefore: "b-" + run, CursorAfter: "a-" + run})
	if err := sizeEmissionChunks(chunks); err != nil {
		t.Fatal(err)
	}
	for index := range chunks {
		chunks[index].Ordinal = index
		chunks[index].ManifestDigest = preparedChunkDigest(chunks[index])
	}
	return chunks
}

func (env emptyEffectsEnv) prepare(t *testing.T, ctx context.Context, chunks []PreparedProviderChunk) []PreparedProviderChunk {
	t.Helper()
	prepared, err := env.repository.PrepareChunkGroup(ctx, env.claim, chunks, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

// drain is the executor's per-sub-chunk step: load the sidecar, commit its effects, mark it.
func (env emptyEffectsEnv) drain(
	ctx context.Context, store ChunkedEffectStore, sink EffectSink, readback EffectReadback, ordinal int,
) (EffectCommitResult, error) {
	clock := func() time.Time { return time.Now().UTC() }
	chunk, err := store.LoadPreparedChunk(ctx, env.claim, ordinal, clock())
	if err != nil {
		return EffectCommitResult{}, err
	}
	result, err := commitPreparedChunk(ctx, env.claim, chunk, sink, readback, clock, store)
	if err != nil {
		return result, err
	}
	return result, store.MarkChunkCommitted(ctx, env.claim, ordinal, chunk.ManifestDigest, clock())
}

type emptyEffectsChunkRow struct {
	ledger, status            string
	nextOrdinal               int
	committedRows             int64
	pendingEntries, committed int
}

func (env emptyEffectsEnv) row(t *testing.T, ctx context.Context, ordinal int) emptyEffectsChunkRow {
	t.Helper()
	var row emptyEffectsChunkRow
	if err := env.pool.QueryRow(ctx, `SELECT ledger::text, status FROM public.sync_run_unit_effect_chunks
		WHERE sync_run_unit_id=$1::uuid AND ordinal=$2`, firstUnitID, ordinal).Scan(&row.ledger, &row.status); err != nil {
		t.Fatal(err)
	}
	if err := env.pool.QueryRow(ctx, `SELECT next_ordinal, committed_rows FROM public.sync_run_unit_chunk_checkpoints
		WHERE sync_run_unit_id=$1::uuid`, firstUnitID).Scan(&row.nextOrdinal, &row.committedRows); err != nil {
		t.Fatal(err)
	}
	row.pendingEntries = strings.Count(row.ledger, `"status": "pending"`)
	row.committed = strings.Count(row.ledger, `"status": "committed"`)
	return row
}

func (env emptyEffectsEnv) caseRows(t *testing.T, ctx context.Context, run string) uint64 {
	t.Helper()
	var count uint64
	if err := env.sink.Conn.QueryRow(ctx, `SELECT count() FROM test_case_results FINAL WHERE org_id = ? AND run_id = ?`,
		"org-acme", run).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// The executor's per-sub-chunk PG cost: 12 statements per effect with rows (Begin tx 5, the sink's 2 lease checks, Commit
// tx 5) and 6 for the mark, which also commits the empty effects. Before CHAOS-14445 every empty effect paid 12 too: 90.
func TestChunkEmptyEffectsStatementCountOnPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	conn := startEmptyEffectsClickHouse(t, ctx)
	effectWithRows := []string{
		"begin", "SELECT TRUE FROM public.sync_run_units", "SELECT chunk.schema_version", "UPDATE public.sync_run_unit_effect_chunks SET ledger=", "commit",
		"SELECT EXISTS", "SELECT EXISTS",
		"begin", "SELECT TRUE FROM public.sync_run_units", "SELECT chunk.schema_version", "UPDATE public.sync_run_unit_effect_chunks SET ledger=", "commit",
	}
	mark := []string{
		"begin", "SELECT TRUE FROM public.sync_run_units", "SELECT chunk.schema_version",
		"UPDATE public.sync_run_unit_effect_chunks SET status='committed', ledger=COALESCE", "UPDATE public.sync_run_unit_chunk_checkpoints", "commit",
	}
	for _, testCase := range []struct {
		name       string
		cases      int
		withSuites bool
	}{
		{name: "one effect with rows", cases: 300},
		{name: "two effects with rows", cases: 100, withSuites: true},
		{name: "every effect empty", cases: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := startEmptyEffectsPostgres(t, ctx, conn)
			run := "empty-effects-count-" + strings.ReplaceAll(testCase.name, " ", "-")
			chunks := emptyEffectsEmission(t, run, testCase.cases, testCase.withSuites)
			prepared := env.prepare(t, ctx, chunks)
			for index, chunk := range prepared {
				if len(chunk.Ledger.Effects) != 7 || chunk.ManifestDigest != chunks[index].ManifestDigest ||
					chunk.ManifestDigest != preparedChunkDigest(chunk) {
					t.Fatalf("ordinal %d: the seven-entry ledger or its digest changed: %d entries", index, len(chunk.Ledger.Effects))
				}
				for entryIndex, entry := range chunk.Ledger.Effects {
					if entry.Destination != chunk.Effects[entryIndex].Destination || entry.ContentDigest != chunk.Effects[entryIndex].ContentDigest {
						t.Fatalf("ordinal %d entry %d does not pair with its batch", index, entryIndex)
					}
				}
				start := env.tracer.mark()
				result, err := env.drain(ctx, env.repository, env.sink, env.sink, chunk.Ordinal)
				if err != nil {
					t.Fatal(err)
				}
				statements := env.tracer.since(start)[1:] // [0] is the drain's LoadPreparedChunk, outside the 90
				withRows := 0
				for _, effect := range chunk.Effects {
					if len(effect.Rows) > 0 {
						withRows++
					}
				}
				want := make([]string, 0, 12*withRows+6)
				for i := 0; i < withRows; i++ {
					want = append(want, effectWithRows...)
				}
				want = append(want, mark...)
				if len(statements) != len(want) || result.Written != 7 {
					t.Fatalf("ordinal %d: %d statements, written %d; WANT %d (12 per effect with rows + 6), written 7:\n%s",
						index, len(statements), result.Written, len(want), strings.Join(statements, "\n"))
				}
				for i := range want {
					if !strings.HasPrefix(statements[i], want[i]) {
						t.Fatalf("ordinal %d statement %d = %q, want prefix %q", index, i, statements[i], want[i])
					}
				}
				row := env.row(t, ctx, chunk.Ordinal)
				if row.status != "committed" || row.committed != 7 || row.pendingEntries != 0 {
					t.Fatalf("ordinal %d after the mark: %+v", index, row)
				}
			}
			if got := env.caseRows(t, ctx, run); got != uint64(testCase.cases) {
				t.Fatalf("test_case_results rows=%d want %d", got, testCase.cases)
			}
		})
	}
}

// faultChunkStore keeps the PostgresRepository's opt-in (embedded) and fails the mark before or after it is durable.
type faultChunkStore struct {
	*PostgresRepository
	failMark, lostAck bool
}

func (store *faultChunkStore) MarkChunkCommitted(ctx context.Context, claim Claim, ordinal int, digest string, now time.Time) error {
	if store.failMark {
		store.failMark = false
		return errEmptyEffectsFault
	}
	err := store.PostgresRepository.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
	if store.lostAck && err == nil {
		store.lostAck = false
		return errEmptyEffectsFault
	}
	return err
}

// legacyChunkStore hides the opt-in: its committer runs every effect through Begin, Write and Commit, as before the change.
type legacyChunkStore struct{ ChunkedEffectStore }

type faultEffectSink struct {
	inner EffectSink
	// failBefore / failAfter name the destination whose next write fails before / after the inner write.
	failBefore, failAfter string
	writes                []string
}

func (sink *faultEffectSink) WriteEffect(ctx context.Context, claim Claim, batch EffectBatch) error {
	sink.writes = append(sink.writes, fmt.Sprintf("%s:%d", batch.Destination, len(batch.Rows)))
	if batch.Destination == sink.failBefore {
		sink.failBefore = ""
		return errEmptyEffectsFault
	}
	if err := sink.inner.WriteEffect(ctx, claim, batch); err != nil {
		return err
	}
	if batch.Destination == sink.failAfter {
		sink.failAfter = ""
		return errEmptyEffectsFault
	}
	return nil
}

func (sink *faultEffectSink) emptyEffectIsNoOp(destination string) bool {
	noOp, ok := sink.inner.(emptyEffectNoOpSink)
	return ok && noOp.emptyEffectIsNoOp(destination)
}

type countingReadback struct {
	inner EffectReadback
	calls []string
}

func (readback *countingReadback) InspectEffect(ctx context.Context, claim Claim, batch EffectBatch) (EffectInspection, error) {
	inspection, err := readback.inner.InspectEffect(ctx, claim, batch)
	readback.calls = append(readback.calls, fmt.Sprintf("%s:%d:%s", batch.Destination, len(batch.Rows), inspection))
	return inspection, err
}

// A crash at each point of a sub-chunk whose empty effects the mark commits, and a resume by the new code or the legacy
// code: every row lands once, the checkpoint counts it once, no empty effect is read back unless legacy code left it
// writing, and the ledger ends fully committed.
func TestChunkEmptyEffectsCrashRecoveryOnPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	conn := startEmptyEffectsClickHouse(t, ctx)
	for _, testCase := range []struct {
		name  string
		cases int
		// crash runs the first attempt and must fail it; resume runs the next attempt (nil: the new code).
		crash        func(env emptyEffectsEnv) error
		resumeLegacy bool
		wantResume   EffectCommitResult
		wantInspect  []string
		wantWrites   []string
	}{
		{name: "after prepare", cases: 50,
			crash:      func(emptyEffectsEnv) error { return errEmptyEffectsFault },
			wantResume: EffectCommitResult{Written: 7}, wantWrites: []string{"test_case_results:50"}},
		{name: "effect with rows written, not committed", cases: 50,
			crash: func(env emptyEffectsEnv) error {
				_, err := env.drain(ctx, env.repository, &faultEffectSink{inner: env.sink, failAfter: "test_case_results"}, env.sink, 0)
				return err
			},
			wantResume: EffectCommitResult{Written: 6, MarkedCommitted: 1}, wantInspect: []string{"test_case_results:50:exact"}},
		{name: "effects committed, mark not", cases: 50,
			crash: func(env emptyEffectsEnv) error {
				_, err := env.drain(ctx, &faultChunkStore{PostgresRepository: env.repository, failMark: true}, env.sink, env.sink, 0)
				return err
			},
			wantResume: EffectCommitResult{Written: 6, Skipped: 1}},
		{name: "every effect empty, mark not", cases: 0,
			crash: func(env emptyEffectsEnv) error {
				_, err := env.drain(ctx, &faultChunkStore{PostgresRepository: env.repository, failMark: true}, env.sink, env.sink, 0)
				return err
			},
			wantResume: EffectCommitResult{Written: 7}},
		{name: "new code crashed before the mark, legacy code resumes", cases: 50, resumeLegacy: true,
			crash: func(env emptyEffectsEnv) error {
				_, err := env.drain(ctx, &faultChunkStore{PostgresRepository: env.repository, failMark: true}, env.sink, env.sink, 0)
				return err
			},
			wantResume: EffectCommitResult{Written: 6, Skipped: 1},
			wantWrites: []string{"ci_acceptance_checks:0", "ci_job_runs:0", "ci_pipeline_runs:0", "coverage_snapshots:0", "test_suite_results:0", "testops_run_report_completeness:0"}},
		{name: "legacy code crashed with an empty effect writing, new code resumes", cases: 50,
			crash: func(env emptyEffectsEnv) error {
				_, err := env.drain(ctx, legacyChunkStore{env.repository}, &faultEffectSink{inner: env.sink, failBefore: "ci_acceptance_checks"}, env.sink, 0)
				return err
			},
			wantResume:  EffectCommitResult{Written: 7, ResetForReplay: 1},
			wantInspect: []string{"ci_acceptance_checks:0:absent"}, wantWrites: []string{"test_case_results:50"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := startEmptyEffectsPostgres(t, ctx, conn)
			run := "empty-effects-crash-" + strings.ReplaceAll(testCase.name, " ", "-")
			env.prepare(t, ctx, emptyEffectsEmission(t, run, testCase.cases, false))
			if err := testCase.crash(env); !errors.Is(err, errEmptyEffectsFault) {
				t.Fatalf("crash: %v", err)
			}
			var store ChunkedEffectStore = env.repository
			if testCase.resumeLegacy {
				store = legacyChunkStore{env.repository}
			}
			sink := &faultEffectSink{inner: env.sink}
			readback := &countingReadback{inner: env.sink}
			result, err := env.drain(ctx, store, sink, readback, 0)
			if err != nil || result != testCase.wantResume {
				t.Fatalf("resume: result=%+v err=%v, want %+v", result, err, testCase.wantResume)
			}
			if strings.Join(readback.calls, ",") != strings.Join(testCase.wantInspect, ",") {
				t.Fatalf("readbacks=%v want %v", readback.calls, testCase.wantInspect)
			}
			if strings.Join(sink.writes, ",") != strings.Join(testCase.wantWrites, ",") {
				t.Fatalf("resume writes=%v want %v", sink.writes, testCase.wantWrites)
			}
			row := env.row(t, ctx, 0)
			if row.status != "committed" || row.committed != 7 || row.nextOrdinal != 1 || row.committedRows != int64(testCase.cases) {
				t.Fatalf("after resume: %+v", row)
			}
			if got := env.caseRows(t, ctx, run); got != uint64(testCase.cases) {
				t.Fatalf("test_case_results rows=%d want %d", got, testCase.cases)
			}
		})
	}
}

// The mark is the only writer of a deferred empty effect, so it must prove each flip, stay fenced by the claim, and write
// nothing on a replay.
func TestChunkEmptyEffectsMarkGuardsOnPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	conn := startEmptyEffectsClickHouse(t, ctx)

	t.Run("a pending effect with rows refuses the mark", func(t *testing.T) {
		env := startEmptyEffectsPostgres(t, ctx, conn)
		prepared := env.prepare(t, ctx, emptyEffectsEmission(t, "empty-effects-guard-rows", 50, false))
		before := env.row(t, ctx, 0)
		err := env.repository.MarkChunkCommitted(ctx, env.claim, 0, prepared[0].ManifestDigest, time.Now().UTC())
		if !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Fatalf("mark err=%v, want ErrChunkCheckpointConflict", err)
		}
		if after := env.row(t, ctx, 0); after != before || after.status != "pending" || after.pendingEntries != 7 {
			t.Fatalf("a refused mark changed the chunk:\nbefore %+v\nafter  %+v", before, after)
		}
	})

	t.Run("a replayed mark leaves the ledger byte-identical", func(t *testing.T) {
		env := startEmptyEffectsPostgres(t, ctx, conn)
		env.prepare(t, ctx, emptyEffectsEmission(t, "empty-effects-guard-replay", 50, false))
		store := &faultChunkStore{PostgresRepository: env.repository, lostAck: true}
		if _, err := env.drain(ctx, store, env.sink, env.sink, 0); !errors.Is(err, errEmptyEffectsFault) {
			t.Fatalf("lost ack: %v", err)
		}
		marked := env.row(t, ctx, 0)
		chunk, err := env.repository.LoadPreparedChunk(ctx, env.claim, 0, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := env.repository.MarkChunkCommitted(ctx, env.claim, 0, chunk.ManifestDigest, time.Now().UTC().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if replayed := env.row(t, ctx, 0); replayed != marked || marked.committed != 7 || marked.committedRows != 50 {
			t.Fatalf("replay changed the chunk:\nmarked   %+v\nreplayed %+v", marked, replayed)
		}
	})

	t.Run("a lost lease refuses the mark of an all-empty chunk", func(t *testing.T) {
		env := startEmptyEffectsPostgres(t, ctx, conn)
		prepared := env.prepare(t, ctx, emptyEffectsEmission(t, "empty-effects-guard-lease", 0, false))
		start := env.tracer.mark()
		result, err := commitPreparedChunk(ctx, env.claim, prepared[0], env.sink, env.sink, func() time.Time { return time.Now().UTC() }, env.repository)
		if err != nil || result.Written != 7 || len(env.tracer.since(start)) != 0 {
			t.Fatalf("all-empty commit: result=%+v err=%v statements=%d", result, err, len(env.tracer.since(start)))
		}
		before := env.row(t, ctx, 0)
		if _, err := env.pool.Exec(ctx, `UPDATE public.sync_run_units SET lease_expires_at = now() - interval '1 minute' WHERE id=$1::uuid`, firstUnitID); err != nil {
			t.Fatal(err)
		}
		err = env.repository.MarkChunkCommitted(ctx, env.claim, 0, prepared[0].ManifestDigest, time.Now().UTC())
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("mark under a lost lease: %v", err)
		}
		if after := env.row(t, ctx, 0); after != before || after.status != "pending" || after.pendingEntries != 7 || after.nextOrdinal != 0 {
			t.Fatalf("a fenced mark changed the chunk:\nbefore %+v\nafter  %+v", before, after)
		}
	})
}
