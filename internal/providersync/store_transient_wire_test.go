package providersync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// wireFault answers one protocol step with a SQLSTATE, `times` times. A class
// 57 or 08 code is sent FATAL and the connection is closed, like a lost
// connection; any other code is an ERROR.
type wireFault struct {
	step  string
	code  string
	times int
}

// storeWire is a scripted PostgreSQL server for the unit writes: every step is
// counted and a fault can be put on any one of them, deterministically, which a
// real server cannot do for a read or a lost acknowledgement. The unit row is
// modelled by one flag: done, once a unit write committed.
type storeWire struct {
	mu               sync.Mutex
	faults           []*wireFault
	done             bool
	updates          int // unit UPDATEs: one per write attempt
	commits          int // write transactions that committed
	reads            int // read-backs
	stamps           int // needs-attention stamp UPDATEs
	wakes            int // dispatch wake INSERTs
	sourceID         string
	firstCommitState string
	probablyReading  bool
	pending          bool
	lockedReads      int
	readAcksLost     int
	clampCeiling     string
	shortStreak      bool
	stampMissing     bool
}

func (w *storeWire) pool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://wire:wire@127.0.0.1/wire?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go w.serve(server)
		return client, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// fault reports whether step fails now, and spends one of its fires.
func (w *storeWire) fault(step string) string {
	for _, fault := range w.faults {
		if fault.step == step && fault.times > 0 {
			fault.times--
			return fault.code
		}
	}
	return ""
}

type wireConn struct {
	backend   *pgproto3.Backend
	status    byte
	wroteUnit bool
	readTx    bool
}

func (w *storeWire) serve(conn net.Conn) {
	defer conn.Close()
	c := &wireConn{backend: pgproto3.NewBackend(conn, conn), status: 'I'}
	if _, err := c.backend.ReceiveStartupMessage(); err != nil {
		return
	}
	c.backend.Send(&pgproto3.AuthenticationOk{})
	c.backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	c.backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if c.backend.Flush() != nil {
		return
	}
	for {
		message, err := c.backend.Receive()
		if err != nil {
			return
		}
		query, ok := message.(*pgproto3.Query)
		if !ok {
			return
		}
		w.mu.Lock()
		keep := w.answer(c, strings.TrimSpace(query.String))
		w.mu.Unlock()
		if !keep {
			_ = c.backend.Flush()
			return
		}
		c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: c.status})
		if c.backend.Flush() != nil {
			return
		}
	}
}

func wireTag(c *wireConn, tag string) {
	c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

func wireRows(c *wireConn, types []uint32, rows ...[]string) {
	fields := make([]pgproto3.FieldDescription, len(types))
	for index, oid := range types {
		fields[index] = pgproto3.FieldDescription{
			Name: []byte(fmt.Sprintf("c%d", index)), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1,
		}
	}
	c.backend.Send(&pgproto3.RowDescription{Fields: fields})
	for _, row := range rows {
		values := make([][]byte, len(row))
		for index, value := range row {
			values[index] = []byte(value)
		}
		c.backend.Send(&pgproto3.DataRow{Values: values})
	}
	wireTag(c, fmt.Sprintf("SELECT %d", len(rows)))
}

// fail sends code; it returns false when the connection is to be closed.
func wireFail(c *wireConn, code string) bool {
	if strings.HasPrefix(code, "57") || strings.HasPrefix(code, "08") {
		c.backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: "injected"})
		return false
	}
	c.backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: "injected"})
	if c.status == 'T' {
		c.status = 'E'
	}
	return true
}

// commitPoint applies a unit write and then answers. A lost-connection fault
// (class 57 or 08) closes the connection after the write took effect, a lost
// acknowledgement; any other fault refuses the COMMIT and nothing is applied.
func (w *storeWire) commitPoint(c *wireConn, tag string) bool {
	code := w.fault("commit")
	lost := strings.HasPrefix(code, "57") || strings.HasPrefix(code, "08")
	if c.wroteUnit && lost {
		w.probablyReading = true
		switch w.firstCommitState {
		case "race":
			w.pending = true
		case "ours", "other", "missing":
		default:
			w.done = true
			w.commits++
		}
	} else if c.wroteUnit && code == "" {
		w.done = true
		w.commits++
	}
	c.wroteUnit = false
	if code != "" {
		c.status = 'I'
		return wireFail(c, code)
	}
	wireTag(c, tag)
	return true
}

func (w *storeWire) answer(c *wireConn, sql string) bool {
	lower := strings.ToLower(sql)
	switch {
	case lower == "begin":
		if w.probablyReading {
			if w.readAcksLost > 0 {
				if code := w.fault("post_read_commit_begin"); code != "" {
					return wireFail(c, code)
				}
			}
			if code := w.fault("read_begin"); code != "" {
				return wireFail(c, code)
			}
		}
		c.status, c.readTx = 'T', false
		wireTag(c, "BEGIN")
	case lower == "rollback":
		if c.readTx {
			c.status, c.readTx = 'I', false
			if code := w.fault("read_end"); code != "" {
				w.readAcksLost++
				return wireFail(c, code)
			}
			// read_commit is the reviewer's fault on ending the read
			// transaction: it fires on its ROLLBACK as on its COMMIT.
			if code := w.fault("read_commit"); code != "" {
				w.readAcksLost++
				if code == "EOF" {
					return false
				}
				return wireFail(c, code)
			}
			w.probablyReading = false
		}
		c.status, c.wroteUnit, c.readTx = 'I', false, false
		wireTag(c, "ROLLBACK")
	case lower == "commit":
		if c.status == 'E' {
			c.status, c.wroteUnit = 'I', false
			wireTag(c, "ROLLBACK")
			return true
		}
		c.status = 'I'
		if c.readTx {
			c.readTx = false
			if code := w.fault("read_end"); code != "" {
				w.readAcksLost++
				return wireFail(c, code)
			}
			if code := w.fault("read_commit"); code != "" {
				w.readAcksLost++
				if code == "EOF" {
					return false
				}
				return wireFail(c, code)
			}
			w.probablyReading = false
			wireTag(c, "COMMIT")
			return true
		}
		return w.commitPoint(c, "COMMIT")
	case strings.HasPrefix(lower, "rollback to savepoint"):
		if code := w.fault("savepoint_rollback"); code != "" {
			return wireFail(c, code)
		}
		c.status = 'T'
		wireTag(c, "ROLLBACK")
	case strings.HasPrefix(lower, "release savepoint"):
		if code := w.fault("savepoint_release"); code != "" {
			return wireFail(c, code)
		}
		wireTag(c, "RELEASE")
	case strings.HasPrefix(lower, "savepoint"):
		if code := w.fault("savepoint"); code != "" {
			return wireFail(c, code)
		}
		wireTag(c, "SAVEPOINT")
	case c.status == 'E':
		c.backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "25P02", Message: "aborted"})
	case strings.HasPrefix(sql, "SELECT set_config("):
		if code := w.fault("read_config"); code != "" {
			return wireFail(c, code)
		}
		wireRows(c, []uint32{25}, []string{"5000ms"})
	case strings.HasPrefix(sql, "SELECT unit.status ="):
		w.reads++
		c.readTx = c.status == 'T'
		if code := w.fault("read_back"); code != "" {
			return wireFail(c, code)
		}
		locking := strings.HasSuffix(strings.ToUpper(sql), "FOR SHARE OF UNIT")
		if locking {
			w.lockedReads++
		}
		applyPending := func() {
			if w.pending {
				w.pending, w.done = false, true
				w.commits++
			}
		}
		if locking {
			applyPending()
		}
		committed, ours := "f", "t"
		if w.done {
			committed, ours = "t", "f"
		} else if w.firstCommitState == "other" {
			ours = "f"
		} else if w.firstCommitState == "missing" {
			wireRows(c, []uint32{16, 16})
			return true
		}
		wireRows(c, []uint32{16, 16}, []string{committed, ours})
		applyPending()
	case strings.HasPrefix(sql, "WITH contention AS"):
		// The budget-contention deferral: one UPDATE ... RETURNING the unit's own
		// deferral count. Zero rows when the unit is not this owner's.
		w.updates++
		if code := w.fault("unit_update"); code != "" {
			return wireFail(c, code)
		}
		if w.done || w.firstCommitState == "other" && w.updates > 1 {
			wireRows(c, []uint32{23})
			return true
		}
		c.wroteUnit = true
		c.backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("c0"), DataTypeOID: 23, DataTypeSize: -1, TypeModifier: -1},
		}})
		c.backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		if c.status == 'I' {
			return w.commitPoint(c, "SELECT 1")
		}
		wireTag(c, "SELECT 1")
	case strings.HasPrefix(sql, "UPDATE public.sync_run_units"):
		w.updates++
		if code := w.fault("unit_update"); code != "" {
			return wireFail(c, code)
		}
		if w.done || w.firstCommitState == "other" && w.updates > 1 {
			wireTag(c, "UPDATE 0")
			return true
		}
		c.wroteUnit = true
		if c.status == 'I' {
			return w.commitPoint(c, "UPDATE 1")
		}
		wireTag(c, "UPDATE 1")
	case strings.HasPrefix(sql, "SELECT 1 FROM public.sync_runs"):
		wireRows(c, []uint32{23}, []string{"1"})
	case strings.HasPrefix(sql, "UPDATE public.sync_runs"):
		if code := w.fault("bump"); code != "" {
			return wireFail(c, code)
		}
		wireRows(c, []uint32{23, 23, 23}, []string{"1", "1", "3"})
	case strings.HasPrefix(sql, "SELECT to_regclass"):
		wireRows(c, []uint32{16}, []string{"f"})
	case strings.Contains(sql, "AS cause_class") && strings.Contains(sql, "FROM public.sync_run_units"):
		if code := w.fault("streak"); code != "" {
			return wireFail(c, code)
		}
		row := []string{w.sourceID, "t", "unclassified", "2026-11-19 00:00:00+00"}
		if w.shortStreak {
			wireRows(c, []uint32{25, 16, 25, 1184}, row)
		} else {
			wireRows(c, []uint32{25, 16, 25, 1184}, row, row, row)
		}
	case strings.HasPrefix(sql, "UPDATE public.integration_datasets"):
		w.stamps++
		if code := w.fault("stamp"); code != "" {
			return wireFail(c, code)
		}
		if w.stampMissing {
			wireTag(c, "UPDATE 0")
		} else {
			wireTag(c, "UPDATE 1")
		}
	case strings.Contains(sql, "'dispatch_sync_run'") && strings.Contains(sql, "INSERT"):
		w.wakes++
		wireTag(c, "INSERT 0 1")
	default:
		wireTag(c, "UPDATE 1")
	}
	return true
}

// wireRun is one write against a storeWire, with the logs and metrics it left.
type wireRun struct {
	err     error
	logs    string
	metrics string
}

func runWireWrite(t *testing.T, w *storeWire, write string, watermark *time.Time) wireRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claim := nativeTestClaim("github", "commits")
	var linearRoute LinearWorkItemsRouteBatch
	if strings.HasPrefix(write, "fail") || strings.Contains(write, "family") {
		claim, linearRoute = linearTypedLifecycleFixture(t)
		claim.LeaseExpiresAt = time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)
	}
	w.sourceID = claim.SourceID
	repository, err := NewPostgresRepository(w.pool(t, ctx), providerfoundation.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	repository.storeRetrySleep = func(context.Context, int) error { return nil }
	previous := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	if w.clampCeiling == "window_end" {
		earlier := start.Add(-2 * time.Hour)
		claim.BeforeAt = &earlier
	}
	end := start.Add(2 * time.Second)
	var run wireRun
	switch write {
	case "complete", "complete_family":
		run.err = repository.Complete(ctx, claim, map[string]any{"records": 1}, watermark, start, end)
	case "typed_family":
		sink := linearWorkItemEffectsFixture(newLinearSemanticEffectBackend(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
		result, _, err := (LinearWorkItemsLifecycle{
			Committer: EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return start }},
		}).Commit(ctx, claim, linearRoute, start)
		if err != nil {
			t.Fatal(err)
		}
		if watermark != nil {
			result.Watermark = *watermark
		}
		run.err = repository.CompleteLinearWorkItemFamily(ctx, claim, result, start, end)
	case "fail_class":
		run.err = repository.FailWithCauseClass(ctx, claim, "provider_error", CauseClassUnclassified, nil, start, end)
	case "fail_duplicate":
		run.err = repository.FailWithDuplicateKeyDetail(ctx, claim, CauseClassUnclassified, "test_suite_results",
			[]DuplicateNaturalKeyField{{Name: "run_id", Value: "1"}}, start, end)
	case "fail":
		run.err = repository.Fail(ctx, claim, "provider_error", start, end)
	case "release":
		run.err = repository.ReleaseForRetry(ctx, claim, end)
	default:
		t.Fatalf("unknown write %q", write)
	}
	slog.SetDefault(previous)
	run.logs = logs.String()
	var metrics bytes.Buffer
	if err := repository.Metrics.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	run.metrics = metrics.String()
	return run
}

func rollupBumpCount(t *testing.T, outcome string) uint64 {
	t.Helper()
	var buffer bytes.Buffer
	if err := providerfoundation.SyncRunRollupBumpedMetricsSource().WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	label := fmt.Sprintf("dev_health_sync_run_rollup_bumped_total{outcome=%q,path=\"provider_unit\"} ", outcome)
	for _, line := range strings.Split(buffer.String(), "\n") {
		if value, ok := strings.CutPrefix(line, label); ok {
			var count uint64
			if _, err := fmt.Sscanf(value, "%d", &count); err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	return 0
}

func dispatchWakesArmed() uint64 {
	return syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed)
}

var wireWrites = []string{"complete", "fail", "release"}

// P1-1 (r1): the COMMIT acknowledgement is lost after the write committed and
// the first read-back fails too (a lost connection, a connection exception, a
// lock timeout). The read-back is tried again and finds the commit.
func TestReadBackRetriesAFailedRead(t *testing.T) {
	for _, write := range wireWrites {
		for _, code := range []string{"57P01", "08006", "55P03"} {
			t.Run(write+"/"+code, func(t *testing.T) {
				w := &storeWire{faults: []*wireFault{
					{step: "commit", code: "57P01", times: 1},
					{step: "read_back", code: code, times: 1},
				}}
				run := runWireWrite(t, w, write, nil)
				if run.err != nil {
					t.Fatalf("%s()=%v, want the second read-back to find the commit", write, run.err)
				}
				if w.updates != 1 || w.commits != 1 || w.reads != 2 {
					t.Fatalf("updates=%d commits=%d reads=%d, want 1, 1, 2", w.updates, w.commits, w.reads)
				}
			})
		}
	}
}

// P1-1 (r1): a read-back that never succeeds leaves the outcome unknown: the
// write answers the transient class after storeWriteAttempts reads, never
// ErrLeaseLost and never a replayed write.
func TestReadBackThatNeverSucceedsIsStoreTransient(t *testing.T) {
	for _, write := range wireWrites {
		t.Run(write, func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{
				{step: "commit", code: "57P01", times: 1},
				{step: "read_back", code: "57P01", times: 100},
			}}
			run := runWireWrite(t, w, write, nil)
			if !errors.Is(run.err, ErrStoreTransient) || errors.Is(run.err, ErrLeaseLost) {
				t.Fatalf("%s()=%v, want ErrStoreTransient", write, run.err)
			}
			if w.reads != storeWriteAttempts || w.updates != 1 {
				t.Fatalf("reads=%d updates=%d, want %d reads and no replayed write", w.reads, w.updates, storeWriteAttempts)
			}
		})
	}
}

// P1-4 (r1): ReleaseForRetry's acknowledgement is lost after the release took
// effect. The read-back (keyed on the release's own updated_at) finds it:
// success, one UPDATE -- not ErrLeaseLost from a replay that matches no row.
func TestReleaseWithALostAcknowledgementReadsBackTheRelease(t *testing.T) {
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "57P01", times: 1}}}
	run := runWireWrite(t, w, "release", nil)
	if run.err != nil {
		t.Fatalf("ReleaseForRetry()=%v, want the read-back to find the release", run.err)
	}
	if w.updates != 1 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("updates=%d commits=%d reads=%d, want 1, 1, 1", w.updates, w.commits, w.reads)
	}
}

// P1-3 (r1): a write the read-back confirmed runs the same post-commit steps as
// an acknowledged one: the wake, the needs-attention line and the failure
// counter once; the rollup bump is a WARN plus the counter's ambiguous_commit
// outcome (its counts are not re-read), never a bump with counts.
func TestCommitConfirmedByTheReadBackRunsThePostCommitSteps(t *testing.T) {
	for _, write := range []string{"complete", "fail"} {
		t.Run(write, func(t *testing.T) {
			outcome := "success"
			if write == "fail" {
				outcome = "failed"
			}
			ambiguousBefore := rollupBumpCount(t, "ambiguous_commit")
			bumpedBefore := rollupBumpCount(t, outcome)
			wakesBefore := dispatchWakesArmed()
			w := &storeWire{faults: []*wireFault{{step: "commit", code: "57P01", times: 1}}}
			run := runWireWrite(t, w, write, nil)
			if run.err != nil {
				t.Fatalf("%s()=%v, want the read-back to find the commit", write, run.err)
			}
			if got := rollupBumpCount(t, "ambiguous_commit") - ambiguousBefore; got != 1 {
				t.Fatalf("ambiguous_commit bumps=%d, want 1", got)
			}
			if got := rollupBumpCount(t, outcome) - bumpedBefore; got != 0 {
				t.Fatalf("%s bumps=%d, want 0 (counts unknown)", outcome, got)
			}
			if got := strings.Count(run.logs, "sync_run.rollup_bump_ambiguous"); got != 1 {
				t.Fatalf("ambiguous rollup WARN lines=%d, want 1", got)
			}
			if got := dispatchWakesArmed() - wakesBefore; got != 1 || w.wakes != 1 {
				t.Fatalf("wakes armed=%d sent=%d, want 1 and 1", got, w.wakes)
			}
			if write == "fail" {
				if got := strings.Count(run.logs, "providersync.needs_attention_stamped"); got != 1 {
					t.Fatalf("needs-attention lines=%d, want 1", got)
				}
				if got := strings.Count(run.metrics, "dev_health_provider_unit_failed_total{"); got != 1 {
					t.Fatalf("unit failed series=%d, want 1", got)
				}
			}
		})
	}
}

// P1-5 (r1): a transient error anywhere in the needs-attention savepoint is
// not swallowed: one cell per class (40P01, 40001, 57P01, 08xxx). The whole
// failure transaction is retried in place and commits once, with its stamp.
func TestNeedsAttentionSavepointPropagatesTransientErrors(t *testing.T) {
	for _, fault := range []wireFault{
		{step: "streak", code: "40P01"},
		{step: "stamp", code: "40001"},
		{step: "savepoint", code: "57P01"},
		{step: "savepoint_release", code: "08006"},
	} {
		t.Run(fault.step+"/"+fault.code, func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{{step: fault.step, code: fault.code, times: 1}}}
			run := runWireWrite(t, w, "fail", nil)
			if run.err != nil {
				t.Fatalf("Fail()=%v, want the retried transaction to commit", run.err)
			}
			if w.updates != 2 || w.commits != 1 {
				t.Fatalf("unit updates=%d commits=%d, want 2 attempts and 1 commit", w.updates, w.commits)
			}
			if got := strings.Count(run.logs, "providersync.needs_attention_stamped"); got != 1 {
				t.Fatalf("needs-attention lines=%d, want 1", got)
			}
		})
	}
}

// P2 (r1): the watermark clamp WARN belongs to a committed write. A write the
// database refuses on every attempt logs no clamp; an acknowledged one logs it
// once.
func TestWatermarkClampIsLoggedOnlyAfterCommit(t *testing.T) {
	skewed := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC).Add(time.Hour)
	for _, testCase := range []struct {
		name   string
		faults []*wireFault
		want   int
	}{
		{"refused_at_commit", []*wireFault{{step: "commit", code: "40001", times: 100}}, 0},
		{"acknowledged", nil, 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			w := &storeWire{faults: testCase.faults}
			run := runWireWrite(t, w, "complete", &skewed)
			if got := strings.Count(run.logs, "sync.watermark.write_clamped"); got != testCase.want {
				t.Fatalf("clamp WARN lines=%d (err=%v), want %d", got, run.err, testCase.want)
			}
		})
	}
}

var round2Writes = []string{"complete", "typed_family", "fail", "fail_class", "fail_duplicate", "release"}

func reviewStatus(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, ErrStoreTransient):
		return "store_transient"
	case errors.Is(err, ErrInvalidConfiguration):
		return "invalid_configuration"
	default:
		return fmt.Sprint(err)
	}
}

func reviewOutcome(t *testing.T, w *storeWire, run wireRun, want string, updates, commits, reads int) {
	t.Helper()
	got := reviewStatus(run.err)
	t.Logf("result=%s committed=%t updates=%d commits=%d reads=%d locking_reads=%d read_commit_disconnects=%d wakes=%d",
		got, w.done, w.updates, w.commits, w.reads, w.lockedReads, w.readAcksLost, w.wakes)
	if got != want || w.updates != updates || w.commits != commits || w.reads != reads {
		t.Errorf("want result=%s updates=%d commits=%d reads=%d", want, updates, commits, reads)
	}
}

func TestRound2ReadBackMatrix(t *testing.T) {
	for _, write := range round2Writes {
		for _, state := range []struct {
			name, outcome    string
			updates, commits int
		}{
			{"committed", "success", 1, 1}, {"ours", "success", 2, 1}, {"other", "lease_lost", 1, 0}, {"missing", "lease_lost", 1, 0},
		} {
			for _, code := range []string{"57P01", "08006", "55P03"} {
				t.Run(write+"/"+state.name+"/"+code, func(t *testing.T) {
					w := &storeWire{firstCommitState: state.name, faults: []*wireFault{
						{step: "commit", code: "57P01", times: 1}, {step: "read_back", code: code, times: 1},
					}}
					result := runWireWrite(t, w, write, nil)
					reviewOutcome(t, w, result, state.outcome, state.updates, state.commits, 2)
				})
			}
		}
		for _, step := range []string{"read_begin", "read_config", "read_back"} {
			for _, n := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/faults_%d", write, step, n), func(t *testing.T) {
					w := &storeWire{faults: []*wireFault{
						{step: "commit", code: "57P01", times: 1}, {step: step, code: "57P01", times: n},
					}}
					run := runWireWrite(t, w, write, nil)
					want, reads := "success", 1
					if n == 3 {
						want, reads = "store_transient", 0
					}
					if step == "read_back" {
						reads += n
					}
					reviewOutcome(t, w, run, want, 1, 1, reads)
				})
			}
		}
	}
}

func TestRound2CommitLockRace(t *testing.T) {
	for _, write := range round2Writes {
		t.Run(write, func(t *testing.T) {
			w := &storeWire{firstCommitState: "race", faults: []*wireFault{{step: "commit", code: "57P01", times: 1}}}
			run := runWireWrite(t, w, write, nil)
			reviewOutcome(t, w, run, "success", 1, 1, 1)
			if w.lockedReads != 1 {
				t.Fatal("did not wait using a row-locking read")
			}
		})
	}
}

func TestRound2PostCommitMatrix(t *testing.T) {
	for _, write := range round2Writes {
		for _, commit := range []string{"ack", "lost_ack", "retry", "refused"} {
			t.Run(write+"/"+commit, func(t *testing.T) {
				w := &storeWire{}
				want, updates, commits, reads := "success", 1, 1, 0
				metric := "success"
				if strings.HasPrefix(write, "fail") {
					metric = "failed"
				}
				switch commit {
				case "lost_ack":
					w.faults = []*wireFault{{step: "commit", code: "57P01", times: 1}}
					metric, reads = "ambiguous_commit", 1
				case "retry":
					w.faults = []*wireFault{{step: "commit", code: "40001", times: 1}}
					updates = 2
				case "refused":
					w.faults = []*wireFault{{step: "commit", code: "40001", times: 3}}
					want, updates, commits = "store_transient", 3, 0
				}
				before := rollupBumpCount(t, metric)
				run := runWireWrite(t, w, write, nil)
				reviewOutcome(t, w, run, want, updates, commits, reads)
				bumps := int(rollupBumpCount(t, metric) - before)
				rollupLogs := strings.Count(run.logs, "sync_run.rollup_bump")
				attention := strings.Count(run.logs, "providersync.needs_attention_stamped")
				failureMetric := strings.Count(run.metrics, "dev_health_provider_unit_failed_total{")
				wantRollup, wantFail := commits, 0
				if write == "release" {
					wantRollup = 0
				}
				if strings.HasPrefix(write, "fail") {
					wantFail = commits
				}
				t.Logf("rollup_label=%s rollup_bumps=%d rollup_logs=%d attention=%d failure_series=%d",
					metric, bumps, rollupLogs, attention, failureMetric)
				if bumps != wantRollup || rollupLogs != wantRollup || w.wakes != wantRollup || attention != wantFail || failureMetric != wantFail {
					t.Errorf("want rollup bumps/logs/wakes=%d attention/failure series=%d", wantRollup, wantFail)
				}
			})
		}
	}
}

func TestRound2SavepointMatrix(t *testing.T) {
	for _, write := range []string{"fail", "fail_class", "fail_duplicate"} {
		for _, branch := range []string{"savepoint", "streak", "stamp", "savepoint_release", "rollback_streak_error", "rollback_short_streak", "rollback_stamp_error", "rollback_zero_stamp"} {
			for _, code := range []string{"40P01", "40001", "57P01", "08006"} {
				for _, n := range []int{1, 3} {
					t.Run(fmt.Sprintf("%s/%s/%s/faults_%d", write, branch, code, n), func(t *testing.T) {
						w := &storeWire{}
						step := branch
						if strings.HasPrefix(branch, "rollback") {
							step = "savepoint_rollback"
						}
						w.faults = []*wireFault{{step: step, code: code, times: n}}
						switch branch {
						case "rollback_streak_error":
							w.faults = append(w.faults, &wireFault{step: "streak", code: "22000", times: n})
						case "rollback_short_streak":
							w.shortStreak = true
						case "rollback_stamp_error":
							w.faults = append(w.faults, &wireFault{step: "stamp", code: "22000", times: n})
						case "rollback_zero_stamp":
							w.stampMissing = true
						}
						run := runWireWrite(t, w, write, nil)
						if n == 1 {
							reviewOutcome(t, w, run, "success", 2, 1, 0)
						} else {
							reviewOutcome(t, w, run, "store_transient", 3, 0, 0)
						}
					})
				}
			}
		}
	}
}

func TestRound2ClampMatrix(t *testing.T) {
	for _, write := range []string{"complete", "complete_family", "typed_family"} {
		for _, bound := range []string{"window_end", "now"} {
			for _, failure := range []string{"refused_update_40P01", "refused_update_42501", "refused_commit", "ack", "lost_ack", "retry"} {
				t.Run(write+"/"+bound+"/"+failure, func(t *testing.T) {
					w := &storeWire{}
					// Fixtures have a July 2026 cutoff. Wall clock is chosen
					// first for the later cutoff in runWireWrite.
					skewed := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC).Add(time.Hour)
					w.clampCeiling = bound
					want := 1
					if write != "complete" {
						want = len(workitemcontract.FamilyDatasets())
					}
					switch failure {
					case "refused_update_40P01":
						w.faults = []*wireFault{{step: "unit_update", code: "40P01", times: 3}}
						want = 0
					case "refused_update_42501":
						w.faults = []*wireFault{{step: "unit_update", code: "42501", times: 1}}
						want = 0
					case "refused_commit":
						w.faults = []*wireFault{{step: "commit", code: "40001", times: 3}}
						want = 0
					case "lost_ack":
						w.faults = []*wireFault{{step: "commit", code: "57P01", times: 1}}
					case "retry":
						w.faults = []*wireFault{{step: "commit", code: "40001", times: 1}}
					}
					run := runWireWrite(t, w, write, &skewed)
					warnings := strings.Count(run.logs, "sync.watermark.write_clamped")
					t.Logf("result=%s commits=%d updates=%d clamp_warnings=%d", reviewStatus(run.err), w.commits, w.updates, warnings)
					if warnings != want || warnings > 0 && !strings.Contains(run.logs, "ceiling="+bound) {
						t.Errorf("want %d warnings with bound %s, logs=%q", want, bound, run.logs)
					}
				})
			}
		}
	}
}

func TestRound2ConfirmedSelectSurvivesLostReadCommitAck(t *testing.T) {
	for _, write := range round2Writes {
		for _, scenario := range []struct {
			name, state, want string
			extra             []*wireFault
			updates, commits  int
		}{
			{"continued_outage", "committed", "success", []*wireFault{{step: "read_commit", code: "EOF", times: 1}, {step: "post_read_commit_begin", code: "57P01", times: 2}}, 1, 1},
			{"repeat_disconnect", "committed", "success", []*wireFault{{step: "read_commit", code: "08006", times: 3}}, 1, 1},
			{"lease_taken", "other", "lease_lost", []*wireFault{{step: "read_commit", code: "EOF", times: 1}, {step: "post_read_commit_begin", code: "57P01", times: 2}}, 1, 0},
			{"same_owner", "ours", "success", []*wireFault{{step: "read_commit", code: "08006", times: 3}}, 2, 1},
			{"row_gone_control", "missing", "lease_lost", []*wireFault{{step: "read_commit", code: "EOF", times: 1}, {step: "post_read_commit_begin", code: "57P01", times: 2}}, 1, 0},
		} {
			t.Run(write+"/"+scenario.name, func(t *testing.T) {
				faults := append([]*wireFault{{step: "commit", code: "57P01", times: 1}}, scenario.extra...)
				w := &storeWire{firstCommitState: scenario.state, faults: faults}
				run := runWireWrite(t, w, write, nil)
				reviewOutcome(t, w, run, scenario.want, scenario.updates, scenario.commits, 1)
				if w.readAcksLost == 0 {
					t.Fatal("premise: the fault on ending the read transaction never fired")
				}
				t.Logf("post_commit_rollup_warns=%d needs_attention_infos=%d", strings.Count(run.logs, "sync_run.rollup_bump_ambiguous"), strings.Count(run.logs, "providersync.needs_attention_stamped"))
			})
		}
	}
}

// r2 P1: the read-back's SELECT ... FOR SHARE completed, then ending its
// lock-only transaction loses the connection (read_end: FATAL on its COMMIT or
// ROLLBACK, every time). The outcome comes from the completed SELECT, so a
// confirmed commit is success, not ErrStoreTransient after failed re-reads. The
// control: a SELECT that itself fails every time still answers ErrStoreTransient.
func TestReadBackOutcomeSurvivesALostReadTransactionEnd(t *testing.T) {
	for _, write := range round2Writes {
		t.Run(write+"/read_end_lost", func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{
				{step: "commit", code: "57P01", times: 1},
				{step: "read_end", code: "08006", times: 100},
			}}
			run := runWireWrite(t, w, write, nil)
			reviewOutcome(t, w, run, "success", 1, 1, 1)
			if w.readAcksLost != 1 {
				t.Fatalf("read transaction ends lost=%d, want 1 (premise: the fault fired)", w.readAcksLost)
			}
		})
		t.Run(write+"/select_fails_control", func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{
				{step: "commit", code: "57P01", times: 1},
				{step: "read_back", code: "57P01", times: 100},
			}}
			run := runWireWrite(t, w, write, nil)
			reviewOutcome(t, w, run, "store_transient", 1, 1, storeWriteAttempts)
		})
	}
}
