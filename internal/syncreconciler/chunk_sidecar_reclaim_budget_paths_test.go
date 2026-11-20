package syncreconciler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// slowSite names the one reclaim SQL step that is slow on the server: it waits
// for the stage deadline.
type slowSite string

const (
	slowSiteNone     slowSite = ""
	slowSiteTickLock slowSite = "tick_lock"
	slowSiteBegin    slowSite = "begin"
	slowSiteBacklog  slowSite = "backlog_count"
	slowSiteTicks             = 10
)

type slowSiteRow struct {
	scan func(dest ...any) error
}

func (row slowSiteRow) Scan(dest ...any) error { return row.scan(dest...) }

// slowSiteTx answers the reclaim's statements at once (every delete takes its whole
// byte reservation and a full row batch) except the slow site.
type slowSiteTx struct {
	pgx.Tx
	slow  slowSite
	rows  int64
	calls *slowSiteCalls
}

// slowSiteCalls counts the server work a tick started.
type slowSiteCalls struct {
	locks, counts int
}

func (tx slowSiteTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	switch sql {
	case selectChunkReclaimCandidateSQL:
		return slowSiteRow{func(dest ...any) error {
			*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = "org-a", "unit", "gen"
			return nil
		}}
	case deleteChunkReclaimBatchSQL:
		return slowSiteRow{func(dest ...any) error {
			*dest[0].(*int64), *dest[1].(*int64) = tx.rows, args[4].(int64)
			return nil
		}}
	case countChunkReclaimRemainingSQL:
		tx.calls.counts++
		return slowSiteRow{func(dest ...any) error {
			if tx.slow == slowSiteBacklog {
				<-ctx.Done()
				return ctx.Err()
			}
			*dest[0].(*int64) = 1
			return nil
		}}
	}
	return slowSiteRow{func(...any) error { return errors.New("unexpected statement") }}
}

func (slowSiteTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("DELETE 0"), nil
}

func (slowSiteTx) Commit(context.Context) error   { return nil }
func (slowSiteTx) Rollback(context.Context) error { return nil }

func newSlowSiteReclaim(t *testing.T, slow slowSite) (*ChunkSidecarReclaim, *slowSiteCalls) {
	t.Helper()
	calls := &slowSiteCalls{}
	config := DefaultChunkSidecarReclaimConfig()
	begin := func(ctx context.Context) (pgx.Tx, error) {
		if slow == slowSiteBegin {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return slowSiteTx{slow: slow, rows: int64(config.RowsPerStatement), calls: calls}, nil
	}
	tickLock := func(ctx context.Context) (func(), bool, error) {
		calls.locks++
		if slow == slowSiteTickLock {
			<-ctx.Done()
			return nil, false, ctx.Err()
		}
		return func() {}, true, nil
	}
	reclaim, err := newChunkSidecarReclaim(begin, tickLock, config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)
	reclaim.clock = func() time.Time { return now }
	return reclaim, calls
}

// stepSlowSiteTicks runs ticks under a short stage budget and counts, by operation,
// the ticks the stage deadline cut and the ticks the byte cap throttled.
func stepSlowSiteTicks(t *testing.T, reclaim *ChunkSidecarReclaim) (cut, throttled int) {
	t.Helper()
	now := time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)
	for range slowSiteTicks {
		stageCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		result, err := reclaim.Step(stageCtx, now, 100)
		cut += boolCount(errors.Is(err, context.DeadlineExceeded))
		throttled += boolCount(result.Throttled)
		cancel()
	}
	return cut, throttled
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

// CHAOS-14380 r1 cell (verdict e839127e1b49, P1 class 1): a reclaim step the
// stage deadline cuts outside the delete statement (the tick lock, BEGIN, the
// backlog count) is charged like a cut statement, so a slow server gets at most
// chunkReclaimDeadlineCutsPerWindow budget-long ticks before the cap throttles.
// At fb04ed13 those cuts were free: 10 of 10 ticks cut, none throttled.
func TestChunkSidecarReclaimSlowStepOutsideTheStatementIsChargedAndThrottles(t *testing.T) {
	for _, slow := range []slowSite{slowSiteTickLock, slowSiteBegin, slowSiteBacklog} {
		t.Run(string(slow), func(t *testing.T) {
			reclaim, _ := newSlowSiteReclaim(t, slow)
			cut, throttled := stepSlowSiteTicks(t, reclaim)
			t.Logf("slow %s: cut=%d/%d throttled=%d", slow, cut, slowSiteTicks, throttled)
			if cut > wantSlowSiteDeadlineCuts || throttled == 0 {
				t.Errorf("slow %s: %d of %d ticks cut by the stage deadline, %d throttled; want at most %d cut, then throttled",
					slow, cut, slowSiteTicks, throttled, wantSlowSiteDeadlineCuts)
			}
		})
	}
}

// wantSlowSiteDeadlineCuts is chunkReclaimDeadlineCutsPerWindow restated.
const wantSlowSiteDeadlineCuts = 8

// CHAOS-14380 r1 cell (P1 class 1, after the cap): a tick the cap does not
// admit takes no tick lock and runs no backlog count. At fb04ed13 it ran both,
// and a slow count spent the whole budget on every throttled tick.
func TestChunkSidecarReclaimTickTheCapDoesNotAdmitRunsNoServerWork(t *testing.T) {
	reclaim, calls := newSlowSiteReclaim(t, slowSiteBacklog)
	limit := DefaultChunkSidecarReclaimConfig().BytesPerMinute
	spent := reclaim.limiter.reserve(reclaim.clock(), limit)
	reclaim.limiter.settle(spent, limit, reclaim.clock())
	reclaim.setActive(true)
	cut, throttled := stepSlowSiteTicks(t, reclaim)
	t.Logf("cap spent before the ticks: cut=%d/%d throttled=%d tick_locks=%d backlog_counts=%d",
		cut, slowSiteTicks, throttled, calls.locks, calls.counts)
	if cut != 0 || throttled != slowSiteTicks || calls.locks != 0 || calls.counts != 0 {
		t.Errorf("%d ticks the cap did not admit: %d cut, %d throttled, %d tick locks, %d backlog counts; "+
			"want every tick throttled with no cut, no lock and no count", slowSiteTicks, cut, throttled, calls.locks, calls.counts)
	}
}

// CHAOS-14380 r1 cell (P1 class 2): after 10 yields the floor run is due even
// when tick 11 is not pressured; at fb04ed13 a fast tick 11 ran the
// normal-size Step.
func TestMutationPipelineChunkSidecarReclaimFloorIsDueAfterTenYieldsWhenPressureClears(t *testing.T) {
	const materializerBudget = 40 * time.Millisecond
	slow := true
	reclaim := &floorCountingReclaim{}
	config := DefaultMutationPipelineConfig()
	config.StageBudgets[StageMaterializer] = materializerBudget
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) { return LeaseRepairResult{}, nil }),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			if slow {
				time.Sleep(materializerBudget * 3 / 4)
			}
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) { return Observation{}, nil }),
		nil, nil, nil,
		noopTerminalOutboxClose(),
		noopOrphanedUnitRepair(),
		reclaim,
		config,
	)
	if err != nil {
		t.Fatal(err)
	}
	for tick := 1; tick <= 10; tick++ {
		if _, err := pipeline.Step(context.Background(), time.Now(), 10); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
	}
	slow = false
	if _, err := pipeline.Step(context.Background(), time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	t.Logf("after 10 yields and a fast tick 11: steps=%d floors=%d", reclaim.steps, reclaim.floors)
	if reclaim.floors != 1 || reclaim.steps != 0 {
		t.Errorf("fast tick 11 after 10 yields: %d normal steps, %d floor runs; want the floor run", reclaim.steps, reclaim.floors)
	}
}

// CHAOS-14380 r1 cell (P2): pressure is the budget used from the armed stage
// deadline, so a setup stall (the telemetry mutex, held by a metrics scrape or
// a readiness check) counts. At fb04ed13 the clock started after the setup.
func TestMutationPipelineChunkSidecarReclaimPressureCountsBudgetSpentBeforeTheCallback(t *testing.T) {
	const leaseBudget = 400 * time.Millisecond
	for name, testCase := range map[string]struct {
		hold     time.Duration
		callback func(context.Context) error
	}{
		"deadline before the callback": {600 * time.Millisecond, func(ctx context.Context) error { return ctx.Err() }},
		"deadline in the callback": {250 * time.Millisecond, func(ctx context.Context) error {
			<-ctx.Done()
			return reclaimError(ctx, ErrUnavailable)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			reclaimCalls := 0
			config := DefaultMutationPipelineConfig()
			config.StageBudgets[StageLeaseRepair] = leaseBudget
			pipeline, err := NewMutationPipeline(
				pipelineLeaseRepairFunc(func(ctx context.Context, _ time.Time, _ int) (LeaseRepairResult, error) {
					return LeaseRepairResult{}, testCase.callback(ctx)
				}),
				pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
					return TerminalDeliveryRepairResult{}, nil
				}),
				pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
					return MaterializerResult{}, nil
				}),
				pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
					return KernelResult{}, nil
				}),
				pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) { return Observation{}, nil }),
				nil, nil, nil,
				noopTerminalOutboxClose(),
				noopOrphanedUnitRepair(),
				pipelineChunkSidecarReclaimFunc(func(context.Context, time.Time, int) (ChunkSidecarReclaimResult, error) {
					reclaimCalls++
					return ChunkSidecarReclaimResult{}, nil
				}),
				config,
			)
			if err != nil {
				t.Fatal(err)
			}
			pipeline.stages.mu.Lock()
			done := make(chan error, 1)
			go func() {
				_, stepErr := pipeline.Step(context.Background(), time.Now(), 10)
				done <- stepErr
			}()
			time.Sleep(testCase.hold)
			pipeline.stages.mu.Unlock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			failures, _ := pipeline.stages.snapshot()
			t.Logf("%s: lease_repair failures=%d reclaim calls=%d", name, failures[StageLeaseRepair], reclaimCalls)
			if failures[StageLeaseRepair] != 1 {
				t.Fatalf("lease repair failures = %d, want 1 (its budget was spent)", failures[StageLeaseRepair])
			}
			if reclaimCalls != 0 {
				t.Errorf("reclaim ran %d times in a tick whose lease repair spent its budget; want a yield", reclaimCalls)
			}
		})
	}
}

// CHAOS-14380 cell: remaining_units=-1 means "not measured" (a tick the cap did
// not admit, or a tick cut before or at the backlog count), never "no backlog":
// every such tick returns and logs done=false, so a reader of the
// syncreconciler.chunk_sidecar_reclaim line cannot take it as done.
func TestChunkSidecarReclaimUnmeasuredBacklogIsNeverDone(t *testing.T) {
	now := time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)
	capSpent := func(reclaim *ChunkSidecarReclaim) {
		limit := DefaultChunkSidecarReclaimConfig().BytesPerMinute
		spent := reclaim.limiter.reserve(reclaim.clock(), limit)
		reclaim.limiter.settle(spent, limit, reclaim.clock())
	}
	for name, testCase := range map[string]struct {
		slow    slowSite
		prepare func(*ChunkSidecarReclaim)
	}{
		"not admitted by the cap": {slowSiteNone, capSpent},
		"tick lock cut":           {slowSiteTickLock, func(*ChunkSidecarReclaim) {}},
		"begin cut":               {slowSiteBegin, func(*ChunkSidecarReclaim) {}},
		"backlog count cut":       {slowSiteBacklog, func(*ChunkSidecarReclaim) {}},
	} {
		t.Run(name, func(t *testing.T) {
			reclaim, _ := newSlowSiteReclaim(t, testCase.slow)
			reclaim.setActive(true)
			testCase.prepare(reclaim)
			var logs strings.Builder
			original := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			stageCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			result, _ := reclaim.Step(stageCtx, now, 100)
			cancel()
			slog.SetDefault(original)
			if result.RemainingUnits != -1 || result.Done {
				t.Fatalf("result = %+v, want remaining_units -1 and done false", result)
			}
			lines := 0
			for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var line struct {
					Msg            string `json:"msg"`
					RemainingUnits *int64 `json:"remaining_units"`
					Done           *bool  `json:"done"`
				}
				if err := json.Unmarshal([]byte(raw), &line); err != nil {
					t.Fatalf("log line %q: %v", raw, err)
				}
				if line.Msg != "syncreconciler.chunk_sidecar_reclaim" || line.RemainingUnits == nil || *line.RemainingUnits != -1 {
					continue
				}
				lines++
				if line.Done == nil || *line.Done {
					t.Errorf("logged %s, want done=false beside remaining_units=-1", raw)
				}
			}
			if lines != 1 {
				t.Fatalf("logged %d tick lines with remaining_units=-1, want 1: %s", lines, logs.String())
			}
		})
	}
}
