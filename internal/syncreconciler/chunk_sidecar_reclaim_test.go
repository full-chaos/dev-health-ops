package syncreconciler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func freeTickLock(context.Context) (func(), bool, error) { return func() {}, true, nil }

func TestDefaultChunkSidecarReclaimConfigPinsTheBounds(t *testing.T) {
	t.Parallel()
	got := DefaultChunkSidecarReclaimConfig()
	want := ChunkSidecarReclaimConfig{
		RowsPerStatement:  256,
		BytesPerStatement: 32 << 20,
		BytesPerMinute:    256 << 20,
		StatementsPerTick: 4,
	}
	if got != want {
		t.Fatalf("DefaultChunkSidecarReclaimConfig() = %+v, want %+v", got, want)
	}
	if !got.valid() {
		t.Fatal("the default config must be valid")
	}
}

func TestChunkSidecarReclaimRefusesInvalidConfiguration(t *testing.T) {
	t.Parallel()
	begin := func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") }
	for name, mutate := range map[string]func(*ChunkSidecarReclaimConfig){
		"zero rows":            func(config *ChunkSidecarReclaimConfig) { config.RowsPerStatement = 0 },
		"too many rows":        func(config *ChunkSidecarReclaimConfig) { config.RowsPerStatement = 1001 },
		"zero statement bytes": func(config *ChunkSidecarReclaimConfig) { config.BytesPerStatement = 0 },
		"minute below one row": func(config *ChunkSidecarReclaimConfig) { config.BytesPerMinute = maxChunkPayloadBytes - 1 },
		"zero statements":      func(config *ChunkSidecarReclaimConfig) { config.StatementsPerTick = 0 },
		"unbounded statements": func(config *ChunkSidecarReclaimConfig) { config.StatementsPerTick = 65 },
	} {
		config := DefaultChunkSidecarReclaimConfig()
		mutate(&config)
		if _, err := newChunkSidecarReclaim(begin, freeTickLock, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: error = %v, want ErrInvalidConfiguration", name, err)
		}
	}
	if _, err := newChunkSidecarReclaim(nil, freeTickLock, DefaultChunkSidecarReclaimConfig()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil begin: error = %v, want ErrInvalidConfiguration", err)
	}
	if _, err := newChunkSidecarReclaim(begin, nil, DefaultChunkSidecarReclaimConfig()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil tick lock: error = %v, want ErrInvalidConfiguration", err)
	}
	if _, err := NewChunkSidecarReclaim(nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil pool: error = %v, want ErrInvalidConfiguration", err)
	}
}

func TestChunkReclaimLimiterIsASixtySecondWindowFromSettlement(t *testing.T) {
	t.Parallel()
	limiter := newChunkReclaimLimiter(256 << 20)
	start := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	first := limiter.reserve(start, 100<<20)
	second := limiter.reserve(start, 150<<20)
	if first == nil || second == nil {
		t.Fatal("both reservations fit the window")
	}
	// Settled 30 s after admission: the charge runs from settlement.
	limiter.settle(first, 100<<20, start.Add(30*time.Second))
	limiter.settle(second, 150<<20, start)
	for _, check := range []struct {
		at   time.Duration
		want int64
	}{
		{59 * time.Second, 6 << 20},
		{60 * time.Second, 156 << 20},
		{89 * time.Second, 156 << 20},
		{90 * time.Second, 256 << 20},
	} {
		if got := limiter.available(start.Add(check.at)); got != check.want {
			t.Errorf("available at +%s = %d, want %d", check.at, got, check.want)
		}
	}
}

func TestChunkReclaimLimiterCountsAnUnsettledReservationPastTheWindow(t *testing.T) {
	t.Parallel()
	limiter := newChunkReclaimLimiter(4 << 20)
	start := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	inFlight := limiter.reserve(start, 4<<20)
	if inFlight == nil {
		t.Fatal("first reservation must fit")
	}
	if again := limiter.reserve(start.Add(5*time.Minute), 4<<20); again != nil {
		t.Fatal("an unsettled reservation still holds its bytes after 60 s")
	}
	limiter.settle(inFlight, 0, start.Add(5*time.Minute))
	if got := limiter.available(start.Add(5 * time.Minute)); got != 4<<20 {
		t.Fatalf("a released reservation still counts: available %d", got)
	}
}

func TestChunkReclaimLimiterAdmitsOverlappingReservationsOnce(t *testing.T) {
	t.Parallel()
	limiter := newChunkReclaimLimiter(4 << 20)
	now := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	granted := make(chan int64, 16)
	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if spend := limiter.reserve(now, 4<<20); spend != nil {
				granted <- spend.bytes
			}
		}()
	}
	wait.Wait()
	close(granted)
	var total int64
	for bytes := range granted {
		total += bytes
	}
	if total != 4<<20 {
		t.Fatalf("16 overlapping reservations were granted %d bytes, want exactly the 4 MiB window", total)
	}
}

func TestChunkSidecarReclaimProductionReclaimersShareOneLimiter(t *testing.T) {
	t.Parallel()
	pool := &pgxpool.Pool{}
	first, err := NewChunkSidecarReclaim(pool)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewChunkSidecarReclaim(pool)
	if err != nil {
		t.Fatal(err)
	}
	if first.limiter != second.limiter || first.limiter != processChunkReclaimLimiter {
		t.Fatal("production reclaimers must share the process limiter")
	}
}

// The statement cap may not go below the largest row the stage can delete
// (ck_sync_chunk_payload_bytes), so a reservation always covers the first row.
func TestChunkSidecarReclaimConfigRefusesAStatementCapBelowOneRow(t *testing.T) {
	t.Parallel()
	begin := func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") }
	config := DefaultChunkSidecarReclaimConfig()
	config.BytesPerStatement = maxChunkPayloadBytes - 1
	if _, err := newChunkSidecarReclaim(begin, freeTickLock, config); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("BytesPerStatement %d below the %d-byte row bound: error = %v, want ErrInvalidConfiguration",
			config.BytesPerStatement, maxChunkPayloadBytes, err)
	}
	config.BytesPerStatement = maxChunkPayloadBytes
	if _, err := newChunkSidecarReclaim(begin, freeTickLock, config); err != nil {
		t.Fatalf("BytesPerStatement at the row bound must be valid: %v", err)
	}
}

func TestChunkSidecarReclaimConfigCannotRaiseTheBounds(t *testing.T) {
	t.Parallel()
	begin := func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") }
	for name, mutate := range map[string]func(*ChunkSidecarReclaimConfig){
		"257 rows":            func(config *ChunkSidecarReclaimConfig) { config.RowsPerStatement = 257 },
		"33 MiB statement":    func(config *ChunkSidecarReclaimConfig) { config.BytesPerStatement = 33 << 20 },
		"258 MiB per minute":  func(config *ChunkSidecarReclaimConfig) { config.BytesPerMinute = 258 << 20 },
		"5 statements a tick": func(config *ChunkSidecarReclaimConfig) { config.StatementsPerTick = 5 },
	} {
		config := DefaultChunkSidecarReclaimConfig()
		mutate(&config)
		if _, err := newChunkSidecarReclaim(begin, freeTickLock, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: error = %v, want ErrInvalidConfiguration", name, err)
		}
	}
	lower := ChunkSidecarReclaimConfig{RowsPerStatement: 1, BytesPerStatement: maxChunkPayloadBytes, BytesPerMinute: maxChunkPayloadBytes, StatementsPerTick: 1}
	if _, err := newChunkSidecarReclaim(begin, freeTickLock, lower); err != nil {
		t.Fatalf("lower bounds must stay valid: %v", err)
	}
}

func TestMutationPipelineRefusesATypedNilChunkSidecarReclaim(t *testing.T) {
	t.Parallel()
	for name, reclaim := range map[string]ChunkSidecarReclaimStepper{
		"nil pointer":  (*ChunkSidecarReclaim)(nil),
		"nil function": pipelineChunkSidecarReclaimFunc(nil),
	} {
		if _, err := NewMutationPipeline(
			pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) { return LeaseRepairResult{}, nil }),
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
			reclaim,
			DefaultMutationPipelineConfig(),
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: error = %v, want ErrInvalidConfiguration", name, err)
		}
	}
}

// A repair or kernel failure aborts the delivery stages; it must not starve the
// cleanup of units that are already terminal.
func TestMutationPipelineRunsChunkSidecarReclaimWhenEarlierStagesAbort(t *testing.T) {
	sentinel := errors.New("stage unavailable")
	for name, failing := range map[string]StageName{"lease repair": StageLeaseRepair, "kernel": StageKernel} {
		reclaimCalls := 0
		pipeline, err := NewMutationPipeline(
			pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
				if failing == StageLeaseRepair {
					return LeaseRepairResult{}, sentinel
				}
				return LeaseRepairResult{}, nil
			}),
			pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
				return TerminalDeliveryRepairResult{}, nil
			}),
			pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
				return MaterializerResult{}, nil
			}),
			pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
				if failing == StageKernel {
					return KernelResult{}, sentinel
				}
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
			DefaultMutationPipelineConfig(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pipeline.Step(context.Background(), time.Now(), 10); err != nil {
			t.Fatalf("%s: tick error %v", name, err)
		}
		if reclaimCalls != 1 {
			t.Errorf("%s failed: reclaim ran %d times, want 1", name, reclaimCalls)
		}
	}
}

func TestChunkSidecarReclaimStepRefusesBadArguments(t *testing.T) {
	t.Parallel()
	reclaim, err := newChunkSidecarReclaim(func(context.Context) (pgx.Tx, error) {
		t.Fatal("begin must not run for refused arguments")
		return nil, nil
	}, freeTickLock, DefaultChunkSidecarReclaimConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	if _, err := reclaim.Step(context.Background(), time.Time{}, 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("zero now: error = %v", err)
	}
	if _, err := reclaim.Step(context.Background(), now, 0); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("zero limit: error = %v", err)
	}
	var missing *ChunkSidecarReclaim
	if _, err := missing.Step(context.Background(), now, 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil reclaim: error = %v", err)
	}
}

func TestMutationPipelineCountsChunkSidecarReclaimLockSkips(t *testing.T) {
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) { return LeaseRepairResult{}, nil }),
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
			return ChunkSidecarReclaimResult{LockSkipped: true, RemainingUnits: -1}, nil
		}),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for tick := 0; tick < 2; tick++ {
		if _, err := pipeline.Step(context.Background(), time.Now(), 10); err != nil {
			t.Fatal(err)
		}
	}
	var rendered strings.Builder
	if err := pipeline.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.String(), "\nsync_reconciler_chunk_sidecar_reclaim_lock_skipped_total 2\n") {
		t.Fatalf("lock-skip counter not rendered as 2:\n%s", rendered.String())
	}
}
