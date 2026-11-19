package syncreconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

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
		if _, err := newChunkSidecarReclaim(begin, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: error = %v, want ErrInvalidConfiguration", name, err)
		}
	}
	if _, err := newChunkSidecarReclaim(nil, DefaultChunkSidecarReclaimConfig()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil begin: error = %v, want ErrInvalidConfiguration", err)
	}
	if _, err := NewChunkSidecarReclaim(nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("nil pool: error = %v, want ErrInvalidConfiguration", err)
	}
}

func TestChunkSidecarReclaimAllowanceIsASixtySecondWindow(t *testing.T) {
	t.Parallel()
	reclaim := &ChunkSidecarReclaim{config: DefaultChunkSidecarReclaimConfig()}
	start := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	limit := reclaim.config.BytesPerMinute
	reclaim.record(start, 100<<20)
	reclaim.record(start.Add(30*time.Second), 150<<20)
	for _, check := range []struct {
		at   time.Duration
		want int64
	}{
		{0, limit - 250<<20},
		{59 * time.Second, limit - 250<<20},
		{60 * time.Second, limit - 150<<20},
		{90 * time.Second, limit},
	} {
		if got := reclaim.allowance(start.Add(check.at)); got != check.want {
			t.Errorf("allowance at +%s = %d, want %d", check.at, got, check.want)
		}
	}
}

func TestChunkSidecarReclaimStepRefusesBadArguments(t *testing.T) {
	t.Parallel()
	reclaim, err := newChunkSidecarReclaim(func(context.Context) (pgx.Tx, error) {
		t.Fatal("begin must not run for refused arguments")
		return nil, nil
	}, DefaultChunkSidecarReclaimConfig())
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
