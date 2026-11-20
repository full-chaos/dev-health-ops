//go:build integration

package syncreconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	chunkLeakRun       = "00000000-0000-4000-8000-000000014243"
	chunkLeakFailed    = "00000000-0000-4000-8000-000000014244"
	chunkLeakRunning   = "00000000-0000-4000-8000-000000014245"
	chunkLeakChunksPer = 3
	chunkMiB           = 1 << 20
)

func chunkReclaimUnitID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", 14300+index)
}

func seedChunkSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, unitID string, chunks int, at time.Time) {
	t.Helper()
	seedChunkSidecarsSized(t, ctx, pool, orgID, unitID, chunks, 12, at)
}

func seedChunkSidecarsSized(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, unitID string, chunks, payloadBytes int, at time.Time) {
	t.Helper()
	generation := "sync-unit:" + unitID
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_unit_chunk_checkpoints
		(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
		 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
		VALUES ($1, $2::uuid, $3, 'github', 'cicd', 'v1', $4, $5, $5, 'worker-a', $4, $4, $4)`,
		orgID, unitID, generation, at, chunks); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_unit_effect_chunks
		(org_id, sync_run_unit_id, generation, route_version, ordinal, total_chunks,
		 payload, ledger, payload_bytes, manifest_digest, status, created_at, updated_at)
		SELECT $1, $2::uuid, $3, 'v1', ordinal, 0, '{"rows":[]}'::jsonb, '{}'::jsonb, $4,
		       repeat('a', 64), 'committed', $5, $5
		FROM generate_series(0, $6::int - 1) AS ordinal`,
		orgID, unitID, generation, payloadBytes, at, chunks); err != nil {
		t.Fatal(err)
	}
}

func countChunkSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) (checkpoints, chunks int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id = $1::uuid),
		       (SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id = $1::uuid)`,
		unitID).Scan(&checkpoints, &chunks); err != nil {
		t.Fatal(err)
	}
	return checkpoints, chunks
}

func setChunkReclaimUnitStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID, status string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		UPDATE public.sync_run_units
		SET status = $2::text,
		    lease_owner = CASE WHEN $2::text = 'running' THEN lease_owner ELSE NULL END,
		    lease_expires_at = CASE WHEN $2::text = 'running' THEN lease_expires_at ELSE NULL END
		WHERE id = $1::uuid`, unitID, status); err != nil {
		t.Fatal(err)
	}
}

// seedChunkReclaimUnit seeds one unit of run chunkLeakRun in the given status
// with its checkpoint and chunk rows.
func seedChunkReclaimUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID, status string, chunks, payloadBytes int, now time.Time) {
	t.Helper()
	seedLeaseRepairUnit(t, ctx, pool, unitID, chunkLeakRun, "org-a", "github", "backfill", "cicd", 0, now.Add(time.Hour))
	setChunkReclaimUnitStatus(t, ctx, pool, unitID, status)
	seedChunkSidecarsSized(t, ctx, pool, "org-a", unitID, chunks, payloadBytes, now.Add(-time.Hour))
}

// chunkReclaimVenue is the migrated schema on one admin pool. The domain-role
// run of the reclaimer (grants, 0 denials) lives in package chunkreclaimrolecell,
// so this package stays outside the posture-test list.
type chunkReclaimVenue struct {
	Admin *pgxpool.Pool
}

func startChunkReclaimVenue(t *testing.T, ctx context.Context) chunkReclaimVenue {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return chunkReclaimVenue{Admin: pool}
}

func resetChunkReclaimVenue(t *testing.T, ctx context.Context, venue chunkReclaimVenue) {
	t.Helper()
	resetLeaseRepairTables(t, ctx, venue.Admin)
	seedLeaseRepairRun(t, ctx, venue.Admin, chunkLeakRun, "org-a", "running")
}

func newTestChunkReclaim(t *testing.T, pool *pgxpool.Pool, config ChunkSidecarReclaimConfig) *ChunkSidecarReclaim {
	t.Helper()
	reclaim, err := NewChunkSidecarReclaimWithConfig(pool, config)
	if err != nil {
		t.Fatal(err)
	}
	return reclaim
}

func stepChunkReclaim(t *testing.T, ctx context.Context, reclaim *ChunkSidecarReclaim, now time.Time) ChunkSidecarReclaimResult {
	t.Helper()
	result, err := reclaim.Step(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func wantChunkSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, wantCheckpoints, wantChunks int) {
	t.Helper()
	if checkpoints, chunks := countChunkSidecars(t, ctx, pool, unitID); checkpoints != wantCheckpoints || chunks != wantChunks {
		t.Fatalf("unit %s sidecars = %d checkpoints / %d chunks, want %d / %d", unitID, checkpoints, chunks, wantCheckpoints, wantChunks)
	}
}

// CHAOS-14243 red cell: lease repair terminalizes a chunked unit as failed
// (worker_lost) and nothing else reclaims its stored chunk rows. The same
// scenario as the migrated domain role is chunkreclaimrolecell's cell.
func TestChunkSidecarsOfLeaseRepairFailedUnitAreReclaimed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startChunkReclaimVenue(t, ctx)
	now := time.Date(2026, time.November, 19, 17, 3, 0, 0, time.UTC)
	resetChunkReclaimVenue(t, ctx, venue)
	seedLeaseRepairUnit(t, ctx, venue.Admin, chunkLeakFailed, chunkLeakRun, "org-a", "github", "backfill", "cicd", 0, now.Add(-time.Minute))
	seedLeaseRepairUnit(t, ctx, venue.Admin, chunkLeakRunning, chunkLeakRun, "org-a", "github", "backfill", "cicd", 0, now.Add(time.Hour))
	seedChunkSidecars(t, ctx, venue.Admin, "org-a", chunkLeakFailed, chunkLeakChunksPer, now.Add(-time.Hour))
	seedChunkSidecars(t, ctx, venue.Admin, "org-a", chunkLeakRunning, chunkLeakChunksPer, now.Add(-time.Hour))

	repair, err := NewLeaseRepair(venue.Admin)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("lease repair result = %+v, want exactly one failed unit", result)
	}
	var status string
	if err := venue.Admin.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id = $1::uuid`, chunkLeakFailed).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("unit status = %q, want failed", status)
	}

	reclaim, err := NewChunkSidecarReclaim(venue.Admin)
	if err != nil {
		t.Fatal(err)
	}
	reclaimed := stepChunkReclaim(t, ctx, reclaim, now)
	if reclaimed.Units != 1 || reclaimed.Rows != chunkLeakChunksPer || reclaimed.RemainingUnits != 0 {
		t.Fatalf("reclaim result = %+v, want 1 unit, %d rows, 0 remaining", reclaimed, chunkLeakChunksPer)
	}

	if checkpoints, chunks := countChunkSidecars(t, ctx, venue.Admin, chunkLeakRunning); checkpoints != 1 || chunks != chunkLeakChunksPer {
		t.Fatalf("running unit sidecars = %d checkpoints / %d chunks, want 1 / %d untouched", checkpoints, chunks, chunkLeakChunksPer)
	}
	if checkpoints, chunks := countChunkSidecars(t, ctx, venue.Admin, chunkLeakFailed); checkpoints != 0 || chunks != 0 {
		t.Fatalf("failed unit sidecars = %d checkpoints / %d chunks, want 0 / 0 (terminal unit's chunk rows were never reclaimed)", checkpoints, chunks)
	}
}

func TestChunkSidecarReclaimPostgresContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startChunkReclaimVenue(t, ctx)
	now := time.Date(2026, time.November, 19, 18, 0, 0, 0, time.UTC)
	unlimited := DefaultChunkSidecarReclaimConfig()
	oneStatement := unlimited
	oneStatement.StatementsPerTick = 1

	t.Run("row cap: each statement deletes at most 64 rows and the checkpoint goes last", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(1)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 150, 1, now)
		reclaim := newTestChunkReclaim(t, venue.Admin, oneStatement)
		for index, want := range []struct {
			rows, units, checkpoints, chunks int
		}{{64, 0, 1, 86}, {64, 0, 1, 22}, {22, 1, 0, 0}} {
			result := stepChunkReclaim(t, ctx, reclaim, now)
			if result.Statements != 1 || result.Rows != int64(want.rows) || result.Units != want.units {
				t.Fatalf("statement %d = %+v, want %d rows and %d units", index, result, want.rows, want.units)
			}
			wantChunkSidecars(t, ctx, venue.Admin, unit, want.checkpoints, want.chunks)
		}
	})

	t.Run("byte cap: a statement deletes at most 4 MiB of payload_bytes", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(2)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 6, chunkMiB, now)
		reclaim := newTestChunkReclaim(t, venue.Admin, oneStatement)
		result := stepChunkReclaim(t, ctx, reclaim, now)
		if result.Rows != 4 || result.Bytes != 4*chunkMiB || result.Units != 0 {
			t.Fatalf("first statement = %+v, want 4 rows / 4 MiB and no unit", result)
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 2)
		result = stepChunkReclaim(t, ctx, reclaim, now)
		if result.Rows != 2 || result.Bytes != 2*chunkMiB || result.Units != 1 {
			t.Fatalf("second statement = %+v, want 2 rows / 2 MiB and the unit", result)
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 0, 0)
	})

	t.Run("byte cap: the smallest legal cap still deletes one largest row", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(3)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 2, maxChunkPayloadBytes, now)
		small := oneStatement
		small.BytesPerStatement = maxChunkPayloadBytes
		reclaim := newTestChunkReclaim(t, venue.Admin, small)
		result := stepChunkReclaim(t, ctx, reclaim, now)
		if result.Rows != 1 || result.Bytes != maxChunkPayloadBytes {
			t.Fatalf("statement = %+v, want exactly one %d-byte row", result, maxChunkPayloadBytes)
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 1)
	})

	t.Run("rate cap: at most BytesPerMinute in any 60 s window", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(4)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 10, chunkMiB, now)
		limited := DefaultChunkSidecarReclaimConfig()
		limited.BytesPerMinute = 4 * chunkMiB
		reclaim := newTestChunkReclaim(t, venue.Admin, limited)
		clock := now
		reclaim.clock = func() time.Time { return clock }
		var logs bytes.Buffer
		original := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		defer slog.SetDefault(original)
		for index, want := range []struct {
			at        time.Duration
			bytes     int64
			throttled bool
			chunks    int
		}{
			{0, 4 * chunkMiB, true, 6},
			{30 * time.Second, 0, true, 6},
			{59 * time.Second, 0, true, 6},
			{61 * time.Second, 4 * chunkMiB, true, 2},
			{122 * time.Second, 2 * chunkMiB, false, 0},
		} {
			clock = now.Add(want.at)
			result := stepChunkReclaim(t, ctx, reclaim, clock)
			if result.Bytes != want.bytes || result.Throttled != want.throttled {
				t.Fatalf("tick %d at +%s = %+v, want %d bytes, throttled %v", index, want.at, result, want.bytes, want.throttled)
			}
			wantCheckpoints := 1
			if want.chunks == 0 {
				wantCheckpoints = 0
			}
			wantChunkSidecars(t, ctx, venue.Admin, unit, wantCheckpoints, want.chunks)
		}
		slog.SetDefault(original)
		type tickLine struct {
			Msg            string `json:"msg"`
			Bytes          int64  `json:"bytes"`
			Throttled      bool   `json:"throttled"`
			RemainingUnits int64  `json:"remaining_units"`
			Done           bool   `json:"done"`
		}
		var lines []tickLine
		for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var line tickLine
			if err := json.Unmarshal([]byte(raw), &line); err != nil {
				t.Fatalf("log line %q: %v", raw, err)
			}
			if line.Msg == "syncreconciler.chunk_sidecar_reclaim" {
				lines = append(lines, line)
			}
		}
		wantLines := []tickLine{
			{Bytes: 4 * chunkMiB, Throttled: true, RemainingUnits: 1},
			{Throttled: true, RemainingUnits: 1},
			{Throttled: true, RemainingUnits: 1},
			{Bytes: 4 * chunkMiB, Throttled: true, RemainingUnits: 1},
			{Bytes: 2 * chunkMiB, Done: true},
		}
		if len(lines) != len(wantLines) {
			t.Fatalf("reclaim logged %d tick lines, want one per tick (%d): %s", len(lines), len(wantLines), logs.String())
		}
		for index, want := range wantLines {
			want.Msg = "syncreconciler.chunk_sidecar_reclaim"
			if lines[index] != want {
				t.Errorf("tick line %d = %+v, want %+v", index, lines[index], want)
			}
		}
	})

	t.Run("only terminal units: planned, dispatching, running and retrying keep every row", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		statuses := []string{"planned", "dispatching", "running", "retrying", "success", "failed"}
		for index, status := range statuses {
			seedChunkReclaimUnit(t, ctx, venue.Admin, chunkReclaimUnitID(10+index), status, 5, 1, now)
		}
		reclaim := newTestChunkReclaim(t, venue.Admin, unlimited)
		result := stepChunkReclaim(t, ctx, reclaim, now)
		if result.Units != 2 || result.Rows != 10 || result.RemainingUnits != 0 {
			t.Fatalf("reclaim result = %+v, want the 2 terminal units (10 rows), 0 remaining", result)
		}
		for index, status := range statuses {
			if status == "success" || status == "failed" {
				wantChunkSidecars(t, ctx, venue.Admin, chunkReclaimUnitID(10+index), 0, 0)
				continue
			}
			wantChunkSidecars(t, ctx, venue.Admin, chunkReclaimUnitID(10+index), 1, 5)
		}
		if again := stepChunkReclaim(t, ctx, reclaim, now); again.Statements != 0 || again.Rows != 0 {
			t.Fatalf("second pass = %+v, want no work", again)
		}
	})

	t.Run("never locks the unit row", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(20)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 300, 1, now)
		holder, err := venue.Admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx, `SELECT 1 FROM public.sync_run_units WHERE id = $1::uuid FOR UPDATE`, unit); err != nil {
			t.Fatal(err)
		}
		reclaim := newTestChunkReclaim(t, venue.Admin, unlimited)
		stepCtx, stepCancel := context.WithTimeout(ctx, 10*time.Second)
		defer stepCancel()
		for step := 0; step < 3; step++ {
			if _, err := reclaim.Step(stepCtx, now, 100); err != nil {
				t.Fatalf("reclaim while another transaction holds the unit row: %v", err)
			}
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 0, 0)
	})

	t.Run("two reclaimers share the backlog without error", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		for index := 0; index < 4; index++ {
			seedChunkReclaimUnit(t, ctx, venue.Admin, chunkReclaimUnitID(30+index), "failed", 300, 1, now)
		}
		first := newTestChunkReclaim(t, venue.Admin, unlimited)
		second := newTestChunkReclaim(t, venue.Admin, unlimited)
		var mu sync.Mutex
		var rows int64
		var units int
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, reclaim := range []*ChunkSidecarReclaim{first, second} {
			wg.Add(1)
			go func(reclaim *ChunkSidecarReclaim) {
				defer wg.Done()
				for step := 0; step < 10; step++ {
					result, err := reclaim.Step(ctx, now, 100)
					if err != nil {
						errs <- err
						return
					}
					mu.Lock()
					rows += result.Rows
					units += result.Units
					mu.Unlock()
				}
			}(reclaim)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if rows != 1200 || units != 4 {
			t.Fatalf("two reclaimers deleted %d rows / %d units, want 1200 / 4", rows, units)
		}
		for index := 0; index < 4; index++ {
			wantChunkSidecars(t, ctx, venue.Admin, chunkReclaimUnitID(30+index), 0, 0)
		}
	})

	t.Run("done follows the backlog count, not candidate selection", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		free, held := chunkReclaimUnitID(40), chunkReclaimUnitID(41)
		seedChunkReclaimUnit(t, ctx, venue.Admin, free, "failed", 5, 1, now)
		seedChunkReclaimUnit(t, ctx, venue.Admin, held, "failed", 5, 1, now)
		holder, err := venue.Admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx, `SELECT 1 FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id = $1::uuid FOR UPDATE`, held); err != nil {
			t.Fatal(err)
		}
		reclaim := newTestChunkReclaim(t, venue.Admin, unlimited)
		result := stepChunkReclaim(t, ctx, reclaim, now)
		if result.Units != 1 || result.Done || result.RemainingUnits != 1 {
			t.Fatalf("with the last checkpoint locked elsewhere: %+v, want 1 unit, remaining 1, not done", result)
		}
		if result = stepChunkReclaim(t, ctx, reclaim, now); result.Statements != 0 || result.Done || result.RemainingUnits != 1 {
			t.Fatalf("an active reclaimer whose only candidate is locked: %+v, want no statement, remaining 1, not done", result)
		}
		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if result = stepChunkReclaim(t, ctx, reclaim, now); result.Units != 1 || !result.Done || result.RemainingUnits != 0 {
			t.Fatalf("after the lock is gone: %+v, want the last unit and done", result)
		}
		if result = stepChunkReclaim(t, ctx, reclaim, now); result.Statements != 0 || result.Done || result.RemainingUnits != -1 {
			t.Fatalf("idle tick after done: %+v, want no statement and no backlog read", result)
		}
	})

	t.Run("a failure after a commit still logs the committed work", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(50)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 300, 1, now)
		begins := 0
		reclaim, err := newChunkSidecarReclaim(func(beginCtx context.Context) (pgx.Tx, error) {
			begins++
			if begins > 1 {
				return nil, errors.New("begin refused")
			}
			return venue.Admin.Begin(beginCtx)
		}, poolChunkSidecarReclaimTickLock(venue.Admin), unlimited)
		if err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		original := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		result, stepErr := reclaim.Step(ctx, now, 100)
		slog.SetDefault(original)
		if !errors.Is(stepErr, ErrUnavailable) {
			t.Fatalf("step error = %v, want ErrUnavailable", stepErr)
		}
		if result.Rows != 64 || result.RemainingUnits != -1 || result.Done {
			t.Fatalf("result = %+v, want the 64 committed rows, remaining -1, not done", result)
		}
		if !strings.Contains(logs.String(), `"msg":"syncreconciler.chunk_sidecar_reclaim","bytes":64,"rows":64,"units":0,"statements":1,"throttled":false,"remaining_units":-1,"done":false,"error":`) {
			t.Fatalf("the tick line does not carry the committed work: %s", logs.String())
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 236)
	})

	t.Run("an active reclaimer logs a tick that fails before any commit", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(55)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 300, 1, now)
		config := unlimited
		config.StatementsPerTick = 1
		refuse := false
		reclaim, err := newChunkSidecarReclaim(func(beginCtx context.Context) (pgx.Tx, error) {
			if refuse {
				return nil, errors.New("begin refused")
			}
			return venue.Admin.Begin(beginCtx)
		}, poolChunkSidecarReclaimTickLock(venue.Admin), config)
		if err != nil {
			t.Fatal(err)
		}
		if first := stepChunkReclaim(t, ctx, reclaim, now); first.Rows != 64 || first.RemainingUnits != 1 {
			t.Fatalf("first tick = %+v, want 64 rows and 1 remaining unit", first)
		}
		refuse = true
		var logs bytes.Buffer
		original := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		result, stepErr := reclaim.Step(ctx, now, 100)
		slog.SetDefault(original)
		if !errors.Is(stepErr, ErrUnavailable) || result.Statements != 0 || result.RemainingUnits != -1 {
			t.Fatalf("failed tick = %+v / %v, want no statement, remaining -1, ErrUnavailable", result, stepErr)
		}
		if !strings.Contains(logs.String(), `"msg":"syncreconciler.chunk_sidecar_reclaim","bytes":0,"rows":0,"units":0,"statements":0,"throttled":false,"remaining_units":-1,"done":false,"error":`) {
			t.Fatalf("an active reclaimer's failed tick wrote no tick line: %s", logs.String())
		}
	})

	t.Run("cluster lock: two reclaimers at once, only one deletes in a tick", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(70)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 20, chunkMiB, now)
		limited := DefaultChunkSidecarReclaimConfig()
		limited.BytesPerMinute = 4 * chunkMiB
		entered, gate := make(chan struct{}), make(chan struct{})
		var once sync.Once
		first, err := newChunkSidecarReclaim(func(beginCtx context.Context) (pgx.Tx, error) {
			once.Do(func() {
				close(entered)
				<-gate
			})
			return venue.Admin.Begin(beginCtx)
		}, poolChunkSidecarReclaimTickLock(venue.Admin), limited)
		if err != nil {
			t.Fatal(err)
		}
		second, err := newChunkSidecarReclaim(venue.Admin.Begin, poolChunkSidecarReclaimTickLock(venue.Admin), limited)
		if err != nil {
			t.Fatal(err)
		}
		first.clock = func() time.Time { return now }
		second.clock = func() time.Time { return now }
		type outcome struct {
			result ChunkSidecarReclaimResult
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			result, stepErr := first.Step(ctx, now, 100)
			done <- outcome{result, stepErr}
		}()
		<-entered
		skipped, err := second.Step(ctx, now, 100)
		close(gate)
		held := <-done
		if err != nil || held.err != nil {
			t.Fatalf("step errors: %v / %v", held.err, err)
		}
		if !skipped.LockSkipped || skipped.Bytes != 0 || second.LockSkippedTicks() != 1 {
			t.Fatalf("second reclaimer during the first one's tick = %+v (skipped ticks %d), want a skipped tick and no delete", skipped, second.LockSkippedTicks())
		}
		if held.result.LockSkipped || held.result.Bytes != 4*chunkMiB {
			t.Fatalf("lock holder = %+v, want 4 MiB deleted", held.result)
		}
		if total := held.result.Bytes + skipped.Bytes; total > limited.BytesPerMinute {
			t.Fatalf("both reclaimers deleted %d bytes in one tick, cap %d", total, limited.BytesPerMinute)
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 16)
	})

	t.Run("cluster lock: held by another session, the tick is skipped and counted", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(71)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 5, 1, now)
		holder, err := venue.Admin.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Release()
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, chunkSidecarReclaimLockKey); err != nil {
			t.Fatal(err)
		}
		reclaim := newTestChunkReclaim(t, venue.Admin, unlimited)
		result, err := reclaim.Step(ctx, now, 100)
		if err != nil || !result.LockSkipped || result.Statements != 0 || reclaim.LockSkippedTicks() != 1 {
			t.Fatalf("tick under another session's lock = %+v / %v (skipped ticks %d), want skipped, no error, counter 1", result, err, reclaim.LockSkippedTicks())
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 5)
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, chunkSidecarReclaimLockKey); err != nil {
			t.Fatal(err)
		}
		if result = stepChunkReclaim(t, ctx, reclaim, now); result.LockSkipped || result.Units != 1 || reclaim.LockSkippedTicks() != 1 {
			t.Fatalf("tick after the lock is free = %+v (skipped ticks %d), want the unit reclaimed", result, reclaim.LockSkippedTicks())
		}
		var held int
		if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted`).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != 0 {
			t.Fatalf("%d advisory locks still held after the tick; the reclaimer must release its tick lock", held)
		}
	})

	t.Run("a COMMIT whose reply is lost keeps its whole reservation", func(t *testing.T) {
		resetChunkReclaimVenue(t, ctx, venue)
		unit := chunkReclaimUnitID(60)
		seedChunkReclaimUnit(t, ctx, venue.Admin, unit, "failed", 10, chunkMiB, now)
		config := unlimited
		reclaim, err := newChunkSidecarReclaim(func(beginCtx context.Context) (pgx.Tx, error) {
			tx, err := venue.Admin.Begin(beginCtx)
			return lostCommitReplyTx{Tx: tx}, err
		}, poolChunkSidecarReclaimTickLock(venue.Admin), config)
		if err != nil {
			t.Fatal(err)
		}
		reclaim.clock = func() time.Time { return now }
		if _, err := reclaim.Step(ctx, now, 1); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("step error = %v, want ErrUnavailable", err)
		}
		wantChunkSidecars(t, ctx, venue.Admin, unit, 1, 6)
		if got := reclaim.limiter.available(now.Add(59 * time.Second)); got != config.BytesPerMinute-4*chunkMiB {
			t.Fatalf("available after a lost COMMIT reply = %d, want the 4 MiB reservation still charged", got)
		}
	})
}

type lostCommitReplyTx struct {
	pgx.Tx
}

func (tx lostCommitReplyTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return errors.New("unexpected EOF after COMMIT")
}
