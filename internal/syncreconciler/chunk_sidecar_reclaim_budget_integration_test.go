//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// wantDeadlineCutsPerWindow is chunkReclaimDeadlineCutsPerWindow, restated so
// the cells compile and run red at the base that has no such bound.
const wantDeadlineCutsPerWindow = 8

const chunkReclaimServerDeletesSQL = `
SELECT count(*)
FROM pg_catalog.pg_stat_activity
WHERE pid <> pg_catalog.pg_backend_pid()
  AND state = 'active'
  AND query LIKE '%DELETE FROM public.sync_run_unit_effect_chunks%'`

// chunkReclaimBudgetVenue is one PostgreSQL with the migrated schema: direct is
// a superuser pool for seeding and reading pg_stat_activity, domain is built
// by the production pool constructor (4 connections), directly or through a
// transaction-mode PgBouncer.
type chunkReclaimBudgetVenue struct {
	direct *pgxpool.Pool
	domain *pgxpool.Pool
}

func startChunkReclaimBudgetVenue(t *testing.T, ctx context.Context, throughPgBouncer bool) chunkReclaimBudgetVenue {
	t.Helper()
	const (
		user     = "devhealth_domain"
		password = "test-only-reclaim-pw"
		database = "devhealth"
	)
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: containers.PostgresImage,
		Env: map[string]string{"POSTGRES_USER": user, "POSTGRES_PASSWORD": password, "POSTGRES_DB": database,
			"POSTGRES_INITDB_ARGS": "--auth-host=scram-sha-256"},
		Networks: []string{nw.Name}, NetworkAliases: map[string][]string{nw.Name: {"pg"}}, ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	address := func(container testcontainers.Container, port string) string {
		host, err := container.Host(ctx)
		if err != nil {
			t.Fatal(err)
		}
		mapped, err := container.MappedPort(ctx, port)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, mapped.Port(), database)
	}
	direct, err := pgxpool.New(ctx, address(pg, "5432/tcp"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(direct.Close)
	pgschema.Apply(ctx, t, direct)
	domainAddress := address(pg, "5432/tcp")
	if throughPgBouncer {
		bouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: chunkReclaimPgBouncerImage,
			Env: map[string]string{
				"DB_HOST": "pg", "DB_PORT": "5432", "DB_NAME": database, "DB_USER": user, "DB_PASSWORD": password,
				"LISTEN_PORT": "6432", "POOL_MODE": "transaction", "AUTH_TYPE": "scram-sha-256",
				"DEFAULT_POOL_SIZE": "16", "MAX_CLIENT_CONN": "64",
			},
			Networks: []string{nw.Name}, ExposedPorts: []string{"6432/tcp"},
			WaitingFor: wait.ForListeningPort("6432/tcp").WithStartupTimeout(60 * time.Second),
		}, Started: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bouncer.Terminate(context.Background()) })
		domainAddress = address(bouncer, "6432/tcp")
	}
	domain, err := postgresstore.New(ctx, postgresstore.DefaultConfig(domainAddress))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	return chunkReclaimBudgetVenue{direct: direct, domain: domain}
}

// holdFirstChunkRows locks chunk row 0 of each unit, so a reclaim DELETE of
// that unit waits for the lock: a stand-in for a DELETE that is slow on the
// server.
func holdFirstChunkRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, units []string) pgx.Tx {
	t.Helper()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, `
		SELECT 1 FROM public.sync_run_unit_effect_chunks
		WHERE sync_run_unit_id = ANY($1::uuid[]) AND ordinal = 0
		FOR UPDATE`, units); err != nil {
		t.Fatal(err)
	}
	return blocker
}

func seedChunkReclaimBudgetUnits(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, count int) []string {
	t.Helper()
	resetChunkReclaimVenue(t, ctx, chunkReclaimVenue{Admin: pool})
	units := make([]string, 0, count)
	for index := range count {
		unit := chunkReclaimUnitID(80 + index)
		seedChunkReclaimUnit(t, ctx, pool, unit, "failed", 64, chunkMiB, now)
		units = append(units, unit)
	}
	return units
}

func stageStepChunkReclaim(ctx context.Context, reclaim *ChunkSidecarReclaim, now time.Time) (time.Duration, error) {
	stageCtx, cancel := context.WithTimeout(ctx, DefaultStageBudgets()[StageChunkSidecarReclaim])
	defer cancel()
	started := time.Now()
	_, err := reclaim.Step(stageCtx, now, 100)
	return time.Since(started), err
}

// CHAOS-14380 cell: a reclaim statement that outlives the stage budget ends on
// the server with the stage, directly and through the transaction-mode pooler
// the domain DSN uses. Work left running after Step returns would run beside
// the next stages of the pipeline.
func TestChunkSidecarReclaimStatementEndsOnTheServerWithTheStageBudget(t *testing.T) {
	for _, throughPgBouncer := range []bool{false, true} {
		t.Run(fmt.Sprintf("pgbouncer=%t", throughPgBouncer), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			venue := startChunkReclaimBudgetVenue(t, ctx, throughPgBouncer)
			now := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
			units := seedChunkReclaimBudgetUnits(t, ctx, venue.direct, now, 2)
			holdFirstChunkRows(t, ctx, venue.direct, units)
			reclaim, err := NewChunkSidecarReclaim(venue.domain)
			if err != nil {
				t.Fatal(err)
			}
			reclaim.limiter = newChunkReclaimLimiter(DefaultChunkSidecarReclaimConfig().BytesPerMinute)
			const grace = 500 * time.Millisecond
			for tick := range units {
				elapsed, stepErr := stageStepChunkReclaim(ctx, reclaim, now)
				if stepErr == nil {
					t.Fatalf("tick %d: Step returned nil while its DELETE waited past the stage budget", tick)
				}
				time.Sleep(grace)
				var running int
				if err := venue.direct.QueryRow(ctx, chunkReclaimServerDeletesSQL).Scan(&running); err != nil {
					t.Fatal(err)
				}
				t.Logf("tick %d: Step returned after %s (%v); %s later %d reclaim DELETE(s) run on the server",
					tick, elapsed.Round(time.Millisecond), stepErr, grace, running)
				if running != 0 {
					t.Errorf("tick %d: %d reclaim DELETE(s) still run on the server %s after the stage budget ended, want 0",
						tick, running, grace)
				}
			}
		})
	}
}

// CHAOS-14380 red cell (C1): a statement the stage budget cuts stays charged
// to the byte limiter, so on a slow server the 256 MiB/min cap throttles the
// reclaim after wantDeadlineCutsPerWindow budget-long ticks. At
// origin/main the cut statement was refunded: the next tick re-ran it for the
// whole budget, every tick, and the cap never engaged.
func TestChunkSidecarReclaimDeadlineCutStatementsStayChargedAndThrottle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startChunkReclaimBudgetVenue(t, ctx, false)
	now := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
	units := seedChunkReclaimBudgetUnits(t, ctx, venue.direct, now, 1)
	holdFirstChunkRows(t, ctx, venue.direct, units)
	reclaim, err := NewChunkSidecarReclaim(venue.domain)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultChunkSidecarReclaimConfig()
	reclaim.limiter = newChunkReclaimLimiter(config.BytesPerMinute)
	budget := DefaultStageBudgets()[StageChunkSidecarReclaim]
	const ticks = 10
	cut, throttled := 0, 0
	for tick := range ticks {
		stageCtx, stageCancel := context.WithTimeout(ctx, budget)
		result, stepErr := reclaim.Step(stageCtx, now, 100)
		stageCancel()
		if errors.Is(stepErr, context.DeadlineExceeded) && result.Bytes == 0 {
			cut++
		}
		if result.Throttled {
			throttled++
		}
		t.Logf("tick %d: throttled=%t (%v); limiter available %d bytes", tick, result.Throttled, stepErr,
			reclaim.limiter.available(time.Now()))
	}
	if cut > wantDeadlineCutsPerWindow || throttled == 0 || cut+throttled != ticks {
		t.Errorf("%d of %d ticks were cut by the %s budget with nothing deleted and %d were throttled; want at most %d cut "+
			"ticks in the window and every later tick throttled (a cut statement stays charged)",
			cut, ticks, budget, throttled, wantDeadlineCutsPerWindow)
	}
}

// CHAOS-14380 cell (C2): after a cut statement the limiter keeps the charge
// (BytesPerMinute/wantDeadlineCutsPerWindow), the next statement is
// half as large, and a committed statement grows it back to the cap.
func TestChunkSidecarReclaimDeadlineCutChargesAndHalvesTheNextStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startChunkReclaimBudgetVenue(t, ctx, false)
	now := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
	resetChunkReclaimVenue(t, ctx, chunkReclaimVenue{Admin: venue.direct})
	unit := chunkReclaimUnitID(90)
	seedChunkReclaimUnit(t, ctx, venue.direct, unit, "failed", 10, chunkMiB, now)
	blocker := holdFirstChunkRows(t, ctx, venue.direct, []string{unit})
	config := DefaultChunkSidecarReclaimConfig()
	config.StatementsPerTick = 1
	reclaim, err := NewChunkSidecarReclaimWithConfig(venue.domain, config)
	if err != nil {
		t.Fatal(err)
	}
	stageCtx, stageCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, stepErr := reclaim.Step(stageCtx, now, 100)
	stageCancel()
	if stepErr == nil {
		t.Fatal("Step returned nil while its DELETE waited past the budget")
	}
	charge := config.BytesPerMinute / wantDeadlineCutsPerWindow
	if got := reclaim.limiter.available(time.Now()); got != config.BytesPerMinute-charge {
		t.Fatalf("limiter available after a cut statement = %d, want %d (the cut statement charged %d)",
			got, config.BytesPerMinute-charge, charge)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for index, want := range []int64{2 * chunkMiB, 4 * chunkMiB} {
		result := stepChunkReclaim(t, ctx, reclaim, now)
		if result.Bytes != want {
			t.Fatalf("statement %d after the cut = %+v, want %d bytes", index, result, want)
		}
	}
	wantChunkSidecars(t, ctx, venue.direct, unit, 1, 4)
}

// CHAOS-14380 cell (C6): the floor run after a capped yield streak is one
// statement of at most one largest row (2 MiB).
func TestChunkSidecarReclaimStepAtFloorRunsOneTwoMiBStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := startChunkReclaimBudgetVenue(t, ctx, false)
	now := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
	resetChunkReclaimVenue(t, ctx, chunkReclaimVenue{Admin: venue.direct})
	unit := chunkReclaimUnitID(91)
	seedChunkReclaimUnit(t, ctx, venue.direct, unit, "failed", 10, chunkMiB, now)
	reclaim, err := NewChunkSidecarReclaimWithConfig(venue.domain, DefaultChunkSidecarReclaimConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, err := reclaim.StepAtFloor(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.Statements != 1 || result.Rows != 2 || result.Bytes != 2*chunkMiB {
		t.Fatalf("floor run = %+v, want 1 statement of 2 rows / 2 MiB", result)
	}
	wantChunkSidecars(t, ctx, venue.direct, unit, 1, 8)
}
