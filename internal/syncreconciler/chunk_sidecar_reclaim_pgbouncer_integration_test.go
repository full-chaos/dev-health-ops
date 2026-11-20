//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// chunkReclaimPgBouncerImage is goWorkers.pgbouncer.image in
// deploy/helm/dev-health/values.yaml, the pooler the domain DSN goes through.
const chunkReclaimPgBouncerImage = "edoburu/pgbouncer:1.25.2@sha256:4c1ca296ef525f108f5d3552cc337c0c09587cf8dae7f0067fd93349e47dc1cd"

// The domain pool reaches PostgreSQL through PgBouncer in TRANSACTION mode
// (chart go-pgbouncer.yaml: POSTGRES_URI -> go-pgbouncer-transaction). Two
// reclaimer processes take the tick lock at the same time through that pooler:
// exactly one leads, and no advisory lock is left on any server connection
// after the tick.
func TestChunkSidecarReclaimTickLockThroughATransactionModePooler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
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
	bouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: chunkReclaimPgBouncerImage,
		Env: map[string]string{
			"DB_HOST": "pg", "DB_PORT": "5432", "DB_NAME": database, "DB_USER": user, "DB_PASSWORD": password,
			"LISTEN_PORT": "6432", "POOL_MODE": "transaction", "AUTH_TYPE": "scram-sha-256",
			"DEFAULT_POOL_SIZE": "2", "MAX_CLIENT_CONN": "64",
		},
		Networks: []string{nw.Name}, ExposedPorts: []string{"6432/tcp"},
		WaitingFor: wait.ForListeningPort("6432/tcp").WithStartupTimeout(60 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bouncer.Terminate(context.Background()) })

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
	pooled := func() *pgxpool.Pool {
		pool, err := postgresstore.New(ctx, postgresstore.DefaultConfig(address(bouncer, "6432/tcp")))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	first, second := pooled(), pooled()
	advisoryLocks := func() int {
		t.Helper()
		var held int
		if err := direct.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted`).Scan(&held); err != nil {
			t.Fatal(err)
		}
		return held
	}

	for round := 0; round < 3; round++ {
		tickRound(t, round, first, second, advisoryLocks)
	}

	// A full tick through the pooler reclaims a terminal unit and leaves no lock.
	now := time.Date(2026, time.November, 19, 23, 0, 0, 0, time.UTC)
	resetLeaseRepairTables(t, ctx, direct)
	seedLeaseRepairRun(t, ctx, direct, chunkLeakRun, "org-a", "running")
	unit := chunkReclaimUnitID(80)
	seedChunkReclaimUnit(t, ctx, direct, unit, "failed", 200, 1, now)
	reclaim, err := NewChunkSidecarReclaim(first)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reclaim.Step(ctx, now, 100)
	if err != nil || result.LockSkipped || result.Units != 1 || result.Rows != 200 {
		t.Fatalf("tick through the transaction pooler = %+v / %v, want the unit (200 rows) reclaimed", result, err)
	}
	wantChunkSidecars(t, ctx, direct, unit, 0, 0)
	if held := advisoryLocks(); held != 0 {
		t.Fatalf("%d advisory locks left after a full tick through the pooler", held)
	}
}

// tickRound takes the tick lock from both pools at the same time and checks
// that exactly one leads and that the lock is gone from the server afterwards.
func tickRound(t *testing.T, round int, first, second *pgxpool.Pool, advisoryLocks func() int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	type taken struct {
		acquired bool
		release  func()
		err      error
	}
	results := make(chan taken, 2)
	proceed := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(proceed) }) }
	// Every exit, a failed assertion included, lets the holders end their
	// ticks; otherwise the pools' cleanup waits for their connections forever.
	defer releaseAll()
	for _, pool := range []*pgxpool.Pool{first, second} {
		go func(pool *pgxpool.Pool) {
			release, acquired, err := poolChunkSidecarReclaimTickLock(pool)(ctx)
			results <- taken{acquired, release, err}
			<-proceed
			if acquired {
				release()
			}
		}(pool)
	}
	leaders := 0
	for range 2 {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("round %d: tick lock through the transaction pooler: %v", round, outcome.err)
		}
		if outcome.acquired {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("round %d: %d of 2 concurrent ticks hold the tick lock, want exactly 1", round, leaders)
	}
	if held := advisoryLocks(); held != 1 {
		t.Fatalf("round %d: %d advisory locks on the server during the tick, want 1", round, held)
	}
	releaseAll()
	deadline := time.Now().Add(10 * time.Second)
	for advisoryLocks() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("round %d: the tick lock is still held on a server connection after the tick", round)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
