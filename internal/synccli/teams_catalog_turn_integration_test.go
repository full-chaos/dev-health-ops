//go:build integration

package synccli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// atomicCarryConn is teamCatalogCarryConn with counters that a goroutine can read while the run lives.
type atomicCarryConn struct {
	driver.Conn
	queries, batches atomic.Int32
}

func (conn *atomicCarryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries.Add(1)
	return nil, errors.New("count read failed")
}

func (conn *atomicCarryConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.batches.Add(1)
	return nil, errors.New("no batch")
}

func (conn *atomicCarryConn) Close() error { return nil }

func startCatalogTurnPostgres(ctx context.Context, t *testing.T) string {
	t.Helper()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	setup, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	pgschema.Apply(ctx, t, setup)
	setup.Close()
	return pg.URI
}

func runCatalogTeamsAgainst(ctx context.Context, pgURI string, conn *atomicCarryConn) (int, string) {
	d := defaultDeps()
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	d.doer = &http.Client{Transport: refusingTransport{calls: new(int)}}
	var stderr strings.Builder
	code := runTeams(ctx, cli.Env{
		Args:   []string{"--provider", "github", "--owner", "acme", "--org", catalogTurnOrg, "--auth", "token-for-test", "--db", pgURI},
		Lookup: lookup(map[string]string{"CLICKHOUSE_URI": dsnValue}),
		Stdout: io.Discard, Stderr: &stderr,
	}, d)
	return code, stderr.String()
}

// CHAOS-9182: a worker holds the catalog turn of the organization and provider; the operator CLI must wait for it
// instead of running over it, and run once it is released.
func TestCatalogTeamsWaitsForAHeldWorkerTurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	pgURI := startCatalogTurnPostgres(ctx, t)
	holderPool, err := pgxpool.New(ctx, pgURI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(holderPool.Close)
	release, err := jobruntime.NewTeamCatalogTurn(holderPool).Serialize(ctx, catalogTurnOrg, "github")
	if err != nil {
		t.Fatal(err)
	}

	conn := &atomicCarryConn{}
	done := make(chan int, 1)
	go func() {
		code, _ := runCatalogTeamsAgainst(ctx, pgURI, conn)
		done <- code
	}()

	time.Sleep(3 * time.Second)
	if got := conn.queries.Load(); got != 0 {
		t.Fatalf("the CLI made %d store reads while a worker held the catalog turn, want 0", got)
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("the CLI did not run after the turn was released")
	}
	if got := conn.queries.Load(); got != 1 {
		t.Errorf("after the release the CLI made %d store reads, want 1", got)
	}
}

// A CLI that is stopped while it waits writes nothing and fails.
func TestCatalogTeamsStoppedWhileWaitingForTheTurnWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	pgURI := startCatalogTurnPostgres(ctx, t)
	holderPool, err := pgxpool.New(ctx, pgURI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(holderPool.Close)
	release, err := jobruntime.NewTeamCatalogTurn(holderPool).Serialize(ctx, catalogTurnOrg, "github")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	conn := &atomicCarryConn{}
	waitCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	code, _ := runCatalogTeamsAgainst(waitCtx, pgURI, conn)

	if code == cli.ExitOK {
		t.Error("a CLI stopped while it waited for the turn exited 0")
	}
	if conn.queries.Load() != 0 || conn.batches.Load() != 0 {
		t.Errorf("the stopped CLI touched the store: %d reads, %d batches", conn.queries.Load(), conn.batches.Load())
	}
}
