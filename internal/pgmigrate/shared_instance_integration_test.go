//go:build integration

package pgmigrate_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// One private PostgreSQL container serves every test of this package that only
// creates scratch databases. It is a container (not a remote DSN) because the
// oracles dump databases with the server's own pg_dump. A test that changes
// server-wide state (a role, a role default) takes isolatedInstance instead.
var (
	sharedMu       sync.Mutex
	sharedServer   *containers.Instance
	sharedBaseline []string
)

const serverStateQuery = `
SELECT 'database ' || datname FROM pg_database
UNION ALL SELECT 'role ' || rolname FROM pg_roles
UNION ALL SELECT 'role setting ' || coalesce(setdatabase::text, '') || ' ' || coalesce(setrole::text, '') || ' ' || array_to_string(setconfig, ',') FROM pg_db_role_setting
UNION ALL SELECT 'slot ' || slot_name FROM pg_replication_slots
UNION ALL SELECT 'prepared transaction ' || gid FROM pg_prepared_xacts
ORDER BY 1`

func serverState(ctx context.Context, uri string) ([]string, error) {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, serverStateQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var state []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			return nil, err
		}
		state = append(state, row)
	}
	return state, rows.Err()
}

func sharedInstance(t testing.TB) *containers.Instance {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedServer != nil {
		return sharedServer
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgresIsolated(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	if instance.Container == nil {
		t.Fatal("the oracles dump databases with the server's own pg_dump and need a container instance, not a remote DSN")
	}
	baseline, err := serverState(ctx, instance.URI)
	if err != nil {
		_ = instance.Close(context.Background())
		t.Fatalf("read the server state: %v", err)
	}
	sharedServer, sharedBaseline = instance, baseline
	finishShared = finishSharedInstance
	return instance
}

func isolatedInstance(t testing.TB) *containers.Instance {
	t.Helper()
	instance, err := containers.StartPostgresIsolated(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	if instance.Container == nil {
		t.Fatal("the oracles dump databases with the server's own pg_dump and need a container instance, not a remote DSN")
	}
	return instance
}

func finishSharedInstance() int {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedServer == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	code := 0
	after, err := serverState(ctx, sharedServer.URI)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: shared postgres: read the server state after the run: %v\n", err)
		code = 1
	} else {
		for _, row := range after {
			if !slices.Contains(sharedBaseline, row) {
				fmt.Fprintf(os.Stderr, "FAIL: shared postgres leak: new %s\n", row)
				code = 1
			}
		}
		for _, row := range sharedBaseline {
			if !slices.Contains(after, row) {
				fmt.Fprintf(os.Stderr, "FAIL: shared postgres leak: gone %s\n", row)
				code = 1
			}
		}
	}
	if err := sharedServer.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: shared postgres: terminate: %v\n", err)
		code = 1
	}
	sharedServer = nil
	return code
}
