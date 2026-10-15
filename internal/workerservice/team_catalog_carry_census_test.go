package workerservice

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// teamCatalogCarryConn answers every read with an error and refuses every
// other call: a collector that runs before the carry fails another way.
type teamCatalogCarryConn struct {
	driver.Conn
	queries int
	batches int
	err     error
}

func (conn *teamCatalogCarryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	return nil, conn.err
}

func (conn *teamCatalogCarryConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.batches++
	return nil, errors.New("no batch")
}

// TestEveryRegisteredTeamCatalogCollectorCarriesFirstCensus runs every
// collector of the worker's registry with a store whose first read fails:
// each must stop on the carry's count read, before it reads a provider or
// the store for itself.
func TestEveryRegisteredTeamCatalogCollectorCarriesFirstCensus(t *testing.T) {
	failed := errors.New("count read failed")
	conn := &teamCatalogCarryConn{err: failed}
	registry := newNativeTeamCatalogCollectors(conn, nil, nil, nil, nil)
	if len(registry) < 4 {
		t.Fatalf("registry = %d collectors, want every provider", len(registry))
	}
	for provider, collector := range registry {
		conn.queries, conn.batches = 0, 0
		_, err := collector.CollectTeamCatalog(context.Background(),
			providersync.TeamCatalogReference{OrgID: "org-1", SyncRunID: "run", Strict: true},
			providerfoundation.Credential{Provider: provider}, nil,
			providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now())
		if !errors.Is(err, failed) || !strings.Contains(err.Error(), "team id carry: count") || conn.queries != 1 || conn.batches != 0 {
			t.Errorf("%s: err = %v, reads = %d, batches = %d; want only the carry's count read and its error", provider, err, conn.queries, conn.batches)
		}
	}
}

// TestNativeTeamCatalogRegistryIsOnlyHandedToTheDispatchers fails when the
// registry variable in sync_dispatch.go is used in any way but: built by
// newNativeTeamCatalogCollectors, then handed whole to the two dispatchers.
// An index write or a second registry would add a collector the carry does
// not wrap; the dispatchers refuse it (providersync.RequireCarried), and
// this test fails first.
func TestNativeTeamCatalogRegistryIsOnlyHandedToTheDispatchers(t *testing.T) {
	data, err := os.ReadFile("sync_dispatch.go")
	if err != nil {
		t.Fatal(err)
	}
	allowed := []*regexp.Regexp{
		regexp.MustCompile(`^nativeTeamCatalogCollectors := newProductionTeamCatalogCollectors\(clickhouseConnection, postgresDatabase\.pools\.Domain\)$`),
		regexp.MustCompile(`^Native:\s+nativeTeamCatalogCollectors,$`),
		regexp.MustCompile(`^native:\s+nativeTeamCatalogCollectors,$`),
	}
	used := 0
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") || !strings.Contains(trimmed, "nativeTeamCatalogCollectors") || strings.HasPrefix(trimmed, "func newNativeTeamCatalogCollectors(") {
			continue
		}
		used++
		ok := false
		for _, pattern := range allowed {
			ok = ok || pattern.MatchString(trimmed)
		}
		if !ok {
			t.Errorf("sync_dispatch.go: %q uses the collector registry outside the wrapped path", trimmed)
		}
	}
	if used != len(allowed) {
		t.Errorf("registry used on %d lines, want %d (build, executor, post-sync dispatcher)", used, len(allowed))
	}
}

type recordingTurn struct {
	mu    sync.Mutex
	calls [][2]string
}

func (turn *recordingTurn) Serialize(_ context.Context, orgID, provider string) (func(), error) {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	turn.calls = append(turn.calls, [2]string{orgID, provider})
	return func() {}, nil
}

// CHAOS-9140: every collector of the worker's registry takes the per-
// (organization, provider) turn before the carry reads anything, and the
// production wiring hands the registry the Postgres turn.
func TestEveryRegisteredTeamCatalogCollectorTakesItsTurnFirst(t *testing.T) {
	failed := errors.New("count read failed")
	conn := &teamCatalogCarryConn{err: failed}
	turn := &recordingTurn{}
	registry := newNativeTeamCatalogCollectors(conn, turn, nil, nil, nil)
	for provider, collector := range registry {
		turn.calls = nil
		_, err := collector.CollectTeamCatalog(context.Background(),
			providersync.TeamCatalogReference{OrgID: "org-1", SyncRunID: "run", Strict: true},
			providerfoundation.Credential{Provider: provider}, nil,
			providersync.TeamCatalogSelections{Teams: true}, time.Now())
		if !errors.Is(err, failed) || len(turn.calls) != 1 || turn.calls[0] != [2]string{"org-1", provider} {
			t.Errorf("%s: err = %v, turns = %v; want the turn of (org-1, %s) taken once before the carry's read", provider, err, turn.calls, provider)
		}
	}
	// The production constructor, built from a pool (no connection is made), hands every
	// collector the lease turn: checked on what it returns, not on the text of its call site.
	pool, err := pgxpool.New(context.Background(), "postgres://turn:turn@127.0.0.1:1/turn?pool_max_conns=4")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	production := newProductionTeamCatalogCollectors(&teamCatalogCarryConn{}, pool)
	if len(production) < 4 {
		t.Fatalf("production registry = %d collectors, want every provider", len(production))
	}
	for provider, collector := range production {
		carried, ok := collector.(providersync.CarryFirstTeamCatalogCollector)
		if !ok {
			t.Errorf("%s: %T is not behind the carry wrapper", provider, collector)
			continue
		}
		if _, ok := carried.Serializer.(jobruntime.TeamCatalogTurn); !ok {
			t.Errorf("%s: serializer is %T, want the catalog lease turn (CHAOS-9140)", provider, carried.Serializer)
		}
	}
}
