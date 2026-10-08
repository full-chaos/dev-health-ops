package workerservice

import (
	"context"
	"errors"
	"strings"
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
	registry := newNativeTeamCatalogCollectors(conn)
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
