package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type zeroCountConn struct{ driver.Conn }

func (zeroCountConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &zeroCountRows{}, nil
}

func (zeroCountConn) Close() error { return nil }

type zeroCountRows struct {
	driver.Rows
	read bool
}

func (rows *zeroCountRows) Next() bool {
	next := !rows.read
	rows.read = true
	return next
}

func (rows *zeroCountRows) Scan(dest ...any) error {
	for _, target := range dest {
		*target.(*uint64) = 0
	}
	return nil
}

func (rows *zeroCountRows) Err() error   { return nil }
func (rows *zeroCountRows) Close() error { return nil }

func carriedForTest(collectors map[string]providersync.TeamCatalogCollector) map[string]providersync.TeamCatalogCollector {
	return providersync.CarryFirstTeamCatalogCollectors(zeroCountConn{}, collectors)
}

func TestTeamCatalogDiscoveryExecutorRefusesAnUncarriedCollector(t *testing.T) {
	collector := &fakeTeamCatalogCollector{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:     map[string]providersync.TeamCatalogCollector{"linear": collector},
		Clients:    &fakeProviderClientResolver{},
		Selections: &fakeTeamCatalogSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
	}
	_, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if !errors.Is(err, providersync.ErrTeamCatalogCollectorNotCarried) {
		t.Fatalf("error=%v want=%v", err, providersync.ErrTeamCatalogCollectorNotCarried)
	}
	if collector.gotRef.OrgID != "" {
		t.Fatalf("an uncarried collector ran: ref=%+v", collector.gotRef)
	}
}
