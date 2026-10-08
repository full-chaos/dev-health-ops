package workerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
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

func TestTeamCatalogAutoimportDispatcherRefusesAnUncarriedCollector(t *testing.T) {
	native := &linearCollectorSpy{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "linear", nil },
		native:          map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections: fakeAutoimportSelectionsResolver{
			selections: providersync.TeamCatalogSelections{Teams: true},
		},
	}
	err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	})
	if !errors.Is(err, providersync.ErrTeamCatalogCollectorNotCarried) {
		t.Fatalf("error=%v want=%v", err, providersync.ErrTeamCatalogCollectorNotCarried)
	}
	if native.called {
		t.Fatal("an uncarried collector ran")
	}
}
