//go:build integration

package chschema

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/operationalbackfill"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func startInstance(ctx context.Context, t *testing.T) *containers.Instance {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	return instance
}

// operationalContracts reads every operational table back from the server and
// returns the contract the Go reader classifies each as, and whether
// schema_migrations records migration 067.
func operationalContracts(ctx context.Context, t *testing.T, instance *containers.Instance) (map[string]operationalbackfill.Contract, bool) {
	t.Helper()
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, "SELECT name, create_table_query FROM system.tables WHERE database = currentDatabase() AND name LIKE 'operational\\\\_%' AND engine NOT LIKE '%View'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]operationalbackfill.Contract{}
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		contract, err := operationalbackfill.TableContract(ddl, name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got[name] = contract
	}
	var recorded uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = '067_operational_ordering_contract.py'").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	return got, recorded == 1
}

func TestApplyBuildsProductionsContract(t *testing.T) {
	ctx := context.Background()
	instance := startInstance(ctx, t)
	Apply(ctx, t, instance)
	contracts, recorded := operationalContracts(ctx, t, instance)
	if len(contracts) != 12 || !recorded {
		t.Fatalf("%d operational tables, 067 recorded %v; want 12 and true", len(contracts), recorded)
	}
	for name, contract := range contracts {
		if contract != operationalbackfill.ContractCurrent {
			t.Errorf("%s is contract %v, want 2", name, contract)
		}
	}
	// A second Apply on the migrated database is the migrator's no-op.
	Apply(ctx, t, instance)
}

func TestApplyOrderingContract1BuildsTheLegacyShape(t *testing.T) {
	ctx := context.Background()
	instance := startInstance(ctx, t)
	ApplyOrderingContract1(ctx, t, instance)
	contracts, recorded := operationalContracts(ctx, t, instance)
	if len(contracts) != 12 || recorded {
		t.Fatalf("%d operational tables, 067 recorded %v; want 12 and false", len(contracts), recorded)
	}
	for name, contract := range contracts {
		if contract != operationalbackfill.ContractLegacy {
			t.Errorf("%s is contract %v, want 1", name, contract)
		}
	}
}
