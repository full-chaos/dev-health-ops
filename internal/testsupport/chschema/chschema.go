// Package chschema applies the REAL ClickHouse migration chain to a throwaway
// test container.
//
// It exists because hand-typed DDL in a Go test is not a schema: it is a
// SECOND, unversioned copy of one, and the test that reads it back can only
// ever confirm what the test itself declared. The derived work-item tables
// caught this concretely -- the hand-typed work_item_team_attributions carried
// the PRE-053 enums, missing the `issue_project` and `manual_fallback` source
// values and the `manual` and `none` confidence values that the production
// resolver actually emits. Every test over that table was green while an insert
// of a genuinely reachable row would have been rejected by the real column.
//
// So no DDL is authored here. The schema is built by the same migrator
// production runs (`dho migrate clickhouse upgrade`, internal/chmigrate): the
// checked-in head, then every chain file after it. No process is started and
// no Python is involved.
//
// The head is production's ordering contract 2. The retired Python chain
// defaulted to contract 1 when OPERATIONAL_ORDERING_CONTRACT was unset, so a
// test that still needs the legacy table shape asks for it by name
// (ApplyOrderingContract1); Apply never builds it silently.
//
// SHARED, and table-agnostic by construction: Apply takes no table list and
// migrates the database to the chain's head, so any package needing real
// tables gets all of them at once.
package chschema

import (
	"context"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/operationalordering"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Apply migrates the container to the head of the chain, ordering contract 2.
//
// It FAILS the test rather than skipping. A skip here would silently drop every
// schema-dependent assertion in the calling package while the package still
// reported ok, which is precisely the unmeasured-but-green shape this helper
// was written to remove.
//
// A process environment that names another contract
// (OPERATIONAL_ORDERING_CONTRACT set to anything but 2) is refused: the Go
// migrator would build contract 2 regardless, and a test that believes it got
// contract 1 would be asserting against the wrong schema.
func Apply(ctx context.Context, t *testing.T, instance *containers.Instance) {
	t.Helper()
	if err := checkContractEnv(os.LookupEnv(chmigrate.OrderingContractEnv)); err != nil {
		t.Fatalf("chschema: %v", err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	migrate(ctx, t, instance, baseline)
}

// ApplyOrderingContract1 migrates the container to the head of the chain as the
// legacy ordering contract 1 builds it. Production refuses contract 1
// (`dho migrate clickhouse` exits with settings_mismatch), so this exists only
// for tests of the code that still reads or migrates that shape.
func ApplyOrderingContract1(ctx context.Context, t *testing.T, instance *containers.Instance) {
	t.Helper()
	baseline, err := LoadContract1Baseline()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	migrate(ctx, t, instance, baseline)
}

// checkContractEnv refuses an environment that names a contract Apply does not
// build. Unset is accepted: it names nothing, and Apply builds contract 2.
func checkContractEnv(value string, set bool) error {
	if _, err := operationalordering.ResolveValue(value, set); err != nil {
		return fmt.Errorf("%s=%q, but Apply builds production's contract 2; use ApplyOrderingContract1 for the legacy shape",
			chmigrate.OrderingContractEnv, value)
	}
	return nil
}

func migrate(ctx context.Context, t *testing.T, instance *containers.Instance, baseline chmigrate.Baseline) {
	t.Helper()
	if instance == nil {
		t.Fatal("chschema: no ClickHouse instance")
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	config := chstorage.DefaultConfig(instance.URI)
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	conn, err := chstorage.Open(ctx, config)
	if err != nil {
		t.Fatalf("chschema: open: %v", err)
	}
	defer conn.Close()
	db, database, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	if database == "" {
		t.Fatal("chschema: the connection names no database")
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("chschema: migrate %s: %v", database, err)
	}
}
