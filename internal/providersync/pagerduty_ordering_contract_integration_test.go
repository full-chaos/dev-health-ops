//go:build integration

package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Every PagerDuty sink writes, reads back and replays exactly against the
// real migrated tables in both column shapes, with the deployment env set to
// each accepted value (including the values that disagree with the table).
func TestEveryPagerDutySinkWritesAndReadsBackInBothTableShapes(t *testing.T) {
	for _, tableShape := range []struct {
		name     string
		migrate  string
		contract operationalStorageContract
	}{
		{name: "contract1", migrate: "<unset>", contract: operationalLegacyContract},
		{name: "contract2", migrate: "2", contract: operationalCurrentContract},
	} {
		t.Run(tableShape.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			instance, err := containers.StartClickHouse(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer closeCancel()
				if err := instance.Close(closeCtx); err != nil {
					t.Errorf("terminate ClickHouse: %v", err)
				}
			})
			// The migration chain builds the contract-2 tables only when the
			// env asks for them (migration 067).
			setProbeEnv(t, tableShape.migrate)
			chschema.Apply(ctx, t, instance)
			conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			for _, env := range []string{"<unset>", "1", "2"} {
				setProbeEnv(t, env)
				orgID := "org-" + tableShape.name + "-env" + strings.Trim(env, "<>")
				for _, sinkCase := range pagerDutyContractSinkCases(t, orgID) {
					label := "env=" + env + " sink=" + sinkCase.name
					contract, err := (*operationalTableContracts)(nil).resolve(ctx, conn, sinkCase.table)
					if err != nil || contract != tableShape.contract {
						t.Fatalf("%s: table %s contract=%d err=%v want %d", label, sinkCase.table, contract, err, tableShape.contract)
					}
					sink := sinkCase.build(conn)
					if inspection, err := sink.InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectAbsent {
						t.Fatalf("%s: before write inspection=%s error=%v", label, inspection, err)
					}
					if err := sink.WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
						t.Fatalf("%s: write: %v", label, err)
					}
					if inspection, err := sink.InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
						t.Fatalf("%s: after write inspection=%s error=%v", label, inspection, err)
					}
					if err := sink.WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
						t.Fatalf("%s: replay write: %v", label, err)
					}
					if inspection, err := sink.InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
						t.Fatalf("%s: after replay inspection=%s error=%v", label, inspection, err)
					}
					if tableShape.contract == operationalCurrentContract {
						var stored, invalid uint64
						if err := conn.QueryRow(ctx,
							"SELECT count(), countIf(ordering_contract != 2 OR source_revision = 0 OR ingest_revision = 0 OR source_conflict_key = '') FROM "+
								sinkCase.table+" WHERE org_id = ?", orgID,
						).Scan(&stored, &invalid); err != nil {
							t.Fatalf("%s: count: %v", label, err)
						}
						if stored == 0 || invalid != 0 {
							t.Fatalf("%s: stored=%d rows with missing ordering values=%d", label, stored, invalid)
						}
					}
				}
			}
		})
	}
}

// One sink value per sink/destination writes on a contract-1 table, the
// table is migrated to contract 2 by the production migration chain, and the
// same sink values write, read back and replay exactly on the new shape.
func TestEveryPagerDutySinkFollowsATableMigratedBetweenCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	setProbeEnv(t, "<unset>")
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	before := pagerDutyContractSinkCases(t, "org-before-migration")
	after := pagerDutyContractSinkCases(t, "org-after-migration")
	sinks := make([]pagerDutyContractEffects, len(before))
	for i, sinkCase := range before {
		sinks[i] = sinkCase.build(conn)
		if err := sinks[i].WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
			t.Fatalf("%s: contract-1 write: %v", sinkCase.name, err)
		}
		if inspection, err := sinks[i].InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
			t.Fatalf("%s: contract-1 inspection=%s error=%v", sinkCase.name, inspection, err)
		}
	}

	setProbeEnv(t, "2")
	chschema.Apply(ctx, t, instance)

	for i, sinkCase := range after {
		contract, err := (*operationalTableContracts)(nil).resolve(ctx, conn, sinkCase.table)
		if err != nil || contract != operationalCurrentContract {
			t.Fatalf("%s: migrated table contract=%d err=%v", sinkCase.name, contract, err)
		}
		if err := sinks[i].WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
			t.Fatalf("%s: write after migration: %v", sinkCase.name, err)
		}
		if inspection, err := sinks[i].InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
			t.Fatalf("%s: after migration inspection=%s error=%v", sinkCase.name, inspection, err)
		}
		if err := sinks[i].WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
			t.Fatalf("%s: replay after migration: %v", sinkCase.name, err)
		}
		if inspection, err := sinks[i].InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
			t.Fatalf("%s: replay inspection=%s error=%v", sinkCase.name, inspection, err)
		}
	}
}

// racedShapeConn reports one stale shape for the first system.columns read
// and the real table afterwards: the same observable sequence as a table
// migrated between a call's shape read and its statements.
type racedShapeConn struct {
	driver.Conn
	stale string
	lies  int
}

func (conn *racedShapeConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if conn.lies > 0 && strings.Contains(query, "system.columns") {
		conn.lies--
		names := []string{"org_id", "id"}
		if conn.stale == probeTableCurrent {
			names = append(names, operationalOrderingColumnNames...)
		}
		return &probeNameRows{names: names}, nil
	}
	return conn.Conn.Query(ctx, query, args...)
}

// Both directions of a raced shape, against the real migrated tables: the
// raced INSERT is refused by ClickHouse with zero rows stored, a raced
// readback is an error (never absent, never exact), and the following
// honest call succeeds.
func TestPagerDutyRacedShapeIsRefusedAndTheNextCallSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	setProbeEnv(t, "<unset>")
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	for _, direction := range []struct {
		name  string
		table string // the table's real shape
		stale string // the shape the raced call read
	}{
		{name: "table1-read2", table: probeTableLegacy, stale: probeTableCurrent},
		{name: "table2-read1", table: probeTableCurrent, stale: probeTableLegacy},
	} {
		if direction.table == probeTableCurrent {
			setProbeEnv(t, "2")
			chschema.Apply(ctx, t, instance)
		}
		count := len(pagerDutyContractSinkCases(t, "org-count"))
		for i := range count {
			orgID := fmt.Sprintf("org-race-%s-%d", direction.name, i)
			sinkCase := pagerDutyContractSinkCases(t, orgID)[i]
			label := direction.name + " sink=" + sinkCase.name
			raced := func() pagerDutyContractEffects {
				return sinkCase.build(&racedShapeConn{Conn: conn, stale: direction.stale, lies: 1})
			}
			honest := sinkCase.build(conn)
			stored := func() uint64 {
				var rows uint64
				if err := conn.QueryRow(ctx, "SELECT count() FROM "+sinkCase.table+" WHERE org_id = ?", orgID).Scan(&rows); err != nil {
					t.Fatalf("%s: count: %v", label, err)
				}
				return rows
			}
			// The raced readback is refused by the executing server: a
			// contract-2 SELECT on a contract-1 table names unknown columns, and
			// a legacy SELECT on a contract-2 table trips the shape guard.
			wantRefusal := "source_revision"
			if direction.stale == probeTableLegacy {
				wantRefusal = "legacy readback refused"
			}
			racedReadback := func(when string) {
				inspection, err := raced().InspectEffect(ctx, sinkCase.claim, sinkCase.effect)
				if err == nil || inspection != EffectConflict || !strings.Contains(err.Error(), wantRefusal) {
					t.Fatalf("%s: raced readback %s inspection=%s err=%v want refusal %q", label, when, inspection, err, wantRefusal)
				}
				t.Logf("%s: raced readback %s refused: %.160s", label, when, err.Error())
			}
			racedReadback("before write")
			err := raced().WriteEffect(ctx, sinkCase.claim, sinkCase.effect)
			if err == nil {
				t.Fatalf("%s: raced write was accepted", label)
			}
			if rows := stored(); rows != 0 {
				t.Fatalf("%s: raced write stored %d rows (err=%v)", label, rows, err)
			}
			t.Logf("%s: raced write refused, 0 rows: %v", label, err)
			if err := honest.WriteEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil {
				t.Fatalf("%s: write after the raced one: %v", label, err)
			}
			if inspection, err := honest.InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
				t.Fatalf("%s: honest inspection=%s err=%v", label, inspection, err)
			}
			racedReadback("after write")
			if inspection, err := honest.InspectEffect(ctx, sinkCase.claim, sinkCase.effect); err != nil || inspection != EffectExact {
				t.Fatalf("%s: honest inspection after raced readback=%s err=%v", label, inspection, err)
			}
		}
	}
}
