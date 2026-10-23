package providersync

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type engineShapeConn struct {
	driver.Conn
	engineFull string
	sortingKey string
}

type engineShapeRow struct{ conn engineShapeConn }

func (conn engineShapeConn) QueryRow(context.Context, string, ...any) driver.Row {
	return engineShapeRow{conn: conn}
}

func (row engineShapeRow) Err() error           { return nil }
func (row engineShapeRow) ScanStruct(any) error { return errors.New("not used") }
func (row engineShapeRow) Scan(dest ...any) error {
	*dest[0].(*string) = row.conn.engineFull
	*dest[1].(*string) = row.conn.sortingKey
	return nil
}

const shapeCloudPath = "'/clickhouse/tables/{uuid}/{shard}', '{replica}'"

func TestTableShapeGuardsReadEveryEngineSpelling(t *testing.T) {
	t.Parallel()
	ownershipSpec := ownershipCompactionSpecs[0]
	ownershipKey := "org_id, provider, " + ownershipSpec.natural + ", team_id, source, valid_from"
	dependencyKey := "org_id, source_work_item_id, target_work_item_id, relationship_type"
	forms := []struct {
		name     string
		engine   func(column string) string
		accepted bool
	}{
		{"self-hosted", func(c string) string { return "ReplacingMergeTree(" + c + ") ORDER BY x" }, true},
		{"self-hosted backticked", func(c string) string { return "ReplacingMergeTree(`" + c + "`) ORDER BY x" }, true},
		{"replicated", func(c string) string {
			return "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', " + c + ") ORDER BY x"
		}, true},
		{"cloud shared", func(c string) string { return "SharedReplacingMergeTree(" + shapeCloudPath + ", " + c + ") ORDER BY x" }, true},
		{"cloud shared wrong version", func(string) string { return "SharedReplacingMergeTree(" + shapeCloudPath + ", other_at) ORDER BY x" }, false},
		{"cloud shared no version", func(string) string { return "SharedReplacingMergeTree(" + shapeCloudPath + ") ORDER BY x" }, false},
		{"cloud shared MergeTree", func(string) string { return "SharedMergeTree(" + shapeCloudPath + ") ORDER BY x" }, false},
		{"cloud shared without replication arguments", func(c string) string { return "SharedReplacingMergeTree(" + c + ") ORDER BY x" }, false},
		{"unparseable", func(c string) string { return "SharedReplacingMergeTree('/p', '{r}', " + c }, false},
	}
	for _, form := range forms {
		t.Run("ownership/"+form.name, func(t *testing.T) {
			t.Parallel()
			conn := engineShapeConn{engineFull: form.engine("updated_at"), sortingKey: ownershipKey}
			err := verifyOwnershipTableShape(context.Background(), conn, ownershipSpec)
			if form.accepted && err != nil {
				t.Fatalf("refused an accepted engine: %v", err)
			}
			if !form.accepted && !errors.Is(err, ErrOwnershipCompactionTable) {
				t.Fatalf("error = %v, want ErrOwnershipCompactionTable", err)
			}
		})
		t.Run("dependency/"+form.name, func(t *testing.T) {
			t.Parallel()
			conn := engineShapeConn{engineFull: form.engine("last_synced"), sortingKey: dependencyKey}
			err := verifyDependencyTableShape(context.Background(), conn)
			if form.accepted && err != nil {
				t.Fatalf("refused an accepted engine: %v", err)
			}
			if !form.accepted && !errors.Is(err, ErrWorkItemDependencyDrainTable) {
				t.Fatalf("error = %v, want ErrWorkItemDependencyDrainTable", err)
			}
		})
	}
}

func TestTableShapeGuardsStillRefuseAWrongSortingKeyOnCloud(t *testing.T) {
	t.Parallel()
	engine := "SharedReplacingMergeTree(" + shapeCloudPath + ", last_synced) ORDER BY x"
	conn := engineShapeConn{engineFull: engine, sortingKey: "org_id"}
	if err := verifyDependencyTableShape(context.Background(), conn); !errors.Is(err, ErrWorkItemDependencyDrainTable) {
		t.Fatalf("error = %v, want ErrWorkItemDependencyDrainTable", err)
	}
}
