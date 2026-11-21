//go:build integration

package home

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// Each fixed hash list of rowview equals the stored columns of its table at the migration head: every column but
// the sorting key and computed_at, in table order. A new column moves the list.
func TestRepoVersionColumnsAreTheStoredColumnsAtTheMigrationHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, _ := newHomeTestClickHouse(ctx, t)
	tables := map[string]bool{}
	for _, spec := range metrics {
		if spec.Table != "work_item_metrics_daily" && spec.Table != "work_item_state_durations_daily" {
			tables[spec.Table] = true
		}
	}
	for table := range tables {
		var sortingKey string
		if err := admin.QueryRow(ctx, "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = ?", table).Scan(&sortingKey); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		key := strings.Split(sortingKey, ", ")
		rows, err := admin.Query(ctx, "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
		if err != nil {
			t.Fatal(err)
		}
		var stored []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			if name != "computed_at" && !slices.Contains(key, name) {
				stored = append(stored, name)
			}
		}
		_ = rows.Close()
		if got := rowview.VersionColumns(table); !slices.Equal(got, stored) {
			t.Errorf("%s: rowview.VersionColumns = %v, the table stores %v (key %s)", table, got, stored, sortingKey)
		}
	}
	if len(tables) == 0 {
		t.Fatal("no repository-keyed metric table")
	}
}
