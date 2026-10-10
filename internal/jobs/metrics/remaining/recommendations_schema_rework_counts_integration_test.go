//go:build integration

package remaining

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
)

// The recommendations job reads the pull request rework ratio from the stored
// counts of repo_metrics_daily. Its construction check must name those
// columns: a database that does not hold one of them (the migration that adds
// them is not applied) is refused at construction, once and loudly, and not at
// the first read of every partition.
func TestRecommendationsSchemaCheckNamesTheReworkCountColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// The store helper stops its container in a cleanup with this context:
	// cancel after it, not before.
	t.Cleanup(cancel)
	conn := membershipMigratedClickHouse(t, ctx)
	if err := verifyRecommendationsSchema(ctx, conn); err != nil {
		t.Fatalf("the schema of the migration chain is refused: %v", err)
	}
	// Each count column the read uses, dropped in turn and put back with its
	// own type: the three the migration adds, and the merged count the table
	// held from its first version.
	for _, column := range prrework.CountColumns {
		var columnType string
		if err := conn.QueryRow(ctx,
			"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'repo_metrics_daily' AND name = ?", column,
		).Scan(&columnType); err != nil {
			t.Fatalf("read the type of %s: %v", column, err)
		}
		if err := conn.Exec(ctx, "ALTER TABLE repo_metrics_daily DROP COLUMN "+column); err != nil {
			t.Fatalf("drop %s: %v", column, err)
		}
		err := verifyRecommendationsSchema(ctx, conn)
		if !errors.Is(err, ErrRecommendationsSchemaIncompatible) || !strings.Contains(err.Error(), column) {
			t.Errorf("a schema with no %s: the check gives %v, want ErrRecommendationsSchemaIncompatible that names the column", column, err)
		}
		if err := conn.Exec(ctx, "ALTER TABLE repo_metrics_daily ADD COLUMN "+column+" "+columnType); err != nil {
			t.Fatalf("add %s again: %v", column, err)
		}
	}
	// The deprecated ratio column is not read any more: a schema with no such
	// column is accepted.
	if err := conn.Exec(ctx, "ALTER TABLE repo_metrics_daily DROP COLUMN "+prrework.DeprecatedRatioColumn); err != nil {
		t.Fatalf("drop the deprecated column: %v", err)
	}
	if err := verifyRecommendationsSchema(ctx, conn); err != nil {
		t.Errorf("a schema with no deprecated ratio column is refused: %v", err)
	}
}
