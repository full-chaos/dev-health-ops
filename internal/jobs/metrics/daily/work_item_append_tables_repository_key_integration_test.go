//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
)

// repositoryKeyViolation says why a table that holds one row for each
// repository partition of a day would lose rows at a merge, or "" when it
// cannot. A plain MergeTree keeps every row, so its sorting key is free. Every
// other engine of the family merges the rows of one sorting key into one, so
// its sorting key must hold repo_id.
func repositoryKeyViolation(engine, sortingKey string) string {
	if engine == "MergeTree" || engine == "ReplicatedMergeTree" {
		return ""
	}
	for _, column := range strings.Split(sortingKey, ",") {
		if strings.TrimSpace(column) == "repo_id" {
			return ""
		}
	}
	return "engine " + engine + " merges the rows of one sorting key, and the sorting key (" + sortingKey + ") has no repo_id"
}

// The daily job writes issue_type_metrics_daily, investment_metrics_daily and
// investment_classifications_daily one repository partition at a time, and the
// readers take the newest row of each key with repo_id in the key. That is
// safe only while a merge cannot drop the row of one repository in favour of
// the row of another. The test reads the engine and the sorting key of the
// three tables from a ClickHouse built by the migration chain.
func TestWorkItemAppendTablesCannotMergeTheRowsOfTwoRepositories(t *testing.T) {
	for _, guard := range []struct {
		engine, sortingKey string
		violation          bool
	}{
		{"MergeTree", "org_id, day, team_id", false},
		{"ReplacingMergeTree", "org_id, day, repo_id, team_id", false},
		{"ReplacingMergeTree", "org_id, day, team_id", true},
		{"ReplicatedReplacingMergeTree", "org_id, day, team_id", true},
		{"SummingMergeTree", "org_id, day, team_id", true},
		{"AggregatingMergeTree", "org_id, day, source_repo_id", true},
	} {
		if got := repositoryKeyViolation(guard.engine, guard.sortingKey) != ""; got != guard.violation {
			t.Fatalf("repositoryKeyViolation(%q, %q) reports a violation = %v, want %v", guard.engine, guard.sortingKey, got, guard.violation)
		}
	}

	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	for _, table := range []string{
		"issue_type_metrics_daily", "investment_metrics_daily", "investment_classifications_daily",
	} {
		var engine, sortingKey string
		var hasRepository uint64
		if err := conn.QueryRow(ctx, `
SELECT engine, sorting_key,
       (SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = 'repo_id')
FROM system.tables
WHERE database = currentDatabase() AND name = ?`, table, table).Scan(&engine, &sortingKey, &hasRepository); err != nil {
			t.Fatalf("%s: read the engine and the sorting key: %v", table, err)
		}
		if engine == "" || sortingKey == "" || hasRepository != 1 {
			t.Fatalf("%s: engine=%q sorting key=%q repo_id columns=%d; want a table with a repo_id column", table, engine, sortingKey, hasRepository)
		}
		if violation := repositoryKeyViolation(engine, sortingKey); violation != "" {
			t.Errorf("%s: %s: a merge would keep the row of one repository only", table, violation)
		}
	}
}
