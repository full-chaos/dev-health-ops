package aianalytics

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// bucketsPRClient serves two pull requests, one per AI kind, to the attributed pull request read. Like the package
// fixtures, it applies a predicate only when the statement binds it: a bound kinds list keeps the rows of those kinds.
// Every other read answers no rows.
type bucketsPRClient struct {
	prStatements int
}

func (c *bucketsPRClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if !strings.Contains(st, "repo_id_str, number, kind, work_type") {
		return &scriptedRows{}, nil
	}
	c.prStatements++
	var kinds []string
	for _, bd := range b {
		if bd.Name == "kinds" {
			kinds, _ = bd.Value.([]string)
		}
	}
	merged := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	var rows [][]any
	for _, r := range []struct {
		number uint32
		kind   string
	}{{1, "ai_assisted"}, {2, "agent_created"}} {
		if kinds != nil && !slices.Contains(kinds, r.kind) {
			continue
		}
		rows = append(rows, []any{"00000000-0000-0000-0000-00000000b001", r.number, r.kind, "pull_request", "PR synthetic", merged})
	}
	return &scriptedRows{rows: rows}, nil
}

// aiAttributedPrs with scope.buckets [AGENT_CREATED] serves the agent-created pull requests only, as
// aiAttributionOverview does with the same scope.
func TestAttributedPrsScopeBucketsNarrowTheRows(t *testing.T) {
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}

	control := &bucketsPRClient{}
	all, err := AttributedPrs(context.Background(), control, "org-synthetic-1", dr, nil, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if control.prStatements != 1 || len(all.Rows) != 2 {
		t.Fatalf("control: no scope: %d pull request reads, %d rows, want 1 read and 2 rows", control.prStatements, len(all.Rows))
	}
	t.Logf("control: no scope serves %d rows", len(all.Rows))

	client := &bucketsPRClient{}
	scope := &model.AIScopeInput{Buckets: []model.AIAttributionBucketInput{model.AIAttributionBucketInputAgentCreated}}
	got, err := AttributedPrs(context.Background(), client, "org-synthetic-1", dr, scope, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, r := range got.Rows {
		kinds = append(kinds, derefString(r.Kind))
	}
	if len(kinds) != 1 || kinds[0] != "agent_created" {
		t.Errorf("scope.buckets [AGENT_CREATED]: rows of kinds %v, want [agent_created]: the buckets never reach the pull request read", kinds)
	}
}
