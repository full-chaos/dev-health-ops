package remaining

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// The required collapse key, restated on purpose: dropping a column from
// capacityTableRequirements must fail here, not follow the edit silently.
var capacityRequiredCollapseKey = []string{
	"org_id", "provider", "day", "work_scope_id", "team_id", "row_view",
}

func TestCapacityNewestRowsCollapsesByTheRequiredKeyAndVersion(t *testing.T) {
	query := capacityNewestRows("org_id = {org_id:String}")

	limitBy := regexp.MustCompile(`LIMIT 1 BY (.+)\)$`).FindStringSubmatch(query)
	if limitBy == nil {
		t.Fatalf("no trailing LIMIT 1 BY in %q", query)
	}
	var got []string
	for _, column := range strings.Split(limitBy[1], ",") {
		got = append(got, strings.TrimSpace(column))
	}
	if strings.Join(got, ",") != strings.Join(capacityRequiredCollapseKey, ",") {
		t.Errorf("LIMIT 1 BY columns = %v, want %v", got, capacityRequiredCollapseKey)
	}

	order := strings.Index(query, "ORDER BY computed_at DESC, cityHash64(*) DESC")
	if order < 0 {
		t.Errorf("newest row must be chosen by computed_at DESC with a content tie-break: %q", query)
	}
	where := strings.Index(query, "WHERE org_id = {org_id:String}")
	limit := strings.Index(query, "LIMIT 1 BY")
	if where < 0 || order < where || limit < order {
		t.Errorf("scope filter must come before ORDER BY and LIMIT 1 BY: %q", query)
	}
	if strings.Contains(query, "FINAL") {
		t.Errorf("collapse must not depend on FINAL: %q", query)
	}
}

func TestCapacityReadsAllCollapseThroughNewestRows(t *testing.T) {
	var code []string
	for _, line := range strings.Split(capacityClickHouseSource(t), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "//") {
			code = append(code, line)
		}
	}
	source := strings.Join(code, "\n")

	if strings.Contains(source, "work_item_metrics_daily FINAL") {
		t.Error("a capacity read still uses FINAL, which collapses by the full deployed sorting key")
	}
	if got := strings.Count(source, "FROM work_item_metrics_daily"); got != 1 {
		t.Errorf("FROM work_item_metrics_daily appears %d times; only capacityNewestRows may read the table", got)
	}
	// throughput, backlog (outer + max(day)), discover scopes
	if got := len(regexp.MustCompile(`capacityNewestRows\(`).FindAllString(source, -1)); got != 5 {
		t.Errorf("capacityNewestRows( appears %d times, want 1 definition + 4 reads", got)
	}
	for _, call := range regexp.MustCompile(`capacityNewestRows\([^)]*\)`).FindAllString(source, -1) {
		if strings.Contains(call, "liverow") {
			t.Errorf("live-row predicate must sit outside the collapse, found in %s", call)
		}
	}
}

type queryCapturingConn struct {
	driverConnStub
	queries *[]string
}

func (conn queryCapturingConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	*conn.queries = append(*conn.queries, query)
	return nil, errors.New("captured")
}

func (conn queryCapturingConn) QueryRow(_ context.Context, query string, _ ...any) chdriver.Row {
	*conn.queries = append(*conn.queries, query)
	return capturedRow{}
}

type capturedRow struct{}

func (capturedRow) Err() error           { return errors.New("captured") }
func (capturedRow) Scan(...any) error    { return errors.New("captured") }
func (capturedRow) ScanStruct(any) error { return errors.New("captured") }

// A filter on a non-key column must run AFTER the collapse. Inside the
// subquery an older live version would survive when the newest version is a
// retraction row; FINAL would have returned the newest and the live-row
// filter would then have excluded it.
func TestCapacityLiveRowFilterRunsAfterTheCollapse(t *testing.T) {
	var queries []string
	executor := &CapacityExecutor{conn: queryCapturingConn{queries: &queries}}
	ctx := context.Background()
	today := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)

	_, _ = executor.loadThroughput(ctx, "org", capacityTarget{}, 30, today)
	_, _ = executor.loadBacklog(ctx, "org", capacityTarget{})
	_, _ = executor.resolveScopes(ctx, "org", capacityScope{AllTeams: true})
	if len(queries) != 3 {
		t.Fatalf("captured %d queries, want 3", len(queries))
	}

	live := liverow.Predicate("work_item_metrics_daily", "")
	for index, query := range queries {
		end := strings.Index(query, "LIMIT 1 BY")
		if end < 0 {
			t.Fatalf("query %d has no collapse: %s", index, query)
		}
		collapse := query[:end]
		if strings.Contains(collapse, live) {
			t.Errorf("query %d applies the live-row filter before the collapse: %s", index, query)
		}
	}
	for _, index := range []int{0, 2} {
		if !strings.Contains(queries[index], live) {
			t.Errorf("query %d lost the live-row filter: %s", index, queries[index])
		}
		rest := queries[index][strings.LastIndex(queries[index], "LIMIT 1 BY"):]
		if !strings.Contains(rest, live) {
			t.Errorf("query %d: live-row filter is not after the collapse: %s", index, queries[index])
		}
	}
}
