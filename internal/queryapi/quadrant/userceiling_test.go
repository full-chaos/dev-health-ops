package quadrant

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
)

// CHAOS-13087: the person grain under a repository ceiling is the recompute of the items linked to its repositories.

func utc(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }

type itemRows struct {
	index int
	done  time.Time
}

func (s *itemRows) Next() bool   { s.index++; return s.index <= 2 }
func (s *itemRows) Close() error { return nil }
func (s *itemRows) Err() error   { return nil }
func (s *itemRows) Scan(dest ...any) error {
	started := utc(2)
	id, assignee, status, completed := "jira:A-1", "alice", "done", &s.done
	if s.index == 2 {
		id, assignee, status, completed = "jira:A-2", "bob", "in_progress", nil
	}
	values := []any{"11111111-1111-1111-1111-111111111111", utc(4), id, "jira", status, "", "", "", "", utc(1), completed, "task", []string{assignee}, &started}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = values[i].(string)
		case *time.Time:
			*p = values[i].(time.Time)
		case **time.Time:
			if i < len(values) {
				*p, _ = values[i].(*time.Time)
			}
		case *[]string:
			*p = values[i].([]string)
		case **float64:
			*p = nil
		default:
			return fmt.Errorf("unexpected destination %T", d)
		}
	}
	return nil
}

type noRows struct{}

func (noRows) Next() bool        { return false }
func (noRows) Scan(...any) error { return nil }
func (noRows) Close() error      { return nil }
func (noRows) Err() error        { return nil }

type linkedFake struct{}

func (linkedFake) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "FROM work_items FINAL") {
		return &itemRows{done: utc(3).Add(2 * time.Hour)}, nil
	}
	return noRows{}, nil
}

func userDaily() *linkeditems.UserDaily {
	return linkeditems.NewUserDaily(linkeditems.Loader{Client: linkedFake{}, OrgID: "org", RepoIDs: []string{"r1"}, WindowStart: utc(1), WindowEnd: utc(8), Budget: linkeditems.UserBudget})
}

func TestLinkedUserMetricIsTheAggregateOfThePersonsLinkedItems(t *testing.T) {
	ctx := context.Background()
	rows, err := fetchLinkedUserMetric(ctx, userDaily(), PersonMetrics["throughput"], utc(1), utc(8), bucketWindow, []string{"alice"})
	if err != nil || len(rows) != 1 || rows[0].EntityID != "alice" || rows[0].Value != 1 || !rows[0].Bucket.Equal(utc(1)) {
		t.Fatalf("alice throughput rows %v err %v, want one row of 1 in the window bucket", rows, err)
	}
	// C2/C3: nobody else is read: bob is not asked for, carol has no linked item.
	for _, who := range []string{"carol", ""} {
		if rows, _ := fetchLinkedUserMetric(ctx, userDaily(), PersonMetrics["throughput"], utc(1), utc(8), bucketWindow, []string{who}); len(rows) != 0 {
			t.Fatalf("%q has rows %v, want none", who, rows)
		}
	}
	wip, err := fetchLinkedUserMetric(ctx, userDaily(), PersonMetrics["wip"], utc(1), utc(8), bucketWindow, []string{"bob"})
	if err != nil || len(wip) != 1 || wip[0].Value <= 0 || wip[0].Value >= 1 {
		t.Fatalf("bob wip %v err %v, want a mean over the window's days in (0, 1)", wip, err)
	}
	// A cycle time is the mean of the rows that hold one: bob has none.
	if cycle, _ := fetchLinkedUserMetric(ctx, userDaily(), PersonMetrics["cycle_time"], utc(1), utc(8), bucketWindow, []string{"bob"}); len(cycle) != 0 {
		t.Fatalf("bob cycle rows %v, want none", cycle)
	}
	if _, err := fetchLinkedUserMetric(ctx, userDaily(), PersonMetrics["churn"], utc(1), utc(8), bucketWindow, []string{"alice"}); err == nil {
		t.Fatal("a metric of another table was served from the recompute")
	}
}

func TestIntersectHoldsNamedRepositoriesToTheCeiling(t *testing.T) {
	got := intersect([]string{"AAA", "bbb", "ccc"}, []string{"aaa", "ccc", "ddd"})
	if strings.Join(got, ",") != "AAA,ccc" {
		t.Fatalf("intersect = %v", got)
	}
	if got := intersect([]string{"x"}, nil); len(got) != 0 || got == nil {
		t.Fatalf("intersect with an empty ceiling = %#v, want empty non-nil", got)
	}
}
