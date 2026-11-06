package people

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13087: under a repository ceiling the per-person work-item metrics are recomputed from the items linked to the
// allowed repositories. These cells run the recompute over a fake of the two statements of the loader.

type linkedItem struct {
	id, assignee, status string
	completed            *time.Time
}

type itemScan struct {
	items []linkedItem
	index int
}

func (s *itemScan) Next() bool   { s.index++; return s.index <= len(s.items) }
func (s *itemScan) Close() error { return nil }
func (s *itemScan) Err() error   { return nil }
func (s *itemScan) Scan(dest ...any) error {
	item := s.items[s.index-1]
	started := day(2026, 1, 2)
	values := []any{"11111111-1111-1111-1111-111111111111", day(2026, 1, 4), item.id, "jira", item.status, "", "", "", "",
		day(2026, 1, 1), item.completed, "task", []string{item.assignee}, &started, (*time.Time)(nil), (*float64)(nil)}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = values[i].(string)
		case *time.Time:
			*p = values[i].(time.Time)
		case **time.Time:
			*p, _ = values[i].(*time.Time)
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

type emptyScan struct{}

func (emptyScan) Next() bool        { return false }
func (emptyScan) Scan(...any) error { return nil }
func (emptyScan) Close() error      { return nil }
func (emptyScan) Err() error        { return nil }

func linkedClient(t *testing.T, fail error) (QueryClient, *int) {
	reads := 0
	done := day(2026, 1, 3).Add(2 * time.Hour)
	items := []linkedItem{{"jira:A-1", "alice", "done", &done}, {"jira:A-2", "bob", "in_progress", nil}}
	return fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		reads++
		if fail != nil {
			return nil, fail
		}
		if len(query) > 0 && contains(query, "FROM work_items FINAL") {
			return &itemScan{items: items}, nil
		}
		return emptyScan{}, nil
	}}, &reads
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

type matcherScan struct {
	ids   []string
	index int
}

func (s *matcherScan) Next() bool   { s.index++; return s.index <= len(s.ids) }
func (s *matcherScan) Close() error { return nil }
func (s *matcherScan) Err() error   { return nil }
func (s *matcherScan) Scan(dest ...any) error {
	*dest[0].(*string) = fmt.Sprintf("acme/r%d", s.index)
	*dest[1].(*[]string) = []string{s.ids[s.index-1]}
	return nil
}

type matcherClient struct{ ids []string }

func (c matcherClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &matcherScan{ids: c.ids}, nil
}

// ceilingOf is the ceiling the decision scope of an allowlist resolves to over the given repository ids.
func ceilingOf(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = fmt.Sprintf("acme/r%d", i+1)
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), matcherClient{ids: ids}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling %v restricted %v err %v", ceiling, ceiling.Restricted(), err)
	}
	return ceiling
}

func TestRecomputedValueIsTheAggregateOfThePersonsLinkedItems(t *testing.T) {
	client, _ := linkedClient(t, nil)
	scope := newReadScope(client, "org", ceilingOf(t, "11111111-1111-1111-1111-111111111111"), day(2026, 1, 1), day(2026, 1, 6))
	ctx := context.Background()
	if v, has, err := scope.recomputedValue(ctx, "items_completed", "sum", []string{"alice"}, day(2026, 1, 1), day(2026, 1, 6)); err != nil || !has || v != 1 {
		t.Fatalf("alice completed = %v has %v err %v, want 1", v, has, err)
	}
	if v, has, _ := scope.recomputedValue(ctx, "items_completed", "sum", []string{"bob"}, day(2026, 1, 1), day(2026, 1, 6)); !has || v != 0 {
		t.Fatalf("bob completed = %v has %v, want 0 and data", v, has)
	}
	if v, has, _ := scope.recomputedValue(ctx, "wip_count_end_of_day", "avg", []string{"bob"}, day(2026, 1, 1), day(2026, 1, 6)); !has || v < 0.79 || v > 0.81 {
		t.Fatalf("bob wip = %v has %v, want 0.8 (open from the 2nd, 4 of 5 days)", v, has)
	}
	// C2/C3: a person with no linked item has no row at all.
	if v, has, _ := scope.recomputedValue(ctx, "items_completed", "sum", []string{"carol"}, day(2026, 1, 1), day(2026, 1, 6)); has || v != 0 {
		t.Fatalf("carol = %v has %v, want no data", v, has)
	}
	series, err := scope.recomputedSeries(ctx, "items_completed", "sum", []string{"alice"}, day(2026, 1, 1), day(2026, 1, 6))
	if err != nil || len(series) == 0 {
		t.Fatalf("series %v err %v", series, err)
	}
	total := 0.0
	for i, row := range series {
		total += row.Value
		if i > 0 && !series[i-1].Day.Before(row.Day) {
			t.Fatalf("series is not by day: %v", series)
		}
	}
	if total != 1 {
		t.Fatalf("series sums to %v, want 1", total)
	}
	// The window cuts the days: a window before the completion has none of it.
	if v, _, _ := scope.recomputedValue(ctx, "items_completed", "sum", []string{"alice"}, day(2026, 1, 1), day(2026, 1, 3)); v != 0 {
		t.Fatalf("alice completed before the 3rd = %v, want 0", v)
	}
}

// C13: a bound hit of the recompute is an error, never a value over the organization.
func TestRecomputedValueBoundHitIsAnError(t *testing.T) {
	client, _ := linkedClient(t, errors.New("max_execution_time exceeded"))
	scope := newReadScope(client, "org", ceilingOf(t, "11111111-1111-1111-1111-111111111111"), day(2026, 1, 1), day(2026, 1, 6))
	if _, _, err := scope.recomputedValue(context.Background(), "items_completed", "sum", []string{"alice"}, day(2026, 1, 1), day(2026, 1, 6)); err == nil {
		t.Fatal("a bound hit answered a value")
	}
}

// C6: an empty ceiling reads no item.
func TestRecomputeOfAnEmptyCeilingReadsNothing(t *testing.T) {
	client, reads := linkedClient(t, nil)
	scope := newReadScope(client, "org", ceilingOf(t), day(2026, 1, 1), day(2026, 1, 6))
	if v, has, err := scope.recomputedValue(context.Background(), "items_completed", "sum", []string{"alice"}, day(2026, 1, 1), day(2026, 1, 6)); err != nil || has || v != 0 || *reads != 0 {
		t.Fatalf("v %v has %v err %v reads %d, want nothing read", v, has, err, *reads)
	}
}

func TestUnrestrictedScopeIsTodaysStatement(t *testing.T) {
	scope := readScope{}
	for _, table := range []string{"work_item_cycle_times", "user_metrics_daily", "work_item_user_metrics_daily"} {
		if term, bindings := scope.termFor(table); term != "" || bindings != nil || scope.recomputed(table) {
			t.Fatalf("%s: term %q bindings %v recomputed %v for an unrestricted scope", table, term, bindings, scope.recomputed(table))
		}
	}
}
