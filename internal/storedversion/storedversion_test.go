package storedversion

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type emptyRows struct{ driver.Rows }

func (emptyRows) Next() bool                       { return false }
func (emptyRows) Close() error                     { return nil }
func (emptyRows) Err() error                       { return nil }
func (emptyRows) ColumnTypes() []driver.ColumnType { return nil }

type recordingQuerier struct {
	calls int
	args  []any
	err   error
}

func (q *recordingQuerier) Query(_ context.Context, _ string, args ...any) (driver.Rows, error) {
	q.calls = q.calls + 1
	q.args = args
	if q.err != nil {
		return nil, q.err
	}
	return emptyRows{}, nil
}

var pullRequests = Contract{Writer: "test", Table: "git_pull_requests", Columns: []Column{
	{Name: "repo_id", Rule: Identity}, {Name: "number", Rule: Identity},
	{Name: "title", Rule: Stated},
	{Name: "merged_at", Rule: Unstated, Fields: []string{"mergedAt"}, Terminal: true},
	{Name: "reviews_count", Rule: NoField},
}}

const pullRequestInsert = "INSERT INTO git_pull_requests (repo_id,number,title,merged_at,reviews_count)"

func TestApplyRefusesAKeptColumnItsInsertDoesNotWrite(t *testing.T) {
	_, err := pullRequests.Apply(context.Background(), &recordingQuerier{}, "org-1",
		"INSERT INTO git_pull_requests (repo_id,number,title,merged_at)",
		[]Row{{Values: []any{uuid.Nil, uint32(1), "t", nil}}})
	if err == nil {
		t.Fatal("Apply accepted a kept column the insert does not write")
	}
}

func TestApplyRefusesARowWithTheWrongArity(t *testing.T) {
	_, err := pullRequests.Apply(context.Background(), &recordingQuerier{}, "org-1", pullRequestInsert,
		[]Row{{Values: []any{uuid.Nil, uint32(1)}}})
	if err == nil {
		t.Fatal("Apply accepted a row with fewer values than insert columns")
	}
}

func TestApplyFailsWhenTheReadFails(t *testing.T) {
	querier := &recordingQuerier{err: errors.New("clickhouse unavailable")}
	if _, err := pullRequests.Apply(context.Background(), querier, "org-1", pullRequestInsert,
		[]Row{{Values: []any{uuid.Nil, uint32(1), "t", nil, uint32(0)}}}); err == nil {
		t.Fatal("Apply succeeded over a failed read")
	}
}

func TestApplyReadNamesEachKeyValueOnce(t *testing.T) {
	querier := &recordingQuerier{}
	repo := uuid.MustParse("5e1f2b1a-7c2d-4e8f-9a0b-1c2d3e4f5a6b")
	row := func() Row { return Row{Values: []any{repo, uint32(7), "t", nil, uint32(0)}} }
	if _, err := pullRequests.Apply(context.Background(), querier, "org-1", pullRequestInsert, []Row{row(), row()}); err != nil {
		t.Fatal(err)
	}
	if len(querier.args) != 3 {
		t.Fatalf("query args = %v", querier.args)
	}
	for _, arg := range querier.args[1:] {
		if values, ok := arg.([]any); !ok || len(values) != 1 {
			t.Fatalf("key filter %v, want one value for two rows sharing a key", arg)
		}
	}
}

func TestApplyFoldsRowsSharingAKeyWithoutAHeldVersion(t *testing.T) {
	merged := "2026-06-03T09:00:00Z"
	rows := []Row{
		{Values: []any{uuid.Nil, uint32(1), "a", merged, uint32(3)}, Carry: map[string]bool{"reviews_count": true}},
		{Values: []any{uuid.Nil, uint32(1), "b", nil, uint32(0)}, Carry: map[string]bool{"reviews_count": true}},
	}
	outcomes, err := pullRequests.Apply(context.Background(), &recordingQuerier{}, "org-1", pullRequestInsert, rows)
	if err != nil {
		t.Fatal(err)
	}
	if rows[1].Values[3] != merged || rows[1].Values[4] != uint32(3) || len(outcomes[1].Refused) != 1 {
		t.Fatalf("second row = %v, outcome %+v; want the first row's merged_at and reviews_count", rows[1].Values, outcomes[1])
	}
}

func TestCarryMarksNoFieldAlwaysAndUnstatedOnlyWhenEveryFieldIsUnstated(t *testing.T) {
	contract := Contract{Columns: []Column{
		{Name: "a", Rule: NoField},
		{Name: "b", Rule: Unstated, Fields: []string{"x", "y"}},
		{Name: "c", Rule: Stated},
	}}
	stated := map[string]bool{"y": true}
	carry := contract.Carry(func(field string) bool { return !stated[field] })
	if !carry["a"] || carry["b"] || carry["c"] {
		t.Fatalf("carry = %v, want only a", carry)
	}
	stated = map[string]bool{}
	if carry = contract.Carry(func(field string) bool { return !stated[field] }); !carry["b"] {
		t.Fatalf("carry = %v, want b when x and y are both unstated", carry)
	}
}

func TestApplyWithoutRowsReadsNothing(t *testing.T) {
	querier := &recordingQuerier{}
	outcomes, err := pullRequests.Apply(context.Background(), querier, "org-1", pullRequestInsert, nil)
	if err != nil || outcomes != nil || querier.calls != 0 {
		t.Fatalf("outcomes = %v, err = %v, reads = %d; want no read for an empty batch", outcomes, err, querier.calls)
	}
}
