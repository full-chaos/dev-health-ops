package investmentexplain

import (
	"context"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type rowsClient struct{ rows [][]any }

func (c rowsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &fixtureRowScanner{rows: c.rows}, nil
}

// CHAOS-9865: the quote stamp and count of the data version are scalar subqueries, typed Nullable(String).
// A NULL is its own key part: it never shares a cache key with an empty string.
func TestDataVersionKeepsANullDistinctFromEmpty(t *testing.T) {
	version := func(quotesLatest any) string {
		reader, err := NewReader(rowsClient{rows: [][]any{{"2026-10-13 10:00:00.000", "4", quotesLatest, "7"}}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.FetchInvestmentDataVersion(context.Background(), "org")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	null, empty := version(nil), version("")
	if null == empty {
		t.Fatalf("a NULL stamp and an empty stamp share the key %q", null)
	}
}

// CHAOS-9865: team_id and team_name are Nullable(String). A NULL is read as no team / no name, not as ""
// the reader invented: an assignment with a NULL id casts no vote, and a NULL name falls back to the id.
func TestWorkItemTeamAssignmentsKeepNullAsNil(t *testing.T) {
	teamID := "t1"
	reader, err := NewReader(rowsClient{rows: [][]any{
		{"w-none", nil, nil},
		{"w-id-only", &teamID, nil},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.FetchWorkItemTeamAssignments(context.Background(), "org", []string{"w-none", "w-id-only"})
	if err != nil {
		t.Fatal(err)
	}
	if a := got["w-none"]; a.TeamID != nil || a.TeamName != nil {
		t.Errorf("w-none = %+v, want a nil id and a nil name", a)
	}
	if a := got["w-id-only"]; a.TeamID == nil || *a.TeamID != "t1" || a.TeamName != nil {
		t.Errorf("w-id-only = %+v, want id t1 and a nil name", a)
	}
	if id, label := majorityTeamForIssues([]string{"w-none"}, got); id != "unassigned" || label != "Unassigned" {
		t.Errorf("a NULL team = (%q, %q), want unassigned", id, label)
	}
	if id, label := majorityTeamForIssues([]string{"w-id-only"}, got); id != "t1" || label != "t1" {
		t.Errorf("an id with a NULL name = (%q, %q), want the id as its label", id, label)
	}
}
