package flame

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// issueFlameFixture builds a fakeQueryClient serving one
// work_item_cycle_times row (fetchIssueQuery's own column order: provider,
// type, status, created_at, started_at, completed_at).
func issueFlameFixture(t *testing.T, status string, createdAt time.Time, startedAt, completedAt any) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times FINAL") {
			t.Fatalf("unexpected query: %s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{"github", "issue", status, createdAt, startedAt, completedAt}}}, nil
	}}
}

// TestBuildIssueFlameResponseEndSelection pins the issue end selection's
// named cases through flameIntervalEnd, the same rule
// buildDeploymentFlameResponse uses: completed_at wins when known;
// otherwise the request clock, but only for a genuinely non-terminal
// WorkItemStatusCategory (backlog, todo, in_progress, in_review, blocked);
// otherwise no measurable duration -- a "done" or "canceled" work item
// with no completed_at (a sync/lookup gap) must never render as still
// active through now().
func TestBuildIssueFlameResponseEndSelection(t *testing.T) {
	fixedNow := time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC)
	createdAt := day(2025, 1, 1, 0, 0, 0)
	completedAt := day(2025, 1, 5, 10, 0, 0)

	t.Run("in_progress, no completed_at: renders, end = now", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "in_progress", createdAt, nil, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.End != fixedNow {
			t.Fatalf("end = %v, want %v (now)", got.Timeline.End, fixedNow)
		}
	})

	t.Run("terminal (done), no completed_at: 422 unchanged", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "done", createdAt, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal work item with no completion signal never invents a duration", err)
		}
	})

	t.Run("terminal (canceled), no completed_at: 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "canceled", createdAt, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a canceled work item with no completion signal never invents a duration", err)
		}
	})

	t.Run("terminal (done) WITH completed_at: 200, real duration visible", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "done", createdAt, nil, completedAt)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.End != completedAt {
			t.Fatalf("end = %v, want %v (completed_at)", got.Timeline.End, completedAt)
		}
		if got.Timeline.End.Sub(got.Timeline.Start) <= 0 {
			t.Fatalf("duration=%s, want > 0", got.Timeline.End.Sub(got.Timeline.Start))
		}
	})

	// The remaining four non-terminal WorkItemStatusCategory spellings not
	// covered by in_progress above.
	for _, status := range []string{"backlog", "todo", "in_review", "blocked"} {
		t.Run("running status "+status+": renders, end = now", func(t *testing.T) {
			withFixedFlameClock(t, fixedNow)
			client := issueFlameFixture(t, status, createdAt, nil, nil)
			got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if got.Timeline.End != fixedNow {
				t.Fatalf("end = %v, want %v (now)", got.Timeline.End, fixedNow)
			}
		})
	}

	t.Run(`literal category "unknown", no completed_at: treated as terminal, 422`, func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "unknown", createdAt, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- the category's own \"unknown\" value never invents a duration", err)
		}
	})

	t.Run("unrecognized status, no completed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "some_future_category", createdAt, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an unrecognized status never invents a duration", err)
		}
	})

	t.Run("empty status, no completed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := issueFlameFixture(t, "", createdAt, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an empty status never invents a duration", err)
		}
	})
}
