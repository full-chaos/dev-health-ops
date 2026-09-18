package flame

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// pullRequestFlameFixture builds a fakeQueryClient serving one
// git_pull_requests row (fetchPullRequestQuery's own column order: title,
// state, created_at, first_review_at, merged_at, closed_at) with no
// reviews.
func pullRequestFlameFixture(t *testing.T, state string, createdAt time.Time, firstReviewAt, mergedAt, closedAt any) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM git_pull_requests FINAL"):
			return &fixtureRowScanner{rows: [][]any{{"a pr", state, createdAt, firstReviewAt, mergedAt, closedAt}}}, nil
		case strings.Contains(query, "FROM git_pull_request_reviews FINAL"):
			return &fixtureRowScanner{}, nil
		}
		t.Fatalf("unexpected query: %s", query)
		return nil, nil
	}}
}

// TestBuildPRFlameResponseEndSelection pins the PR end selection's named
// cases through flameIntervalEnd, the same rule buildDeploymentFlameResponse
// uses: a real end (merged_at, else closed_at) wins when known; otherwise
// the request clock, but only for a genuinely non-terminal ("open") state;
// otherwise no measurable duration -- a terminal PR with neither merged_at
// nor closed_at (a sync/lookup gap) must never render as still under
// review through now().
func TestBuildPRFlameResponseEndSelection(t *testing.T) {
	fixedNow := time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC)
	createdAt := day(2025, 1, 1, 0, 0, 0)
	mergedAt := day(2025, 1, 2, 10, 0, 0)
	closedAt := day(2025, 1, 3, 10, 0, 0)

	t.Run("open, no merged_at/closed_at: renders, end = now", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "open", createdAt, nil, nil, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.End != fixedNow {
			t.Fatalf("end = %v, want %v (now)", got.Timeline.End, fixedNow)
		}
	})

	t.Run("terminal (closed, not merged), no merged_at/closed_at: 422 unchanged", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "closed", createdAt, nil, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal PR with no end signal never invents a duration", err)
		}
	})

	t.Run("terminal (merged) WITH merged_at: 200, real duration visible", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "merged", createdAt, nil, mergedAt, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.End != mergedAt {
			t.Fatalf("end = %v, want %v (merged_at)", got.Timeline.End, mergedAt)
		}
		if got.Timeline.End.Sub(got.Timeline.Start) <= 0 {
			t.Fatalf("duration=%s, want > 0", got.Timeline.End.Sub(got.Timeline.Start))
		}
	})

	t.Run("terminal (merged) but merged_at missing, closed_at also missing: 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "merged", createdAt, nil, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a data inconsistency (state=merged, no merged_at) is still terminal, no invented duration", err)
		}
	})

	t.Run("terminal (closed) WITH closed_at: 200, end = closed_at", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "closed", createdAt, nil, nil, closedAt)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.End != closedAt {
			t.Fatalf("end = %v, want %v (closed_at)", got.Timeline.End, closedAt)
		}
	})

	t.Run("unrecognized state, no merged_at/closed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "some_future_state", createdAt, nil, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an unrecognized state never invents a duration", err)
		}
	})

	t.Run("empty state, no merged_at/closed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := pullRequestFlameFixture(t, "", createdAt, nil, nil, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":1"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an empty state never invents a duration", err)
		}
	})
}
