package flame

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// deploymentFlameFixture builds a fakeQueryClient serving one deployments
// row with the given column values, in fetchDeploymentQuery's own column
// order (status, environment, started_at, finished_at, deployed_at,
// merged_at).
func deploymentFlameFixture(t *testing.T, status, environment string, startedAt, finishedAt, deployedAt, mergedAt any) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM deployments FINAL") {
			t.Fatalf("unexpected query: %s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{status, environment, startedAt, finishedAt, deployedAt, mergedAt}}}, nil
	}}
}

// withFixedFlameClock overrides the package's own now() for the duration of
// the test, restoring it on cleanup -- the same "request clock, injectable
// for tests" nowLike relies on (flame.go's own now var).
func withFixedFlameClock(t *testing.T, fixed time.Time) {
	t.Helper()
	previous := now
	now = func() time.Time { return fixed }
	t.Cleanup(func() { now = previous })
}

// TestBuildDeploymentFlameResponseEndSelection pins the deployment end
// selection's named cases, each with its exact frame set (or its 422). The
// "now" fallback is honest only for a deployment still running: a running
// status with no finish renders, end = the request clock. A terminal status
// (or an unrecognized/empty one, treated as terminal) never gets an
// invented duration -- a success row carrying only deployed_at (no
// started_at/finished_at) stays 422, unchanged, the same as a terminal
// failure with no finished_at; a terminal failure WITH finished_at renders
// with its real, visible duration. merged_at's presence or absence only
// changes whether a "Queue waiting" frame exists.
func TestBuildDeploymentFlameResponseEndSelection(t *testing.T) {
	fixedNow := time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC)
	deployedAt := day(2026, 1, 10, 10, 0, 0)
	startedAt := day(2026, 1, 10, 10, 1, 0)
	finishedAt := day(2026, 1, 10, 10, 5, 0)
	mergedAt := day(2026, 1, 9, 9, 0, 0)

	rootID := "deploy:" + repoID + ":x"
	pipelineID := rootID + ":pipeline"
	queueID := rootID + ":queue"

	t.Run("running, not finished: renders, end = now", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "in_progress", "production", startedAt, nil, deployedAt, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		want := &Response{
			Entity:   map[string]any{"repo_id": repoID, "deployment_id": "x", "status": "in_progress", "environment": "production"},
			Timeline: Timeline{Start: startedAt, End: fixedNow},
			Frames: []Frame{
				{ID: rootID, ParentID: nil, Label: "Deployment lifecycle", Start: startedAt, End: fixedNow, State: "ci", Category: "planned"},
				{ID: pipelineID, ParentID: strPtr(rootID), Label: "Deploy pipeline", Start: startedAt, End: fixedNow, State: "ci", Category: "planned"},
			},
		}
		assertFlameResponsesEqual(t, got, want)
	})

	t.Run("running status wins over deployed_at regardless of deployed_at's position relative to start: end = now, not deployed_at", func(t *testing.T) {
		// The pinned rule has exactly one condition on the now() fallback:
		// a non-terminal status. It does not carve out an exception for a
		// deployed_at that happens to sit after the computed start -- a
		// running deployment must still render with an open duration
		// through the request clock, even under an ordering the writer
		// never actually produces (deployment_writer_ordering_test.go's
		// own doc comment: deployed_at <= started_at < finished_at, merged_
		// at precedes creation). Hand-built to violate that ordering so
		// this is executed, not merely argued.
		withFixedFlameClock(t, fixedNow)
		mergeBeforeStart := day(2026, 1, 10, 9, 30, 0)
		deployedAfterStart := day(2026, 1, 10, 9, 45, 0)
		client := deploymentFlameFixture(t, "in_progress", "production", nil, nil, deployedAfterStart, mergeBeforeStart)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.Start != mergeBeforeStart || got.Timeline.End != fixedNow {
			t.Fatalf("timeline=%+v, want start=%v end=%v (now, not deployed_at=%v)", got.Timeline, mergeBeforeStart, fixedNow, deployedAfterStart)
		}
	})

	t.Run("finished: end = finished_at", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "success", "production", startedAt, finishedAt, deployedAt, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		want := &Response{
			Entity:   map[string]any{"repo_id": repoID, "deployment_id": "x", "status": "success", "environment": "production"},
			Timeline: Timeline{Start: startedAt, End: finishedAt},
			Frames: []Frame{
				{ID: rootID, ParentID: nil, Label: "Deployment lifecycle", Start: startedAt, End: finishedAt, State: "ci", Category: "planned"},
				{ID: pipelineID, ParentID: strPtr(rootID), Label: "Deploy pipeline", Start: startedAt, End: finishedAt, State: "ci", Category: "planned"},
			},
		}
		assertFlameResponsesEqual(t, got, want)
	})

	t.Run("terminal success, only deployed_at (legacy row): 422 unchanged", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "success", "production", nil, nil, deployedAt, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal row with no finish signal has no measurable duration", err)
		}
	})

	t.Run("merged_at present before a real started_at: adds a Queue waiting frame", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "in_progress", "production", startedAt, nil, deployedAt, mergedAt)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		want := &Response{
			Entity:   map[string]any{"repo_id": repoID, "deployment_id": "x", "status": "in_progress", "environment": "production"},
			Timeline: Timeline{Start: startedAt, End: fixedNow},
			Frames: []Frame{
				{ID: rootID, ParentID: nil, Label: "Deployment lifecycle", Start: startedAt, End: fixedNow, State: "ci", Category: "planned"},
				{ID: queueID, ParentID: strPtr(rootID), Label: "Queue waiting", Start: mergedAt, End: startedAt, State: "waiting", Category: "planned"},
				{ID: pipelineID, ParentID: strPtr(rootID), Label: "Deploy pipeline", Start: startedAt, End: fixedNow, State: "ci", Category: "planned"},
			},
		}
		assertFlameResponsesEqual(t, got, want)
	})

	t.Run("merged_at absent: no Queue waiting frame", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "in_progress", "production", startedAt, nil, deployedAt, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		for _, frame := range got.Frames {
			if strings.HasSuffix(frame.ID, ":queue") {
				t.Fatalf("Queue waiting frame present despite no merged_at: %+v", got.Frames)
			}
		}
	})

	t.Run("FAILED deployment: end = the failure status's own timestamp, duration visible", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		failedAt := day(2026, 1, 10, 10, 3, 0)
		client := deploymentFlameFixture(t, "failure", "production", startedAt, failedAt, deployedAt, nil)
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		want := &Response{
			Entity:   map[string]any{"repo_id": repoID, "deployment_id": "x", "status": "failure", "environment": "production"},
			Timeline: Timeline{Start: startedAt, End: failedAt},
			Frames: []Frame{
				{ID: rootID, ParentID: nil, Label: "Deployment lifecycle", Start: startedAt, End: failedAt, State: "ci", Category: "planned"},
				{ID: pipelineID, ParentID: strPtr(rootID), Label: "Deploy pipeline", Start: startedAt, End: failedAt, State: "ci", Category: "planned"},
			},
		}
		assertFlameResponsesEqual(t, got, want)
		gotDuration := got.Timeline.End.Sub(got.Timeline.Start)
		wantDuration := failedAt.Sub(startedAt)
		if gotDuration != wantDuration {
			t.Fatalf("failure duration=%s want=%s -- the failure's own duration must be visible, not collapsed to zero", gotDuration, wantDuration)
		}
	})

	t.Run("terminal failure, no finished_at: 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "failure", "production", startedAt, nil, deployedAt, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal failure with no finish signal has no measurable duration", err)
		}
	})

	t.Run("unrecognized status, only deployed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "some_future_provider_state", "production", nil, nil, deployedAt, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an unrecognized status never invents a duration", err)
		}
	})

	t.Run("empty status, only deployed_at: treated as terminal, 422", func(t *testing.T) {
		withFixedFlameClock(t, fixedNow)
		client := deploymentFlameFixture(t, "", "production", nil, nil, deployedAt, nil)
		_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 422 {
			t.Fatalf("err = %v, want *RequestError{422, ...} -- an empty status never invents a duration", err)
		}
	})

	// GitLab's own running vocabulary (created, running) is disjoint from
	// GitHub's (pending, queued, in_progress) -- both sets are exercised
	// here so either provider's non-terminal spelling is honored.
	for _, status := range []string{"created", "running"} {
		t.Run("gitlab running status "+status+": renders, end = now", func(t *testing.T) {
			withFixedFlameClock(t, fixedNow)
			client := deploymentFlameFixture(t, status, "production", startedAt, nil, deployedAt, nil)
			got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if got.Timeline.End != fixedNow {
				t.Fatalf("end = %v, want %v (now)", got.Timeline.End, fixedNow)
			}
		})
	}

	// GitLab's own terminal vocabulary (success, failed, canceled, blocked)
	// gets no invented duration either, same as GitHub's.
	for _, status := range []string{"failed", "canceled", "blocked"} {
		t.Run("gitlab terminal status "+status+", only deployed_at: 422", func(t *testing.T) {
			withFixedFlameClock(t, fixedNow)
			client := deploymentFlameFixture(t, status, "production", nil, nil, deployedAt, nil)
			_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
			reqErr, ok := AsRequestError(err)
			if !ok || reqErr.Status != 422 {
				t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal status never invents a duration", err)
			}
		})
	}

	// GitHub's remaining non-terminal spelling (pending) not covered by the
	// running/not-finished case above, which already exercises in_progress.
	for _, status := range []string{"pending", "queued"} {
		t.Run("github running status "+status+": renders, end = now", func(t *testing.T) {
			withFixedFlameClock(t, fixedNow)
			client := deploymentFlameFixture(t, status, "production", startedAt, nil, deployedAt, nil)
			got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if got.Timeline.End != fixedNow {
				t.Fatalf("end = %v, want %v (now)", got.Timeline.End, fixedNow)
			}
		})
	}

	// GitHub's remaining terminal spellings not covered by success/failure
	// above.
	for _, status := range []string{"error", "inactive"} {
		t.Run("github terminal status "+status+", only deployed_at: 422", func(t *testing.T) {
			withFixedFlameClock(t, fixedNow)
			client := deploymentFlameFixture(t, status, "production", nil, nil, deployedAt, nil)
			_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":x"})
			reqErr, ok := AsRequestError(err)
			if !ok || reqErr.Status != 422 {
				t.Fatalf("err = %v, want *RequestError{422, ...} -- a terminal status never invents a duration", err)
			}
		})
	}
}

func assertFlameResponsesEqual(t *testing.T, got, want *Response) {
	t.Helper()
	gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want)
	if gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}
