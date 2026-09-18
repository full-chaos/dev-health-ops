package providersync

import (
	"strings"
	"testing"
	"time"
)

// TestResolveGitLabDeploymentMergeRequestRequiresMergedState pins the class
// ruling: only a merge request whose own state is "merged" ever supplies
// merged_at; an unmerged first entry is never a substitute pick, no matter
// its position in the list.
func TestResolveGitLabDeploymentMergeRequestRequiresMergedState(t *testing.T) {
	t.Parallel()

	t.Run("unmerged first, merged later picks the merged one", func(t *testing.T) {
		t.Parallel()
		number, mergedAt := resolveGitLabDeploymentMergeRequest([]map[string]any{
			{"iid": "44", "state": "opened", "merged_at": ""},
			{"iid": "45", "state": "merged", "merged_at": "2026-07-21T10:00:00Z"},
		})
		want := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
		if number == nil || *number != 45 || mergedAt == nil || !mergedAt.Equal(want) {
			t.Fatalf("number=%v mergedAt=%v want number=45 mergedAt=%v", number, mergedAt, want)
		}
	})

	t.Run("unmerged only stays nil", func(t *testing.T) {
		t.Parallel()
		number, mergedAt := resolveGitLabDeploymentMergeRequest([]map[string]any{
			{"iid": "44", "state": "opened", "merged_at": ""},
			{"iid": "46", "state": "closed", "merged_at": ""},
		})
		if number != nil || mergedAt != nil {
			t.Fatalf("number=%v mergedAt=%v want nil, nil (no merged entry)", number, mergedAt)
		}
	})

	t.Run("empty list stays nil", func(t *testing.T) {
		t.Parallel()
		number, mergedAt := resolveGitLabDeploymentMergeRequest(nil)
		if number != nil || mergedAt != nil {
			t.Fatalf("number=%v mergedAt=%v want nil, nil (empty input)", number, mergedAt)
		}
	})
}

// TestGitLabDeploymentLifecycleReadsDeployableObject pins the verified
// docs.gitlab.com/api/deployments/ "List project deployments" shape: the
// deployment object itself carries no top-level started_at/finished_at --
// both live under its nested "deployable" (CI job) object.
func TestGitLabDeploymentLifecycleReadsDeployableObject(t *testing.T) {
	t.Parallel()

	t.Run("deployable present supplies both timestamps", func(t *testing.T) {
		t.Parallel()
		startedAt, finishedAt := gitLabDeploymentLifecycle("502", map[string]any{
			"created_at": "2026-07-22T10:00:00Z",
			"deployable": map[string]any{
				"started_at":  "2026-07-22T09:58:00Z",
				"finished_at": "2026-07-22T10:05:00Z",
				"status":      "success",
			},
		})
		wantStarted := time.Date(2026, 7, 22, 9, 58, 0, 0, time.UTC)
		wantFinished := time.Date(2026, 7, 22, 10, 5, 0, 0, time.UTC)
		if startedAt == nil || !startedAt.Equal(wantStarted) || finishedAt == nil || !finishedAt.Equal(wantFinished) {
			t.Fatalf("startedAt=%v finishedAt=%v want started=%v finished=%v", startedAt, finishedAt, wantStarted, wantFinished)
		}
	})

	t.Run("no deployable stays nil, not copied from created_at", func(t *testing.T) {
		t.Parallel()
		startedAt, finishedAt := gitLabDeploymentLifecycle("502", map[string]any{
			"created_at": "2026-07-22T10:00:00Z",
		})
		if startedAt != nil || finishedAt != nil {
			t.Fatalf("startedAt=%v finishedAt=%v want nil, nil (no deployable object)", startedAt, finishedAt)
		}
	})

	t.Run("deployable still running has null started_at, present finished_at stays absent when both missing", func(t *testing.T) {
		t.Parallel()
		startedAt, finishedAt := gitLabDeploymentLifecycle("502", map[string]any{
			"created_at": "2026-07-22T10:00:00Z",
			"deployable": map[string]any{
				"started_at":  nil,
				"finished_at": nil,
				"status":      "running",
			},
		})
		if startedAt != nil || finishedAt != nil {
			t.Fatalf("startedAt=%v finishedAt=%v want nil, nil (deployable still running)", startedAt, finishedAt)
		}
	})

	t.Run("deployable present but wrong shape logs and stays nil", func(t *testing.T) {
		// Not t.Parallel(): captureSlog swaps the process-global slog default,
		// which a concurrently running captureSlog test would race.
		log := captureSlog(t)
		startedAt, finishedAt := gitLabDeploymentLifecycle("777", map[string]any{
			"created_at": "2026-07-22T10:00:00Z",
			"deployable": "not-an-object",
		})
		if startedAt != nil || finishedAt != nil {
			t.Fatalf("startedAt=%v finishedAt=%v want nil, nil (deployable is not an object)", startedAt, finishedAt)
		}
		if !strings.Contains(log.String(), "gitlab_deployments.deployable_shape_unexpected") || !strings.Contains(log.String(), "deployment_id=777") {
			t.Fatalf("expected a deployable_shape_unexpected log line for deployment_id=777, got: %s", log.String())
		}
	})
}
