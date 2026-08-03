package providersync

import (
	"testing"
	"time"
)

func TestCompareGitLabCICDPipelineVersionRejectsEveryMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 123000000, time.UTC)
	queued := now.Add(-2 * time.Minute)
	finished := now.Add(-time.Minute)
	status := "success"
	expected := gitLabCICDPipelineRow{
		OrgID: "org", RepoID: "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
		RunID: "901", Status: &status, QueuedAt: &queued, StartedAt: queued,
		FinishedAt: &finished, RetryCount: 2, LastSynced: now,
	}
	if got := compareGitLabCICDPipelineVersion(expected, expected, true); got != EffectExact {
		t.Fatalf("exact=%s", got)
	}
	if got := compareGitLabCICDPipelineVersion(expected, expected, false); got != EffectAbsent {
		t.Fatalf("missing=%s", got)
	}
	stale := expected
	stale.LastSynced = now.Add(-time.Millisecond)
	if got := compareGitLabCICDPipelineVersion(expected, stale, true); got != EffectAbsent {
		t.Fatalf("stale=%s", got)
	}

	otherStatus := "failed"
	otherQueued := queued.Add(time.Millisecond)
	otherFinished := finished.Add(time.Millisecond)
	for name, mutate := range map[string]func(*gitLabCICDPipelineRow){
		"newer version": func(row *gitLabCICDPipelineRow) { row.LastSynced = now.Add(time.Millisecond) },
		"org id":        func(row *gitLabCICDPipelineRow) { row.OrgID = "other-org" },
		"repo id":       func(row *gitLabCICDPipelineRow) { row.RepoID = "b6a5cafb-6680-a10a-9e41-a5ef763ca016" },
		"run id":        func(row *gitLabCICDPipelineRow) { row.RunID = "902" },
		"status":        func(row *gitLabCICDPipelineRow) { row.Status = &otherStatus },
		"queued at":     func(row *gitLabCICDPipelineRow) { row.QueuedAt = &otherQueued },
		"started at":    func(row *gitLabCICDPipelineRow) { row.StartedAt = otherQueued },
		"finished at":   func(row *gitLabCICDPipelineRow) { row.FinishedAt = &otherFinished },
		"retry count":   func(row *gitLabCICDPipelineRow) { row.RetryCount++ },
	} {
		t.Run(name, func(t *testing.T) {
			actual := expected
			mutate(&actual)
			if got := compareGitLabCICDPipelineVersion(expected, actual, true); got != EffectConflict {
				t.Fatalf("inspection=%s actual=%+v", got, actual)
			}
		})
	}
}
