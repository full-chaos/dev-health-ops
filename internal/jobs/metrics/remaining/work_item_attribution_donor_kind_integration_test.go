//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// CHAOS-14013 on a real store: loadDonorKinds reads provider and Type from work_items FINAL, org-scoped,
// and the shared predicate refuses a pull request, an empty Type and an id with no row.
func TestLoadDonorKindsOnARealStore(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	orgID := "org-donor-kinds-" + uuid.NewString()
	other := "org-donor-kinds-other-" + uuid.NewString()
	at := time.Now().UTC().Add(-time.Hour)
	seed := func(org, id, provider, itemType string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, status, created_at, org_id, last_synced) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)`,
			uuid.Nil, id, provider, itemType, at, org, at); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	seed(orgID, "gh-pr", "github", "pr")
	seed(orgID, "gh-issue", "github", "issue")
	seed(orgID, "gl-mr", "gitlab", "merge_request")
	seed(orgID, "jira-empty", "jira", "")
	seed(orgID, "linear-task", "linear", "task")
	seed(other, "other-org-pr", "github", "pr")

	executor := &WorkItemAttributionExecutor{conn: conn}
	kinds, err := executor.loadDonorKinds(ctx, orgID, []string{"gh-pr", "gh-issue", "gl-mr", "jira-empty", "linear-task", "other-org-pr", "no-row"})
	if err != nil {
		t.Fatalf("loadDonorKinds: %v", err)
	}
	for id, want := range map[string]string{
		"gh-pr": "pr_kind", "gh-issue": "", "gl-mr": "pr_kind", "jira-empty": "empty_type", "linear-task": "",
		"other-org-pr": "unknown", "no-row": "unknown",
	} {
		kind, known := kinds[id]
		if got := teamattribution.LinkedIssueDonorRefusal(kind, known); got != want {
			t.Errorf("donor %s refusal = %q, want %q (kind %+v known %v)", id, got, want, kind, known)
		}
	}
}
