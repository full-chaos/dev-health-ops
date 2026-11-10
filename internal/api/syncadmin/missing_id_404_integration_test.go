//go:build integration

package syncadmin

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// Both manual-sync routes look their target up first and answer 404 before any
// hand-off is minted or awaited. A missing id and another org's id get the same
// answer: the lookup is scoped to the caller's org.

func (v *cascadeVenue) jobRows() int {
	return v.count(`SELECT count(*) FROM backfill_jobs`) + v.count(`SELECT count(*) FROM job_runs`)
}

func missingIDCells(t *testing.T, route func(id string) string, body string, otherOrgID func(cascadeSeeded) uuid.UUID, notFound string) {
	t.Helper()
	v := startCascadeVenue(t, false)
	orgB := uuid.NewString()
	pgseed.Org(context.Background(), t, v.pool, orgB, "enterprise")
	oneA := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(oneA, v.org, 1)
	oneB := v.seedIn(orgB, 1)
	ids := map[string]string{"missing id": uuid.NewString(), "another org's id": otherOrgID(oneB).String()}
	rows, jobs, waits := v.handoffRows(), v.jobRows(), awaitCount(t)
	for name, id := range ids {
		status, text := v.call("POST", route(id), body)
		if status != http.StatusNotFound || text != notFound {
			t.Fatalf("%s: %d %s, want 404 %s", name, status, text, notFound)
		}
	}
	if got := v.handoffRows(); got != rows {
		t.Fatalf("refused requests wrote %d hand-off rows", got-rows)
	}
	if got := v.jobRows(); got != jobs {
		t.Fatalf("refused requests wrote %d jobs", got-jobs)
	}
	if got := awaitCount(t); got != waits {
		t.Fatalf("refused requests waited for the scheduler %d times", got-waits)
	}
}

// TestIntegrationSyncMissingOrOtherOrgIDIs404BeforeAnyHandOff: POST
// /integrations/{id}/sync.
func TestIntegrationSyncMissingOrOtherOrgIDIs404BeforeAnyHandOff(t *testing.T) {
	missingIDCells(t, func(id string) string { return "/api/v1/admin/integrations/" + id + "/sync" }, `{}`,
		func(one cascadeSeeded) uuid.UUID { return one.integration },
		`{"detail":"Integration not found"}`)
}

// TestSyncConfigTriggerMissingOrOtherOrgIDIs404BeforeAnyHandOff: POST
// /sync-configs/{id}/trigger.
func TestSyncConfigTriggerMissingOrOtherOrgIDIs404BeforeAnyHandOff(t *testing.T) {
	missingIDCells(t, func(id string) string { return "/api/v1/admin/sync-configs/" + id + "/trigger" }, "",
		func(one cascadeSeeded) uuid.UUID { return one.config },
		`{"detail":"Sync configuration not found"}`)
}
