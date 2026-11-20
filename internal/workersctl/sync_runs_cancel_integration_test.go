//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
)

const (
	syncRunsCancelIntegrationOrg = "00000000-0000-4000-8000-000000143830"
	syncRunsCancelIntegrationRun = "00000000-0000-4000-8000-000000143831"
)

// `sync-runs cancel` end to end on the production grants: the domain write runs as the restricted
// domain login, the audit row completes with the write's outcome, a River job the post-commit cancel
// could not reach is counted on its own stderr line with exit 0, a second call is already_terminal,
// and another organization's run is sync_run_not_found.
func TestSyncRunsCancelRunsAsTheDomainRoleAndReportsRiverFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, uri, roles := startJobRouteHarness(t, ctx)
	pgseed.EnsureSyncRun(ctx, t, admin, pgseed.SyncRun{ID: syncRunsCancelIntegrationRun, OrgID: syncRunsCancelIntegrationOrg, Status: "dispatching", TotalUnits: 2})
	dispatching := uuid.NewString()
	for _, unit := range []pgseed.SyncRunUnit{
		{ID: uuid.NewString(), Status: "planned", DatasetKey: "prs"},
		{ID: dispatching, Status: "dispatching", DatasetKey: "commits"},
	} {
		unit.RunID, unit.OrgID = syncRunsCancelIntegrationRun, syncRunsCancelIntegrationOrg
		pgseed.InsertSyncRunUnit(ctx, t, admin, unit)
	}
	pgseed.WorkerJobOutbox(ctx, t, admin, "sync.provider_unit:"+dispatching, "sync.provider_unit", "delivered")

	auditor := &recordingAuditor{}
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
	runtime.pools = &postgresstore.RuntimePools{Domain: connectAsRole(t, ctx, uri, roles.domain, workerctlDomainPass)}
	run := func(org string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := dispatch(ctx, runtime, []string{
			"sync-runs", "cancel", "--org", org, "--run", syncRunsCancelIntegrationRun,
			"--reason", "operator_test", "--correlation-id", "corr-1",
		}, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	// commandBackend answers every job lookup with an error: the one delivered job is not reached.
	code, stdout, stderr := run(syncRunsCancelIntegrationOrg)
	var report struct {
		Cancel struct {
			Outcome     string `json:"outcome"`
			UnitsFailed int    `json:"units_failed"`
			RiverJobs   struct {
				Cancelled int `json:"cancelled"`
				Failed    int `json:"cancel_failed"`
			} `json:"river_jobs"`
		} `json:"sync_run_cancel"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout %q: %v (stderr %q)", stdout, err, stderr)
	}
	if code != 0 || stderr != "river_cancel_failed=1\n" || report.Cancel.Outcome != "cancelled" ||
		report.Cancel.UnitsFailed != 2 || report.Cancel.RiverJobs.Failed != 1 || report.Cancel.RiverJobs.Cancelled != 0 {
		t.Fatalf("code=%d stdout=%s stderr=%q, want exit 0, cancelled, 2 units failed and river_cancel_failed=1", code, stdout, stderr)
	}
	var runStatus string
	if err := admin.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, syncRunsCancelIntegrationRun).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" {
		t.Fatalf("run status=%s, want failed", runStatus)
	}

	code, stdout, stderr = run(syncRunsCancelIntegrationOrg)
	if err := json.Unmarshal([]byte(stdout), &report); err != nil || code != 0 || stderr != "" || report.Cancel.Outcome != "already_terminal" {
		t.Fatalf("second call: code=%d stdout=%s stderr=%q err=%v, want already_terminal", code, stdout, stderr, err)
	}
	code, stdout, stderr = run("00000000-0000-4000-8000-0000001438ff")
	if code != 1 || stdout != "" || stderr != "{\"error\":{\"code\":\"sync_run_not_found\"}}\n" {
		t.Fatalf("other org: code=%d stdout=%q stderr=%q, want sync_run_not_found", code, stdout, stderr)
	}

	want := []joboperator.AuditStatus{joboperator.AuditSucceeded, joboperator.AuditSucceeded, joboperator.AuditFailed}
	if len(auditor.events) != 3 || len(auditor.statuses) != 3 {
		t.Fatalf("audit intents=%d statuses=%v, want 3", len(auditor.events), auditor.statuses)
	}
	for index, status := range want {
		event := auditor.events[index]
		if auditor.statuses[index] != status || event.Action != joboperator.ActionSyncRunCancel || event.ResourceType != "sync_run" ||
			event.ReasonCode != "operator_test" || event.CorrelationID != "corr-1" {
			t.Fatalf("audit %d: %+v completed %s, want jobs.cancel on sync_run with reason and correlation, completed %s",
				index, event, auditor.statuses[index], status)
		}
	}
}
