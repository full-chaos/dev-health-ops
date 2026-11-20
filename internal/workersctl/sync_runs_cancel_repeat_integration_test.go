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
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	syncRunsRepeatOrg = "00000000-0000-4000-8000-000000143840"
	syncRunsRepeatRun = "00000000-0000-4000-8000-000000143841"
)

func syncRunsRepeatDomainSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT row_to_json(r) FROM public.sync_runs AS r WHERE r.id = $1::uuid),
  'units', (SELECT json_agg(u ORDER BY u.id) FROM public.sync_run_units AS u WHERE u.sync_run_id = $1::uuid),
  'outbox', (SELECT json_agg(o ORDER BY o.kind) FROM public.sync_dispatch_outbox AS o WHERE o.sync_run_id = $1::uuid),
  'worker_outbox', (SELECT json_agg(w ORDER BY w.dedupe_key) FROM public.worker_job_outbox AS w)
)::text`, syncRunsRepeatRun).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// As the restricted domain login with production grants: the verb cancels no River job (stdout
// carries no River counts, stderr is empty), and a repeat call on the closed run changes no domain
// row and still writes one audit pair, completed succeeded, with outcome already_terminal.
func TestSyncRunsCancelLeavesRiverJobsAloneAndAuditsARepeatCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, uri, roles := startJobRouteHarness(t, ctx)
	pgseed.EnsureSyncRun(ctx, t, admin, pgseed.SyncRun{ID: syncRunsRepeatRun, OrgID: syncRunsRepeatOrg, Status: "dispatching", TotalUnits: 2})
	dispatching := uuid.NewString()
	for _, unit := range []pgseed.SyncRunUnit{
		{ID: uuid.NewString(), Status: "planned", DatasetKey: "prs"},
		{ID: dispatching, Status: "dispatching", DatasetKey: "commits"},
	} {
		unit.RunID, unit.OrgID = syncRunsRepeatRun, syncRunsRepeatOrg
		pgseed.InsertSyncRunUnit(ctx, t, admin, unit)
	}
	pgseed.WorkerJobOutbox(ctx, t, admin, "sync.provider_unit:"+dispatching, "sync.provider_unit", "delivered")

	auditor := &recordingAuditor{}
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
	runtime.pools = &postgresstore.RuntimePools{Domain: connectAsRole(t, ctx, uri, roles.domain, workerctlDomainPass)}
	run := func() (int, map[string]any, string) {
		var stdout, stderr bytes.Buffer
		code := dispatch(ctx, runtime, []string{
			"sync-runs", "cancel", "--org", syncRunsRepeatOrg, "--run", syncRunsRepeatRun,
			"--reason", "operator_test", "--correlation-id", "corr-1",
		}, &stdout, &stderr)
		var report struct {
			Cancel map[string]any `json:"sync_run_cancel"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("stdout %q: %v (stderr %q)", stdout.String(), err, stderr.String())
		}
		return code, report.Cancel, stderr.String()
	}

	code, first, stderr := run()
	if _, hasRiver := first["river_jobs"]; code != 0 || stderr != "" || hasRiver || first["outcome"] != "cancelled" {
		t.Fatalf("first call: code=%d report=%v stderr=%q, want cancelled, no River counts and an empty stderr", code, first, stderr)
	}
	before := syncRunsRepeatDomainSnapshot(t, ctx, admin)
	code, second, stderr := run()
	if code != 0 || stderr != "" || second["outcome"] != "already_terminal" {
		t.Fatalf("repeat call: code=%d report=%v stderr=%q, want already_terminal", code, second, stderr)
	}
	if after := syncRunsRepeatDomainSnapshot(t, ctx, admin); after != before {
		t.Fatalf("the repeat call changed domain rows:\nbefore %s\nafter  %s", before, after)
	}
	if len(auditor.events) != 2 || len(auditor.statuses) != 2 {
		t.Fatalf("audit intents=%d statuses=%v, want one pair per call", len(auditor.events), auditor.statuses)
	}
	for index, event := range auditor.events {
		if auditor.statuses[index] != joboperator.AuditSucceeded || event.Action != joboperator.ActionSyncRunCancel ||
			event.ResourceType != "sync_run" || event.ResourceID != syncRunsRepeatOrg+":"+syncRunsRepeatRun {
			t.Fatalf("audit %d: %+v completed %s, want jobs.cancel on the sync_run completed succeeded", index, event, auditor.statuses[index])
		}
	}
}
