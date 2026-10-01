//go:build integration

package workerservice

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// r1 (CHAOS-7132): the post-sync seam records a degraded leg on the run's result itself, keeping every
// other key, with the detail sanitized and bounded, and refuses a run row that does not exist.
func TestRecordSyncRunDegradedLegsMergesIntoTheRunResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	const runID, orgID = "00000000-0000-4000-8000-0000000000b1", "00000000-0000-4000-8000-0000000000b2"
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: orgID, Status: "success", TotalUnits: 14, CompletedUnits: 14,
		ResultJSON: `{"completed_units":14,"failed_units":0}`})

	// Drive the real post-sync dispatcher: a collector reports a failed additive leg for a run that
	// already finished (finalize ran before this seam). The state under test is sync_runs.result.
	spy := &linearCollectorSpy{result: providersync.TeamCatalogResult{TeamsWritten: 2, DegradedLegs: []providersync.DegradedLeg{{
		Dataset: "teams", Leg: "jira_atlassian_teams", Outcome: "failed", Reason: "unclassified",
		Detail: "Invalid Organization Ari: some-uuid Authorization: Bearer secret-token-value"}}}}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "linear", nil },
		native:          map[string]providersync.TeamCatalogCollector{"linear": spy},
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		recordDegraded:  recordSyncRunDegradedLegs(pool),
	}
	if err := dispatcher.TeamAutoImport(ctx, syncdispatchruntime.DomainReference{OrganizationID: orgID, SyncRunID: runID}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT result::text FROM sync_runs WHERE id = $1::uuid`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Completed int                 `json:"completed_units"`
		Failed    int                 `json:"failed_units"`
		Degraded  []map[string]string `json:"degraded"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.Completed != 14 || result.Failed != 0 {
		t.Errorf("the other result keys were not kept: %s", raw)
	}
	if len(result.Degraded) != 1 || result.Degraded[0]["leg"] != "jira_atlassian_teams" || result.Degraded[0]["outcome"] != "failed" {
		t.Fatalf("the degraded leg is missing: %s", raw)
	}
	if strings.Contains(string(raw), "secret-token-value") {
		t.Errorf("the stored detail carries a token: %s", raw)
	}
	if err := recordSyncRunDegradedLegs(pool)(ctx, "another-org", runID, spy.result.DegradedLegs); err == nil {
		t.Error("a run of another organization must not be written")
	}
}
