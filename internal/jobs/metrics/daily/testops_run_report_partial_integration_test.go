//go:build integration

package daily

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-13826: a GitHub run with one report artifact skipped is synced through the production chunked route into ClickHouse (the
// real migration chain) and read through the real daily readers. The skip is recorded only in the unit result (Postgres); the
// day reads as a complete run, and no ClickHouse row names the run partial.

const partialRunID = "43600000077"

// partialPass is attemptsStack.pass that also returns the final emission's unit result.
func (stack attemptsStack) partialPass(t *testing.T, org, runID string, since time.Time, state attemptsRunState) map[string]any {
	t.Helper()
	before := since.Add(time.Hour)
	capability, _ := providersync.Capability("github", "tests")
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
	}
	doer := &attemptsDoer{t: t, runID: runID, state: state}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: stack.conn, Lease: lease}
	var result map[string]any
	destinations := map[string]bool{}
	err = providersync.GitHubTestsRouteHandler{}.CollectChunks(stack.ctx, claim, providerfoundation.Credential{}, client, before.Add(5*time.Minute), "",
		func(emission providersync.ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				destinations[effect.Destination] = true
				if len(effect.Rows) == 0 {
					continue
				}
				if err := sink.WriteEffect(stack.ctx, claim, effect); err != nil {
					return fmt.Errorf("write %s: %w", effect.Destination, err)
				}
			}
			if emission.Final {
				result = emission.Batch.Result
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	t.Logf("effect destinations: %v", destinations)
	return result
}

func TestTestopsRunWithASkippedArtifactIsNamedPartial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	stack := attemptsStack{ctx: ctx, conn: conn}

	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	window := day.Add(10 * time.Hour)
	const made = "2026-07-22T10:20:00Z"
	passing := attemptsJUnit(t, "pkg/a", "2026-07-22T10:02:00", 0.2, map[string]string{"TestA1": "passed", "TestA2": "passed"})
	failing := attemptsJUnit(t, "pkg/b", "2026-07-22T10:03:00", 0.3, map[string]string{"TestB1": "failed"})
	state := func(artifacts ...attemptsArtifact) attemptsRunState {
		return attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: artifacts}
	}

	cells := []struct {
		name      string
		skipped   attemptsArtifact
		wantCause string
	}{
		// The archive will not open: the route skips it with an unreadable_archive marker in the unit result.
		{"R1_unreadable_archive", attemptsArtifact{202, "go-test-b", made, false, []byte("not a zip archive")}, "unreadable_archive"},
		// GitHub lists the artifact as expired: the route skips it with no marker at all.
		{"R2_expired_artifact", attemptsArtifact{202, "go-test-b", made, true, failing}, "artifact_expired"},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			org := "org-13826-" + uuid.NewString()[:8]
			result := stack.partialPass(t, org, partialRunID, window, state(attemptsArtifact{101, "go-test-a", made, false, passing}, cell.skipped))
			t.Logf("unit result (Postgres sync_run_units.result): reports_complete=%v skipped_artifacts=%v incomplete=%v",
				result["reports_complete"], result["skipped_artifacts"], result["incomplete"])
			repoID := stack.repoID(t, org)
			metric := stack.metric(t, org, repoID, day)
			t.Logf("daily TestOps record: total=%d passed=%d failed=%d pass_rate=%v failure_rate=%v",
				metric.TotalCases, metric.PassedCount, metric.FailedCount, metric.PassRate, metric.FailureRate)

			var complete uint8
			var reasons []string
			row := conn.QueryRow(ctx, `SELECT complete, reasons FROM testops_run_report_completeness FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ?`, org, repoID, partialRunID)
			if err := row.Scan(&complete, &reasons); err != nil {
				t.Fatalf("RED: no per-run completeness row in ClickHouse for run %s (the day reads total=%d passed=%d failed=%d as a complete run): %v",
					partialRunID, metric.TotalCases, metric.PassedCount, metric.FailedCount, err)
			}
			if complete != 0 || len(reasons) != 1 || reasons[0] != cell.wantCause {
				t.Fatalf("completeness row complete=%d reasons=%v, want complete=0 reasons=[%s]", complete, reasons, cell.wantCause)
			}
		})
	}
}
