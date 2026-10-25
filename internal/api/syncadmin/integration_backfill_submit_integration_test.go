//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The integrations backfill route (POST /integrations/{id}/backfill) answers 202
// at once: no wait for the scheduler's plan. These tests run with NO scheduler
// and the route's default 30 s wait, and assert deterministic cost: no wait call
// (the await counter does not move) and the rows written.

const integrationWindow = `{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z"}`

func (v *cascadeVenue) integrationPath(one cascadeSeeded) string {
	return "/api/v1/admin/integrations/" + one.integration.String() + "/backfill"
}

func (v *cascadeVenue) addSources(one cascadeSeeded, org string, n int) {
	v.t.Helper()
	for index := 0; index < n; index++ {
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', $4, $4, $4, $5::json, true, now(), now())`,
			uuid.New(), org, one.integration, fmt.Sprintf("acme/%s-%d", one.integration, index), fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, one.config.String()))
	}
}

func (v *cascadeVenue) seedIn(org string, sources int) cascadeSeeded {
	v.t.Helper()
	previous := v.org
	v.org = org
	defer func() { v.org = previous }()
	one := v.seed("github", `["git"]`, []string{"repo-metadata", "commits"})
	v.addSources(one, org, sources)
	return one
}

func awaitCount(t *testing.T) uint64 {
	t.Helper()
	var out bytes.Buffer
	if err := synchandoff.AwaitMetricsSource().WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, match := range regexp.MustCompile(`(?m)^sync_manual_trigger_await_outcome_total\{[^}]*\} (\d+)$`).FindAllStringSubmatch(out.String(), -1) {
		var n uint64
		fmt.Sscan(match[1], &n)
		total += n
	}
	return total
}

func (v *cascadeVenue) handoffRows() int {
	return v.count(`SELECT count(*) FROM scheduled_sync_occurrences`) + v.count(`SELECT count(*) FROM sync_manual_triggers`)
}

type integrationAccepted struct {
	Status        string `json:"status"`
	IntegrationID string `json:"integration_id"`
	OccurrenceID  string `json:"occurrence_id"`
	TaskID        string `json:"task_id"`
	SyncRunID     string `json:"sync_run_id"`
	TotalUnits    *int   `json:"total_units"`
}

func acceptedIntegration(t *testing.T, status int, body string) integrationAccepted {
	t.Helper()
	var out integrationAccepted
	if status != http.StatusAccepted || json.Unmarshal([]byte(body), &out) != nil || out.Status != "accepted" || out.OccurrenceID == "" {
		t.Fatalf("submit: %d %s, want 202 accepted with an occurrence", status, body)
	}
	if out.SyncRunID != "" || out.TotalUnits != nil || out.TaskID != "occurrence:"+out.OccurrenceID {
		t.Fatalf("submit: %s names a run or units, or a task that is not the occurrence", body)
	}
	return out
}

// TestIntegrationBackfillSubmitReturnsAtOnceForAWideWindowAndManyRequests: a
// 20-year window and 40 more submits, no scheduler, the 30 s wait configured: each
// answers 202 with its occurrence, the route never calls the wait (the await
// counter stays put), and every submit wrote its occurrence and trigger.
func TestIntegrationBackfillSubmitReturnsAtOnceForAWideWindowAndManyRequests(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, []string{"repo-metadata", "commits"})
	v.addSources(one, v.org, 30)
	path := v.integrationPath(one)
	before := awaitCount(t)
	status, body := v.call("POST", path, `{"selector":{"since":"2006-01-01T00:00:00Z","before":"2026-01-01T00:00:00Z"}}`)
	first := acceptedIntegration(t, status, body)
	if first.IntegrationID != one.integration.String() {
		t.Fatalf("integration_id %q", first.IntegrationID)
	}
	occurrences := map[string]bool{first.OccurrenceID: true}
	for index := 0; index < 40; index++ {
		day := index%27 + 1
		window := fmt.Sprintf(`{"since":"2025-%02d-%02dT00:00:00Z","before":"2025-%02d-%02dT12:00:00Z"}`, index/27+1, day, index/27+1, day)
		status, body := v.call("POST", path, window)
		occurrences[acceptedIntegration(t, status, body).OccurrenceID] = true
	}
	if len(occurrences) != 41 {
		t.Fatalf("%d distinct occurrences for 41 submits", len(occurrences))
	}
	if got := awaitCount(t); got != before {
		t.Fatalf("the route waited for the scheduler %d times", got-before)
	}
	for table, query := range map[string]string{
		"occurrences": `SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = $1 AND reconcile_status = 'pending'`,
		"triggers":    `SELECT count(*) FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o USING (occurrence_id) WHERE o.sync_config_id = $1 AND t.mode = 'backfill'`,
	} {
		if got := v.count(query, one.config); got != 41 {
			t.Fatalf("%s: %d rows for 41 accepted submits", table, got)
		}
	}
}

// TestIntegrationBackfillSubmitRefusalsWriteNothing: every refusal answers as it
// did and leaves no occurrence or trigger.
func TestIntegrationBackfillSubmitRefusalsWriteNothing(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(one, v.org, 1)
	path := v.integrationPath(one)
	unselected := uuid.New()
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', 'acme/unselected', 'acme/unselected', 'acme/unselected', '{}'::json, true, now(), now())`, unselected, v.org, one.integration)
	for name, tc := range map[string]struct {
		body   string
		status int
	}{
		"source id that is not a uuid": {`{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z","source_ids":["nope"]}`, 400},
		"unselected source":            {fmt.Sprintf(`{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z","source_ids":[%q]}`, unselected), 400},
		"no window":                    {`{}`, 422},
	} {
		if status, text := v.call("POST", path, tc.body); status != tc.status {
			t.Fatalf("%s: %d %s, want %d", name, status, text, tc.status)
		}
	}
	if got := v.handoffRows(); got != 0 {
		t.Fatalf("refused submits left %d rows", got)
	}
}

// TestIntegrationBackfillSinceNotBeforeBeforeIs400BeforeAnyWrite: a window whose
// since is not before its before is refused before any write.
func TestIntegrationBackfillSinceNotBeforeBeforeIs400BeforeAnyWrite(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(one, v.org, 1)
	for name, body := range map[string]string{
		"since after before":  `{"since":"2026-09-10T00:00:00Z","before":"2026-09-01T00:00:00Z"}`,
		"since equals before": `{"since":"2026-09-10T00:00:00Z","before":"2026-09-10T00:00:00Z"}`,
	} {
		status, text := v.call("POST", v.integrationPath(one), body)
		if status != http.StatusBadRequest || text != `{"detail":"Backfill since must be before"}` {
			t.Fatalf("%s: %d %s", name, status, text)
		}
	}
	if got := v.handoffRows(); got != 0 {
		t.Fatalf("a refused window left %d rows", got)
	}
}

// TestIntegrationBackfillSubmitWithoutThePermissionWritesNothing: the guard
// answers before the hand-off (org and role are checked before the transaction).
func TestIntegrationBackfillSubmitWithoutThePermissionWritesNothing(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(one, v.org, 1)
	if status, text := v.callAs("member", v.org, "POST", v.integrationPath(one), integrationWindow, nil); status != http.StatusForbidden {
		t.Fatalf("member: %d %s, want 403", status, text)
	}
	if got := v.handoffRows(); got != 0 {
		t.Fatalf("a refused member wrote %d rows", got)
	}
}

// TestIntegrationBackfillAnotherOrgsIntegrationWritesNothingAndNeverLinks: org B
// naming org A's integration gets 404 and nothing is written; org B's own submit
// next to org A's occurrence (planned) never names or touches it.
func TestIntegrationBackfillAnotherOrgsIntegrationWritesNothingAndNeverLinks(t *testing.T) {
	v := startCascadeVenue(t, false)
	orgB := uuid.NewString()
	pgseed.Org(context.Background(), t, v.pool, orgB, "enterprise")
	oneA := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(oneA, v.org, 1)
	oneB := v.seedIn(orgB, 1)
	status, text := v.call("POST", v.integrationPath(oneA), integrationWindow)
	a := acceptedIntegration(t, status, text)
	run, jobRun := uuid.New(), uuid.New()
	v.exec(`INSERT INTO job_runs (id, job_id, status, triggered_by, created_at) VALUES ($1, $2, 0, 'backfill', now())`, jobRun, oneA.job)
	v.exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, 'backfill', 'backfill', 'planned', 0, 0, 0, now())`, run, v.org, oneA.integration)
	v.exec(`UPDATE scheduled_sync_occurrences SET sync_run_id = $2, job_run_id = $3, reconcile_status = 'completed' WHERE occurrence_id = $1`, a.OccurrenceID, run, jobRun)
	rows := v.handoffRows()

	status, text = v.callAs("admin", orgB, "POST", v.integrationPath(oneA), integrationWindow, nil)
	if status != http.StatusNotFound || text != `{"detail":"Integration not found"}` {
		t.Fatalf("org B on org A's integration: %d %s, want 404", status, text)
	}
	if got := v.handoffRows(); got != rows {
		t.Fatalf("another org's submit wrote %d rows", got-rows)
	}
	status, text = v.callAs("admin", orgB, "POST", v.integrationPath(oneB), integrationWindow, nil)
	b := acceptedIntegration(t, status, text)
	if b.OccurrenceID == a.OccurrenceID || strings.Contains(text, a.OccurrenceID) || strings.Contains(text, run.String()) {
		t.Fatalf("org B's answer %s links org A's occurrence or run", text)
	}
	if got := v.count(`SELECT count(*) FROM scheduled_sync_occurrences WHERE org_id = $1 AND occurrence_id = $2`, orgB, a.OccurrenceID); got != 0 {
		t.Fatal("org B owns org A's occurrence")
	}
	if got := v.count(`SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_run_id = $1 AND sync_config_id = $2`, run, oneB.config); got != 0 {
		t.Fatal("org B's configuration is linked to org A's run")
	}
}

// TestIntegrationBackfillScaleRefusalNamesOnlyTheCallersOwnOrgValues: with a unit
// cap, each org's 422 names its own floor; a second, larger org's numbers never
// appear in the smaller org's answer, and the other way round.
func TestIntegrationBackfillScaleRefusalNamesOnlyTheCallersOwnOrgValues(t *testing.T) {
	v := startCascadeVenue(t, false)
	t.Setenv("SYNC_RUN_MAX_UNITS", "10")
	orgB := uuid.NewString()
	pgseed.Org(context.Background(), t, v.pool, orgB, "enterprise")
	small := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(small, v.org, 3)
	large := v.seedIn(orgB, 40)
	window := `{"since":"2025-01-01T00:00:00Z","before":"2025-03-08T00:00:00Z"}`
	status, text := v.call("POST", v.integrationPath(small), window)
	if status != http.StatusUnprocessableEntity || text != `{"detail":"Backfill is too large: at least 15 work units for this window and scope, over the limit of 10. Narrow the window or the scope."}` {
		t.Fatalf("small org: %d %s", status, text)
	}
	status, text = v.callAs("admin", orgB, "POST", v.integrationPath(large), window, nil)
	if status != http.StatusUnprocessableEntity || text != `{"detail":"Backfill is too large: at least 200 work units for this window and scope, over the limit of 10. Narrow the window or the scope."}` {
		t.Fatalf("large org: %d %s", status, text)
	}
	if got := v.handoffRows(); got != 0 {
		t.Fatalf("refused submits left %d rows", got)
	}
	status, text = v.call("POST", v.integrationPath(small), `{"since":"2025-01-01T00:00:00Z","before":"2025-02-12T00:00:00Z"}`)
	acceptedIntegration(t, status, text)
}

// TestSyncConfigBackfillScaleRefusalIsByteIdentical pins the sync-config route's
// 422 (status and body) so moving the scale check cannot change it.
func TestSyncConfigBackfillScaleRefusalIsByteIdentical(t *testing.T) {
	v, _, path := submitVenue(t, 3)
	t.Setenv("SYNC_RUN_MAX_UNITS", "10")
	status, text := v.call("POST", path, `{"selector":{"since":"2025-01-01T00:00:00Z","before":"2025-03-08T00:00:00Z"}}`)
	const want = `{"detail":"Backfill is too large: at least 15 work units for this window and scope, over the limit of 10. Narrow the window or the scope."}`
	if status != http.StatusUnprocessableEntity || text != want {
		t.Fatalf("%d %s, want 422 %s", status, text, want)
	}
}
