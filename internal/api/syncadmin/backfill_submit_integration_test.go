//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// submitVenue is a planner-managed configuration with n selected sources and NO
// scheduler running: a submit that waited for the plan would wait out
// SYNC_MANUAL_TRIGGER_AWAIT_SECONDS, which these tests set far above the 30 s failure guard of submitBounded.
func submitVenue(t *testing.T, sources int) (*cascadeVenue, cascadeSeeded, string) {
	t.Helper()
	v := startCascadeVenue(t, false)
	t.Setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "120")
	one := v.seed("github", `["git"]`, []string{"repo-metadata", "commits"})
	for index := 0; index < sources; index++ {
		name := fmt.Sprintf("acme/repo-%d", index)
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', $4, $4, $4, $5::json, true, now(), now())`,
			uuid.New(), v.org, one.integration, name, fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, one.config.String()))
	}
	return v, one, "/api/v1/admin/sync-configs/" + one.config.String() + "/backfill"
}

func (v *cascadeVenue) callAs(role, org, method, path, body string, headers map[string]string) (int, string) {
	v.t.Helper()
	request, err := http.NewRequest(method, v.server.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		v.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+sign(v.t, role, org))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		v.t.Fatal(err)
	}
	defer response.Body.Close()
	text, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(text)
}

func (v *cascadeVenue) count(sql string, args ...any) int {
	v.t.Helper()
	var n int
	if err := v.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		v.t.Fatal(err)
	}
	return n
}

type acceptedBody struct {
	Status       string `json:"status"`
	JobID        string `json:"backfill_job_id"`
	OccurrenceID string `json:"occurrence_id"`
	TaskID       string `json:"task_id"`
	SyncRunID    string `json:"sync_run_id"`
}

func accepted(t *testing.T, status int, body string) acceptedBody {
	t.Helper()
	var out acceptedBody
	if status != http.StatusAccepted || json.Unmarshal([]byte(body), &out) != nil || out.Status != "accepted" || out.JobID == "" || out.OccurrenceID == "" {
		t.Fatalf("submit: %d %s, want 202 accepted with a job and an occurrence", status, body)
	}
	if out.SyncRunID != "" || out.TaskID != "occurrence:"+out.OccurrenceID {
		t.Fatalf("submit: %s names a run, or a task that is not the occurrence", body)
	}
	return out
}

// submitBounded is v.call with a failure-only guard: a submit returns at once, so a run never reaches the guard; a submit
// that waits for the plan stays on the 120 s await of submitVenue and fails here, naming the request.
func submitBounded(t *testing.T, v *cascadeVenue, what, path, body string) (int, string) {
	t.Helper()
	type answer struct {
		status int
		body   string
	}
	done := make(chan answer, 1)
	go func() {
		status, text := v.call("POST", path, body)
		done <- answer{status, text}
	}()
	select {
	case got := <-done:
		return got.status, got.body
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: the submit is still waiting after the 30 s guard: it waited for the plan instead of returning at once", what)
		return 0, ""
	}
}

// TestBackfillSubmitReturnsAtOnceForAWideWindowAndManyRequests is the large case:
// a 20-year window, then 40 submits in a row, each answered 202 inside the normal
// request budget while no scheduler exists, each with its own job row that the job
// read serves as pending; the progress read does not fail on the unplanned job.
func TestBackfillSubmitReturnsAtOnceForAWideWindowAndManyRequests(t *testing.T) {
	v, one, path := submitVenue(t, 30)
	wide := `{"selector":{"since":"2006-01-01T00:00:00Z","before":"2026-01-01T00:00:00Z"}}`
	status, body := submitBounded(t, v, "a 20-year window", path, wide)
	first := accepted(t, status, body)

	jobs := map[string]bool{first.JobID: true}
	for index := 0; index < 40; index++ {
		day := index%27 + 1
		window := fmt.Sprintf(`{"selector":{"since":"2025-%02d-%02dT00:00:00Z","before":"2025-%02d-%02dT12:00:00Z"}}`, index/27+1, day, index/27+1, day)
		status, body := submitBounded(t, v, fmt.Sprintf("submit %d", index), path, window)
		jobs[accepted(t, status, body).JobID] = true
	}
	if len(jobs) != 41 {
		t.Fatalf("%d distinct jobs for 41 accepted submits", len(jobs))
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs WHERE sync_config_id = $1 AND celery_task_id LIKE 'occurrence:%'`, one.config); got != 41 {
		t.Fatalf("%d job rows for 41 accepted submits, want one each", got)
	}
	if got := v.count(`SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = $1`, one.config); got != 41 {
		t.Fatalf("%d occurrences, want 41", got)
	}
	status, body = v.call("GET", "/api/v1/admin/backfill-jobs/"+first.JobID, "")
	if status != http.StatusOK || !strings.Contains(body, first.JobID) {
		t.Fatalf("progress read of an unplanned job: %d %s", status, body)
	}
	status, body = v.call("GET", "/api/v1/admin/backfill-jobs", "")
	if status != http.StatusOK || !strings.Contains(body, `"total":41`) {
		t.Fatalf("job list: %d %s", status, body)
	}
}

// TestEveryRefusedBackfillSubmitWritesNoJobRow: a refusal after the hand-off's
// own checks rolls the occurrence, trigger and job back together.
func TestEveryRefusedBackfillSubmitWritesNoJobRow(t *testing.T) {
	v, one, path := submitVenue(t, 2)
	unselected := uuid.New()
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', 'acme/unselected', 'acme/unselected', 'acme/unselected', '{}'::json, true, now(), now())`, unselected, v.org, one.integration)
	for name, body := range map[string]string{
		"unselected source": fmt.Sprintf(`{"selector":{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z","source_ids":[%q]}}`, unselected),
		"bad window":        `{"selector":{"since":"2026-09-02T00:00:00Z","before":"2026-09-01T00:00:00Z"}}`,
	} {
		if status, text := v.call("POST", path, body); status < 400 {
			t.Fatalf("%s: %d %s, want a refusal", name, status, text)
		}
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs`) + v.count(`SELECT count(*) FROM scheduled_sync_occurrences`) +
		v.count(`SELECT count(*) FROM sync_manual_triggers`); got != 0 {
		t.Fatalf("refused submits left %d rows", got)
	}
}

// TestBackfillSubmitWithoutThePermissionWritesNothing: the guard answers before
// the hand-off, as it did when the job row was written after the wait.
func TestBackfillSubmitWithoutThePermissionWritesNothing(t *testing.T) {
	v, _, path := submitVenue(t, 1)
	body := `{"selector":{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z"}}`
	status, text := v.callAs("member", v.org, "POST", path, body, nil)
	if status != http.StatusForbidden {
		t.Fatalf("member: %d %s, want 403", status, text)
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs`) + v.count(`SELECT count(*) FROM scheduled_sync_occurrences`); got != 0 {
		t.Fatalf("a refused member wrote %d rows", got)
	}
	status, text = v.callAs("admin", uuid.NewString(), "POST", path, body, nil)
	if status != http.StatusNotFound {
		t.Fatalf("another org's admin: %d %s, want 404 (config not found)", status, text)
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs`); got != 0 {
		t.Fatalf("another org's submit wrote %d jobs", got)
	}
}

// TestBackfillSubmitIdempotencyKey: a repeat with the same key returns the same
// job and writes nothing; concurrent repeats collapse to one; another key, another
// configuration and an invalid key behave as their own.
func TestBackfillSubmitIdempotencyKey(t *testing.T) {
	v, one, path := submitVenue(t, 2)
	body := `{"selector":{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z"}}`
	key := map[string]string{"Idempotency-Key": "wizard-1"}
	status, text := v.callAs("admin", v.org, "POST", path, body, key)
	first := accepted(t, status, text)
	status, text = v.callAs("admin", v.org, "POST", path, body, key)
	repeat := accepted(t, status, text)
	if repeat.JobID != first.JobID || repeat.OccurrenceID != first.OccurrenceID {
		t.Fatalf("repeat %+v differs from %+v", repeat, first)
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs WHERE sync_config_id = $1`, one.config); got != 1 {
		t.Fatalf("%d jobs after a repeat, want 1", got)
	}
	if got := v.count(`SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = $1`, one.config); got != 1 {
		t.Fatalf("%d occurrences after a repeat, want 1", got)
	}

	status, text = v.callAs("admin", v.org, "POST", path, body, map[string]string{"Idempotency-Key": "wizard-2"})
	if other := accepted(t, status, text); other.JobID == first.JobID {
		t.Fatal("another key returned the first job")
	}

	concurrent := map[string]string{"Idempotency-Key": "burst"}
	var group sync.WaitGroup
	results := make([]acceptedBody, 6)
	for index := range results {
		group.Add(1)
		go func() {
			defer group.Done()
			status, text := v.callAs("admin", v.org, "POST", path, body, concurrent)
			if status == http.StatusAccepted {
				_ = json.Unmarshal([]byte(text), &results[index])
			}
		}()
	}
	group.Wait()
	for index, result := range results {
		if result.JobID == "" || result.JobID != results[0].JobID {
			t.Fatalf("concurrent repeat %d: %+v, want job %s", index, result, results[0].JobID)
		}
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs WHERE celery_task_id LIKE 'idem:%' AND sync_config_id = $1`, one.config); got != 3 {
		t.Fatalf("%d keyed jobs, want 3 (wizard-1, wizard-2, burst)", got)
	}

	for _, bad := range []string{strings.Repeat("k", 129), "has space", "semi;colon"} {
		if status, text := v.callAs("admin", v.org, "POST", path, body, map[string]string{"Idempotency-Key": bad}); status != http.StatusUnprocessableEntity {
			t.Fatalf("key %q: %d %s, want 422", bad, status, text)
		}
	}
}

// TestBackfillSubmitRefusesARunTheSchedulerWouldRefuse: with SYNC_RUN_MAX_UNITS
// set, a window and scope whose unit floor is over it is refused 422 at submit with
// the caller's own numbers and nothing written; a window at the cap is accepted.
func TestBackfillSubmitRefusesARunTheSchedulerWouldRefuse(t *testing.T) {
	v, _, path := submitVenue(t, 3)
	t.Setenv("SYNC_RUN_MAX_UNITS", "10")
	over := `{"selector":{"since":"2025-01-01T00:00:00Z","before":"2025-03-08T00:00:00Z"}}`
	status, text := v.call("POST", path, over)
	if status != http.StatusUnprocessableEntity || !strings.Contains(text, "at least 15 work units") || !strings.Contains(text, "limit of 10") {
		t.Fatalf("over the cap: %d %s", status, text)
	}
	if got := v.count(`SELECT count(*) FROM backfill_jobs`) + v.count(`SELECT count(*) FROM scheduled_sync_occurrences`); got != 0 {
		t.Fatalf("a refused submit wrote %d rows", got)
	}
	atCap := `{"selector":{"since":"2025-01-01T00:00:00Z","before":"2025-02-12T00:00:00Z"}}`
	status, text = v.call("POST", path, atCap)
	accepted(t, status, text)
}

// TestOccurrenceSyncRunIDIsOrgScoped: the lazy occurrence read filters on the
// occurrence id AND the org in one query: another org's read finds no run, the
// owner's read sees the run once the scheduler planned it, or the quarantine.
func TestOccurrenceSyncRunIDIsOrgScoped(t *testing.T) {
	v, one, path := submitVenue(t, 1)
	status, text := v.call("POST", path, `{"selector":{"since":"2026-09-01T00:00:00Z","before":"2026-09-02T00:00:00Z"}}`)
	submitted := accepted(t, status, text)
	reader := store{pool: v.pool}
	ctx := context.Background()
	planned, err := reader.occurrencePlanOf(ctx, v.org, submitted.OccurrenceID)
	if err != nil || planned.RunID != nil || planned.ErrorCode != "" {
		t.Fatalf("unplanned occurrence: %v %v, want empty", planned, err)
	}
	run, jobRun := uuid.New(), uuid.New()
	v.exec(`INSERT INTO job_runs (id, job_id, status, triggered_by, created_at) VALUES ($1, $2, 0, 'backfill', now())`, jobRun, one.job)
	v.exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, 'backfill', 'backfill', 'planned', 0, 0, 0, now())`, run, v.org, one.integration)
	v.exec(`UPDATE scheduled_sync_occurrences SET sync_run_id = $2, job_run_id = $3, reconcile_status = 'completed' WHERE occurrence_id = $1`,
		submitted.OccurrenceID, run, jobRun)
	if got, err := reader.occurrencePlanOf(ctx, v.org, submitted.OccurrenceID); err != nil || got.RunID == nil || *got.RunID != run || got.ErrorCode != "" {
		t.Fatalf("owner read of a planned occurrence: %v %v, want run %s", got, err, run)
	}
	if got, err := reader.occurrencePlanOf(ctx, uuid.NewString(), submitted.OccurrenceID); err != nil || got.RunID != nil || got.ErrorCode != "" {
		t.Fatalf("another org's read of a planned occurrence: %v %v, want not found, never a run", got, err)
	}
	other := submitVenueSecond(t, v, one)
	v.exec(`UPDATE scheduled_sync_occurrences SET sync_run_id = NULL, job_run_id = NULL, reconcile_status = 'quarantined', reconcile_error_code = 'invalid_plan', reconcile_error_at = now() WHERE occurrence_id = $1`, other)
	if got, err := reader.occurrencePlanOf(ctx, v.org, other); err != nil || got.ErrorCode != "invalid_plan" || got.RunID != nil {
		t.Fatalf("quarantined occurrence: %v %v, want code invalid_plan", got, err)
	}
	if got, err := reader.occurrencePlanOf(ctx, uuid.NewString(), other); err != nil || got.ErrorCode != "" {
		t.Fatalf("another org's read of a quarantined occurrence: %v %v, want not found", got, err)
	}
}

// submitVenueSecond submits a second window and returns its occurrence id.
func submitVenueSecond(t *testing.T, v *cascadeVenue, one cascadeSeeded) string {
	t.Helper()
	status, text := v.call("POST", "/api/v1/admin/sync-configs/"+one.config.String()+"/backfill", `{"selector":{"since":"2026-08-01T00:00:00Z","before":"2026-08-02T00:00:00Z"}}`)
	return accepted(t, status, text).OccurrenceID
}
