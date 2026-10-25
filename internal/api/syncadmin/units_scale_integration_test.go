//go:build integration

package syncadmin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

func seedUnitsRun(t *testing.T, v *cascadeVenue, sources int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	one := v.seed("github", `["git"]`, nil)
	run := uuid.New()
	v.exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, 'manual', 'incremental', 'running', 0, 0, 0, now())`, run, v.org, one.integration)
	ids := make([]uuid.UUID, sources)
	for i := range ids {
		ids[i] = uuid.New()
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', $4, $5, $6, '{}'::json, true, now(), now())`, ids[i], v.org, one.integration,
			fmt.Sprintf("acme/r%d", i), fmt.Sprintf("r%d", i), fmt.Sprintf("acme/r%d", i))
	}
	return run, ids
}

func (v *cascadeVenue) callAsOrg(org, path string) (int, string) {
	v.t.Helper()
	request, err := http.NewRequest(http.MethodGet, v.server.URL+path, nil)
	if err != nil {
		v.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+sign(v.t, "admin", org))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		v.t.Fatal(err)
	}
	defer response.Body.Close()
	text, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(text)
}

// TestRunUnitsPagedReadAnswersFromSQL pins the paged read on Postgres: the
// rollups read only error_category and retry_exhausted from the stored result
// (an explicit null stays distinct from a missing key), the filters narrow the
// page and its filtered total but never the rollups, and another org's run id
// answers 404 with no unit and no count.
func TestRunUnitsPagedReadAnswersFromSQL(t *testing.T) {
	v := startCascadeVenue(t, false)
	run, sources := seedUnitsRun(t, v, 2)
	insert := func(dataset, status, result string) {
		v.exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
status, attempts, result, since_at, before_at, available_at, created_at, updated_at)
SELECT $1, $2, $3, integration_id, $4, 'github', $5, 'medium', 'incremental', $6, 1, $7::json,
 '2026-04-01 00:00:00+00', '2026-04-10 00:00:00+00', now(), now(), now() FROM sync_runs WHERE id = $3`,
			uuid.New(), v.org, run, sources[len(dataset)%2], dataset, status, result)
	}
	insert("commits", "failed", `{"error_category":"auth"}`)
	insert("commits", "failed", `{"error_category":null}`)
	insert("commits", "failed", `{}`)
	insert("files", "retrying", `{"error_category":"budget_deferred","retry_exhausted":true}`)
	insert("files", "retrying", `{"error_category":"worker_lost_retry_exhausted"}`)
	insert("files", "success", `[1]`)
	insert("tags", "success", `{"retry_exhausted":"true"}`)

	status, whole := v.call("GET", "/api/v1/admin/sync-runs/"+run.String()+"/units", "")
	if status != http.StatusOK {
		t.Fatalf("whole run: %d %s", status, whole)
	}
	status, paged := v.call("GET", "/api/v1/admin/sync-runs/"+run.String()+"/units?offset=0&limit=2&status=failed,retrying", "")
	if status != http.StatusOK {
		t.Fatalf("paged: %d %s", status, paged)
	}
	var wholeBody, pagedBody map[string]json.RawMessage
	if err := json.Unmarshal([]byte(whole), &wholeBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(paged), &pagedBody); err != nil {
		t.Fatal(err)
	}
	if _, has := wholeBody["filtered_unit_count"]; has {
		t.Errorf("the whole-run body carries filtered_unit_count")
	}
	for _, key := range []string{"by_status", "by_source", "by_dataset", "by_cost_class", "failed_unit_count", "unit_count",
		"partial_failure_summary", "retry_exhausted_unit_count", "budget_blocked_unit_count", "slowest_unit_ids", "failed_unit_ids"} {
		if string(wholeBody[key]) != string(pagedBody[key]) {
			t.Errorf("rollup %s differs between the whole-run and the paged read: %s vs %s", key, wholeBody[key], pagedBody[key])
		}
	}
	if string(pagedBody["filtered_unit_count"]) != "5" || string(pagedBody["unit_count"]) != "7" {
		t.Errorf("filtered %s of %s, want 5 of 7", pagedBody["filtered_unit_count"], pagedBody["unit_count"])
	}
	var units []json.RawMessage
	_ = json.Unmarshal(pagedBody["units"], &units)
	if len(units) != 2 {
		t.Errorf("page of %d units, want 2", len(units))
	}
	if string(pagedBody["retry_exhausted_unit_count"]) != "2" || string(pagedBody["budget_blocked_unit_count"]) != "1" {
		t.Errorf("retry-exhausted %s, budget-blocked %s, want 2 and 1", pagedBody["retry_exhausted_unit_count"], pagedBody["budget_blocked_unit_count"])
	}
	if !strings.Contains(string(pagedBody["partial_failure_summary"]), `"unknown"`) || !strings.Contains(string(pagedBody["partial_failure_summary"]), `"auth"`) {
		t.Errorf("failed categories lost: %s", pagedBody["partial_failure_summary"])
	}
	for _, query := range []string{"?limit=1001&offset=0", "?offset=-1", "?source_id=x", "?since=yesterday", "?status=", "?status=bogus", "?offset=1000001", "?limit=-1&offset=0"} {
		if code, body := v.call("GET", "/api/v1/admin/sync-runs/"+run.String()+"/units"+query, ""); code != http.StatusUnprocessableEntity {
			t.Errorf("%s: %d %s, want 422", query, code, body)
		}
	}

	// Date window: a unit inside, one that ends before it, one that starts after
	// it, one with NULL bounds and one that spans it; asked through the real SQL.
	window := func(since, before any) {
		v.exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
status, attempts, result, since_at, before_at, available_at, created_at, updated_at)
SELECT $1, $2, $3, integration_id, $4, 'github', 'window', 'medium', 'incremental', 'success', 1, '{}'::json,
 $5::timestamptz, $6::timestamptz, now(), now(), now() FROM sync_runs WHERE id = $3`, uuid.New(), v.org, run, sources[0], since, before)
	}
	window("2026-05-10 00:00:00+00", "2026-05-20 00:00:00+00")
	window("2026-04-01 00:00:00+00", "2026-04-10 00:00:00+00")
	window("2026-06-10 00:00:00+00", "2026-06-20 00:00:00+00")
	window(nil, nil)
	window("2026-04-01 00:00:00+00", "2026-06-30 00:00:00+00")
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"&since=2026-05-12&before=2026-05-18", 3},
		{"&since=2026-05-12", 4},
		{"&before=2026-05-18", 4},
		{"&since=2026-06-25T00:00:00Z", 2},
		{"&before=2026-03-01", 1},
		{"", 5},
	} {
		code, body := v.call("GET", "/api/v1/admin/sync-runs/"+run.String()+"/units?offset=0&limit=100&dataset_key=window"+tc.query, "")
		var parsed struct {
			Filtered int `json:"filtered_unit_count"`
			Units    []struct {
				DatasetKey string `json:"dataset_key"`
			} `json:"units"`
		}
		if err := json.Unmarshal([]byte(body), &parsed); code != http.StatusOK || err != nil || parsed.Filtered != tc.want || len(parsed.Units) != tc.want {
			t.Errorf("window%s: %d filtered %d page %d (%v), want %d", tc.query, code, parsed.Filtered, len(parsed.Units), err, tc.want)
		}
	}

	other := uuid.NewString()
	pgseed.Org(t.Context(), t, v.pool, other, "enterprise")
	for _, query := range []string{"", "?offset=0&limit=5", "?status=failed&offset=0"} {
		code, body := v.callAsOrg(other, "/api/v1/admin/sync-runs/"+run.String()+"/units"+query)
		if code != http.StatusNotFound || strings.Contains(body, "unit_count") || strings.Contains(body, sources[0].String()) {
			t.Errorf("org B reading org A's run %q: %d %s, want 404 with no unit and no count", query, code, body)
		}
	}
}

// TestRunUnitsPagedReadAt100kUnits seeds one run with 100,000 units in
// Postgres and times the paged route (rollups included) through the real
// handler. The page must stay far inside the web's 20 s read deadline.
func TestRunUnitsPagedReadAt100kUnits(t *testing.T) {
	v := startCascadeVenue(t, false)
	run, sources := seedUnitsRun(t, v, 200)
	v.exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
status, attempts, result, since_at, before_at, available_at, created_at, updated_at)
SELECT gen_random_uuid(), $1, $2, r.integration_id, ($3::uuid[])[1 + (g % 200)], 'github',
 (ARRAY['commits','files','work-items','repo-metadata','pull-requests'])[1 + (g % 5)], 'medium', 'incremental',
 (ARRAY['success','success','success','failed','retrying','pending'])[1 + (g % 6)], 1,
 json_build_object('error_category', 'budget_deferred', 'detail', repeat('x', 600)),
 '2026-04-01 00:00:00+00', '2026-04-10 00:00:00+00', now(), now(), now()
FROM sync_runs AS r, generate_series(1, 100000) AS g WHERE r.id = $2`, v.org, run, sources)

	for _, query := range []string{"?offset=0&limit=500", "?offset=500&limit=500&status=failed,retrying&dataset_key=commits"} {
		started := time.Now()
		code, body := v.call("GET", "/api/v1/admin/sync-runs/"+run.String()+"/units"+query, "")
		elapsed := time.Since(started)
		t.Logf("100k units %s: status=%d body_bytes=%d elapsed=%s", query, code, len(body), elapsed)
		if code != http.StatusOK {
			t.Fatalf("%s: %d %.200s", query, code, body)
		}
		if elapsed > 5*time.Second {
			t.Errorf("%s: %s, over the 5 s budget (web read deadline is 20 s)", query, elapsed)
		}
		if len(body) > 2<<20 {
			t.Errorf("%s: body %d bytes, over 2 MiB", query, len(body))
		}
	}
}
