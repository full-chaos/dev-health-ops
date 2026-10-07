//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// gateReaderResult is what every reader of the stored list that runs the
// canonical-incident gate answers for one config: the three routes, the
// scheduler's pre-mint gate and the scheduler's plan.
type gateReaderResult struct {
	syncNow, backfill, putRepositories int
	scheduled                          schedsync.HandoffOutcome
	plan                               string
}

// TestAMirroredTargetNeverRefusesAReaderOfTheStoredList holds every reader of
// sync_configurations.sync_targets that runs the canonical-incident gate to
// one rule: a target a config shows only because a dataset row is on refuses
// nothing, because a save never stores it. The org does NOT have the
// canonical-incident feature.
//
// Each provider gets two configs with the same rows and the same stored list.
// One is saved with the list GET shows (for Jira and GitLab that list names
// the gated target, because the incidents row is on); the twin is never
// saved. The stored lists of the two must be equal after the save, and the
// two must answer the same at every step: before the save, after it, and
// after the incidents row is switched off.
//
// GitHub and Linear have no gated target with a dataset. GitHub's "incidents"
// is in a list only when a request put it there, so it still refuses, saved
// or not; Linear supports no gated target at all.
func TestAMirroredTargetNeverRefusesAReaderOfTheStoredList(t *testing.T) {
	ctx := context.Background()
	pool, org, call, exec := startGateReaderVenue(t)
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	type seeded struct{ integration, config, job uuid.UUID }
	sequence := 0
	seed := func(provider, stored string, rows []string) seeded {
		t.Helper()
		sequence++
		one := seeded{uuid.New(), uuid.New(), uuid.New()}
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, true, now(), now())`, one.integration, org, provider, fmt.Sprintf("integration %d", sequence))
		for _, key := range rows {
			exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, true, '{}'::json)`,
				uuid.New(), org, one.integration, key)
		}
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, true, $6, NULL, now(), now())`,
			one.config, org, fmt.Sprintf("config %d", sequence), provider, stored, one.integration)
		exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, one.job, org, one.config)
		return one
	}
	storedList := func(config uuid.UUID) []string {
		t.Helper()
		var text string
		if err := pool.QueryRow(ctx, `SELECT sync_targets::text FROM sync_configurations WHERE id = $1`, config).Scan(&text); err != nil {
			t.Fatal(err)
		}
		var list []string
		if err := json.Unmarshal([]byte(text), &list); err != nil {
			t.Fatalf("stored list %s: %v", text, err)
		}
		return list
	}
	hour := 0
	// childSource is the source the config under read is pinned to (nil: a
	// whole-integration config).
	var childSource *string
	read := func(label string, one seeded) gateReaderResult {
		t.Helper()
		hour++
		when := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC).Add(time.Duration(hour) * time.Hour)
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s %s %d", label, one.config, hour)))
		occurrenceID := "sha256:" + hex.EncodeToString(digest[:])
		path := "/api/v1/admin/sync-configs/" + one.config.String()
		var result gateReaderResult
		var body string
		result.syncNow, body = call("POST", path+"/trigger", "")
		t.Logf("%s: sync now -> %d %.120s", label, result.syncNow, body)
		result.backfill, body = call("POST", path+"/backfill", `{"since":"2026-09-01","before":"2026-09-03"}`)
		t.Logf("%s: backfill -> %d %.120s", label, result.backfill, body)
		result.putRepositories, body = call("PUT", path+"/repositories", `{"owner":"acme","repos":[]}`)
		t.Logf("%s: put repositories -> %d %.120s", label, result.putRepositories, body)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result.scheduled, err = schedsync.NewOccurrenceCoordinator().Handoff(ctx, tx, schedsync.Occurrence{
			ID: occurrenceID, IdentityVersion: schedsync.OccurrenceIdentityVersion,
			ConfigID: one.config.String(), OrgID: org, JobID: one.job.String(), ScheduledFor: when, ObservedAt: when,
		})
		_ = tx.Rollback(ctx)
		if err != nil {
			t.Fatalf("%s: handoff: %v", label, err)
		}

		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = materializer.Materialize(ctx, tx, schedsync.PendingOccurrence{
			ID: occurrenceID, IdentityVersion: schedsync.OccurrenceIdentityVersion,
			OrgID: org, ConfigID: one.config.String(), JobID: one.job.String(), ScheduledFor: when,
			ConfigActive: true, ConfigPlannerManaged: childSource == nil, ConfigSourceID: childSource, JobStatus: 0, JobType: "sync",
		})
		_ = tx.Rollback(ctx)
		switch {
		case err == nil:
			result.plan = "planned"
		case errors.Is(err, schedsync.ErrOccurrenceIneligible):
			result.plan = "ineligible"
		default:
			t.Fatalf("%s: materialize: %v", label, err)
		}
		t.Logf("%s: scheduled -> %q, plan -> %s", label, result.scheduled, result.plan)
		return result
	}
	refused := gateReaderResult{http.StatusForbidden, http.StatusForbidden, http.StatusForbidden, schedsync.OccurrenceRefusedFeatureDisabled, "ineligible"}

	for _, testCase := range []struct {
		provider, stored string
		rows             []string
		// mirrored is the gated target the config shows because the incidents
		// row is on, and that a save must never store ("" when the provider
		// has none).
		mirrored string
		// explicit: the stored list names a gated target a request put
		// there, so every reader refuses and the save itself is refused.
		explicit bool
	}{
		{provider: "jira", stored: `["work-items"]`, rows: []string{"work-items", "incidents"}, mirrored: "operational"},
		{provider: "gitlab", stored: `["git"]`, rows: []string{"commits", "incidents"}, mirrored: "incidents"},
		{provider: "github", stored: `["git","incidents"]`, rows: []string{"commits"}, explicit: true},
		{provider: "linear", stored: `["work-items"]`, rows: []string{"work-items"}},
	} {
		saved := seed(testCase.provider, testCase.stored, testCase.rows)
		twin := seed(testCase.provider, testCase.stored, testCase.rows)
		compare := func(step string) {
			t.Helper()
			got := read(testCase.provider+" saved, "+step, saved)
			want := read(testCase.provider+" twin, "+step, twin)
			if got != want {
				t.Errorf("%s, %s: the saved config answers %+v, the config that was never saved answers %+v", testCase.provider, step, got, want)
			}
			// The twin is the reference: it must be in the state the case names.
			if testCase.explicit && want != refused {
				t.Errorf("%s, %s: a gated target a request put in the list answers %+v, want every reader to refuse: %+v", testCase.provider, step, want, refused)
			}
			if !testCase.explicit && (want.syncNow == http.StatusForbidden || want.backfill == http.StatusForbidden ||
				want.putRepositories == http.StatusForbidden || want.scheduled != schedsync.OccurrenceMinted) {
				t.Fatalf("harness: %s, %s: the config that was never saved is refused: %+v", testCase.provider, step, want)
			}
		}
		compare("before the save")

		path := "/api/v1/admin/sync-configs/" + saved.config.String()
		status, body := call("GET", path, "")
		if status != 200 {
			t.Fatalf("GET: %d %s", status, body)
		}
		var shown struct {
			SyncTargets []string `json:"sync_targets"`
		}
		if err := json.Unmarshal([]byte(body), &shown); err != nil {
			t.Fatal(err)
		}
		echo, _ := json.Marshal(shown.SyncTargets)
		status, body = call("PATCH", path, `{"sync_targets":`+string(echo)+`,"initial_sync_depth":30}`)
		stored := storedList(saved.config)
		t.Logf("%s: GET shows %s; PATCH of that list -> %d; stored list now %v", testCase.provider, echo, status, stored)
		wantSave := http.StatusOK
		if testCase.explicit {
			wantSave = http.StatusForbidden
		}
		if status != wantSave {
			t.Fatalf("%s: the save of the shown list: %d, want %d: %s", testCase.provider, status, wantSave, body)
		}
		// The state the test exists to reach: the saved list named the gated
		// target (the row is on) and the save did not store it.
		if testCase.mirrored != "" && !slices.Contains(shown.SyncTargets, testCase.mirrored) {
			t.Fatalf("harness: %s: the saved list %v does not name %q: nothing is measured", testCase.provider, shown.SyncTargets, testCase.mirrored)
		}
		if !slices.Equal(stored, storedList(twin.config)) {
			t.Errorf("%s: after a save of the shown list %v the stored list is %v, the list of the config that was never saved is %v: "+
				"the save stored an item no request asked for", testCase.provider, shown.SyncTargets, stored, storedList(twin.config))
		}
		compare("after a save of the shown list")

		if testCase.mirrored == "" {
			continue
		}
		// The incidents row goes off on a side path: the plan-time gate on
		// the rows no longer applies.
		for _, one := range []seeded{saved, twin} {
			exec(`UPDATE integration_datasets SET is_enabled = false WHERE integration_id = $1 AND dataset_key = 'incidents'`, one.integration)
		}
		got := read(testCase.provider+" saved, incidents row off", saved)
		want := read(testCase.provider+" twin, incidents row off", twin)
		if got != want {
			t.Errorf("%s, incidents row off: the saved config answers %+v, the config that was never saved answers %+v", testCase.provider, got, want)
		}
		if want.scheduled != schedsync.OccurrenceMinted || want.plan != "planned" {
			t.Fatalf("harness: %s, incidents row off: the config that was never saved is not planned: %+v", testCase.provider, want)
		}
	}

	// The stored list of a whole-integration config names a gated target
	// that has a dataset: a request asked for it (a save stores nothing
	// else), so every reader of the list refuses, whatever the state of the
	// incidents row.
	for _, testCase := range []struct {
		provider, stored string
		rows             []string
	}{
		{"jira", `["work-items","operational"]`, []string{"work-items", "incidents"}},
		{"gitlab", `["git","incidents"]`, []string{"commits", "incidents"}},
	} {
		one := seed(testCase.provider, testCase.stored, testCase.rows)
		if got := read(testCase.provider+" stored gated target, incidents row on", one); got != refused {
			t.Errorf("%s, stored list %s, incidents row on: %+v, want every reader to refuse: %+v", testCase.provider, testCase.stored, got, refused)
		}
		exec(`UPDATE integration_datasets SET is_enabled = false WHERE integration_id = $1 AND dataset_key = 'incidents'`, one.integration)
		if got := read(testCase.provider+" stored gated target, incidents row off", one); got != refused {
			t.Errorf("%s, stored list %s, incidents row off: %+v, want every reader to refuse: %+v", testCase.provider, testCase.stored, got, refused)
		}
	}

	// A child config (pinned to one source): the list is the selection a
	// request made. A gated target in it refuses every reader, though it has a dataset and
	// that dataset's row is off (so only the gate on the list can refuse).
	child := seed("gitlab", `["incidents"]`, []string{"commits"})
	source := uuid.New()
	exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'gitlab', 'repo', 'acme/child', 'acme/child', 'acme/child', '{}'::json, true, now(), now())`, source, org, child.integration)
	exec(`UPDATE sync_configurations SET source_id = $1, planner_managed = false WHERE id = $2`, source, child.config)
	sourceID := source.String()
	childSource = &sourceID
	if got := read("gitlab child, a gated target in its own list", child); got != refused {
		t.Errorf("gitlab child with a gated target in its list answers %+v, want every reader to refuse: %+v", got, refused)
	}
}

// startGateReaderVenue is a Postgres with the schema, one enterprise org
// that does NOT have the canonical-incident feature, and the sync admin
// routes on it: the pool, the org, an authenticated call and a seeding exec.
func startGateReaderVenue(t *testing.T) (*pgxpool.Pool, string, func(method, path, body string) (int, string), func(sql string, args ...any)) {
	t.Helper()
	t.Setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "0.3")
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	org := uuid.NewString()
	var feature string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, canonicalIncidentFeatureKey).Scan(&feature); err != nil {
		t.Fatal(err)
	}
	pgseed.Org(ctx, t, pool, org, "enterprise")
	pgseed.OrgOverride(ctx, t, pool, org, feature, false)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Pool: pool, Guard: testGuard(t), Logger: quiet()}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	call := func(method, path, body string) (int, string) {
		t.Helper()
		var reader io.Reader
		if body != "" {
			reader = bytes.NewReader([]byte(body))
		}
		request, err := http.NewRequest(method, server.URL+path, reader)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+sign(t, "admin", org))
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		text, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(text)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if status, body := call("GET", "/api/v1/admin/sync-targets", ""); status != 200 || strings.Contains(body, `"incidents"`) || strings.Contains(body, `"operational"`) {
		t.Fatalf("harness: the feature is not off: %d %s", status, body)
	}
	return pool, org, call, exec
}

// TestCreateAnswersTheListItStores: the create stores the submitted list and
// its response carries it, as every later read does. "blame" is a target the
// form does not offer: the create stores it and writes its row.
func TestCreateAnswersTheListItStores(t *testing.T) {
	ctx := context.Background()
	pool, _, call, _ := startGateReaderVenue(t)
	status, body := call("POST", "/api/v1/admin/sync-configs", `{"name":"created","provider":"github","sync_targets":["git","blame"],"sync_options":{"all_repos":true}}`)
	if status != http.StatusCreated {
		t.Fatalf("create: %d %s", status, body)
	}
	var created struct {
		ID          string   `json:"id"`
		SyncTargets []string `json:"sync_targets"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatal(err)
	}
	var stored string
	var blameOn bool
	if err := pool.QueryRow(ctx, `SELECT config.sync_targets::text,
       EXISTS (SELECT 1 FROM integration_datasets AS dataset
               WHERE dataset.integration_id = config.integration_id AND dataset.dataset_key = 'blame' AND dataset.is_enabled)
FROM sync_configurations AS config WHERE config.id = $1::uuid`, created.ID).Scan(&stored, &blameOn); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stored, `"blame"`) || !blameOn {
		t.Fatalf("the stored list is %s and the blame row is on = %v, want the submitted list stored and the row on", stored, blameOn)
	}
	if !slices.Equal(created.SyncTargets, []string{"git", "blame"}) {
		t.Errorf("the create answers %v, want the list it stored [git blame] (stored: %s)", created.SyncTargets, stored)
	}
	status, body = call("GET", "/api/v1/admin/sync-configs/"+created.ID, "")
	var read struct {
		SyncTargets []string `json:"sync_targets"`
	}
	if err := json.Unmarshal([]byte(body), &read); err != nil || status != http.StatusOK {
		t.Fatalf("GET: %d %s %v", status, body, err)
	}
	if !slices.Equal(read.SyncTargets, created.SyncTargets) {
		t.Errorf("the create answers %v and the read %v: they must agree", created.SyncTargets, read.SyncTargets)
	}
}
