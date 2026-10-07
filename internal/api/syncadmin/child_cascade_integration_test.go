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

// A save of a parent configuration cascades its list to every child
// configuration (cascadeToChildren): the list the save stored on the parent,
// never the submitted list. A child's stored list is its own selection and
// every reader gates all of it: the tests below read every reader of the
// child's list before and after a parent save.

type cascadeVenue struct {
	t            *testing.T
	pool         *pgxpool.Pool
	org          string
	server       *httptest.Server
	materializer schedsync.Materializer
	sequence     int
	hour         int
}

type cascadeSeeded struct {
	integration, config, job uuid.UUID
	source                   *string
}

type cascadeResult struct {
	SyncNow, Backfill, PutRepositories, PatchNoList int
	Scheduled                                       schedsync.HandoffOutcome
	Plan                                            string
}

func startCascadeVenue(t *testing.T, incidentFeature bool) *cascadeVenue {
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
	pgseed.OrgOverride(ctx, t, pool, org, feature, incidentFeature)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Pool: pool, Guard: testGuard(t), Logger: quiet()}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &cascadeVenue{t: t, pool: pool, org: org, server: server, materializer: materializer}
}

func (v *cascadeVenue) call(method, path, body string) (int, string) {
	v.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	request, err := http.NewRequest(method, v.server.URL+path, reader)
	if err != nil {
		v.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+sign(v.t, "admin", v.org))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		v.t.Fatal(err)
	}
	defer response.Body.Close()
	text, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(text)
}

func (v *cascadeVenue) exec(sql string, args ...any) {
	v.t.Helper()
	if _, err := v.pool.Exec(context.Background(), sql, args...); err != nil {
		v.t.Fatalf("seed: %v", err)
	}
}

func (v *cascadeVenue) seed(provider, stored string, rows []string) cascadeSeeded {
	v.t.Helper()
	v.sequence++
	one := cascadeSeeded{integration: uuid.New(), config: uuid.New(), job: uuid.New()}
	v.exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, true, now(), now())`, one.integration, v.org, provider, fmt.Sprintf("integration %d", v.sequence))
	for _, key := range rows {
		v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, true, '{}'::json)`,
			uuid.New(), v.org, one.integration, key)
	}
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, true, $6, NULL, now(), now())`,
		one.config, v.org, fmt.Sprintf("config %d", v.sequence), provider, stored, one.integration)
	v.exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, one.job, v.org, one.config)
	return one
}

// child adds a config pinned to one source of the parent's integration, with
// parent_id set (the legacy child the save cascades to).
func (v *cascadeVenue) child(parent cascadeSeeded, provider, stored string) cascadeSeeded {
	v.t.Helper()
	v.sequence++
	source := uuid.New()
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, 'repo', $5, $5, $5, '{}'::json, true, now(), now())`, source, v.org, parent.integration, provider, fmt.Sprintf("acme/child-%d", v.sequence))
	one := cascadeSeeded{integration: parent.integration, config: uuid.New(), job: uuid.New()}
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, parent_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, false, $6, $7, $8, now(), now())`,
		one.config, v.org, fmt.Sprintf("child %d", v.sequence), provider, stored, parent.integration, source, parent.config)
	v.exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, one.job, v.org, one.config)
	text := source.String()
	one.source = &text
	return one
}

func (v *cascadeVenue) stored(config uuid.UUID) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(context.Background(), `SELECT sync_targets::text FROM sync_configurations WHERE id = $1`, config).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

// storedList is the config's stored list as compact JSON, so that two lists
// with the same items compare equal whatever spacing the writer used.
func (v *cascadeVenue) storedList(config uuid.UUID) string {
	v.t.Helper()
	var items []string
	if err := json.Unmarshal([]byte(v.stored(config)), &items); err != nil {
		v.t.Fatalf("stored list of %s: %v", config, err)
	}
	compact, err := json.Marshal(items)
	if err != nil {
		v.t.Fatal(err)
	}
	return string(compact)
}

func (v *cascadeVenue) rows(integration uuid.UUID) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(context.Background(), `SELECT coalesce(string_agg(dataset_key || '=' || is_enabled::text, ',' ORDER BY dataset_key), '')
FROM integration_datasets WHERE integration_id = $1`, integration).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

func (v *cascadeVenue) shown(config uuid.UUID) string {
	v.t.Helper()
	status, body := v.call("GET", "/api/v1/admin/sync-configs/"+config.String(), "")
	if status != 200 {
		v.t.Fatalf("GET: %d %s", status, body)
	}
	var shown struct {
		SyncTargets []string `json:"sync_targets"`
	}
	if err := json.Unmarshal([]byte(body), &shown); err != nil {
		v.t.Fatal(err)
	}
	echo, _ := json.Marshal(shown.SyncTargets)
	return string(echo)
}

func (v *cascadeVenue) plan(label string, one cascadeSeeded, occurrenceID string, when time.Time) string {
	ctx := context.Background()
	tx, err := v.pool.Begin(ctx)
	if err != nil {
		v.t.Fatal(err)
	}
	_, err = v.materializer.Materialize(ctx, tx, schedsync.PendingOccurrence{
		ID: occurrenceID, IdentityVersion: schedsync.OccurrenceIdentityVersion,
		OrgID: v.org, ConfigID: one.config.String(), JobID: one.job.String(), ScheduledFor: when,
		ConfigActive: true, ConfigPlannerManaged: one.source == nil, ConfigSourceID: one.source, JobStatus: 0, JobType: "sync",
	})
	_ = tx.Rollback(ctx)
	switch {
	case err == nil:
		return "planned"
	case errors.Is(err, schedsync.ErrOccurrenceIneligible):
		return "ineligible"
	default:
		return "error: " + err.Error()
	}
}

func (v *cascadeVenue) read(label string, one cascadeSeeded) cascadeResult {
	v.t.Helper()
	ctx := context.Background()
	v.hour++
	when := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC).Add(time.Duration(v.hour) * time.Hour)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s %s %d", label, one.config, v.hour)))
	occurrenceID := "sha256:" + hex.EncodeToString(digest[:])
	path := "/api/v1/admin/sync-configs/" + one.config.String()
	var result cascadeResult
	result.SyncNow, _ = v.call("POST", path+"/trigger", "")
	result.Backfill, _ = v.call("POST", path+"/backfill", `{"since":"2026-09-01","before":"2026-09-03"}`)
	result.PutRepositories, _ = v.call("PUT", path+"/repositories", `{"owner":"acme","repos":[]}`)
	result.PatchNoList, _ = v.call("PATCH", path, `{"initial_sync_depth":30}`)
	tx, err := v.pool.Begin(ctx)
	if err != nil {
		v.t.Fatal(err)
	}
	result.Scheduled, err = schedsync.NewOccurrenceCoordinator().Handoff(ctx, tx, schedsync.Occurrence{
		ID: occurrenceID, IdentityVersion: schedsync.OccurrenceIdentityVersion,
		ConfigID: one.config.String(), OrgID: v.org, JobID: one.job.String(), ScheduledFor: when, ObservedAt: when,
	})
	_ = tx.Rollback(ctx)
	if err != nil {
		v.t.Fatalf("%s: handoff: %v", label, err)
	}
	result.Plan = v.plan(label, one, occurrenceID, when)
	v.t.Logf("STATE %-62s stored=%s rows=[%s] -> %+v", label, v.stored(one.config), v.rows(one.integration), result)
	return result
}

// TestAParentSaveOfTheShownListLeavesEveryReaderOfAChildAsItWas: the org has
// no canonical-incident feature and the incidents row of the integration is
// on, so the list the parent shows names the gated target. A save of that
// list must not write the target to the child: the child's stored list and
// what every reader of it answers stay as they were.
func TestAParentSaveOfTheShownListLeavesEveryReaderOfAChildAsItWas(t *testing.T) {
	v := startCascadeVenue(t, false)
	for _, testCase := range []struct {
		provider, stored, gated string
		rows                    []string
	}{
		{"jira", `["work-items"]`, "operational", []string{"work-items", "incidents"}},
		{"gitlab", `["git"]`, "incidents", []string{"commits", "incidents"}},
	} {
		parent := v.seed(testCase.provider, testCase.stored, testCase.rows)
		child := v.child(parent, testCase.provider, testCase.stored)
		before := v.read(testCase.provider+" child, before the parent save", child)
		shown := v.shown(parent.config)
		if !strings.Contains(shown, `"`+testCase.gated+`"`) {
			t.Fatalf("harness: %s: the parent shows %s, without the gated target %q: nothing is measured", testCase.provider, shown, testCase.gated)
		}
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+shown+`}`)
		if status != http.StatusOK {
			t.Fatalf("%s: the parent save: %d %s", testCase.provider, status, body)
		}
		if got := v.storedList(child.config); got != testCase.stored {
			t.Errorf("%s: the child's stored list is %s after a parent save of the shown list %s, want %s", testCase.provider, got, shown, testCase.stored)
		}
		after := v.read(testCase.provider+" child, after the parent save of the shown list", child)
		if before.SyncNow == http.StatusForbidden || before.Backfill == http.StatusForbidden || before.PatchNoList != http.StatusOK ||
			before.Scheduled != schedsync.OccurrenceMinted || before.Plan != "planned" {
			t.Fatalf("harness: %s: the child is refused before the save: %+v", testCase.provider, before)
		}
		if before != after {
			t.Errorf("%s: the child answers %+v before the parent save and %+v after it", testCase.provider, before, after)
		}
	}
}

// TestAParentSaveGivesAChildOnlyWhatARequestAskedFor: the org has the
// canonical-incident feature, so no save is refused and only the cascade
// decides the child's list. Each case names the list the child must hold
// after the save.
func TestAParentSaveGivesAChildOnlyWhatARequestAskedFor(t *testing.T) {
	v := startCascadeVenue(t, true)
	const shownList = "<the list GET shows>"
	for _, testCase := range []struct {
		name, provider, parentStored string
		rows                         []string
		childStored, submitted       string
		wantChild                    string
	}{
		// A target the parent shows only because a row is on is not stored
		// and never reaches the child, with the feature on too.
		{"jira: a target that shows only because its row is on", "jira", `["work-items"]`, []string{"work-items", "incidents"},
			`["work-items"]`, shownList, `["work-items"]`},
		{"gitlab: a target that shows only because its row is on", "gitlab", `["git"]`, []string{"commits", "incidents"},
			`["git"]`, shownList, `["git"]`},
		// The user adds the gated target on the parent and no row shows it:
		// a request asked for it, so the child takes it.
		{"jira: the save adds the gated target", "jira", `["work-items"]`, []string{"work-items"},
			`["work-items"]`, `["work-items","operational"]`, `["work-items","operational"]`},
		{"gitlab: the save adds the gated target", "gitlab", `["git"]`, []string{"commits"},
			`["git"]`, `["git","incidents"]`, `["git","incidents"]`},
		// The child already holds the target: an earlier request asked for it.
		{"jira: the child already holds the gated target", "jira", `["work-items","operational"]`, []string{"work-items", "incidents"},
			`["work-items","operational"]`, shownList, `["work-items","operational"]`},
		{"gitlab: the child already holds the gated target", "gitlab", `["git","incidents"]`, []string{"commits", "incidents"},
			`["git","incidents"]`, shownList, `["git","incidents"]`},
		// The save drops a target: the child loses it.
		{"jira: the save drops the gated target", "jira", `["work-items","operational"]`, []string{"work-items", "incidents"},
			`["work-items","operational"]`, `["work-items"]`, `["work-items"]`},
		{"gitlab: the save drops the gated target", "gitlab", `["git","incidents"]`, []string{"commits", "incidents"},
			`["git","incidents"]`, `["git"]`, `["git"]`},
		// GitHub "incidents" and Linear "operational" are the target of no
		// dataset of the provider: no row can show them, the parent's list
		// holds them only because a request asked, and the child takes the
		// submitted list as it is.
		{"github: a gated target with no dataset, kept", "github", `["git","incidents"]`, []string{"commits"},
			`["git"]`, shownList, `["git","incidents"]`},
		{"github: a gated target with no dataset, added", "github", `["git"]`, []string{"commits"},
			`["git"]`, `["git","incidents"]`, `["git","incidents"]`},
		{"linear: a gated target with no dataset, kept", "linear", `["work-items","operational"]`, []string{"work-items"},
			`["work-items"]`, shownList, `["work-items","operational"]`},
		{"linear: a gated target with no dataset, added", "linear", `["work-items"]`, []string{"work-items"},
			`["work-items"]`, `["work-items","operational"]`, `["work-items","operational"]`},
		// A base list in the request has no meaning: the save adds the
		// target, its gate reads it (this org has the feature), the parent
		// stores it and the child takes it.
		{"github: a target with no dataset the base names too", "github", `["git"]`, []string{"commits"},
			`["git"]`, `["git","incidents"],"sync_targets_base":["git","incidents"]`, `["git","incidents"]`},
		// A child whose list is not the parent's takes the parent's stored list.
		{"gitlab: a child that is out of step with its parent", "gitlab", `["git"]`, []string{"commits", "incidents"},
			`["prs","incidents"]`, shownList, `["git"]`},
	} {
		parent := v.seed(testCase.provider, testCase.parentStored, testCase.rows)
		child := v.child(parent, testCase.provider, testCase.childStored)
		submitted := testCase.submitted
		if submitted == shownList {
			submitted = v.shown(parent.config)
		}
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+submitted+`}`)
		if status != http.StatusOK {
			t.Errorf("%s: the parent save of %s: %d %s", testCase.name, submitted, status, body)
			continue
		}
		if got := v.storedList(child.config); got != testCase.wantChild {
			t.Errorf("%s: parent save of %s (parent stored %s, rows [%s]): the child's stored list is %s, want %s",
				testCase.name, submitted, v.stored(parent.config), v.rows(parent.integration), got, testCase.wantChild)
		}
	}
}

// TestASaveNeverGivesAChildAGatedTargetItsGateDidNotRead: the org has no
// canonical-incident feature. A child never gets a gated target from a save
// of its parent: every reader of a child gates the child's whole list, so an
// item reaches a child only when the gate of the save read it. What a save
// adds is decided against the list the server shows; a base list in the
// request is client data with no meaning, so each save below adds a gated
// target, its gate reads it and the save is refused: 403, nothing written.
// The last case is the control: a save that adds a target with no gate is
// accepted in the same venue and the child takes it.
func TestASaveNeverGivesAChildAGatedTargetItsGateDidNotRead(t *testing.T) {
	v := startCascadeVenue(t, false)
	accepted := 0
	for _, testCase := range []struct {
		name, provider, stored, body string
		rows                         []string
		// wantChild is the child's list after an accepted save ("" for a
		// save that must be refused).
		wantChild string
	}{
		{"github: a gated target with no dataset, named by the base", "github", `["git"]`,
			`{"sync_targets":["git","incidents"],"sync_targets_base":["git","incidents"]}`, []string{"commits"}, ""},
		{"jira: the gated target of another provider, named by the base", "jira", `["work-items"]`,
			`{"sync_targets":["work-items","incidents"],"sync_targets_base":["work-items","incidents"]}`, []string{"work-items"}, ""},
		{"jira: its own gated target, row off, named by a stale base", "jira", `["work-items"]`,
			`{"sync_targets":["work-items","operational"],"sync_targets_base":["work-items","operational"]}`, []string{"work-items"}, ""},
		{"gitlab: its own gated target, row off, named by a stale base", "gitlab", `["git"]`,
			`{"sync_targets":["git","incidents"],"sync_targets_base":["git","incidents"]}`, []string{"commits"}, ""},
		{"gitlab: the gated target of another provider, named by the base", "gitlab", `["git"]`,
			`{"sync_targets":["git","operational"],"sync_targets_base":["git","operational"]}`, []string{"commits"}, ""},
		{"linear: a gated target with no dataset, named by the base", "linear", `["work-items"]`,
			`{"sync_targets":["work-items","operational"],"sync_targets_base":["work-items","operational"]}`, []string{"work-items"}, ""},
		{"linear: both gated targets, named by the base", "linear", `["work-items"]`,
			`{"sync_targets":["work-items","operational","incidents"],"sync_targets_base":["incidents","operational","work-items"]}`, []string{"work-items"}, ""},
		{"github: a gated target with no dataset, no base (the save adds it)", "github", `["git"]`,
			`{"sync_targets":["git","incidents"]}`, []string{"commits"}, ""},
		{"jira: its own gated target, no base (the save adds it)", "jira", `["work-items"]`,
			`{"sync_targets":["work-items","operational"]}`, []string{"work-items"}, ""},
		{"github: control, a target with no gate (the save adds it)", "github", `["git"]`,
			`{"sync_targets":["git","prs"],"sync_targets_base":["git","prs"]}`, []string{"commits"}, `["git","prs"]`},
	} {
		parent := v.seed(testCase.provider, testCase.stored, testCase.rows)
		child := v.child(parent, testCase.provider, testCase.stored)
		before := v.read(testCase.name+": child, before", child)
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), testCase.body)
		if testCase.wantChild != "" {
			if status != http.StatusOK || v.storedList(child.config) != testCase.wantChild {
				t.Errorf("%s: the save answers %d %.160s and the child's stored list is %s, want 200 and %s",
					testCase.name, status, body, v.storedList(child.config), testCase.wantChild)
			} else {
				accepted++
			}
			continue
		}
		if status != http.StatusForbidden || !strings.Contains(body, "feature_disabled") {
			t.Errorf("%s: the save answers %d %.160s, want 403 feature_disabled", testCase.name, status, body)
		}
		for label, config := range map[string]uuid.UUID{"parent": parent.config, "child": child.config} {
			if got := v.storedList(config); got != testCase.stored {
				t.Errorf("%s: PATCH %s -> %d: the %s's stored list is %s, want %s", testCase.name, testCase.body, status, label, got, testCase.stored)
			}
		}
		after := v.read(testCase.name+": child, after", child)
		if before.SyncNow == http.StatusForbidden || before.Scheduled != schedsync.OccurrenceMinted {
			t.Fatalf("harness: %s: the child is refused before the save: %+v", testCase.name, before)
		}
		if before != after {
			t.Errorf("%s: the child answers %+v before the save and %+v after it", testCase.name, before, after)
		}
	}
	if accepted == 0 {
		t.Fatal("harness: the control save was not accepted: no cascade ran in this venue, nothing is measured")
	}
}

// TestAParentWhoseRowsDoNotOwnItsSelectionGivesAChildItsWholeList: a parent
// with no integration has no dataset rows, its list is its selection and the
// gate of its save reads all of it. Its children take the submitted list as
// it is.
func TestAParentWhoseRowsDoNotOwnItsSelectionGivesAChildItsWholeList(t *testing.T) {
	v := startCascadeVenue(t, true)
	for _, testCase := range []struct{ provider, stored, submitted string }{
		{"jira", `["work-items"]`, `["operational","work-items"]`},
		{"gitlab", `["git"]`, `["git","incidents","prs"]`},
		{"github", `["git"]`, `["incidents","git"]`},
		{"linear", `["work-items"]`, `["work-items","operational"]`},
	} {
		parent, child := uuid.New(), uuid.New()
		for _, row := range []struct {
			id     uuid.UUID
			parent *uuid.UUID
		}{{parent, nil}, {child, &parent}} {
			v.sequence++
			v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, parent_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, false, $6, now(), now())`,
				row.id, v.org, fmt.Sprintf("no integration %d", v.sequence), testCase.provider, testCase.stored, row.parent)
		}
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.String(), `{"sync_targets":`+testCase.submitted+`}`)
		if status != http.StatusOK {
			t.Errorf("%s: the save of %s: %d %.160s", testCase.provider, testCase.submitted, status, body)
			continue
		}
		for label, config := range map[string]uuid.UUID{"parent": parent, "child": child} {
			if got := v.storedList(config); got != testCase.submitted {
				t.Errorf("%s: the %s's stored list is %s after a save of %s, want the submitted list", testCase.provider, label, got, testCase.submitted)
			}
		}
	}
}

// TestASaveOfTheShownListKeepsAGatedRowOn pins a known difference from the
// save that rebuilt the rows from the submitted list. State: the
// incidents row is on, the org has no canonical-incident feature, the stored
// list does not name the gated target. The plan-time gate reads the rows, so
// the whole configuration is not planned. A save of the shown list keeps the
// row (the rows own the selection), so the plan stays ineligible until the
// row goes off; the pre-gates accept the configuration before and after.
// That one gated row stops the whole configuration is tracked in CHAOS-8837.
func TestASaveOfTheShownListKeepsAGatedRowOn(t *testing.T) {
	v := startCascadeVenue(t, false)
	for _, testCase := range []struct {
		provider, stored string
		rows             []string
	}{
		{"jira", `["work-items"]`, []string{"work-items", "incidents"}},
		{"gitlab", `["git"]`, []string{"commits", "incidents"}},
	} {
		parent := v.seed(testCase.provider, testCase.stored, testCase.rows)
		before := v.read(testCase.provider+" whole integration, before the save", parent)
		shown := v.shown(parent.config)
		if status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+shown+`}`); status != http.StatusOK {
			t.Fatalf("%s: the save of the shown list %s: %d %s", testCase.provider, shown, status, body)
		}
		if got := v.stored(parent.config); got != testCase.stored {
			t.Errorf("%s: the stored list after a save of the shown list %s is %s, want it as it was %s", testCase.provider, shown, got, testCase.stored)
		}
		if rows := v.rows(parent.integration); !strings.Contains(rows, "incidents=true") {
			t.Errorf("%s: the rows after a save of the shown list %s are [%s]: the save switched the incidents row off", testCase.provider, shown, rows)
		}
		after := v.read(testCase.provider+" whole integration, after the save of the shown list", parent)
		for label, result := range map[string]cascadeResult{"before": before, "after": after} {
			if result.SyncNow != http.StatusAccepted || result.Backfill != http.StatusAccepted || result.Scheduled != schedsync.OccurrenceMinted {
				t.Errorf("%s, %s the save: a pre-gate refuses the configuration: %+v", testCase.provider, label, result)
			}
			if result.Plan != "ineligible" {
				t.Errorf("%s, %s the save: the plan is %q, want ineligible (the incidents row is on and the org has no feature)", testCase.provider, label, result.Plan)
			}
		}
	}
}

// TestAGatedTargetOfAConfigurationWithNoIntegrationIsStillRefused: a
// configuration with no integration has no dataset rows, so its stored list
// is its selection. A gated target in it
// refuses Sync now and the scheduled run when the org has no feature.
func TestAGatedTargetOfAConfigurationWithNoIntegrationIsStillRefused(t *testing.T) {
	v := startCascadeVenue(t, false)
	for _, testCase := range []struct{ provider, stored string }{
		{"jira", `["work-items","operational"]`},
		{"gitlab", `["git","incidents"]`},
	} {
		v.sequence++
		one := cascadeSeeded{config: uuid.New(), job: uuid.New()}
		v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, false, now(), now())`,
			one.config, v.org, fmt.Sprintf("no integration %d", v.sequence), testCase.provider, testCase.stored)
		v.exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, one.job, v.org, one.config)
		path := "/api/v1/admin/sync-configs/" + one.config.String()
		if status, body := v.call("POST", path+"/trigger", ""); status != http.StatusForbidden {
			t.Errorf("%s with no integration, stored %s: Sync now answers %d %.120s, want 403", testCase.provider, testCase.stored, status, body)
		}
		if status, body := v.call("PATCH", path, `{"initial_sync_depth":30}`); status != http.StatusForbidden {
			t.Errorf("%s with no integration, stored %s: a save with no list answers %d %.120s, want 403", testCase.provider, testCase.stored, status, body)
		}
		when := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC).Add(time.Duration(v.sequence) * time.Hour)
		digest := sha256.Sum256([]byte(one.config.String()))
		tx, err := v.pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := schedsync.NewOccurrenceCoordinator().Handoff(context.Background(), tx, schedsync.Occurrence{
			ID: "sha256:" + hex.EncodeToString(digest[:]), IdentityVersion: schedsync.OccurrenceIdentityVersion,
			ConfigID: one.config.String(), OrgID: v.org, JobID: one.job.String(), ScheduledFor: when, ObservedAt: when,
		})
		_ = tx.Rollback(context.Background())
		if err != nil {
			t.Fatalf("%s: handoff: %v", testCase.provider, err)
		}
		if outcome != schedsync.OccurrenceRefusedFeatureDisabled {
			t.Errorf("%s with no integration, stored %s: the scheduled run is %v, want refused_feature_disabled", testCase.provider, testCase.stored, outcome)
		}
	}
}
