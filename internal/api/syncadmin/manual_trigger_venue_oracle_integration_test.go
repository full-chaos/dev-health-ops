//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	manualVenueKey = "venue-oracle-manual-trigger-key-32-bytes!"
	manualPinned   = "2026-09-24T12:34:56.123456+00:00"
	adminPath      = "/api/v1/admin/sync-configs/"
)

// manualCfg is one seeded configuration. A hand-off occurrence is named by its
// configuration and the pinned instant, so one configuration takes ONE trigger.
type manualCfg struct {
	id, integration uuid.UUID
	sources         []uuid.UUID
}

type manualIDs struct {
	orgA, orgB                                                uuid.UUID
	adminA, memberA, adminB, adminNoOrg                       uuid.UUID
	credOK, credInactive, credFailed, credFailedNoText, credB uuid.UUID
	cfg                                                       map[string]*manualCfg
}

func newManualIDs(names []string) manualIDs {
	v := manualIDs{cfg: map[string]*manualCfg{}}
	for _, target := range []*uuid.UUID{&v.orgA, &v.orgB, &v.adminA, &v.memberA, &v.adminB, &v.adminNoOrg, &v.credOK,
		&v.credInactive, &v.credFailed, &v.credFailedNoText, &v.credB} {
		*target = uuid.New()
	}
	for _, name := range names {
		c := &manualCfg{id: uuid.New(), integration: uuid.New()}
		for range 3 {
			c.sources = append(c.sources, uuid.New())
		}
		v.cfg[name] = c
	}
	return v
}

// manualSpec describes the configuration a request is made against.
type manualSpec struct {
	name        string
	org         string // "A" or "B"
	credential  string // "ok", "inactive", "failed", "failedNoText", or "" for none
	provider    string
	targets     string // JSON
	options     string // JSON
	active      bool
	managed     bool
	pinned      bool // a child pinned to its first source
	noIntegrate bool
}

func manualSpecs() []manualSpec {
	spec := func(name string) manualSpec {
		return manualSpec{name: name, org: "A", credential: "ok", provider: "github", targets: `["git"]`, options: `{}`, active: true, managed: true}
	}
	var out []manualSpec
	add := func(name string, adjust func(*manualSpec)) {
		s := spec(name)
		if adjust != nil {
			adjust(&s)
		}
		out = append(out, s)
	}
	for _, name := range []string{"t-parent", "t-parent-again", "b-legacy-dates", "b-selector", "b-selector-offset", "b-scoped", "b-empty-scope", "b-mixed-scope",
		"b-days-30", "b-days-31", "t-guard-memberA", "t-guard-noorg"} {
		add(name, nil)
	}
	add("t-full-resync", func(s *manualSpec) { s.options = `{"full_resync": true}` })
	add("t-full-resync-falsy", func(s *manualSpec) { s.options = `{"full_resync": 0}` })
	add("t-child-pinned", func(s *manualSpec) { s.pinned, s.managed, s.targets = true, false, `["git", "prs"]` })
	add("t-child-pinned-untargeted", func(s *manualSpec) { s.pinned, s.managed, s.targets = true, false, `[]` })
	add("t-paused", func(s *manualSpec) { s.active = false })
	add("b-paused", func(s *manualSpec) { s.active = false })
	add("t-no-integration", func(s *manualSpec) { s.noIntegrate = true; s.credential = "" })
	add("t-no-credential", func(s *manualSpec) { s.credential = "" })
	add("t-cred-inactive", func(s *manualSpec) { s.credential = "inactive" })
	add("t-cred-failed", func(s *manualSpec) { s.credential = "failed" })
	add("t-cred-failed-notext", func(s *manualSpec) { s.credential = "failedNoText" })
	add("b-cred-failed", func(s *manualSpec) { s.credential = "failed" })
	add("t-pagerduty-wrong-target", func(s *manualSpec) { s.provider, s.targets, s.credential = "pagerduty", `["git"]`, "" })
	add("t-pagerduty-no-credential", func(s *manualSpec) { s.provider, s.targets, s.credential = "pagerduty", `["operational"]`, "" })
	add("t-gated-incidents", func(s *manualSpec) { s.org, s.targets, s.credential = "B", `["incidents"]`, "credB" })
	add("b-gated-incidents", func(s *manualSpec) { s.org, s.targets, s.credential = "B", `["incidents"]`, "credB" })
	add("t-work-items-over-limit", func(s *manualSpec) { s.org, s.targets, s.credential = "B", `["work-items"]`, "credB" })
	add("t-work-items-under-limit", func(s *manualSpec) { s.targets = `["work-items"]` })
	add("b-limit-org-b", func(s *manualSpec) { s.org, s.credential = "B", "credB" })
	add("b-limit-org-b-ok", func(s *manualSpec) { s.org, s.credential = "B", "credB" })
	add("t-legacy-unmanaged", func(s *manualSpec) { s.managed = false })
	add("b-legacy-unmanaged", func(s *manualSpec) { s.managed = false })
	add("t-pending", nil)
	add("b-pending", nil)
	add("b-invalid-source", nil)
	add("t-other-org", func(s *manualSpec) { s.org, s.credential = "B", "credB" })
	return out
}

func manualRequestNames() []string {
	var names []string
	for _, s := range manualSpecs() {
		names = append(names, s.name)
	}
	return names
}

func seedManual(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, v manualIDs) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'manual-a', 'manual-a', '{}', 'enterprise', true, $3, $3), ($2, 'manual-b', 'manual-b', '{}', 'community', true, $3, $3)`, v.orgA, v.orgB, at)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{v.adminA, v.orgA, "manual-admin-a@example.com", "admin"}, {v.memberA, v.orgA, "manual-member-a@example.com", "member"},
		{v.adminB, v.orgB, "manual-admin-b@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'manual-noorg@example.com', true, true, false, 0, $2, $2)`, v.adminNoOrg, at)
	encoded, _ := json.Marshal(map[string]any{"token": "venue-token"})
	raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	var ciphertext string
	if err := json.Unmarshal(raw[0], &ciphertext); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []struct {
		id      uuid.UUID
		org     uuid.UUID
		active  bool
		success any
		text    any
	}{{v.credOK, v.orgA, true, nil, nil}, {v.credInactive, v.orgA, false, nil, nil}, {v.credFailed, v.orgA, true, false, "token rejected by the provider"},
		{v.credFailedNoText, v.orgA, true, false, ""}, {v.credB, v.orgB, true, nil, nil}} {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, last_test_success, last_test_error, created_at, updated_at)
VALUES ($1, $2, 'github', $3, $4, $5, '{}'::json, $6, $7, $8, $8)`, credential.id, credential.org.String(), "cred-"+credential.id.String()[:8], credential.active, ciphertext, credential.success, credential.text, at)
	}
	credentials := map[string]uuid.UUID{"ok": v.credOK, "inactive": v.credInactive, "failed": v.credFailed, "failedNoText": v.credFailedNoText, "credB": v.credB}
	for _, s := range manualSpecs() {
		c := v.cfg[s.name]
		org, orgID := "A", v.orgA
		if s.org == "B" {
			org, orgID = "B", v.orgB
		}
		_ = org
		var credential any
		if id, ok := credentials[s.credential]; ok {
			credential = id
		}
		if s.provider != "github" {
			// PagerDuty has no credential row here: the refusals under test are the
			// ones that need none.
			credential = nil
		}
		var integrationID any
		if !s.noIntegrate {
			integrationID = c.integration
			exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}'::json, true, $6, $6)`, c.integration, orgID.String(), s.provider, credential, "int-"+s.name, at)
		}
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, $6::json, $7, $8, $9, $10, $10)`, c.id, orgID.String(), "cfg-"+s.name, s.provider, s.targets, s.options, s.active, s.managed, integrationID, at)
		if s.noIntegrate {
			continue
		}
		for index, source := range c.sources {
			tag := c.id.String()
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, 'repository', $5, $6, $5, $7::json, $8, $9, $9)`, source, orgID.String(), c.integration, s.provider,
				fmt.Sprintf("acme/%s-%c", s.name, 'a'+index), fmt.Sprintf("%s-%c", s.name, 'a'+index),
				`{"planner_managed_sync_config_id": "`+tag+`"}`, index < 2, at)
		}
		// A stored coverage projection: a trigger that is handed to the scheduler
		// must invalidate it.
		exec(`INSERT INTO sync_coverage_projections (id, org_id, sync_config_id, history_lookback_days, projection_version, generated_at, payload)
VALUES ($1, $2, $3, 30, 1, $4, '{}'::json)`, uuid.New(), orgID.String(), c.id, at)
		if s.pinned {
			// The source is pinned once it exists (the foreign key).
			exec(`UPDATE sync_configurations SET source_id = $1 WHERE id = $2`, c.sources[0], c.id)
		}
		for _, dataset := range []string{"commits", "prs"} {
			exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, true, '{}'::json)`,
				uuid.New(), orgID.String(), c.integration, dataset)
		}
	}
	// The community org is over its work items limit on both ClickHouse planes.
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatalf("seed clickhouse %s: %v", database, err)
		}
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_items (org_id, work_item_id) SELECT '%s', toString(number) FROM numbers(1200)`, v.orgB.String())); err != nil {
			_ = conn.Close()
			t.Fatalf("seed work_items %s: %v", database, err)
		}
		_ = conn.Close()
	}
}

func manualPost(venue *venueoracle.Venue, name, config, path, token string, body *string) venueoracle.Request {
	headers := map[string]string{}
	if token != "" {
		headers["Authorization"] = "Bearer " + venue.Tokens[token]
	}
	if body != nil {
		headers["Content-Type"] = "application/json"
	}
	return venueoracle.Request{Name: name, Method: "POST", Path: adminPath + config + path, Headers: headers, Body: body}
}

func manualRequests(venue *venueoracle.Venue, v manualIDs) []venueoracle.Request {
	json := venueoracle.B64
	id := func(name string) string { return v.cfg[name].id.String() }
	a := "adminA"
	trigger := func(label, name, token string) venueoracle.Request {
		return manualPost(venue, label, id(name), "/trigger", token, nil)
	}
	backfill := func(label, name, token string, body string) venueoracle.Request {
		return manualPost(venue, label, id(name), "/backfill", token, json(body))
	}
	out := []venueoracle.Request{
		// The guard and the config lookup.
		trigger("trigger guard: no token", "t-guard-memberA", ""),
		trigger("trigger guard: member", "t-guard-memberA", "memberA"),
		trigger("trigger guard: no org", "t-guard-noorg", "adminNoOrg"),
		manualPost(venue, "trigger: not a uuid", "zzz", "/trigger", a, nil),
		manualPost(venue, "trigger: unknown config", uuid.NewString(), "/trigger", a, nil),
		trigger("trigger: another org's config", "t-other-org", a),
		backfill("backfill guard: no token", "b-days-30", "", `{"since": "2026-09-01", "before": "2026-09-30"}`),
		backfill("backfill guard: member", "b-days-30", "memberA", `{"since": "2026-09-01", "before": "2026-09-30"}`),
		manualPost(venue, "backfill: the body is validated before the config", uuid.NewString(), "/backfill", a, json(`{}`)),
		manualPost(venue, "backfill: no body", id("b-days-30"), "/backfill", a, nil),
		backfill("backfill: another org's config", "t-other-org", a, `{"since": "2026-09-01", "before": "2026-09-30"}`),
		manualPost(venue, "backfill: not a uuid", "zzz", "/backfill", a, json(`{"since": "2026-09-01", "before": "2026-09-30"}`)),
		// The refusals, each with its own configuration.
		trigger("trigger: paused", "t-paused", a),
		backfill("backfill: paused", "b-paused", a, `{"since": "2026-09-01", "before": "2026-09-30"}`),
		trigger("trigger: no linked integration", "t-no-integration", a),
		trigger("trigger: no credential is allowed", "t-no-credential", a),
		trigger("trigger: credential inactive", "t-cred-inactive", a),
		trigger("trigger: credential failed its last test", "t-cred-failed", a),
		trigger("trigger: credential failed without text", "t-cred-failed-notext", a),
		backfill("backfill: credential failed its last test", "b-cred-failed", a, `{"since": "2026-09-01", "before": "2026-09-30"}`),
		trigger("trigger: pagerduty needs the operational target", "t-pagerduty-wrong-target", a),
		trigger("trigger: pagerduty needs a credential", "t-pagerduty-no-credential", a),
		manualPost(venue, "clock: trigger, gated target (feature on for this org)", id("t-gated-incidents"), "/trigger", "adminB", nil),
		manualPost(venue, "clock: backfill, gated target (feature on for this org)", id("b-gated-incidents"), "/backfill", "adminB", json(`{"since": "2026-09-01", "before": "2026-09-30"}`)),
		manualPost(venue, "trigger: work items over the tier limit", id("t-work-items-over-limit"), "/trigger", "adminB", nil),
		trigger("trigger: work items under the limit", "t-work-items-under-limit", a),
		manualPost(venue, "backfill: over the community backfill_days limit", id("b-limit-org-b"), "/backfill", "adminB", json(`{"since": "2026-01-01", "before": "2026-09-30"}`)),
		backfill("clock: backfill, 31 days (enterprise has no limit)", "b-days-31", a, `{"since": "2026-08-01", "before": "2026-09-01"}`),
		// The 422s.
		backfill("backfill 422: selector mixed with flat fields", "b-days-30", a, `{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z"}, "since": "2026-09-01"}`),
		backfill("backfill 422: naive selector datetime", "b-days-30", a, `{"selector": {"since": "2026-09-01T00:00:00", "before": "2026-09-02T00:00:00Z"}}`),
		backfill("backfill 422: selector before not after since", "b-days-30", a, `{"selector": {"since": "2026-09-02T00:00:00Z", "before": "2026-09-01T00:00:00Z"}}`),
		backfill("backfill 422: flat dates missing one", "b-days-30", a, `{"since": "2026-09-01"}`),
		backfill("backfill 422: a bad date", "b-days-30", a, `{"since": "2026-13-45", "before": "2026-09-30"}`),
		// Runs (each on its own configuration; the scheduler plans them on both planes).
		trigger("clock: trigger, planner-managed parent", "t-parent", a),
		trigger("clock: trigger, full resync option", "t-full-resync", a),
		trigger("clock: trigger, falsy full resync option", "t-full-resync-falsy", a),
		trigger("clock: trigger, child pinned to one source", "t-child-pinned", a),
		trigger("clock: trigger, child pinned, no targets", "t-child-pinned-untargeted", a),
		backfill("clock: backfill, legacy dates", "b-legacy-dates", a, `{"since": "2026-09-01", "before": "2026-09-30"}`),
		backfill("clock: backfill, structured selector", "b-selector", a, `{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-30T00:00:00Z"}}`),
		backfill("clock: backfill, selector with an offset", "b-selector-offset", a, `{"selector": {"since": "2026-09-01T23:00:00-05:00", "before": "2026-09-30T01:30:00+05:30"}}`),
		backfill("clock: backfill, scoped selector", "b-scoped", a,
			`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-05T00:00:00Z", "source_ids": ["`+v.cfg["b-scoped"].sources[0].String()+`"], "dataset_keys": ["commits"]}}`),
		backfill("clock: backfill, explicit empty scope", "b-empty-scope", a,
			`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-05T00:00:00Z", "source_ids": [], "dataset_keys": []}}`),
		backfill("clock: backfill, dataset scope only", "b-mixed-scope", a,
			`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-05T00:00:00Z", "dataset_keys": ["prs"]}}`),
		backfill("clock: backfill, one day", "b-days-30", a, `{"since": "2026-09-01", "before": "2026-09-01"}`),
		// Python does not validate a source id: the scheduler quarantines the occurrence,
		// and both routes answer 202 "failed" with its error code.
		backfill("clock: backfill, a source id that is not a uuid is quarantined", "b-invalid-source", a,
			`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-05T00:00:00Z", "source_ids": ["not-a-uuid"]}}`),
	}
	return out
}

// pendingRequests are answered while the scheduler is stopped: both routes wait
// their bound and answer 202 "pending" with the occurrence's id, the same on
// both planes.
func pendingRequests(venue *venueoracle.Venue, v manualIDs) []venueoracle.Request {
	return []venueoracle.Request{
		manualPost(venue, "pending: trigger, the scheduler is stopped", v.cfg["t-pending"].id.String(), "/trigger", "adminA", nil),
		manualPost(venue, "pending: backfill, the scheduler is stopped", v.cfg["b-pending"].id.String(), "/backfill", "adminA",
			venueoracle.B64(`{"since": "2026-09-01", "before": "2026-09-30"}`)),
	}
}

// divergingRequests are answered differently on purpose: Python plans a
// configuration that is neither planner-managed nor pinned in process; the
// hand-off refuses it, with nothing written.
func divergingRequests(venue *venueoracle.Venue, v manualIDs) []venueoracle.Request {
	return []venueoracle.Request{
		manualPost(venue, "diverging: trigger, legacy configuration", v.cfg["t-legacy-unmanaged"].id.String(), "/trigger", "adminA", nil),
		manualPost(venue, "diverging: backfill, legacy configuration", v.cfg["b-legacy-unmanaged"].id.String(), "/backfill", "adminA",
			venueoracle.B64(`{"since": "2026-09-01", "before": "2026-09-30"}`)),
	}
}

var manualUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// TestManualTriggerVenueOracle sends POST /sync-configs/{id}/trigger and
// /backfill to the real Python api and the Go api, each over its own copy of one
// seeded database and each with the real Go scheduler (the materializer and the
// occurrence reconciler) running over its copy, on one pinned clock. Python's
// routes hand the run to the scheduler too (create_sync_execution_trigger's Go
// path), so both planes are compared on the same answers and the same
// scheduled_sync_occurrences, sync_manual_triggers, sync_runs, sync_run_units
// and backfill_jobs rows.
func TestManualTriggerVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-manual-trigger!"
	v := newManualIDs(manualRequestNames())
	t.Setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "6")
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + manualVenueKey,
			"VENUE_PINNED_NOW=" + manualPinned,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.sync.execution_trigger,dev_health_ops.sync.planner,dev_health_ops.sync.watermarks,dev_health_ops.models.integrations",
			"SYNC_MANUAL_TRIGGER_AWAIT_SECONDS=6",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedManual(t, ctx, admin, venue, v)
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":     token(v.adminA, v.orgA, "manual-admin-a@example.com", "admin"),
				"memberA":    token(v.memberA, v.orgA, "manual-member-a@example.com", "member"),
				"adminB":     token(v.adminB, v.orgB, "manual-admin-b@example.com", "admin"),
				"adminNoOrg": {"user_id": v.adminNoOrg.String(), "email": "manual-noorg@example.com", "org_id": "", "role": "admin"},
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, manualPinned)
	if err != nil {
		t.Fatal(err)
	}
	base := startGoServerWith(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) { deps.Now = func() time.Time { return pinned } })

	// One scheduler per plane's database, as its superuser.
	var paused atomic.Bool
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(manualVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	var goScheduler *pgxpool.Pool
	for _, database := range []string{venue.SourceDB, venue.GoDB} {
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, database))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		if database == venue.GoDB {
			goScheduler = pool
		}
		materializer, err := schedsync.NewNativeMaterializer(pool)
		if err != nil {
			t.Fatal(err)
		}
		materializer.WithCredentialFingerprint(decryptor)
		reconciler, err := schedsync.NewOccurrenceReconciler(pool, materializer)
		if err != nil {
			t.Fatal(err)
		}
		loopCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for loopCtx.Err() == nil {
				if !paused.Load() {
					if _, err := reconciler.Reconcile(loopCtx, pinned.Add(time.Minute), 50); err != nil && loopCtx.Err() == nil {
						t.Logf("reconcile: %v", err)
					}
				}
				select {
				case <-loopCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
		t.Cleanup(func() { stop(); <-done })
	}

	requests := manualRequests(venue, v)
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			if !strings.HasPrefix(request.Name, "clock: ") {
				return body
			}
			seen := map[string]int{}
			return manualUUID.ReplaceAllStringFunc(body, func(id string) string {
				if _, ok := seen[id]; !ok {
					seen[id] = len(seen) + 1
				}
				return fmt.Sprintf("<uuid#%d>", seen[id])
			})
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	// The scheduler stopped: the wait ends with 202 "pending" and the occurrence id.
	paused.Store(true)
	pending := pendingRequests(venue, v)
	pendingReceipt := venueoracle.Diff(t, base, pending, venue.ServePython(t, pending), venueoracle.DiffOptions{})
	t.Logf("pending receipt:\n%s", pendingReceipt)
	paused.Store(false)

	// Ruled divergences, both planes asked: Python plans the legacy configuration
	// in process (202), the hand-off refuses it (409) and writes nothing.
	diverging := divergingRequests(venue, v)
	pythonDiverging := venue.ServePython(t, diverging)
	for index, request := range diverging {
		goResponse := venueoracle.Do(t, base, request)
		if pythonDiverging[index].Status != 202 {
			t.Errorf("%s: python %d %s (the divergence assumes Python plans it)", request.Name, pythonDiverging[index].Status, pythonDiverging[index].Body)
		}
		if goResponse.Status != 409 || !strings.Contains(goResponse.Body, "not one the scheduler can run") {
			t.Errorf("%s: go %d %s", request.Name, goResponse.Status, goResponse.Body)
		}
	}
	var refusedOccurrences int
	if err := goScheduler.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences o JOIN sync_configurations c ON c.id = o.sync_config_id
WHERE c.id = ANY($1::uuid[])`, []uuid.UUID{v.cfg["t-legacy-unmanaged"].id, v.cfg["b-legacy-unmanaged"].id}).Scan(&refusedOccurrences); err != nil || refusedOccurrences != 0 {
		t.Errorf("a refused hand-off wrote %d occurrences (err %v)", refusedOccurrences, err)
	}

	// The paused runs are planned once the scheduler runs again, on both planes.
	for _, database := range []string{venue.SourceDB, venue.GoDB} {
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, database))
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			var open int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences WHERE reconcile_status = 'pending'`).Scan(&open); err != nil {
				t.Fatal(err)
			}
			if open == 0 || time.Now().After(deadline) {
				if open != 0 {
					t.Errorf("%s: %d occurrences still pending after the scheduler resumed", database, open)
				}
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		pool.Close()
	}
	compareManualRows(t, ctx, venue)
}

func compareManualRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue) {
	t.Helper()
	compare := []struct {
		name, query   string
		minSeparators int
	}{
		{"scheduled_sync_occurrences", `SELECT c.name, o.occurrence_id, o.identity_version, o.org_id, o.scheduled_for, o.reconcile_status, o.reconcile_error_code
FROM scheduled_sync_occurrences o JOIN sync_configurations c ON c.id = o.sync_config_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name`, 12},
		{"sync_manual_triggers", `SELECT c.name, m.mode, m.since, m.before, m.source_ids::text, m.dataset_keys::text, m.triggered_by
FROM sync_manual_triggers m JOIN scheduled_sync_occurrences o USING (occurrence_id) JOIN sync_configurations c ON c.id = o.sync_config_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name`, 12},
		{"sync_runs", `SELECT c.name, r.mode, r.status, r.total_units, r.completed_units, r.failed_units, r.triggered_by, r.result::text, r.error
FROM sync_runs r JOIN sync_configurations c ON c.integration_id = r.integration_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name, r.mode`, 12},
		{"sync_run_units", `SELECT c.name, u.dataset_key, u.mode, u.since_at, u.before_at, u.status, u.provider
FROM sync_run_units u JOIN sync_configurations c ON c.integration_id = u.integration_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name, u.source_id::text, u.dataset_key, u.since_at, u.before_at`, 12},
		{"backfill_jobs", `SELECT c.name, b.status, b.since_date, b.before_date, b.total_chunks, b.completed_chunks, b.failed_chunks, b.celery_task_id
FROM backfill_jobs b JOIN sync_configurations c ON c.id = b.sync_config_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name`, 5},
		{"sync_coverage_projections", `SELECT c.name, (p.invalidated_at IS NOT NULL) FROM sync_coverage_projections p JOIN sync_configurations c ON c.id = p.sync_config_id
WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name`, 30},
		{"scheduled_jobs", `SELECT c.name, j.job_type, j.status, j.org_id FROM scheduled_jobs j JOIN sync_configurations c ON c.id = j.sync_config_id WHERE c.name NOT LIKE '%legacy-unmanaged' ORDER BY c.name`, 10},
	}
	for _, table := range compare {
		pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), table.query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table.query)
		if pythonRows != goRows {
			t.Errorf("%s differ\n python:\n%.5000s\n go:\n%.5000s", table.name, pythonRows, goRows)
		}
		if strings.Count(pythonRows, " | ") < table.minSeparators {
			t.Errorf("%s: too few rows compared:\n%s", table.name, pythonRows)
		}
	}
}
