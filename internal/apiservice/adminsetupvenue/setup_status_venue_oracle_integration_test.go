//go:build integration

package adminsetupvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// seeder writes one scenario's rows for one organization. Rows are dated
// relative to now() with explicit offsets, so the ordering setup.py reads
// (created_at) is never a tie.
type seeder struct {
	t     *testing.T
	ctx   context.Context
	admin *pgxpool.Pool
	org   string
}

func (s seeder) exec(sql string, args ...any) {
	s.t.Helper()
	if _, err := s.admin.Exec(s.ctx, sql, args...); err != nil {
		s.t.Fatalf("seed: %v\n%s", err, sql)
	}
}

func (s seeder) credential(provider string, active bool) {
	s.exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'x', '{}'::json, now(), now())`, uuid.New(), s.org, provider, "default-"+uuid.NewString()[:8], active)
}

func (s seeder) integration(provider string) uuid.UUID {
	id := uuid.New()
	s.exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, true, now(), now())`, id, s.org, provider, "integration-"+id.String()[:8])
	return id
}

func (s seeder) source(integration uuid.UUID, enabled bool) {
	s.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', $4, 'repo', $4, '{}'::json, $5, now(), now())`, uuid.New(), s.org, integration, "acme/"+uuid.NewString()[:8], enabled)
}

type configOptions struct {
	provider        string
	options         string
	active          bool
	parent          *uuid.UUID
	integration     *uuid.UUID
	lastSyncSuccess *bool
	lastSyncError   *string
	createdMinutes  int // minutes before now
}

func (s seeder) config(o configOptions) uuid.UUID {
	id := uuid.New()
	if o.options == "" {
		o.options = "{}"
	}
	s.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, parent_id, integration_id, last_sync_success, last_sync_error, created_at, updated_at)
VALUES ($1, $2, $3, $4, '["git"]'::json, $5::json, $6, false, $7, $8, $9, $10, now() - make_interval(mins => $11), now())`,
		id, s.org, "config-"+id.String()[:8], o.provider, o.options, o.active, o.parent, o.integration, o.lastSyncSuccess, o.lastSyncError, o.createdMinutes)
	return id
}

func (s seeder) job(config uuid.UUID) uuid.UUID {
	// One scheduled job per (org, sync config, type): a second call for the
	// same config returns the job the first one made.
	var existing uuid.UUID
	if err := s.admin.QueryRow(s.ctx, `SELECT id FROM scheduled_jobs WHERE sync_config_id = $1`, config).Scan(&existing); err == nil {
		return existing
	}
	id := uuid.New()
	s.exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status, is_running, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, $3, 'sync', 'github', '0 * * * *', 'UTC', '{}'::json, $4, 0, false, 0, 0, now(), now())`, id, s.org, "job-"+id.String()[:8], config)
	return id
}

func (s seeder) run(job uuid.UUID, status int, result string, runError *string, minutesAgo int) {
	if result == "" {
		result = "{}"
	}
	s.exec(`INSERT INTO job_runs (id, job_id, status, result, error, triggered_by, created_at)
VALUES ($1, $2, $3, $4::json, $5, 'venue', now() - make_interval(mins => $6))`, uuid.New(), job, status, result, runError, minutesAgo)
}

func text(value string) *string { return &value }
func flag(value bool) *bool     { return &value }

type scenario struct {
	name string
	seed func(s seeder)
}

const (
	pending   = 0
	running   = 1
	success   = 2
	failed    = 3
	cancelled = 4
)

func scenarios() []scenario {
	// withConfig seeds a github integration and one active parent config.
	withConfig := func(provider, options string, body func(s seeder, config uuid.UUID, integration uuid.UUID)) func(seeder) {
		return func(s seeder) {
			s.credential(provider, true)
			integration := s.integration(provider)
			config := s.config(configOptions{provider: provider, options: options, active: true, integration: &integration, createdMinutes: 10})
			if body != nil {
				body(s, config, integration)
			}
		}
	}
	return []scenario{
		{"no integration", func(seeder) {}},
		{"github credential only", func(s seeder) { s.credential("github", true) }},
		{"jira credential only", func(s seeder) { s.credential("jira", true) }},
		{"inactive credential is ignored", func(s seeder) { s.credential("github", false) }},
		{"two providers sorted", func(s seeder) { s.credential("jira", true); s.credential("github", true) }},
		{"github config, no repositories, no runs", withConfig("github", "", nil)},
		{"github config, all_repos true", withConfig("github", `{"all_repos": true}`, nil)},
		{"github config, all_repos falsy string", withConfig("github", `{"all_repos": ""}`, nil)},
		{"github config, selected repositories", withConfig("github", "", func(s seeder, _ uuid.UUID, integration uuid.UUID) {
			s.source(integration, true)
			s.source(integration, true)
			s.source(integration, false)
		})},
		{"jira config, no runs", withConfig("jira", "", nil)},
		{"run pending", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) { s.run(s.job(c), pending, "", nil, 5) })},
		{"run running", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) { s.run(s.job(c), running, "", nil, 5) })},
		{"run failed with error", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) { s.run(s.job(c), failed, "", text("boom: provider 500"), 5) })},
		{"run failed empty error falls back to the config error", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			c := s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: true, integration: &integration, lastSyncError: text("config-level failure"), createdMinutes: 10})
			s.run(s.job(c), failed, "", text(""), 5)
		}},
		{"run failed with no error anywhere", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) { s.run(s.job(c), failed, "", nil, 5) })},
		{"run cancelled", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) {
			s.run(s.job(c), cancelled, "", text("cancelled by operator"), 5)
		})},
		{"run success", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) { s.run(s.job(c), success, "", nil, 5) })},
		{"run success, result partial_failed", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) {
			s.run(s.job(c), success, `{"sync_run_status": "partial_failed"}`, nil, 5)
		})},
		{"run failed, result partial", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) {
			s.run(s.job(c), failed, `{"sync_run_status": "partial"}`, text("part"), 5)
		})},
		{"newest run failed after an older success", withConfig("github", `{"all_repos": true}`, func(s seeder, c uuid.UUID, _ uuid.UUID) {
			job := s.job(c)
			s.run(job, success, "", nil, 30)
			s.run(job, failed, "", text("later failure"), 5)
		})},
		{"no run, last_sync_success true", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: true, integration: &integration, lastSyncSuccess: flag(true), createdMinutes: 10})
		}},
		{"no run, last_sync_success false", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: true, integration: &integration, lastSyncSuccess: flag(false), createdMinutes: 10})
		}},
		{"no run, error text only", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: true, integration: &integration, lastSyncError: text("configured but broken"), createdMinutes: 10})
		}},
		{"inactive primary config cannot start", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: false, integration: &integration, createdMinutes: 10})
		}},
		{"active older parent beats inactive newer parent", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			s.config(configOptions{provider: "jira", active: true, integration: &integration, lastSyncSuccess: flag(true), createdMinutes: 60})
			s.config(configOptions{provider: "jira", active: false, integration: &integration, lastSyncError: text("inactive newer"), createdMinutes: 1})
		}},
		{"newer of two active parents", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			s.config(configOptions{provider: "jira", active: true, integration: &integration, lastSyncError: text("older"), createdMinutes: 60})
			s.config(configOptions{provider: "jira", active: true, integration: &integration, lastSyncSuccess: flag(true), createdMinutes: 1})
		}},
		{"child configs are not parents", func(s seeder) {
			s.credential("github", true)
			integration := s.integration("github")
			parent := s.config(configOptions{provider: "github", options: `{"all_repos": true}`, active: true, integration: &integration, createdMinutes: 30})
			s.config(configOptions{provider: "github", active: true, parent: &parent, integration: &integration, lastSyncError: text("child failure"), createdMinutes: 1})
		}},
		{"active parents: a running run outranks a newer failed one", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			a := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 60})
			b := s.config(configOptions{provider: "jira", active: true, integration: &integration, lastSyncError: text("config b error"), createdMinutes: 50})
			s.run(s.job(a), running, "", nil, 20)
			s.run(s.job(b), failed, "", text("b failed"), 2)
		}},
		{"active parents: failed outranks cancelled, newest first within a status", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			a := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 60})
			b := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 50})
			c := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 40})
			s.run(s.job(a), cancelled, "", text("a cancelled"), 1)
			s.run(s.job(b), failed, "", text("b failed older"), 10)
			s.run(s.job(c), failed, "", text("c failed newer"), 3)
		}},
		{"a completed run of another parent marks the first sync completed", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			a := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 60})
			b := s.config(configOptions{provider: "jira", active: false, integration: &integration, createdMinutes: 5})
			s.run(s.job(a), pending, "", nil, 1)
			s.run(s.job(b), success, "", nil, 30)
		}},
		{"a partial success run does not mark the first sync completed", func(s seeder) {
			s.credential("jira", true)
			integration := s.integration("jira")
			a := s.config(configOptions{provider: "jira", active: true, integration: &integration, createdMinutes: 60})
			s.run(s.job(a), pending, "", nil, 1)
			s.run(s.job(a), success, `{"sync_run_status": "partial"}`, nil, 30)
		}},
	}
}

// TestAdminSetupStatusVenueOracle answers GET /api/v1/admin/setup/status for
// one organization per first-run state with the real Python api and the real
// Go api, and requires the same status and the same response text.
func TestAdminSetupStatusVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-setup-status-32-by"
	cases := scenarios()
	orgs := make([]uuid.UUID, len(cases))
	admins := make([]uuid.UUID, len(cases))
	for i := range cases {
		orgs[i], admins[i] = uuid.New(), uuid.New()
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			tokens := map[string]map[string]any{}
			for i, c := range cases {
				org := orgs[i].String()
				exec := func(sql string, args ...any) {
					t.Helper()
					if _, err := admin.Exec(ctx, sql, args...); err != nil {
						t.Fatalf("seed: %v\n%s", err, sql)
					}
				}
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'community', 'stripe', true, now(), now())`, orgs[i], fmt.Sprintf("venue-setup-%d", i), fmt.Sprintf("Venue Setup %d", i))
				email := fmt.Sprintf("venue-setup-admin-%d@example.com", i)
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, admins[i], email)
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgs[i], admins[i])
				tokens[fmt.Sprintf("admin%d", i)] = map[string]any{"user_id": admins[i].String(), "email": email, "org_id": org, "role": "admin"}
				c.seed(seeder{t: t, ctx: ctx, admin: admin, org: org})
			}
			return tokens
		},
	})

	requests := make([]venueoracle.Request, len(cases))
	for i, c := range cases {
		requests[i] = venueoracle.Request{
			Name: c.name, Method: "GET", Path: "/api/v1/admin/setup/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[fmt.Sprintf("admin%d", i)]},
		}
	}
	python := venue.ServePython(t, requests)

	goBase := startGoServer(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
	body, _ := json.Marshal(len(requests))
	t.Logf("%s scenarios compared", body)
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	guard := policy.NewGuard(auth, logger)
	client, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatalf("valkey: %v", err)
	}
	t.Cleanup(client.Close)
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: client, Auth: auth, Guard: guard}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}
