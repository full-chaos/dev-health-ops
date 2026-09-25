//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const zeroUnitStampVenueKey = "venue-oracle-zero-unit-stamp-key-32-bytes-ok!"

// pythonZeroUnitPlanProgram runs the planner's own plan_sync_run for each
// integration and prints the persisted run's stamp:
// [total_units, credential_id, auth_source, credential_fingerprint], or
// ["raise <class>", ...].
const pythonZeroUnitPlanProgram = `
import json, sys, uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from dev_health_ops.models.integrations import SyncRun
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
payload = json.loads(sys.stdin.read())
engine = create_engine(payload["uri"], poolclass=NullPool)
out = []
for integration_id in payload["integrations"]:
    with Session(engine) as session:
        try:
            plan = plan_sync_run(session, SyncPlanRequest(integration_id=integration_id, org_id=payload["org"],
                mode="incremental", triggered_by="scheduled"))
            session.commit()
            run = session.get(SyncRun, uuid.UUID(str(plan.sync_run_id)))
            out.append([str(run.total_units), str(run.credential_id or ""), run.auth_source or "", run.credential_fingerprint or ""])
        except Exception as exc:
            out.append(["raise " + type(exc).__name__, "", "", ""])
engine.dispose()
print(json.dumps(out))
`

type zeroUnitStampCase struct {
	name, provider                     string
	integrationID, configID, jobID     string
	credentialPlaintext, credentialCfg string // empty plaintext: environment auth
}

// TestZeroUnitRunAuthStampVenueOracleMatchesLivePython holds the Go
// materializer's run-auth stamp on ZERO-unit plans (every source disabled)
// to Python's plan_sync_run over the same seeded rows (CHAOS-4593 /
// CHAOS-6703): Jira stamps credential_id, auth_source and
// credential_fingerprint anyway, for environment and stored-credential
// auth; every other provider's zero-unit run stays unstamped.
func TestZeroUnitRunAuthStampVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the zero-unit stamp oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	const org = "00000000-0000-4000-8000-000000006703"
	cases := []zeroUnitStampCase{
		{name: "jira environment", provider: "jira"},
		{name: "jira stored credential", provider: "jira",
			credentialPlaintext: `{"email": "e@example.com", "api_token": "t<&>", "base_url": "https://x.atlassian.net/"}`, credentialCfg: `{"cloud_id": "c-1"}`},
		{name: "jira oauth credential", provider: "jira",
			credentialPlaintext: `{"refresh_token": "r", "client_id": "cid", "client_secret": "cs", "oauth_binding_id": "b"}`, credentialCfg: `{}`},
		{name: "github stored credential", provider: "github", credentialPlaintext: `{"token": "ghp_abc"}`, credentialCfg: `{}`},
		{name: "github environment", provider: "github"},
		{name: "gitlab stored credential", provider: "gitlab", credentialPlaintext: `{"token": "glpat-x"}`, credentialCfg: `{}`},
		{name: "linear stored credential", provider: "linear", credentialPlaintext: `{"api_key": "lin_api_x"}`, credentialCfg: `{}`},
	}
	for index := range cases {
		cases[index].integrationID, cases[index].configID, cases[index].jobID = uuid.NewString(), uuid.NewString(), uuid.NewString()
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + zeroUnitStampVenueKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seedZeroUnitStampCases(t, ctx, admin, venue, org, cases)
			return nil
		},
	})

	python := pyoracle.Resolve(t, root)
	parsed, err := url.Parse(venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "postgresql+psycopg2"
	ids := make([]string, len(cases))
	for index, c := range cases {
		ids[index] = c.integrationID
	}
	input, _ := json.Marshal(map[string]any{"uri": parsed.String(), "org": org, "integrations": ids})
	command := exec.Command(python, "-c", pythonZeroUnitPlanProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true", "ENVIRONMENT=test",
		"SETTINGS_ENCRYPTION_KEY="+zeroUnitStampVenueKey)
	for _, name := range []string{"GITHUB_TOKEN", "GITLAB_TOKEN", "JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "LINEAR_API_KEY"} {
		if os.Getenv(name) != "" {
			t.Fatalf("%s is set in the test environment; the environment-auth cases need it unset on both planes", name)
		}
	}
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][4]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v\n%s", err, output)
	}

	config, err := pgxpool.ParseConfig(venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(zeroUnitStampVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.WithCredentialFingerprint(decryptor)

	stamped, unstamped := 0, 0
	for index, c := range cases {
		if strings.HasPrefix(want[index][0], "raise") {
			t.Fatalf("%s: python plan raised: %s", c.name, want[index][0])
		}
		if want[index][0] != "0" {
			t.Fatalf("%s: python planned %s units, want a zero-unit plan", c.name, want[index][0])
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := materializer.Materialize(ctx, tx, PendingOccurrence{
			ID: "occurrence:v1:zero-unit-" + c.integrationID, IdentityVersion: OccurrenceIdentityVersion,
			OrgID: org, ConfigID: c.configID, JobID: c.jobID,
			ScheduledFor: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
			ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("%s: go materialize: %v", c.name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var units int
		var credential, auth, fingerprint *string
		if err := pool.QueryRow(ctx, `SELECT total_units, credential_id::text, auth_source, credential_fingerprint FROM sync_runs WHERE id=$1::uuid`,
			plan.SyncRunID).Scan(&units, &credential, &auth, &fingerprint); err != nil {
			t.Fatal(err)
		}
		got := [4]string{"0", orEmpty(credential), orEmpty(auth), orEmpty(fingerprint)}
		if units != 0 || got != want[index] {
			t.Errorf("%s: go units=%d stamp %v, python %v", c.name, units, got, want[index])
			continue
		}
		if want[index][2] != "" {
			stamped++
		} else {
			unstamped++
		}
	}
	if stamped == 0 || unstamped == 0 {
		t.Fatalf("want both stamped and unstamped zero-unit plans, got %d stamped, %d unstamped", stamped, unstamped)
	}
	t.Logf("%d zero-unit plans: %d stamped identically, %d unstamped on both planes", len(cases), stamped, unstamped)
	venueoracle.WriteProof(t)
}

func orEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// seedZeroUnitStampCases writes the org and, per case, an optional
// Python-encrypted credential, an integration, one DISABLED source (so both
// planners plan zero units), a planner-managed sync configuration and its
// scheduled job.
func seedZeroUnitStampCases(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org string, cases []zeroUnitStampCase) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'zu', 'zu', '{}', 'enterprise', true, $2, $2)`, org, at)
	var calls []venueoracle.PythonCall
	for _, c := range cases {
		if c.credentialPlaintext != "" {
			calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{c.credentialPlaintext}})
		}
	}
	encrypted := venue.CallPython(t, calls...)
	next := 0
	for _, c := range cases {
		var credentialID *string
		if c.credentialPlaintext != "" {
			var ciphertext string
			if err := json.Unmarshal(encrypted[next], &ciphertext); err != nil {
				t.Fatal(err)
			}
			next++
			id := uuid.NewString()
			credentialID = &id
			exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, $5, $6::json, $7, $7)`, id, org, c.provider, "cred-"+id[:8], ciphertext, c.credentialCfg, at)
		}
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}', true, $6, $6)`, c.integrationID, org, c.provider, credentialID, "int-"+c.integrationID[:8], at)
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
	is_enabled, discovered_at, last_seen_at)
VALUES (gen_random_uuid(), $1, $2, $3, 'repository', $4, $4, $4, '{}', false, $5, $5)`, org, c.integrationID, c.provider, "src-"+c.integrationID[:8], at)
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, sync_targets, sync_options, is_active, planner_managed,
	created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '[]', '{"schedule_cron": "0 * * * *"}', true, true, $6, $6)`, c.configID, org, "cfg-"+c.configID[:8], c.provider, c.integrationID, at)
		exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status,
	is_running, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, $3, 'sync', $4, '0 * * * *', 'UTC', '{}', $5, 0, false, 0, 0, $6, $6)`, c.jobID, org, "sync-config-"+c.configID, c.provider, c.configID, at)
	}
}
