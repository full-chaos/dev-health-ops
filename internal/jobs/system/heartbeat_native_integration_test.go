//go:build integration

package system

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// heartbeatOracleFixture is the shape of testdata/heartbeat_python_oracle.json,
// a one-time capture of src/dev_health_ops/workers/system_ops.py's
// phone_home_heartbeat REAL output (now deleted) against the identical seed
// this test writes below -- not a hand-typed guess at what it should have
// returned. See that file's own "_generated_by" field for how it was made.
type heartbeatOracleFixture struct {
	Seed struct {
		Organizations int `json:"organizations"`
		Users         int `json:"users"`
		OrgLicense    struct {
			Tier       string `json:"tier"`
			LicenseKey string `json:"license_key"`
		} `json:"org_license"`
	} `json:"seed"`
	PythonOutput struct {
		OrgCount    int64  `json:"org_count"`
		UserCount   int64  `json:"user_count"`
		Tier        string `json:"tier"`
		LicenseHash string `json:"license_hash"`
	} `json:"python_output"`
}

func loadHeartbeatOracleFixture(t *testing.T) heartbeatOracleFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "heartbeat_python_oracle.json"))
	if err != nil {
		t.Fatalf("read oracle fixture: %v", err)
	}
	var fixture heartbeatOracleFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse oracle fixture: %v", err)
	}
	return fixture
}

// applyHeartbeatSchema creates only the tables phone_home_heartbeat's
// Postgres session touched (Organization, User, OrgLicense, AuditLog) --
// same "create just what this test touches" pattern used by the
// neighbouring operational package's integration tests, rather than running
// the full alembic chain.
func applyHeartbeatSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE TABLE public.organizations (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT NOT NULL
		)`,
		`CREATE TABLE public.users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			email TEXT NOT NULL
		)`,
		`CREATE TABLE public.org_licenses (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id UUID NOT NULL REFERENCES public.organizations(id),
			license_key TEXT,
			tier TEXT NOT NULL DEFAULT 'community'
		)`,
		`CREATE TABLE public.audit_logs (
			id UUID PRIMARY KEY,
			org_id UUID NOT NULL,
			user_id UUID,
			action TEXT NOT NULL,
			resource_type TEXT NOT NULL,
			resource_id TEXT NOT NULL,
			description TEXT,
			changes JSONB,
			request_metadata JSONB,
			status TEXT NOT NULL,
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("schema statement failed: %v\n%s", err, statement)
		}
	}
}

func startHeartbeatPostgres(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyHeartbeatSchema(ctx, t, pool)
	return ctx, pool
}

// TestQueryHeartbeatFactsMatchesThePythonOracle is the red-first parity test:
// it seeds the EXACT fixture the deleted Python phone_home_heartbeat body was
// run against (see testdata/heartbeat_python_oracle.json) and asserts the Go
// compute reaches the identical org_count/user_count/tier/license_hash. This
// intentionally does NOT compare instance_id/version/uptime_seconds/source --
// those three carry this worker's own identity now, by design (see
// heartbeat.go and heartbeat_native.go's doc comments), not a value ported
// from the Python interpreter that used to compute them.
func TestQueryHeartbeatFactsMatchesThePythonOracle(t *testing.T) {
	fixture := loadHeartbeatOracleFixture(t)
	ctx, pool := startHeartbeatPostgres(t)

	var firstOrgID string
	for i := 0; i < fixture.Seed.Organizations; i++ {
		var id string
		if err := pool.QueryRow(ctx,
			`INSERT INTO public.organizations (name) VALUES ($1) RETURNING id::text`,
			"org", // name content is irrelevant to the heartbeat compute
		).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstOrgID = id
		}
	}
	for i := 0; i < fixture.Seed.Users; i++ {
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.users (email) VALUES ($1)`, "user@example.com",
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.org_licenses (org_id, license_key, tier) VALUES ($1, $2, $3)`,
		firstOrgID, fixture.Seed.OrgLicense.LicenseKey, fixture.Seed.OrgLicense.Tier,
	); err != nil {
		t.Fatal(err)
	}

	facts, err := queryHeartbeatFacts(ctx, pool)
	if err != nil {
		t.Fatalf("queryHeartbeatFacts: %v", err)
	}
	if facts.orgCount != fixture.PythonOutput.OrgCount {
		t.Errorf("org_count = %d, oracle said %d", facts.orgCount, fixture.PythonOutput.OrgCount)
	}
	if facts.userCount != fixture.PythonOutput.UserCount {
		t.Errorf("user_count = %d, oracle said %d", facts.userCount, fixture.PythonOutput.UserCount)
	}
	if facts.tier != fixture.PythonOutput.Tier {
		t.Errorf("tier = %q, oracle said %q", facts.tier, fixture.PythonOutput.Tier)
	}
	if facts.licenseHash == nil || *facts.licenseHash != fixture.PythonOutput.LicenseHash {
		got := "<nil>"
		if facts.licenseHash != nil {
			got = *facts.licenseHash
		}
		t.Errorf("license_hash = %s, oracle said %s (same sha256(license_key).hex()[:16] the "+
			"Python body computed)", got, fixture.PythonOutput.LicenseHash)
	}
	if facts.firstOrgID != firstOrgID {
		t.Errorf("firstOrgID = %q, want %q", facts.firstOrgID, firstOrgID)
	}
}

// TestNativeHeartbeatDispatcherWritesAuditLogAndPostsTelemetry exercises the
// full DispatchHeartbeat path: the audit_logs row phone_home_heartbeat used
// to write, and the outbound POST, now both happen without ever calling out
// to the Python API.
func TestNativeHeartbeatDispatcherWritesAuditLogAndPostsTelemetry(t *testing.T) {
	ctx, pool := startHeartbeatPostgres(t)

	var orgID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO public.organizations (name) VALUES ('org') RETURNING id::text`,
	).Scan(&orgID); err != nil {
		t.Fatal(err)
	}

	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		received <- body
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	dispatcher, err := NewNativeHeartbeatDispatcher(
		pool, server.Client(), server.URL, "test-instance", "1.2.3-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.DispatchHeartbeat(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("DispatchHeartbeat: %v", err)
	}

	select {
	case body := <-received:
		if body["instance_id"] != "test-instance" {
			t.Errorf("instance_id = %v, want test-instance", body["instance_id"])
		}
		if body["version"] != "1.2.3-test" {
			t.Errorf("version = %v, want 1.2.3-test", body["version"])
		}
		if _, ok := body["uptime_seconds"].(float64); !ok {
			t.Errorf("uptime_seconds missing or not numeric: %v", body["uptime_seconds"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("telemetry endpoint was never called")
	}

	var (
		action, resourceType, resourceID, source string
	)
	if err := pool.QueryRow(ctx, `
		SELECT action, resource_type, resource_id, request_metadata->>'source'
		FROM public.audit_logs WHERE org_id = $1`, orgID,
	).Scan(&action, &resourceType, &resourceID, &source); err != nil {
		t.Fatalf("audit log row not found: %v", err)
	}
	if action != "other" || resourceType != "other" || resourceID != "phone_home_heartbeat" {
		t.Errorf("audit log identity = (%s, %s, %s), want (other, other, phone_home_heartbeat)",
			action, resourceType, resourceID)
	}
	if source != heartbeatAuditSource {
		t.Errorf("audit log request_metadata.source = %q, want %q", source, heartbeatAuditSource)
	}
}
