//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var (
	bodyReference = regexp.MustCompile(`^Retention run failed\. Reference: ([0-9a-f-]{36})$`)
	logReference  = regexp.MustCompile(`reference=([0-9a-f-]{36})`)
)

// CHAOS-10053 on a migrated Postgres: the execute route answers a failed run
// with an authored sentence and a reference, and logs the driver error under
// that same reference. The admin routes get a database login that cannot read
// audit_logs, so the dry-run count and the delete each fail inside the
// database; a control run on the full login proves the failure comes from the
// missing grant and that the run otherwise succeeds.
func TestRetentionExecuteFailureBodyReferenceMatchesLog(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	orgID, adminID, policyID := uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'ret-fail-org', 'ret-fail-org', 'enterprise', 'stripe', true, now(), now())`, []any{orgID}},
		{`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, features_override, created_at, updated_at)
VALUES ($1, $2, 'enterprise', true, 'saas', 'stripe', '{}'::json, now(), now())`, []any{uuid.New(), orgID}},
		{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'ret-fail-admin@example.com', true, true, false, 0, now(), now())`, []any{adminID}},
		{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, []any{uuid.New(), orgID, adminID}},
		{`INSERT INTO org_retention_policies (id, org_id, resource_type, retention_days, description, is_active, created_at, updated_at)
VALUES ($1, $2, 'audit_logs', 30, NULL, true, now(), now())`, []any{policyID, orgID}},
		{`INSERT INTO audit_logs (id, org_id, action, resource_type, resource_id, changes, request_metadata, status, created_at)
VALUES ($1, $2, 'old-row', 'team', 'r', '{}'::json, '{}'::json, 'success', now() - interval '400 days')`, []any{uuid.New(), orgID}},
	} {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement.query)
		}
	}

	role, err := containers.RoleName("ret_noaudit", instance)
	if err != nil {
		t.Fatal(err)
	}
	const rolePassword = "ret-noaudit-login"
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, rolePassword)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	if _, err := pool.Exec(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "REVOKE SELECT, DELETE ON audit_logs FROM "+role); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = role, rolePassword
	noAudit, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(noAudit.Close)
	var one int
	if err := noAudit.QueryRow(ctx, `SELECT 1 FROM audit_logs LIMIT 1`).Scan(&one); err == nil {
		t.Fatal("the restricted login can read audit_logs, so the count cannot fail")
	}

	const jwtKey = "retention-failure-test-signing-key-32-bytes!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Access(edgetoken.AccessClaims{UserID: adminID.String(), Email: "ret-fail-admin@example.com",
		OrgID: orgID.String(), Role: "admin"}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	serve := func(dbPool *pgxpool.Pool, logs *lockedBuffer) string {
		t.Helper()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mux := http.NewServeMux()
		for _, route := range admin.Routes(admin.Deps{Pool: dbPool, Guard: policy.NewGuard(auth, quiet), Logger: logger}) {
			if strings.HasSuffix(route.Pattern, "/retention-policies/{policy_id}/execute") {
				mux.Handle(route.Method+" "+route.Pattern, route.Handler)
			}
		}
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		return server.URL
	}
	execute := func(base, body string) (int, map[string]any) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/v1/admin/retention-policies/"+policyID.String()+"/execute", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		raw, _ := io.ReadAll(response.Body)
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("body %q is not a JSON object: %v", raw, err)
		}
		return response.StatusCode, decoded
	}

	controlLogs := &lockedBuffer{}
	status, control := execute(serve(pool, controlLogs), `{}`)
	if status != http.StatusOK || control["error"] != nil || control["deleted_count"] != float64(1) {
		t.Fatalf("control dry run on the full login: %d %v, want 200, one old row counted, no error (logs: %s)", status, control, controlLogs.String())
	}

	for _, tc := range []struct {
		name, body string
		dryRun     string
	}{
		{"dry-run count fails", `{}`, "dry_run=true"},
		{"delete fails", `{"dry_run":false}`, "dry_run=false"},
	} {
		logs := &lockedBuffer{}
		status, answer := execute(serve(noAudit, logs), tc.body)
		if status != http.StatusOK {
			t.Errorf("%s: status %d, want 200", tc.name, status)
		}
		text, isText := answer["error"].(string)
		if !isText {
			t.Errorf("%s: the answer %v carries no error: a failed run must be marked failed, never served as a bare count", tc.name, answer)
			continue
		}
		matched := bodyReference.FindStringSubmatch(text)
		if matched == nil {
			t.Errorf("%s: error %q is not the authored sentence with a reference", tc.name, text)
			continue
		}
		if strings.Contains(text, "permission denied") || strings.Contains(text, "audit_logs") {
			t.Errorf("%s: error %q leaks the driver message", tc.name, text)
		}
		logged := logReference.FindAllStringSubmatch(logs.String(), -1)
		if len(logged) != 1 {
			t.Errorf("%s: %d log reference(s), want exactly 1: %s", tc.name, len(logged), logs.String())
			continue
		}
		if logged[0][1] != matched[1] {
			t.Errorf("%s: the log carries reference %s but the body carries %s", tc.name, logged[0][1], matched[1])
		}
		for _, want := range []string{"admin: execute retention policy failed", "permission denied", tc.dryRun, "policy_id=" + policyID.String()} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("%s: the log lacks %q: %s", tc.name, want, logs.String())
			}
		}
	}

	var lastRun *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_run_at FROM org_retention_policies WHERE id = $1`, policyID).Scan(&lastRun); err != nil {
		t.Fatal(err)
	}
	if lastRun != nil {
		t.Errorf("a failed delete stamped the policy (last_run_at %v)", lastRun)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'old-row'`, orgID).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("the old audit row count after the failed delete = %d (err %v), want 1", rows, err)
	}
}
