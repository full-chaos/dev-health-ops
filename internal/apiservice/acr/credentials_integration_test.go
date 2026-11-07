//go:build integration

package acr

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	adminapi "github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestEntitlementRouteUnderTheAPIRolePosture runs the real routes over the
// real alembic schema as the api login the real `migrate roles` and `migrate
// river` commands provision and grant: those grants must be enough for the
// audit insert. A live svc_acr_ credential row is refused with the uniform
// 401, its audit row names no credential and its last_used_at stays NULL; the
// workload token is served and audited with its principal.
func TestEntitlementRouteUnderTheAPIRolePosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	pgschema.Apply(ctx, t, admin)

	const password = "acr-grant-path-probe-pw"
	names := map[string]string{}
	for _, name := range []string{"domain", "queue", "coordinator", "api"} {
		role, err := containers.RoleName("acr_gp_"+name, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[name] = role
	}
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          names["domain"],
		"RIVER_QUEUE_DATABASE_ROLE":           names["queue"],
		"RIVER_COORDINATOR_DATABASE_ROLE":     names["coordinator"],
		"RIVER_DOMAIN_DATABASE_PASSWORD":      password,
		"RIVER_QUEUE_DATABASE_PASSWORD":       password,
		"RIVER_COORDINATOR_DATABASE_PASSWORD": password,
		"API_DATABASE_ROLE":                   names["api"],
		"API_DATABASE_PASSWORD":               password,
	}
	lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
	for _, step := range []struct {
		name string
		run  func(io.Writer, io.Writer) int
	}{
		{"migrate roles", func(o, e io.Writer) int { return rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, o, e) }},
		{"migrate river", func(o, e io.Writer) int { return rivermigrate.Execute(ctx, "dho", nil, lookup, o, e) }},
	} {
		var stdout, stderr bytes.Buffer
		if code := step.run(&stdout, &stderr); code != 0 {
			t.Fatalf("%s: exit %d\nstdout:\n%s\nstderr:\n%s", step.name, code, stdout.String(), stderr.String())
		}
	}
	poolConfig := admin.Config().Copy()
	poolConfig.ConnConfig.User, poolConfig.ConnConfig.Password = names["api"], password
	probe, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()

	const token = "svc_acr_posture-probe-0123456789abcdef"
	if _, err := admin.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["entitlements:read"]'::json, now())`,
		adminapi.HashServiceToken(token), token[:16]); err != nil {
		t.Fatal(err)
	}
	var credentialID string
	if err := admin.QueryRow(ctx, `SELECT id::text FROM internal_service_credentials`).Scan(&credentialID); err != nil {
		t.Fatal(err)
	}

	handler := authServer(t, &fakeStore{}, PostgresCredentialStore{Pool: probe}, nil)
	for _, tc := range []struct {
		header    string
		status    int
		outcome   string
		rows      int
		principal string
	}{
		{"Bearer " + entToken, http.StatusOK, "success", 1, wlSubject},
		{"Bearer " + token, http.StatusUnauthorized, "missing_or_malformed_token", 1, ""},
		{"", http.StatusUnauthorized, "missing_or_malformed_token", 2, ""},
	} {
		request := httptest.NewRequest(http.MethodGet, entitlementPath+someOrg, nil)
		if tc.header != "" {
			request.Header.Set("Authorization", tc.header)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != tc.status {
			t.Fatalf("%q: status = %d %s, want %d", tc.header, recorder.Code, recorder.Body.String(), tc.status)
		}
		var rows, linked, principals int
		if err := admin.QueryRow(ctx, `SELECT count(*), count(credential_id), count(*) FILTER (WHERE principal_id = $2)
			FROM internal_service_credential_audits
			WHERE outcome = $1 AND requested_org_id = $3 AND action = 'acr_entitlement_lookup'`,
			tc.outcome, tc.principal, someOrg).Scan(&rows, &linked, &principals); err != nil {
			t.Fatal(err)
		}
		wantPrincipals := 0
		if tc.principal != "" {
			wantPrincipals = rows
		}
		if rows != tc.rows || linked != 0 || principals != wantPrincipals {
			t.Fatalf("%q: audit rows = %d linked = %d principals = %d, want %d, 0, %d", tc.header, rows, linked, principals, tc.rows, wantPrincipals)
		}
	}
	var stamped bool
	if err := admin.QueryRow(ctx, `SELECT last_used_at IS NOT NULL FROM internal_service_credentials WHERE id = $1::uuid`,
		credentialID).Scan(&stamped); err != nil || stamped {
		t.Fatalf("last_used_at stamped = %v err = %v, want false: a refused svc_acr_ credential is never read", stamped, err)
	}
}
