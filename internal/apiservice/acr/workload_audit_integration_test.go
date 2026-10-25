//go:build integration

package acr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

func auditPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return ctx, pool
}

type auditRowShape struct {
	credential, principal *string
	outcome               string
}

func auditRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []auditRowShape {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT credential_id::text, principal_id, outcome FROM internal_service_credential_audits ORDER BY created_at, outcome`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []auditRowShape
	for rows.Next() {
		var row auditRowShape
		if err := rows.Scan(&row.credential, &row.principal, &row.outcome); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	return out
}

func TestAuditWritesPrincipalIDForWorkloadAccept(t *testing.T) {
	ctx, pool := auditPool(t)
	signer := newWLSigner(t)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Store: &fakeStore{}, Auth: PostgresCredentialStore{Pool: pool}, Workload: wlAuth(t, signer, "dev-health-api")}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	request := httptest.NewRequest(http.MethodGet, entitlementPath+someOrg, nil)
	request.Header.Set("Authorization", "Bearer "+signer.sign(wlHeader(), wlClaims()))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d %s", recorder.Code, recorder.Body.String())
	}
	rows := auditRows(ctx, t, pool)
	if len(rows) != 1 || rows[0].credential != nil || rows[0].principal == nil || *rows[0].principal != wlSubject || rows[0].outcome != "success" {
		t.Fatalf("rows = %+v, want one success row with principal_id %s and no credential", rows, wlSubject)
	}
}

func TestAuditSvcAcrRowHasNullPrincipalID(t *testing.T) {
	ctx, pool := auditPool(t)
	const token = "svc_acr_audit-shape-probe-0123456789"
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["entitlements:read"]'::json, now())`, hashToken(token), token[:16]); err != nil {
		t.Fatal(err)
	}
	handler := authServer(t, &fakeStore{}, PostgresCredentialStore{Pool: pool}, nil)
	if rec := do(handler, entitlementPath+someOrg, "Bearer "+token); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	rows := auditRows(ctx, t, pool)
	if len(rows) != 1 || rows[0].credential == nil || rows[0].principal != nil {
		t.Fatalf("rows = %+v, want one row with a credential and a NULL principal_id", rows)
	}
}

func TestAuditRefusedBeforeVerifyHasNullPrincipalID(t *testing.T) {
	ctx, pool := auditPool(t)
	signer := newWLSigner(t)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Store: &fakeStore{}, Auth: PostgresCredentialStore{Pool: pool}, Workload: wlAuth(t, signer, "dev-health-api")}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	for _, tc := range []struct {
		bearer, outcome string
	}{
		{"Bearer " + signer.sign(wlHeader(), wlClaims()) + "x", outcomeWorkloadRefused},
		{"Bearer " + newWLSigner(t).sign(wlHeader(), wlClaims()), outcomeWorkloadRefused},
		{"Bearer " + signer.sign(wlHeader(), func() map[string]any { c := wlClaims(); c["sub"] = wlOther; return c }()), outcomeWorkloadForbidden},
	} {
		if rec := do(mux, entitlementPath+someOrg, tc.bearer); rec.Code != http.StatusUnauthorized && rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d", rec.Code)
		}
	}
	rows := auditRows(ctx, t, pool)
	if len(rows) != 3 {
		t.Fatalf("rows = %+v, want 3", rows)
	}
	for _, row := range rows {
		if row.principal != nil || row.credential != nil {
			t.Fatalf("a refused request wrote an actor: %+v", row)
		}
	}
}

func TestAuditCheckRefusesBothCredentialAndPrincipal(t *testing.T) {
	ctx, pool := auditPool(t)
	const token = "svc_acr_audit-check-probe-0123456789"
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["entitlements:read"]'::json, now())`, hashToken(token), token[:16]); err != nil {
		t.Fatal(err)
	}
	var credentialID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM internal_service_credentials`).Scan(&credentialID); err != nil {
		t.Fatal(err)
	}
	err := PostgresCredentialStore{Pool: pool}.Audit(ctx, credentialID, wlSubject, someOrg, "success")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("err = %v, want a check violation", err)
	}
}

func TestAuditPrincipalIDLengthBound(t *testing.T) {
	ctx, pool := auditPool(t)
	store := PostgresCredentialStore{Pool: pool}
	if err := store.Audit(ctx, "", "prn_"+strings.Repeat("A", 251), someOrg, "success"); err != nil {
		t.Fatalf("255 characters: %v", err)
	}
	err := store.Audit(ctx, "", "prn_"+strings.Repeat("A", 252), someOrg, "success")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("256 characters: err = %v, want a check violation", err)
	}
}
