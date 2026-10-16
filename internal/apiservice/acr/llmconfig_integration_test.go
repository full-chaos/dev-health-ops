//go:build integration

package acr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresCredentialVerifier(t *testing.T) {
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
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	verifier := PostgresCredentialVerifier{Pool: pool, Now: func() time.Time { return now }}

	insert := func(token, service, scopes string, expires, revoked *time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
			(id, service_name, token_hash, token_prefix, scopes, expires_at, revoked_at, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, $4::json, $5, $6, $7)`,
			service, admin.HashServiceToken(token), token[:16], scopes, expires, revoked, now); err != nil {
			t.Fatal(err)
		}
	}
	past, future := now.Add(-time.Minute), now.Add(time.Hour)
	const good = "svc_acr_good-0123456789abcdef"
	insert(good, "acr", `["entitlements:read","llm_config:read"]`, &future, nil)
	insert("svc_acr_entonly-0123456789abc", "acr", `["entitlements:read"]`, nil, nil)
	insert("svc_acr_revoked-0123456789abc", "acr", `["llm_config:read"]`, nil, &past)
	insert("svc_acr_expired-0123456789abc", "acr", `["llm_config:read"]`, &past, nil)
	insert("svc_worker_other-0123456789ab", "worker-operator", `["llm_config:read"]`, nil, nil)

	cases := []struct {
		name  string
		token string
		want  error
	}{
		{"valid", good, nil},
		{"same prefix, wrong tail", good[:16] + "tampered-tail", ErrCredentialInvalid},
		{"unknown", "svc_acr_unknown-0123456789abc", ErrCredentialInvalid},
		{"revoked", "svc_acr_revoked-0123456789abc", ErrCredentialInvalid},
		{"expired", "svc_acr_expired-0123456789abc", ErrCredentialInvalid},
		{"other service's token", "svc_worker_other-0123456789ab", ErrCredentialInvalid},
		{"missing scope", "svc_acr_entonly-0123456789abc", ErrCredentialScope},
		{"too short", "svc_acr_x", ErrCredentialInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := verifier.Verify(ctx, tc.token, LLMConfigScope)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.want == nil && id == "" {
				t.Fatal("a valid credential must return its id")
			}
		})
	}
}
