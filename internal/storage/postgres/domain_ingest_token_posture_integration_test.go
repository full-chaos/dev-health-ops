//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

const pushExpirySweepSQL = `SELECT id, org_id, source_id, scopes, expires_at FROM public.external_ingest_tokens
WHERE revoked_at IS NULL AND expires_at <= now() ORDER BY expires_at, id LIMIT 1`

func TestDomainPosturePassesWithTokenColumnGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, roles.domain, grantDomainPass)

	if err := CheckDomainAuthorization(ctx, domain, roles.domain, grantSchema); err != nil {
		t.Fatalf("domain posture rejected the token column grant: %v", err)
	}
	if _, err := domain.Exec(ctx, pushExpirySweepSQL); err != nil {
		t.Fatalf("domain role cannot run the push expiry sweep read: %v", err)
	}
	if _, err := domain.Exec(ctx, "SELECT token_hash FROM public.external_ingest_tokens"); err == nil {
		t.Fatal("domain role can read token_hash")
	}
}

func TestDomainPostureRefusesExcessTokenAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, roles.domain, grantDomainPass)

	for _, grant := range []struct{ name, grant, revoke string }{
		{"hash column", "GRANT SELECT (token_hash) ON TABLE public.external_ingest_tokens TO " + roles.domain, "REVOKE SELECT (token_hash) ON TABLE public.external_ingest_tokens FROM " + roles.domain},
		{"table-wide select", "GRANT SELECT ON TABLE public.external_ingest_tokens TO " + roles.domain, "REVOKE SELECT ON TABLE public.external_ingest_tokens FROM " + roles.domain},
		{"revoked_at update", "GRANT UPDATE (revoked_at) ON TABLE public.external_ingest_tokens TO " + roles.domain, "REVOKE UPDATE (revoked_at) ON TABLE public.external_ingest_tokens FROM " + roles.domain},
	} {
		if _, err := admin.Exec(ctx, grant.grant); err != nil {
			t.Fatalf("%s: %v", grant.name, err)
		}
		if err := CheckDomainAuthorization(ctx, domain, roles.domain, grantSchema); err == nil {
			t.Fatalf("%s: domain posture accepted an excess token privilege", grant.name)
		}
		if _, err := admin.Exec(ctx, grant.revoke); err != nil {
			t.Fatalf("%s: %v", grant.name, err)
		}
		if err := CheckDomainAuthorization(ctx, domain, roles.domain, grantSchema); err != nil {
			t.Fatalf("%s: posture did not recover: %v", grant.name, err)
		}
	}
}

func TestDomainPostureReportsMissingTokenColumnGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	domain := connectAs(t, ctx, uri, roles.domain, grantDomainPass)

	if _, err := admin.Exec(ctx, "REVOKE SELECT (scopes) ON TABLE public.external_ingest_tokens FROM "+roles.domain); err != nil {
		t.Fatal(err)
	}
	if err := CheckDomainAuthorization(ctx, domain, roles.domain, grantSchema); err == nil {
		t.Fatal("domain posture accepted a missing external_ingest_tokens.scopes grant")
	}
}
