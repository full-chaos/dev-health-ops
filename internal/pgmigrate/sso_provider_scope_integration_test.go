//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Revision 0155 adds users.sso_provider_id: the provider whose identity an OIDC or SAML account holds. The
// backfill binds an account only when exactly one provider of its protocol belongs to an organization it is a
// member of; every other account stays unbound.

func positionOf0155(t *testing.T, chain []pgmigrate.ChainFile) int {
	t.Helper()
	for i, file := range chain {
		if file.Revision == "0155" {
			return i
		}
	}
	t.Fatal("the chain holds no revision 0155")
	return 0
}

type scopeSeed struct {
	ctx  context.Context
	conn *pgx.Conn
	t    *testing.T
}

func (s scopeSeed) org() uuid.UUID {
	id := uuid.New()
	if _, err := s.conn.Exec(s.ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, 'enterprise', 'stripe', true, '{}', now(), now())`, id, "org-"+id.String()[:8]); err != nil {
		s.t.Fatal(err)
	}
	return id
}

func (s scopeSeed) provider(org uuid.UUID, protocol string) uuid.UUID {
	id := uuid.New()
	if _, err := s.conn.Exec(s.ctx, `INSERT INTO sso_providers (id, org_id, name, protocol, status, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'active', '{}'::json, now(), now())`, id, org, "p-"+id.String()[:8], protocol); err != nil {
		s.t.Fatal(err)
	}
	return id
}

func (s scopeSeed) user(authProvider string, subject *string, orgs ...uuid.UUID) uuid.UUID {
	id := uuid.New()
	if _, err := s.conn.Exec(s.ctx, `INSERT INTO users (id, email, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, true, false, 0, now(), now())`, id, id.String()+"@example.test", authProvider, subject); err != nil {
		s.t.Fatal(err)
	}
	for _, org := range orgs {
		if _, err := s.conn.Exec(s.ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), org, id); err != nil {
			s.t.Fatal(err)
		}
	}
	return id
}

func strptr(s string) *string { return &s }

func TestRevision0155BindsAccountsToExactlyOneProvider(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	idx := positionOf0155(t, chain)
	d := newDownInstance(t)
	uri := d.at(t, idx)
	conn := connect(t, uri)
	ctx := context.Background()
	s := scopeSeed{ctx: ctx, conn: conn, t: t}

	orgA, orgB, orgC, orgD, orgE, orgF := s.org(), s.org(), s.org(), s.org(), s.org(), s.org()
	providerA := s.provider(orgA, "saml")
	s.provider(orgB, "oidc")
	s.provider(orgB, "oidc")
	s.provider(orgC, "saml")
	s.provider(orgD, "oidc")
	s.provider(orgE, "oidc")
	providerF := s.provider(orgF, "oidc")

	bound := s.user("saml", strptr("sub-a"), orgA)
	ambiguousInOrg := s.user("oidc", strptr("sub-b"), orgB)
	wrongProtocol := s.user("oidc", strptr("sub-c"), orgC)
	twoOrgs := s.user("oidc", strptr("sub-de"), orgD, orgE)
	noOrg := s.user("oidc", strptr("sub-none"))
	local := s.user("local", nil, orgF)
	noSubject := s.user("oidc", nil, orgF)
	boundToF := s.user("oidc", strptr("sub-f"), orgF)

	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain[:idx+1]); err != nil {
		t.Fatalf("upgrade to 0155: %v", err)
	}
	want := map[uuid.UUID]*uuid.UUID{
		bound: &providerA, ambiguousInOrg: nil, wrongProtocol: nil, twoOrgs: nil, noOrg: nil, local: nil, noSubject: nil, boundToF: &providerF,
	}
	for id, expected := range want {
		var got *uuid.UUID
		if err := conn.QueryRow(ctx, `SELECT sso_provider_id FROM users WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if (got == nil) != (expected == nil) || (got != nil && *got != *expected) {
			t.Errorf("user %s: sso_provider_id = %v, want %v", id, got, expected)
		}
	}
}

func TestRevision0155KeepsAProvidersSubjectsUniqueAndUnbindsOnProviderDelete(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	d := newDownInstance(t)
	conn := connect(t, d.at(t, len(chain)))
	ctx := context.Background()
	positionOf0155(t, chain)
	s := scopeSeed{ctx: ctx, conn: conn, t: t}
	orgA, orgB := s.org(), s.org()
	providerA, providerB := s.provider(orgA, "oidc"), s.provider(orgB, "oidc")
	insert := func(provider uuid.UUID, subject string) error {
		_, err := conn.Exec(ctx, `INSERT INTO users (id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'oidc', $3, $4, true, true, false, 0, now(), now())`, uuid.New(), uuid.NewString()+"@example.test", subject, provider)
		return err
	}
	if err := insert(providerA, "same"); err != nil {
		t.Fatal(err)
	}
	if err := insert(providerB, "same"); err != nil {
		t.Fatalf("the same subject at another provider: %v", err)
	}
	var pgErr *pgconn.PgError
	if err := insert(providerA, "same"); !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("the same subject at the same provider: err = %v, want a unique violation", err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM sso_providers WHERE id = $1`, providerA); err != nil {
		t.Fatal(err)
	}
	var bound int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM users WHERE auth_provider_id = 'same' AND sso_provider_id IS NULL`).Scan(&bound); err != nil || bound != 1 {
		t.Fatalf("accounts unbound by the provider delete = %d (%v), want 1", bound, err)
	}
}
