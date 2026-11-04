//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func (e *env) seedGitLabProvider(t *testing.T, ctx context.Context, org uuid.UUID, baseURL string) Provider {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config)
VALUES ($1, $2, $3, 'oauth_gitlab', 'active', jsonb_build_object('base_url', $4::text))`, id, org, "gl-"+id.String(), baseURL); err != nil {
		t.Fatalf("seed gitlab provider: %v", err)
	}
	return Provider(id.String())
}

func (e *env) seedGitLabUser(t *testing.T, ctx context.Context, subject string, bound *Provider, org uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var row *uuid.UUID
	if bound != nil {
		r := uuid.MustParse(string(*bound))
		row = &r
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'gitlab', $3, $4, true, true, false, 0, now(), now())`, id, uniqueEmail("gitlab"), subject, row); err != nil {
		t.Fatalf("seed gitlab user: %v", err)
	}
	e.seedMembership(t, ctx, id, org)
	e.project(t, ctx, id)
	return id
}

// A GitLab numeric id is unique per instance: before the users cutover the same id on gitlab.com and on a
// self-hosted host resolves to two accounts, each at its own key; the membership binding of the row key stays.
func TestLegacyGitLabKeyIsScopedToItsInstance(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	row := e.seedGitLabProvider(t, ctx, org, "https://Git.Corp.Example/")
	subject := "gl-" + uuid.NewString()
	social := e.seedGitLabUser(t, ctx, subject, nil, org)
	hosted := e.seedGitLabUser(t, ctx, subject, &row, org)

	hostedKey := ExternalKey{Issuer: "https://git.corp.example", Provider: row, Subject: subject}
	legacy := &LegacyMatch{AuthProvider: "gitlab", MembershipOrg: &org}
	if p, found, err := e.store.IdentityBySubject(ctx, hostedKey.Issuer, hostedKey.Provider, hostedKey.Subject, legacy); err != nil || !found || p.ID != hosted {
		t.Fatalf("self-hosted key = %+v, %v, %v; want the self-hosted account %v", p, found, err, hosted)
	}
	socialKey := ExternalKey{Issuer: "https://gitlab.com", Provider: ProviderGitLab, Subject: subject}
	if p, found, err := e.store.IdentityBySubject(ctx, socialKey.Issuer, socialKey.Provider, socialKey.Subject, socialLegacy(ProviderGitLab)); err != nil || !found || p.ID != social {
		t.Fatalf("gitlab.com social key = %+v, %v, %v; want the gitlab.com account %v", p, found, err, social)
	}
}

// The membership binding stays: the self-hosted account must belong to the provider's organization.
func TestLegacyGitLabInstanceKeyKeepsTheMembershipBinding(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, other := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	row := e.seedGitLabProvider(t, ctx, org, "https://git.corp.example")
	subject := "gl-" + uuid.NewString()
	e.seedGitLabUser(t, ctx, subject, &row, other)
	legacy := &LegacyMatch{AuthProvider: "gitlab", MembershipOrg: &org}
	if p, found, err := e.store.IdentityBySubject(ctx, "https://git.corp.example", row, subject, legacy); !errors.Is(err, ErrLegacyNotMember) || found {
		t.Fatalf("non-member self-hosted account = %+v, %v, %v; want ErrLegacyNotMember", p, found, err)
	}
}

// GitLabInstanceIssuerSQL is the twin of GitLabInstanceIssuer: the same issuer for every base_url, NULL where Go refuses.
func TestGitLabInstanceIssuerSQLMatchesGo(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, base := range []string{
		"", "https://gitlab.com", "https://GitLab.com/", "https://gitlab.com//", "https://gitlab.com/group",
		"https://git.corp.example", "https://Git.Corp.Example:8443/gl/", "HTTPS://git.corp.example", "http://127.0.0.1:8080",
		"https://user:pw@git.corp.example/x", "git.corp.example", "//git.corp.example", "ftp://git.corp.example", "https://",
	} {
		want, err := GitLabInstanceIssuer(base)
		var got *string
		if qerr := e.admin.QueryRow(ctx, `SELECT `+GitLabInstanceIssuerSQL("$1::text"), base).Scan(&got); qerr != nil {
			t.Fatalf("base_url %q: %v", base, qerr)
		}
		switch {
		case err != nil && got != nil:
			t.Fatalf("base_url %q: Go refuses, SQL = %q", base, *got)
		case err == nil && (got == nil || *got != want):
			t.Fatalf("base_url %q: Go = %q, SQL = %v", base, want, got)
		}
	}
}
