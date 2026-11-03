//go:build integration

package decisionstate

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// serviceAccount seeds a service-account principal and its row in org, owned by owner, as serviceaccount.Create writes them.
func (e *env) serviceAccount(t *testing.T, ctx context.Context, org, owner uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.armed(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'service_account', 'ds-sa')`, id)
	e.armed(t, ctx, `INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id) VALUES ($1, $2, $3, $4)`,
		id, org, "ds-sa-"+id.String()[:8], owner)
	return id
}

// key seeds a credential_registry row of principal; expires is a SQL expression over now().
func (e *env) key(t *testing.T, ctx context.Context, principal uuid.UUID, class, expires string, revoked bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.armed(t, ctx, `INSERT INTO auth.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, fingerprint, expires_at, revoked_at, revoked_reason)
VALUES ($1, $2, $3, 'fcsak_', decode(md5($1::uuid::text), 'hex'), 'sha256', substr(md5($1::uuid::text), 1, 16), `+expires+`,
CASE WHEN $4 THEN now() END, CASE WHEN $4 THEN 'owner_request' END)`, id, principal, class, revoked)
	return id
}

func (e *env) grant(t *testing.T, ctx context.Context, principal, org uuid.UUID, repo, extra string) {
	t.Helper()
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id`+extra, principal, org, repo)
}

const liveGrant = `) VALUES ($1, $2, 'viewer', 'repository', $3)`

func (e *env) decide(t *testing.T, ctx context.Context, principal uuid.UUID, org, action, kind, key string) authorization.Decision {
	t.Helper()
	return authorization.NewEvaluator().Evaluate(authorization.Request{Action: action, ResourceKind: kind, OrganizationID: org, ResourceKey: key},
		e.snapshot(t, ctx, principal, org))
}

func (e *env) repoDecision(t *testing.T, ctx context.Context, principal, org uuid.UUID) authorization.Decision {
	t.Helper()
	return e.decide(t, ctx, principal, org.String(), "acr.context.read", "repository", "acme/api")
}

func wantReason(t *testing.T, what string, d authorization.Decision, allowed bool, reason authorization.Reason) {
	t.Helper()
	if d.Allowed != allowed || d.Reason != reason {
		t.Fatalf("%s: allowed %v reason %q, want %v %q", what, d.Allowed, d.Reason, allowed, reason)
	}
}

func TestServiceAccountSnapshotWithALiveGrantIsAllowed(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	sa := e.serviceAccount(t, ctx, org, owner)
	e.grant(t, ctx, sa, org, "acme/api", liveGrant)
	snap := e.snapshot(t, ctx, sa, org.String())
	if snap.PrincipalStatus() != authorization.StatusActive || snap.PlatformRole() != "" {
		t.Fatalf("principal %v platform %q, want active and none", snap.PrincipalStatus(), snap.PlatformRole())
	}
	if row, found := snap.ServiceAccount(org.String()); !found || row.OrgID != org.String() || row.Status != authorization.StatusActive {
		t.Fatalf("service account %+v %v, want active in %s", row, found, org)
	}
	wantReason(t, "live grant", e.repoDecision(t, ctx, sa, org), true, authorization.ReasonAllowedByRepositoryGrant)
}

func TestServiceAccountSnapshotGrantStates(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	none := e.serviceAccount(t, ctx, org, owner)
	wantReason(t, "no grant row", e.repoDecision(t, ctx, none, org), false, authorization.ReasonGrantAbsent)
	revoked := e.serviceAccount(t, ctx, org, owner)
	e.grant(t, ctx, revoked, org, "acme/api", `, revoked_at, revoked_by) VALUES ($1, $2, 'viewer', 'repository', $3, now(), $1)`)
	wantReason(t, "revoked grant", e.repoDecision(t, ctx, revoked, org), false, authorization.ReasonGrantRevoked)
	expired := e.serviceAccount(t, ctx, org, owner)
	e.grant(t, ctx, expired, org, "acme/api", `, expires_at) VALUES ($1, $2, 'viewer', 'repository', $3, now() - interval '1 second')`)
	wantReason(t, "expired grant", e.repoDecision(t, ctx, expired, org), false, authorization.ReasonGrantExpired)
}

// pm Q2: serviceaccount.Disable writes both rows; the decision is service_account_inactive. Either mark alone is enough.
func TestDisabledServiceAccountIsServiceAccountInactive(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	for name, disable := range map[string][]string{
		"both, as Disable writes them": {
			`UPDATE auth.service_accounts SET status = 'disabled', disabled_at = now() WHERE principal_id = $1`,
			`UPDATE auth.principals SET disabled_at = now() WHERE id = $1`,
		},
		"the account row only": {`UPDATE auth.service_accounts SET status = 'disabled', disabled_at = now() WHERE principal_id = $1`},
		"the principal only":   {`UPDATE auth.principals SET disabled_at = now() WHERE id = $1`},
	} {
		sa := e.serviceAccount(t, ctx, org, owner)
		e.grant(t, ctx, sa, org, "acme/api", liveGrant)
		for _, sql := range disable {
			e.armed(t, ctx, sql, sa)
		}
		wantReason(t, name, e.repoDecision(t, ctx, sa, org), false, authorization.ReasonServiceAccountInactive)
	}
}

func TestServiceAccountOrgAndPlatformActionsAreDenied(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	sa := e.serviceAccount(t, ctx, org, owner)
	e.grant(t, ctx, sa, org, "*", liveGrant)
	wantReason(t, "organization action", e.decide(t, ctx, sa, org.String(), "settings.write", "organization", ""), false, authorization.ReasonNoMembership)
	wantReason(t, "platform action", e.decide(t, ctx, sa, "", "admin.users", "platform", ""), false, authorization.ReasonPlatformRoleAbsent)
}

// A membership row of a service account (none is ever written) is not read: no membership default on a repository, no
// organization role, and the users row of the same id gives no platform role.
func TestServiceAccountWithAPlantedMembershipGetsNoDefaultAndNoOrgAction(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	sa := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, true, 0, now(), now())`, sa, "ds-sa-"+sa.String()[:8]+"@example.com")
	e.armed(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'service_account', 'ds-sa')`, sa)
	e.armed(t, ctx, `INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id) VALUES ($1, $2, $3, $4)`,
		sa, org, "ds-sa-"+sa.String()[:8], owner)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'owner', now())`, uuid.New(), sa, org)
	e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, 'active', 'owner', now())`, org, sa)
	snap := e.snapshot(t, ctx, sa, org.String())
	if _, found := snap.Membership(org.String()); found || snap.PlatformRole() != "" {
		t.Fatalf("membership found %v, platform role %q; want neither", found, snap.PlatformRole())
	}
	wantReason(t, "repository, no grant row", e.repoDecision(t, ctx, sa, org), false, authorization.ReasonGrantAbsent)
	wantReason(t, "organization action", e.decide(t, ctx, sa, org.String(), "settings.write", "organization", ""), false, authorization.ReasonNoMembership)
	wantReason(t, "platform action", e.decide(t, ctx, sa, "", "admin.users", "platform", ""), false, authorization.ReasonPlatformRoleAbsent)
}

func TestServiceAccountKeyReadsTheRowAtTheTransactionClock(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.org(t, ctx), e.user(t, ctx, true, false)
	sa := e.serviceAccount(t, ctx, org, owner)
	bare := uuid.New()
	e.armed(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'service_account', 'ds-bare')`, bare)
	live := e.key(t, ctx, sa, "service_account_key", "now() + interval '1 hour'", false)
	revoked := e.key(t, ctx, sa, "service_account_key", "now() + interval '1 hour'", true)
	expired := e.key(t, ctx, sa, "service_account_key", "now() - interval '1 second'", false)
	other := e.key(t, ctx, sa, "workload_token", "now() + interval '1 hour'", false)
	orphan := e.key(t, ctx, bare, "service_account_key", "now() + interval '1 hour'", false)
	read := func(id uuid.UUID) authapi.DecisionKey {
		t.Helper()
		var k authapi.DecisionKey
		if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
			var err error
			k, err = tx.ServiceAccountKey(ctx, id)
			return err
		}); err != nil {
			t.Fatalf("Read: %v", err)
		}
		return k
	}
	k := read(live)
	if !k.Found || k.PrincipalID != sa || k.AccountOrgID != org.String() || k.Class != "service_account_key" || k.DomainOwner != "platform" ||
		k.Revoked || k.NowUnixNano <= 0 || k.ExpiresAtUnixNano <= k.NowUnixNano || k.ExpiresAtUnixNano > k.NowUnixNano+int64(2*time.Hour) {
		t.Fatalf("live key %+v", k)
	}
	if k := read(revoked); !k.Found || !k.Revoked {
		t.Fatalf("revoked key %+v", k)
	}
	if k := read(expired); !k.Found || k.ExpiresAtUnixNano == 0 || k.ExpiresAtUnixNano > k.NowUnixNano {
		t.Fatalf("expired key %+v, want an expiry at or before the clock", k)
	}
	if k := read(other); !k.Found || k.Class != "workload_token" {
		t.Fatalf("another class %+v", k)
	}
	if k := read(orphan); !k.Found || k.PrincipalID != bare || k.AccountOrgID != "" {
		t.Fatalf("key of a principal without an account row %+v, want no account organization", k)
	}
	if k := read(uuid.New()); k.Found || k.NowUnixNano <= 0 {
		t.Fatalf("unknown key %+v, want not found with the clock", k)
	}
}
