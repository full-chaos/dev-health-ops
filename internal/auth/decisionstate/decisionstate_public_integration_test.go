//go:build integration

package decisionstate

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

func (e *env) decide(t *testing.T, ctx context.Context, user, org uuid.UUID) authorization.Decision {
	t.Helper()
	return authorization.NewEvaluator().Evaluate(
		authorization.Request{Action: "settings.write", ResourceKind: "organization", OrganizationID: org.String()},
		e.snapshot(t, ctx, user, org.String()))
}

func (e *env) publicMember(t *testing.T, ctx context.Context, user, org uuid.UUID, role string) {
	t.Helper()
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, $4, now())`, uuid.New(), user, org, role)
}

// D6651: the decision reads public rows only. A public user, organization and membership with no auth copy allow.
func TestPublicRowsWithNoAuthCopyAllow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.publicUser(t, ctx, true, false), e.publicOrg(t, ctx)
	e.publicMember(t, ctx, user, org, "admin")
	if d := e.decide(t, ctx, user, org); !d.Allowed {
		t.Fatalf("public rows, no auth copy: %+v, want allow", d)
	}
}

func TestPublicRowsDenyWhenInactiveOrRemoved(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for name, c := range map[string]struct {
		mutate func(user, org uuid.UUID)
		reason authorization.Reason
	}{
		"public user inactive": {func(user, _ uuid.UUID) {
			e.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user)
		}, authorization.ReasonPrincipalInactive},
		"public organization inactive": {func(_, org uuid.UUID) {
			e.exec(t, ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, org)
		}, authorization.ReasonOrganizationInactive},
		"public membership deleted": {func(user, org uuid.UUID) {
			e.exec(t, ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org)
		}, authorization.ReasonNoMembership},
	} {
		user, org := e.publicUser(t, ctx, true, false), e.publicOrg(t, ctx)
		e.publicMember(t, ctx, user, org, "admin")
		c.mutate(user, org)
		if d := e.decide(t, ctx, user, org); d.Allowed || d.Reason != c.reason {
			t.Errorf("%s: %+v, want deny %s", name, d, c.reason)
		}
	}
}

// D6651: when the auth copy disagrees with the public row, the public row decides.
func TestPublicWinsWhenTheAuthCopyDisagrees(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	t.Run("auth principal disabled, public active: allow", func(t *testing.T) {
		user, org := e.user(t, ctx, true, false), e.org(t, ctx)
		e.publicMember(t, ctx, user, org, "admin")
		e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, user)
		if d := e.decide(t, ctx, user, org); !d.Allowed {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("auth organization disabled, public active: allow", func(t *testing.T) {
		user, org := e.user(t, ctx, true, false), e.org(t, ctx)
		e.publicMember(t, ctx, user, org, "admin")
		e.armed(t, ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, org)
		if d := e.decide(t, ctx, user, org); !d.Allowed {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("auth membership suspended or removed, public row present: allow", func(t *testing.T) {
		for _, status := range []string{"suspended", "removed"} {
			user, org := e.user(t, ctx, true, false), e.org(t, ctx)
			e.publicMember(t, ctx, user, org, "admin")
			e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, $3, 'admin', now())`, org, user, status)
			if d := e.decide(t, ctx, user, org); !d.Allowed {
				t.Fatalf("auth membership %s: %+v", status, d)
			}
		}
	})
	t.Run("auth membership active, no public row: deny", func(t *testing.T) {
		user, org := e.user(t, ctx, true, false), e.org(t, ctx)
		e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, 'active', 'admin', now())`, org, user)
		if d := e.decide(t, ctx, user, org); d.Allowed || d.Reason != authorization.ReasonNoMembership {
			t.Fatalf("%+v", d)
		}
	})
}

// A6: with the platform roles owned by auth, the holder is a public.users row that is active: no auth.principals row is read.
func TestOwnedPlatformAdminReadsThePublicHolder(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	r := e.collecting(t, true)
	noCopy := e.publicUser(t, ctx, true, false)
	e.role(t, ctx, noCopy, "platform_admin")
	if got := e.snapshotWith(t, ctx, r, noCopy, "").PlatformRole(); got != "platform_admin" {
		t.Fatalf("an active public user with an assignment and no auth copy: %q, want platform_admin", got)
	}
	inactive := e.user(t, ctx, false, false)
	e.role(t, ctx, inactive, "platform_admin")
	if got := e.snapshotWith(t, ctx, r, inactive, "").PlatformRole(); got != "" {
		t.Fatalf("an inactive public user with an assignment: %q, want none", got)
	}
	copyOff := e.user(t, ctx, true, false)
	e.role(t, ctx, copyOff, "platform_admin")
	e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, copyOff)
	if got := e.snapshotWith(t, ctx, r, copyOff, "").PlatformRole(); got != "platform_admin" {
		t.Fatalf("a disabled auth copy of an active public user: %q, want platform_admin (public wins)", got)
	}
}
