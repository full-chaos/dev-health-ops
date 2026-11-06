package policy

import (
	"context"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Product rule 1 (D6773): a platform admin needs no membership row in the
// token org, for every token class. The user access token path builds the
// principal the HS256 path builds for the same caller and org: the org of the
// token, the non-member role, the live superuser flag and platform set.
func TestProductRule1RoutedPlatformAdminWithoutMembershipIsTheLegacyPrincipal(t *testing.T) {
	const otherOrg = "66666666-6666-4666-8666-666666666666"
	adminOrgs := rule.PlatformOf(rule.ActionAdminOrgs)
	support := rule.PlatformOf(rule.ActionAdminImpersonate)
	ctx := WithRouteProfile(context.Background(), "GET /p")
	cases := map[string]struct {
		state UserState
		admit bool
	}{
		"superuser":                        {state: UserState{IsActive: true, IsSuperuser: true}, admit: true},
		"admin.orgs platform role":         {state: UserState{IsActive: true, Platform: &adminOrgs}, admit: true},
		"platform role without admin.orgs": {state: UserState{IsActive: true, Platform: &support}},
		"no platform action":               {state: UserState{IsActive: true}},
		"deactivated superuser":            {state: UserState{IsSuperuser: true}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			store := newAccessStore()
			store.users[userID] = tc.state
			routedUser, routedErr := routed(t, store, &fakeValidator{result: userClaims(func(c *AccessTokenClaims) {
				org := otherOrg
				c.OrgID = &org
			})}).Authenticate(ctx, "x.y.z")
			legacyUser, legacyErr := authenticator(t, store).AuthenticateLegacy(ctx, sign(t, claims(func(c jwt.MapClaims) {
				c["org_id"] = otherOrg
				delete(c, "role")
			})))
			if !tc.admit {
				if routedUser != nil || !IsRefusal(routedErr) {
					t.Fatalf("routed user %+v err %v, want a refusal", routedUser, routedErr)
				}
				return
			}
			if routedErr != nil || legacyErr != nil {
				t.Fatalf("routed err %v, legacy err %v", routedErr, legacyErr)
			}
			// The HS256 path keeps the token org only through useTokenOrg /
			// CheckTokenOrg; both admit exactly an admin.orgs holder.
			if err := authenticator(t, store).CheckTokenOrg(ctx, legacyUser); err != nil {
				t.Fatalf("the legacy path refuses this caller: %v", err)
			}
			got := [6]any{routedUser.OrgID, routedUser.Role, routedUser.IsAdmin(), routedUser.IsSuperuser, routedUser.Holds(ActionAdminOrgs), routedUser.ID}
			want := [6]any{legacyUser.OrgID, legacyUser.Role, legacyUser.IsAdmin(), legacyUser.IsSuperuser, legacyUser.Holds(ActionAdminOrgs), legacyUser.ID}
			if got != want || routedUser.OrgID != otherOrg || routedUser.Role != nonMemberRole {
				t.Fatalf("routed principal %v, legacy %v", got, want)
			}
		})
	}
}

// A platform admin who IS a member keeps the membership's role: the parity
// branch is only for a missing row.
func TestRoutedPlatformAdminMemberKeepsTheMembershipRole(t *testing.T) {
	store := newAccessStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	user, err := routed(t, store, &fakeValidator{result: userClaims(nil)}).Authenticate(WithRouteProfile(context.Background(), "GET /p"), "x.y.z")
	if err != nil || user.OrgID != ownOrg || user.Role != "admin" {
		t.Fatalf("user %+v err %v, want the membership role admin in %s", user, err, ownOrg)
	}
}
