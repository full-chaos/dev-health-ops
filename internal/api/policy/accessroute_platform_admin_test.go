package policy

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Product rule 1 (D6773): a platform admin needs no membership row in the
// token org, for every token class. The user access token path builds the
// principal the HS256 path built for the same caller and org: the org of the
// token, the non-member role, the live superuser flag and platform set. The
// HS256 answers are the executed legacy run on main 98c7d4f56 (log
// 13154-legacy-rec-98c7d4f5.log sha256 8ba2f74cdd64, LEGACY-REC lines); a
// caller the HS256 path refused (credential or CheckTokenOrg) is a refusal here.
func TestProductRule1RoutedPlatformAdminWithoutMembershipIsTheLegacyPrincipal(t *testing.T) {
	const otherOrg = "66666666-6666-4666-8666-666666666666"
	adminOrgs := rule.PlatformOf(rule.ActionAdminOrgs)
	support := rule.PlatformOf(rule.ActionAdminImpersonate)
	ctx := WithRouteProfile(context.Background(), "GET /p")
	type principal struct {
		org, role                       string
		isAdmin, isSuperuser, holdsOrgs bool
		id                              string
	}
	cases := map[string]struct {
		state  UserState
		legacy *principal
	}{
		"superuser":                        {state: UserState{IsActive: true, IsSuperuser: true}, legacy: &principal{otherOrg, "member", true, true, true, userID.String()}},
		"admin.orgs platform role":         {state: UserState{IsActive: true, Platform: &adminOrgs}, legacy: &principal{otherOrg, "member", true, false, true, userID.String()}},
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
			if tc.legacy == nil {
				if routedUser != nil || !IsRefusal(routedErr) {
					t.Fatalf("routed user %+v err %v, want a refusal", routedUser, routedErr)
				}
				return
			}
			if routedErr != nil {
				t.Fatalf("routed err %v", routedErr)
			}
			got := principal{routedUser.OrgID, routedUser.Role, routedUser.IsAdmin(), routedUser.IsSuperuser, routedUser.Holds(ActionAdminOrgs), routedUser.ID.String()}
			if got != *tc.legacy || routedUser.Role != nonMemberRole {
				t.Fatalf("routed principal %+v, legacy %+v", got, *tc.legacy)
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

// Product rule 1 (D6773): a user access token with no org claim (an org-less
// caller, a superuser or not) gets the role the HS256 path gave the same
// caller: go-api's login mints role "member" with no org
// (internal/api/session/tokens.go:156) and userFromClaims defaults an absent
// role to "member". Both org claim forms (absent, empty) are org-less.
func TestRoutedUserWithoutAnOrgClaimHasTheLegacyMemberRole(t *testing.T) {
	ctx := WithRouteProfile(context.Background(), "GET /p")
	empty := ""
	for name, org := range map[string]*string{"no org claim": nil, "empty org claim": &empty} {
		for _, superuser := range []bool{true, false} {
			store := newAccessStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: superuser}
			user, err := routed(t, store, &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.OrgID = org })}).Authenticate(ctx, "x.y.z")
			if err != nil || user.OrgID != "" || user.Role != "member" {
				t.Fatalf("%s, superuser %v: user %+v err %v, want org \"\" and the legacy role \"member\"", name, superuser, user, err)
			}
		}
	}
}
