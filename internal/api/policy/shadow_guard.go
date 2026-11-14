package policy

import "net/http"

// SetShadow installs s beside the guard; nil is off. Call it once, before the guard serves.
func (g *Guard) SetShadow(s *Shadow) { g.pdp.shadow = s }

// observeShadow is the guard's one call site of the shadow core: the caller as the legacy
// decision saw it, that decision, and the route's gate. It never changes the answer.
func (g *Guard) observeShadow(r *http.Request, user *User, bearer string, legacyAllowed bool, gt gate, level Authz) {
	g.observeShadowAs(r, user, bearer, legacyAllowed, ImpersonationFrom(r.Context()) != nil, gt, level)
}

// observeShadowAs is observeShadow with the impersonation state given: the guard read a live row itself, with no
// session context.
func (g *Guard) observeShadowAs(r *http.Request, user *User, bearer string, legacyAllowed, impersonating bool, gt gate, level Authz) {
	if g.pdp.shadow == nil || user == nil {
		return
	}
	ctx := r.Context()
	g.pdp.shadow.Observe(ctx, ShadowObservation{
		Pattern: r.Pattern, Bearer: bearer, OrgID: user.OrgID, Caller: shadowCallerKind(user),
		UserAccessToken: user.accessToken, Impersonating: impersonating, Read: readMethod(r.Method), LegacyAllowed: legacyAllowed,
		AdminOr: gt.kind == gateAdminOr, orgless: orglessInputsOf(level, gt, user),
	})
}

func shadowCallerKind(user *User) string {
	return ShadowCaller(user.Holds(ActionAdminOrgs), user.Role)
}

// ShadowCaller is the closed caller kind of a verified user, the same rule the guard's shadow hook uses.
func (u *User) ShadowCaller() string { return shadowCallerKind(u) }

// ShadowEnabled reports whether a shadow is installed beside the guard.
func (g *Guard) ShadowEnabled() bool { return g.pdp.shadow != nil }
