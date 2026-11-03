package policy

import "net/http"

// SetShadow installs s beside the guard; nil is off. Call it once, before the guard serves.
func (g *Guard) SetShadow(s *Shadow) { g.shadow = s }

// observeShadow is the guard's one call site of the shadow core: the caller as the legacy
// decision saw it, and that decision. It never changes the answer.
func (g *Guard) observeShadow(r *http.Request, user *User, bearer string, legacyAllowed bool) {
	if g.shadow == nil || user == nil {
		return
	}
	ctx := r.Context()
	g.shadow.Observe(ctx, ShadowObservation{
		Pattern: r.Pattern, Bearer: bearer, OrgID: user.OrgID, Caller: shadowCallerKind(user),
		UserAccessToken: user.accessToken, Impersonating: ImpersonationFrom(ctx) != nil, LegacyAllowed: legacyAllowed,
	})
}

func shadowCallerKind(user *User) string {
	switch {
	case user.Holds(ActionAdminOrgs):
		return ShadowCallerSuperuser
	case user.Role == "owner" || user.Role == "admin":
		return ShadowCallerAdmin
	}
	return ShadowCallerMember
}

// ShadowEnabled reports whether a shadow is installed beside the guard.
func (g *Guard) ShadowEnabled() bool { return g.shadow != nil }
