package policy

import "context"

// ActorChain is the one delegation a request runs under: the real actor behind
// the effective subject. A delegation has one hop; a nested one never reaches
// go-api (authverify's closed act claim and auth-service's nested-delegation
// refusal).
type ActorChain struct {
	// Actor is the real actor's users id.
	Actor string
	// Subject is the effective subject's users id.
	Subject string
	// OrgID is the organization the delegation acts in; "" when the bearer names none.
	OrgID string
	// SessionID is the delegation's session: the delegated bearer's live sid, or
	// the go-api impersonation row's id; "" for a token without sid.
	SessionID string
}

// ActorChain returns the delegation the caller's verified bearer names: a
// delegated session's act claim, or a legacy token's impersonating_user_id.
// A missing or empty actor is no delegation.
func (u *User) ActorChain() (ActorChain, bool) {
	if u == nil || u.ImpersonatedBy == nil || *u.ImpersonatedBy == "" {
		return ActorChain{}, false
	}
	chain := ActorChain{Actor: *u.ImpersonatedBy, Subject: u.UserID, OrgID: u.OrgID}
	if u.SessionID != nil {
		chain.SessionID = u.SessionID.String()
	}
	return chain, true
}

// SessionRowChain returns the delegation of the request's live go-api
// impersonation row, which the Impersonation middleware set for the admin's
// own bearer.
func SessionRowChain(ctx context.Context) (ActorChain, bool) {
	session := ImpersonationFrom(ctx)
	if session == nil {
		return ActorChain{}, false
	}
	return ActorChain{Actor: session.AdminUserID.String(), Subject: session.TargetUserID.String(),
		OrgID: session.TargetOrgID.String(), SessionID: session.ID.String()}, true
}
