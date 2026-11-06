package policy

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// TestActorChainOfTheBearer: the chain names the actor, the subject, the
// bearer's org and its live sid; no actor or an empty one is no chain.
func TestActorChainOfTheBearer(t *testing.T) {
	sid := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	actor, empty := actorID.String(), ""
	for name, user := range map[string]*User{"nil user": nil, "no actor": {UserID: userID.String()}, "empty actor": {UserID: userID.String(), ImpersonatedBy: &empty}} {
		if chain, ok := user.ActorChain(); ok {
			t.Errorf("%s: chain %+v, want none", name, chain)
		}
	}
	user := &User{UserID: userID.String(), OrgID: ownOrg, ImpersonatedBy: &actor}
	want := ActorChain{Actor: actor, Subject: userID.String(), OrgID: ownOrg}
	if chain, ok := user.ActorChain(); !ok || chain != want {
		t.Fatalf("no sid: %+v %v, want %+v", chain, ok, want)
	}
	user.SessionID = &sid
	want.SessionID = sid.String()
	if chain, ok := user.ActorChain(); !ok || chain != want {
		t.Fatalf("sid: %+v %v, want %+v", chain, ok, want)
	}
}

// TestSessionRowChain: the chain of a live go-api row is admin -> target in the
// target's org, with the row's id; no row is no chain.
func TestSessionRowChain(t *testing.T) {
	if chain, ok := SessionRowChain(context.Background()); ok {
		t.Fatalf("no row: chain %+v, want none", chain)
	}
	row := &Impersonation{ID: uuid.MustParse("88888888-8888-4888-8888-888888888888"), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	want := ActorChain{Actor: userID.String(), Subject: targetUserID.String(), OrgID: targetOrgID.String(), SessionID: row.ID.String()}
	if chain, ok := SessionRowChain(WithImpersonation(context.Background(), row)); !ok || chain != want {
		t.Fatalf("row: %+v %v, want %+v", chain, ok, want)
	}
}
