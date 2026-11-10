package oidc

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// A sign-in whose identity key holds no account and whose verified email an account holds is refused with the one
// generic 409 email collision, and nothing is provisioned, joined or issued, with auto_provision on. EmailInUse
// carries no account class, so a platform admin's account gets the same answer as any other: the email is never a
// link to an existing account.
func TestResolveRefusesAnEmailAPlatformAdminHoldsAsItRefusesAnyAccount(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		t.Run(protocol, func(t *testing.T) {
			ids := &resolveIdentities{err: ErrIdentityNotFound, inUse: true}
			jit := &resolveJIT{}
			login, failure, sessions := resolveRun(protocol, ids, resolveMembers{}, jit)
			wantRefused(t, failure, http.StatusConflict, ReasonEmailCollision)
			if failure.Detail != conflictMessage || login.PrincipalID != uuid.Nil {
				t.Fatalf("failure %+v login %+v, want the generic conflict and no account", failure, login)
			}
			if ids.provision != 0 || len(jit.calls) != 0 || len(jit.new) != 0 || len(sessions.issued) != 0 {
				t.Fatalf("provisioned %d, jit %d/%d, sessions %d; want nothing", ids.provision, len(jit.calls), len(jit.new), len(sessions.issued))
			}
		})
	}
}

// A platform admin whose account holds the identity key still signs in by that key.
func TestResolveKeySignInOfAPlatformAdminStillSignsIn(t *testing.T) {
	admin := Principal{ID: uuid.New(), Email: "root.admin@example.com", Active: true}
	ids := &resolveIdentities{principal: admin}
	_, failure, sessions := resolveRun("oidc", ids, resolveMembers{found: true, role: "member"}, nil)
	if failure != nil || len(sessions.issued) != 1 || sessions.issued[0].PrincipalID != admin.ID {
		t.Fatalf("failure %+v sessions %+v, want one session for the platform admin %v", failure, sessions.issued, admin.ID)
	}
	if ids.provision != 0 {
		t.Fatalf("the key sign-in provisioned %d accounts", ids.provision)
	}
}
