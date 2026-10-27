package oidc

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestResolveRefusesAServiceProviderPrincipalAsDisabled(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		t.Run(protocol, func(t *testing.T) {
			ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true, ServiceProvider: true}}
			jit := &resolveJIT{}
			login, failure, sessions := resolveRun(protocol, ids, resolveMembers{found: true, role: "member"}, jit)
			wantRefused(t, failure, http.StatusForbidden, ReasonPrincipalDeactivated)
			if len(sessions.issued) != 0 || len(jit.calls) != 0 || ids.provision != 0 || login.Tokens.AccessToken != "" {
				t.Fatalf("sessions %d, jit %d, provisions %d, login %+v; want none", len(sessions.issued), len(jit.calls), ids.provision, login)
			}
		})
	}
}

func TestResolveStillSignsInANormalPrincipal(t *testing.T) {
	ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
	_, failure, sessions := resolveRun("oidc", ids, resolveMembers{found: true, role: "member"}, nil)
	if failure != nil || len(sessions.issued) != 1 {
		t.Fatalf("failure %v, sessions %d; want one session", failure, len(sessions.issued))
	}
}
