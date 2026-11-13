package apiservice

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/acrcredentials"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const revokeRow = "POST /api/v1/acr/credentials/{credential_id}/revoke"

type revokeMembers map[uuid.UUID]string

func (m revokeMembers) Membership(_ context.Context, user, _ uuid.UUID) (string, bool, error) {
	role, ok := m[user]
	return role, ok, nil
}

type recordedRevoker struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordedRevoker) Revoke(_ context.Context, org, credential, actorType, actorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, strings.Join([]string{org, credential, actorType, actorID}, "|"))
	return nil
}

func (r *recordedRevoker) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.calls
	r.calls = nil
	return out
}

func revokeRoutes(members acrcredentials.Members, revoker acrcredentials.Revoker) func(*testing.T, Deps) []httpapi.Route {
	return func(_ *testing.T, deps Deps) []httpapi.Route {
		return acrcredentials.Routes(acrcredentials.Deps{Guard: deps.Guard, Members: members, Revoker: revoker, Logger: quietLogger()})
	}
}

// M-2 of CHAOS-13604: with a configured revoker and member read, the revoke row answers each caller class the same
// with the cell on as off, and the revoke the handler sends (organization, credential, actor type, actor) is the same.
func TestRevokeRowWithAConfiguredRevokerAnswersEveryCallerClassAsLegacy(t *testing.T) {
	members := revokeMembers{probeUser: "admin", probeMember: "member", cellViewer: "viewer", cellPlatformMember: "member", usersCellOwner: "owner"}
	offRevoker, onRevoker := &recordedRevoker{}, &recordedRevoker{}
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerScoped(t, nil, withBoundaryCallers, revokeRoutes(members, offRevoker), nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withBoundaryCallers, revokeRoutes(members, onRevoker), nil)
	offCallers, onCallers := boundaryCallers(offMinter), boundaryCallers(onMinter)
	for _, class := range boundaryClasses {
		pdp.register(onCallers)
		pdp.reset("evaluate")
		legacy := handlerDecidedDo(off, revokeRow, offCallers[class].token, nil)
		got := handlerDecidedDo(on, revokeRow, onCallers[class].token, nil)
		legacyCalls, newCalls := offRevoker.take(), onRevoker.take()
		if got.status != legacy.status || got.body != legacy.body {
			t.Errorf("%s: new %d %s, legacy %d %s", class, got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
		}
		if strings.Join(newCalls, ";") != strings.Join(legacyCalls, ";") {
			t.Errorf("%s: revoke calls %q, legacy %q", class, newCalls, legacyCalls)
		}
		t.Logf("CELL\t%s\t%s\tlegacy=%d:%s revokes=%d\tnew=%d:%s revokes=%d", revokeRow, class, legacy.status, short(legacy.body), len(legacyCalls), got.status, short(got.body), len(newCalls))
		if class == "org admin" && (got.status != http.StatusNoContent || len(newCalls) != 1 || !strings.Contains(newCalls[0], "|admin|")) {
			t.Errorf("org admin: %d %s revokes %q, want 204 and one admin revoke", got.status, got.body, newCalls)
		}
		if class == "org member" && (got.status != http.StatusNoContent || len(newCalls) != 1 || !strings.Contains(newCalls[0], "|user|")) {
			t.Errorf("org member: %d %s revokes %q, want 204 and one user revoke", got.status, got.body, newCalls)
		}
	}
}
