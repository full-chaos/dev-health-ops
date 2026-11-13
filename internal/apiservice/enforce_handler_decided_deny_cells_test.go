package apiservice

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const noRowClass = "org user, no row in the token org"

var (
	deniedCellNoRowUser = uuid.MustParse("00000000-0000-4000-8000-0000000001d1")
	deniedCellNoRowSID  = uuid.MustParse("00000000-0000-4000-8000-0000000001d2")
)

// withDeniedCellCallers adds an active user with a live session and no membership row in the token organization.
func withDeniedCellCallers(store *probeStore) {
	withBoundaryCallers(store)
	withUsersCellCallers(store)
	store.users[deniedCellNoRowUser] = probeUserRow{state: policy.UserState{IsActive: true}, email: "norow@example.test"}
	store.sessions[deniedCellNoRowSID] = policy.SessionRow{PrincipalID: deniedCellNoRowUser, LegacyTokenVersion: i64ptr(0)}
}

type deniedClass struct {
	usersCaller
	headers map[string]string
}

// deniedClasses are the deny-side caller classes of the handler-decided rows: a user with no row in the token
// organization, an X-Org-Id of another organization, no organization claim (with and without a platform role), a live
// impersonation row, and a platform admin with no membership.
func deniedClasses(t *testing.T, minter *eddsaMinter) []deniedClass {
	t.Helper()
	byName := map[string]usersCaller{}
	for _, c := range usersCallers(t, minter) {
		byName[c.name] = c
	}
	noRow := usersCaller{
		name:   noRowClass,
		token:  minter.mint(deniedCellNoRowUser, map[string]any{"sid": sessionid.Wire(deniedCellNoRowSID)}),
		snap:   cellSnapshot{},
		judged: true,
	}
	otherOrg := byName["org admin"]
	otherOrg.name = "org admin, X-Org-Id of another org"
	classes := []deniedClass{
		{usersCaller: noRow},
		{usersCaller: otherOrg, headers: map[string]string{"X-Org-Id": probeOtherOrg.String()}},
	}
	for _, name := range []string{"no platform role, no org claim", "platform admin, no org claim", "impersonating platform admin", "platform admin, no membership"} {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("no caller class %q", name)
		}
		classes = append(classes, deniedClass{usersCaller: c})
	}
	return classes
}

// M-1 of CHAOS-13604: on the three handler-decided rows, the deny-side classes answer the same with the cells on (the
// real evaluator decides) as with the cells off, status and body, on the real handler.
func TestHandlerDecidedRowsAnswerTheDenySideClassesAsLegacy(t *testing.T) {
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerScoped(t, nil, withDeniedCellCallers, realRoutes, nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withDeniedCellCallers, realRoutes, nil)
	offClasses, onClasses := deniedClasses(t, offMinter), deniedClasses(t, onMinter)
	judged := make([]usersCaller, 0, len(onClasses))
	for _, c := range onClasses {
		judged = append(judged, c.usersCaller)
	}
	for key, action := range handlerDecidedActions {
		for i := range onClasses {
			pdp.registerUsers(judged)
			pdp.reset("evaluate")
			if onClasses[i].name == noRowClass {
				continue // a named answer change (acp ruling (b)), pinned apart: orgs/me by the cell below and TestLegacyNoRowTokenOrgAnswersVenueOracle, the revoke row by the revoke cell below
			}
			legacy := handlerDecidedDo(off, key, offClasses[i].token, offClasses[i].headers)
			if calls := pdp.calls(); len(calls) != 0 {
				t.Fatalf("%s / %s: %d decision point calls with the cell off", key, onClasses[i].name, len(calls))
			}
			got := handlerDecidedDo(on, key, onClasses[i].token, onClasses[i].headers)
			calls := pdp.calls()
			result := "EQUAL"
			if got.status != legacy.status || got.body != legacy.body {
				result = "DIFF"
				t.Errorf("%s / %s: new %d %s, legacy %d %s", key, onClasses[i].name, got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
			}
			t.Logf("CELL\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\tpdp_calls=%d(%s)\t%s", key, onClasses[i].name, legacy.status, short(legacy.body), got.status, short(got.body), len(calls), action, result)
		}
	}
}

// M-1 ruling (b), the revoke row: a token-organization caller with no membership row. Legacy: the handler's own 403
// "Organization access denied" (acrcredentials/routes.go:149-151, no revoke sent). With the cell on: the decision point
// denies, 403 "Access denied", no revoke sent. A named answer change, pinned here.
func TestNoRowTokenOrgCallerOnTheRevokeRowIsANamedAnswerChange(t *testing.T) {
	members := revokeMembers{}
	offRevoker, onRevoker := &recordedRevoker{}, &recordedRevoker{}
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerScoped(t, nil, withDeniedCellCallers, revokeRoutes(members, offRevoker), nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withDeniedCellCallers, revokeRoutes(members, onRevoker), nil)
	token := func(m *eddsaMinter) string {
		return m.mint(deniedCellNoRowUser, map[string]any{"sid": sessionid.Wire(deniedCellNoRowSID)})
	}
	offToken, onToken := token(offMinter), token(onMinter)
	pdp.registerUsers([]usersCaller{{name: noRowClass, token: onToken, snap: cellSnapshot{}, judged: true}})
	pdp.reset("evaluate")
	legacy := handlerDecidedDo(off, revokeRow, offToken, nil)
	got := handlerDecidedDo(on, revokeRow, onToken, nil)
	if legacy.status != 403 || strings.TrimSpace(legacy.body) != `{"detail":"Organization access denied"}` {
		t.Errorf("legacy: %d %s, want 403 Organization access denied", legacy.status, legacy.body)
	}
	if got.status != 403 || strings.TrimSpace(got.body) != `{"detail":"Access denied"}` {
		t.Errorf("with the cell on: %d %s, want 403 Access denied", got.status, got.body)
	}
	if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != "acr.credential.revoke" || calls[0].OrganizationID != probeOrg.String() {
		t.Errorf("decision point calls %+v, want one acr.credential.revoke on the token org", calls)
	}
	if c := append(offRevoker.take(), onRevoker.take()...); len(c) != 0 {
		t.Errorf("revokes sent: %q", c)
	}
	t.Logf("CELL\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\tANSWER CHANGE", revokeRow, noRowClass, legacy.status, strings.TrimSpace(legacy.body), got.status, strings.TrimSpace(got.body))
}

// M-1 ruling (b), the orgs/me row: with the org cell on, a token-organization caller with no membership row is denied by
// the decision point, 403 "Access denied", asked once on the token organization. The legacy answer (200 with the
// organization's profile) needs a real Postgres: TestLegacyNoRowTokenOrgAnswersVenueOracle holds it.
func TestNoRowTokenOrgCallerOnOrgsMeIsDeniedByTheDecisionPoint(t *testing.T) {
	pdp := newCellPDP(t)
	on, minter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withDeniedCellCallers, realRoutes, nil)
	token := minter.mint(deniedCellNoRowUser, map[string]any{"sid": sessionid.Wire(deniedCellNoRowSID)})
	pdp.registerUsers([]usersCaller{{name: noRowClass, token: token, snap: cellSnapshot{}, judged: true}})
	pdp.reset("evaluate")
	got := handlerDecidedDo(on, "GET /api/v1/orgs/me", token, nil)
	if got.status != 403 || strings.TrimSpace(got.body) != `{"detail":"Access denied"}` {
		t.Errorf("orgs/me, no row: %d %s, want 403 Access denied", got.status, got.body)
	}
	if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != "org.read" || calls[0].OrganizationID != probeOrg.String() {
		t.Errorf("decision point calls %+v, want one org.read on the token organization", calls)
	}
}
