//go:build integration

package session_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// acceptRoute is auth-service's invite accept route as go-api's client sees
// it: onCall runs during the call (while go-api waits on it), then the route
// answers err or an accepted invite of ownedOrgD.
type acceptRoute struct {
	mu       sync.Mutex
	requests []authprovision.AcceptRequest
	onCall   func(authprovision.AcceptRequest)
	err      error
}

func (a *acceptRoute) AcceptInvite(_ context.Context, in authprovision.AcceptRequest) (authprovision.Accepted, error) {
	a.mu.Lock()
	a.requests = append(a.requests, in)
	onCall, err := a.onCall, a.err
	a.mu.Unlock()
	if onCall != nil {
		onCall(in)
	}
	if err != nil {
		return authprovision.Accepted{}, err
	}
	return authprovision.Accepted{InviteID: uuid.New(), OrgID: uuid.MustParse(ownedOrgD), MembershipID: uuid.New(), Role: "member"}, nil
}

var errNotThisRoute = errors.New("acceptRoute: only the accept route is served")

func (a *acceptRoute) AddMember(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, *uuid.UUID) (authprovision.Membership, error) {
	return authprovision.Membership{}, errNotThisRoute
}
func (a *acceptRoute) ChangeRole(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (authprovision.Membership, error) {
	return authprovision.Membership{}, errNotThisRoute
}
func (a *acceptRoute) RemoveMember(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return errNotThisRoute
}
func (a *acceptRoute) TransferOwnership(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return errNotThisRoute
}
func (a *acceptRoute) CreateInvite(context.Context, authprovision.InviteRequest) (authprovision.Invite, error) {
	return authprovision.Invite{}, errNotThisRoute
}
func (a *acceptRoute) Deprovision(context.Context, uuid.UUID, uuid.UUID) error {
	return errNotThisRoute
}

// writeCounts is every go-api row an invite join writes today.
func writeCounts(t *testing.T, ctx context.Context, st stack) [4]int {
	t.Helper()
	var out [4]int
	if err := st.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM memberships), (SELECT count(*) FROM org_invites WHERE status <> 'pending'),
  (SELECT count(*) FROM audit_logs), (SELECT count(*) FROM refresh_tokens)`).Scan(&out[0], &out[1], &out[2], &out[3]); err != nil {
		t.Fatal(err)
	}
	return out
}

// With the switch on, accept-invite and onboard join_org ask auth-service:
// go-api holds no transaction and no row lock during the call, writes no
// membership, invite or audit row of its own, then issues the session for the
// organization and role auth answered.
func TestInviteJoinsGoThroughAuthWithNoGoAPITransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &acceptRoute{}
	st := startStackProvisioned(t, ctx, authmail.Config{}, nil, nil, route)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	fresh := newcomer(t, ctx, st)
	var callerOpen, lockable []string
	route.onCall = func(in authprovision.AcceptRequest) {
		var open int
		if err := st.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND state LIKE 'idle in transaction%' AND pid <> pg_backend_pid()`).Scan(&open); err != nil {
			t.Errorf("pg_stat_activity: %v", err)
		}
		if open != 0 {
			callerOpen = append(callerOpen, in.Description)
		}
		tx, err := st.pool.Begin(context.Background())
		if err != nil {
			t.Errorf("begin: %v", err)
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		var one int
		if err := tx.QueryRow(context.Background(), `SELECT 1 FROM users WHERE id = $1 FOR UPDATE NOWAIT`, in.PrincipalID).Scan(&one); err != nil {
			lockable = append(lockable, in.Description+": "+err.Error())
		}
	}
	before := writeCounts(t, ctx, st)
	for _, step := range []struct {
		name, path, token, body, description string
	}{
		{"accept-invite", "/api/v1/auth/accept-invite", alice, `{"token": "a.b"}`, "Invite accepted"},
		{"onboard", "/api/v1/auth/onboard", fresh, `{"action": "join_org", "invite_code": "a.b"}`, "Invite accepted during onboarding"},
	} {
		status, body := changeRequest(t, ctx, st, http.MethodPost, step.path, step.token, step.body)
		if status != http.StatusOK || sessionscenario.Field(body, "org_id") != ownedOrgD || sessionscenario.Field(body, "role") != "member" ||
			sessionscenario.Field(body, "access_token") == "" {
			t.Fatalf("%s: %d %s", step.name, status, body)
		}
		route.mu.Lock()
		last := route.requests[len(route.requests)-1]
		route.mu.Unlock()
		if last.Token != "a.b" || last.Description != step.description {
			t.Fatalf("%s: auth request %+v", step.name, last)
		}
	}
	if len(callerOpen) != 0 || len(lockable) != 0 {
		t.Fatalf("go-api held a transaction %v or a caller row lock %v during the auth call", callerOpen, lockable)
	}
	after := writeCounts(t, ctx, st)
	if after[0] != before[0] || after[1] != before[1] || after[2] != before[2] {
		t.Fatalf("go-api wrote rows of its own: memberships/invites/audit %v -> %v", before[:3], after[:3])
	}
}

// A refused accept is today's answer, and no session is issued; an unusable
// token is refused before any call.
func TestInviteJoinRefusalsIssueNoSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &acceptRoute{err: &authprovision.Refused{Status: 400, Reason: "invite_invalid"}}
	st := startStackProvisioned(t, ctx, authmail.Config{}, nil, nil, route)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	before := writeCounts(t, ctx, st)
	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/accept-invite", alice, `{"token": "a.b"}`)
	if status != http.StatusBadRequest || body != `{"detail":{"message":"Invalid or expired invite"}}` {
		t.Fatalf("refused accept: %d %s", status, body)
	}
	route.mu.Lock()
	route.err = authprovision.ErrUnavailable
	route.mu.Unlock()
	status, body = changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/accept-invite", alice, `{"token": "a.b"}`)
	if status != http.StatusServiceUnavailable || body != `{"detail":{"message":"Authentication service unavailable"}}` {
		t.Fatalf("auth down: %d %s", status, body)
	}
	calls := len(route.requests)
	status, body = changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/accept-invite", alice, `{"token": ""}`)
	if status != http.StatusBadRequest || body != `{"detail":{"message":"Invalid or expired invite"}}` || len(route.requests) != calls {
		t.Fatalf("empty token: %d %s, %d calls", status, body, len(route.requests)-calls)
	}
	if after := writeCounts(t, ctx, st); after != before {
		t.Fatalf("a refused join wrote rows or issued a session: %v -> %v", before, after)
	}
}
