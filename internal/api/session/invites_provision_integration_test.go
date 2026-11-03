//go:build integration

package session_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
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
	mu        sync.Mutex
	requests  []authprovision.AcceptRequest
	onCall    func(authprovision.AcceptRequest)
	err       error
	onboards  []authprovision.OnboardRequest
	onOnboard func(authprovision.OnboardRequest)
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
func (a *acceptRoute) OnboardOrganization(_ context.Context, in authprovision.OnboardRequest) (authprovision.Born, error) {
	a.mu.Lock()
	a.onboards = append(a.onboards, in)
	onCall, err := a.onOnboard, a.err
	a.mu.Unlock()
	if onCall != nil {
		onCall(in)
	}
	if err != nil {
		return authprovision.Born{}, err
	}
	member := uuid.New()
	return authprovision.Born{OrgID: uuid.MustParse(ownedOrgD), MembershipID: &member}, nil
}
func (a *acceptRoute) CreateOrganization(context.Context, authprovision.CreateOrganizationRequest) (authprovision.Born, error) {
	return authprovision.Born{}, errNotThisRoute
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

// joinTimeout bounds one join request: a go-api lock or transaction held
// across the auth call can block go-api's own session issue for good.
const joinTimeout = 20 * time.Second

// joinRequest is changeRequest with joinTimeout. A request with no answer in
// time fails at once, naming the likely cause and what the lock probe saw.
func joinRequest(t *testing.T, ctx context.Context, st stack, path, token, body string, lockable func() []string) (int, string) {
	t.Helper()
	bounded, cancel := context.WithTimeout(ctx, joinTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(bounded, http.MethodPost, st.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Forwarded-For", "203.0.113.9")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s: no answer within %s (%v): go-api holds a row lock or a transaction across the auth call that blocks its own join; lock probe: %v",
			path, joinTimeout, err, lockable())
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
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
		status, body := joinRequest(t, ctx, st, step.path, step.token, step.body, func() []string { return lockable })
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

// With the switch on, onboard create_org asks auth-service, with no go-api
// transaction open and no go-api row written, then issues the owner's session.
func TestOnboardCreateOrgGoesToAuthWithTheSwitchOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &acceptRoute{}
	st := startStackProvisioned(t, ctx, authmail.Config{}, nil, nil, route)
	fresh := newcomer(t, ctx, st)
	var open int
	route.onOnboard = func(authprovision.OnboardRequest) {
		if err := st.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND state LIKE 'idle in transaction%' AND pid <> pg_backend_pid()`).Scan(&open); err != nil {
			t.Errorf("pg_stat_activity: %v", err)
		}
	}
	var orgsBefore int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM organizations`).Scan(&orgsBefore); err != nil {
		t.Fatal(err)
	}
	before := writeCounts(t, ctx, st)
	status, body := joinRequest(t, ctx, st, "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Acme Ops"}`, func() []string { return nil })
	if status != http.StatusOK || sessionscenario.Field(body, "org_id") != ownedOrgD || sessionscenario.Field(body, "role") != "owner" ||
		sessionscenario.Field(body, "access_token") == "" {
		t.Fatalf("create_org: %d %s", status, body)
	}
	if len(route.onboards) != 1 || route.onboards[0].Name != "Acme Ops" || route.onboards[0].Description != "Organization created during onboarding" {
		t.Fatalf("auth requests %+v", route.onboards)
	}
	if open != 0 {
		t.Fatalf("go-api held %d transactions during the auth call", open)
	}
	var orgsAfter int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM organizations`).Scan(&orgsAfter); err != nil {
		t.Fatal(err)
	}
	after := writeCounts(t, ctx, st)
	if orgsAfter != orgsBefore || after[0] != before[0] || after[2] != before[2] {
		t.Fatalf("go-api wrote rows of its own: organizations %d -> %d, memberships/audit %v -> %v", orgsBefore, orgsAfter, before, after)
	}
}

func TestOnboardCreateOrgRefusalAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &acceptRoute{err: &authprovision.Refused{Status: 409, Reason: "already_member"}}
	st := startStackProvisioned(t, ctx, authmail.Config{}, nil, nil, route)
	fresh := newcomer(t, ctx, st)
	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Acme Ops"}`)
	if status != http.StatusBadRequest || body != `{"detail":{"message":"Already onboarded"}}` {
		t.Fatalf("a raced member: %d %s", status, body)
	}
	route.mu.Lock()
	route.err = authprovision.ErrUnavailable
	route.mu.Unlock()
	status, body = changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Acme Ops"}`)
	if status != http.StatusServiceUnavailable || body != `{"detail":{"message":"Authentication service unavailable"}}` {
		t.Fatalf("auth down: %d %s", status, body)
	}
	calls := len(route.onboards)
	status, _ = changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "   "}`)
	if status != http.StatusUnprocessableEntity || len(route.onboards) != calls {
		t.Fatalf("a blank name: %d, calls %d -> %d", status, calls, len(route.onboards))
	}
}

// The switch off: go-api's own transaction writes the rows; no auth call.
func TestOnboardCreateOrgUnchangedSwitchOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStackProvisioned(t, ctx, authmail.Config{}, nil, nil, nil)
	fresh := newcomer(t, ctx, st)
	status, body := joinRequest(t, ctx, st, "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Local Ops"}`, func() []string { return nil })
	if status != http.StatusOK || sessionscenario.Field(body, "role") != "owner" {
		t.Fatalf("create_org: %d %s", status, body)
	}
	var rows int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM organizations o JOIN memberships m ON m.org_id = o.id
WHERE o.name = 'Local Ops' AND m.role = 'owner' AND o.slug LIKE 'local-ops-%'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("go-api rows = %d, err %v", rows, err)
	}
}
