package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// One identity per /graphql request: the principal, the org (the verified
// scope org: the X-Org-Id after the membership check, else the token's org),
// the role the caller holds IN THAT ORG, and the impersonation target. Every
// consumer reads that one value: the org scope that verified it, the response
// headers, the resolvers (through their claims), and the identity log line.
// These cells run the real middleware, the real pipeline and the real gqlgen
// executor with its resolvers; only the stores are fakes, so what a resolver
// did is observed, not inferred from source.

const (
	irBusFactorDocument  = "query IdentityBusFactor($orgId: String!) { busFactor(orgId: $orgId) { orgId } }"
	irDataHealthDocument = "query IdentityDataHealth($team: ID!) { dataHealth(team: $team) { __typename } }"
)

// recordingClickHouse answers every statement with no rows and records each
// string a statement was bound with, so a test sees which org a resolver read.
type recordingClickHouse struct {
	mu    sync.Mutex
	bound []string
}

func (c *recordingClickHouse) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, binding := range bindings {
		if text, ok := binding.Value.(string); ok {
			c.bound = append(c.bound, text)
		}
	}
	return emptyRowsDrilldownScanner{}, nil
}

func (c *recordingClickHouse) boundOrgs(orgs ...uuid.UUID) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for _, text := range c.bound {
		for _, org := range orgs {
			if text == org.String() {
				seen[text] = true
			}
		}
	}
	return seen
}

// identityResolutionHarness is /graphql over the real chain and executor.
func identityResolutionHarness(t *testing.T, store policy.Store) (http.Handler, *recordingClickHouse) {
	t.Helper()
	verifier, _ := iaVerifier(t)
	clickHouse := &recordingClickHouse{}
	executor := newGraphQLServer(&graph.Resolver{ClickHouse: clickHouse})
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"busFactor": true, "dataHealth": true})
	mux.Register("busFactor", executor)
	mux.Register("dataHealth", executor)
	byDigest := map[string]string{digestHex(irBusFactorDocument): "busFactor", digestHex(irDataHealthDocument): "dataHealth"}
	auth := ecEdgeAuth(t, store)
	pipeline := newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, auth, store, "", nil)
	return internalidentity.Public(graphQLEdgeChain(pipeline, graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})), clickHouse
}

func irGraphQL(t *testing.T, handler http.Handler, token, orgHeader, document string, variables map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	if orgHeader != "" {
		request.Header.Set("X-Org-Id", orgHeader)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// irCaptureIdentityLog sends the Debug log to a buffer for the test.
func irCaptureIdentityLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// TestGraphQLEdgeResolvesOneIdentityPerRequest: a caller whose token names
// org A with role "owner" holds "member" in A and "admin" in B. Selecting B
// (X-Org-Id) runs the resolvers in B with B's role; selecting nothing runs
// them in A with A's membership role, never the token's; selecting an org the
// caller does not belong to is the scope's 403 and nothing runs. The store is
// read once per fact, and the resolvers read no identity state at all.
func TestGraphQLEdgeResolvesOneIdentityPerRequest(t *testing.T) {
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "owner", tokenVersion: 5})
	stranger := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	for name, tc := range map[string]struct {
		header   string
		org      uuid.UUID
		role     string
		operator bool // dataHealth is served (admin/owner/operator in the org)
		reads    string
	}{
		"no X-Org-Id: the token's org, the membership role there": {"", ecOrg, "member", false, irFacts(irUser(), irMember(ecOrg))},
		"X-Org-Id B: B, the role held in B":                       {irOtherOrg.String(), irOtherOrg, "admin", true, irFacts(irUser(), irMember(irOtherOrg))},
		"X-Org-Id B spelled without hyphens: still B":             {strings.ReplaceAll(irOtherOrg.String(), "-", ""), irOtherOrg, "admin", true, irFacts(irUser(), irMember(irOtherOrg))},
	} {
		store := newCountingIdentityStore(&fakeEdgeStore{
			states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
			found:   map[uuid.UUID]bool{ecUser: true},
			members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true, {ecUser, irOtherOrg}: true},
			roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "member", {ecUser, irOtherOrg}: "admin"},
		})
		handler, clickHouse := identityResolutionHarness(t, store)
		logged := irCaptureIdentityLog(t)

		busFactor := irGraphQL(t, handler, token, tc.header, irBusFactorDocument, map[string]any{"orgId": tc.org.String()})
		if busFactor.Code != http.StatusOK || !strings.Contains(busFactor.Body.String(), `"busFactor":{"orgId":"`+tc.org.String()+`"}`) {
			t.Errorf("%s: busFactor %d %s, want the resolver to answer for %s", name, busFactor.Code, busFactor.Body.String(), tc.org)
		}
		if got := clickHouse.boundOrgs(ecOrg, irOtherOrg); len(got) != 1 || !got[tc.org.String()] {
			t.Errorf("%s: ClickHouse was read for %v, want only %s", name, got, tc.org)
		}
		if got := store.reads(); got != tc.reads {
			t.Errorf("%s: identity reads %q, want %q (the resolvers read none)", name, got, tc.reads)
		}
		if line := logged.String(); !strings.Contains(line, "org_id="+tc.org.String()) || !strings.Contains(line, "role="+tc.role) {
			t.Errorf("%s: identity log %q, want org_id=%s role=%s", name, line, tc.org, tc.role)
		}
		if busFactor.Header().Get("X-Impersonating") != "" {
			t.Errorf("%s: X-Impersonating set without a session", name)
		}

		dataHealth := irGraphQL(t, handler, token, tc.header, irDataHealthDocument, map[string]any{"team": "t1"})
		denied := strings.Contains(dataHealth.Body.String(), "Data health requires operator access")
		if dataHealth.Code != http.StatusOK || denied == tc.operator {
			t.Errorf("%s: dataHealth %d %s, want served=%v for role %q", name, dataHealth.Code, dataHealth.Body.String(), tc.operator, tc.role)
		}
	}

	// An org the caller does not belong to: the scope's 403, nothing runs.
	store := newCountingIdentityStore(&fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	})
	handler, clickHouse := identityResolutionHarness(t, store)
	refused := irGraphQL(t, handler, token, stranger.String(), irBusFactorDocument, map[string]any{"orgId": stranger.String()})
	if refused.Code != http.StatusForbidden || refused.Body.String() != `{"detail": "X-Org-Id not permitted for this user"}` || len(clickHouse.bound) != 0 {
		t.Errorf("non-member X-Org-Id: %d %s bound=%v, want the 403 and nothing run", refused.Code, refused.Body.String(), clickHouse.bound)
	}

	// The membership row cannot be read: refused (the 500), nothing runs.
	failing := &fakeEdgeStore{
		states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:  map[uuid.UUID]bool{ecUser: true}, errIsMember: errors.New("membership read failed"),
	}
	handler, clickHouse = identityResolutionHarness(t, failing)
	for _, header := range []string{"", irOtherOrg.String()} {
		recorder := irGraphQL(t, handler, token, header, irBusFactorDocument, map[string]any{"orgId": ecOrg.String()})
		if recorder.Code != http.StatusInternalServerError || len(clickHouse.bound) != 0 {
			t.Errorf("membership unreadable (X-Org-Id %q): %d bound=%v, want the 500 and nothing run", header, recorder.Code, clickHouse.bound)
		}
	}
}

// TestGraphQLEdgeImpersonationIsTheOneIdentity: while impersonating, the
// target's org and role are the identity every consumer reads, whatever org
// the client selects: the headers name the target, the resolvers answer for
// the target's org, and the log line says so.
func TestGraphQLEdgeImpersonationIsTheOneIdentity(t *testing.T) {
	session := &policy.Impersonation{AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: irTargetOrg, TargetRole: "viewer"}
	store := newCountingIdentityStore(&fakeEdgeStore{
		states:   map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:    map[uuid.UUID]bool{ecUser: true},
		members:  map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true, {ecUser, irOtherOrg}: true},
		sessions: map[uuid.UUID]*policy.Impersonation{ecUser: session},
	})
	handler, clickHouse := identityResolutionHarness(t, store)
	logged := irCaptureIdentityLog(t)
	recorder := irGraphQL(t, handler, irToken(t, true), irOtherOrg.String(), irBusFactorDocument, map[string]any{"orgId": irTargetOrg.String()})
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"orgId":"`+irTargetOrg.String()+`"`) ||
		recorder.Header().Get("X-Impersonated-User-Id") != ecTarget.String() {
		t.Fatalf("%d %s headers=%v, want the target's org answered under the target's headers", recorder.Code, recorder.Body.String(), recorder.Header())
	}
	if got := clickHouse.boundOrgs(ecOrg, irOtherOrg, irTargetOrg); len(got) != 1 || !got[irTargetOrg.String()] {
		t.Errorf("ClickHouse was read for %v, want only the target's org", got)
	}
	if line := logged.String(); !strings.Contains(line, "org_id="+irTargetOrg.String()) || !strings.Contains(line, "role=viewer") || !strings.Contains(line, "impersonation_active=true") {
		t.Errorf("identity log %q, want the target's org and role", line)
	}
}

// TestRoleInOrgDecisionTable: the role a caller holds in the request's org,
// over every combination of member, superuser and whose org it is.
func TestRoleInOrgDecisionTable(t *testing.T) {
	own, other := ecOrg.String(), irOtherOrg.String()
	for name, tc := range map[string]struct {
		superuser bool
		org       string
		member    bool
		role      string
		allowed   bool
	}{
		"member of the token's org":                  {false, own, true, "viewer", true},
		"member of another org":                      {false, other, true, "viewer", true},
		"superuser, member":                          {true, other, true, "viewer", true},
		"superuser, not a member, another org":       {true, other, false, "", true},
		"superuser, not a member, the token's org":   {true, own, false, "", false},
		"not a superuser, not a member, another org": {false, other, false, "", false},
		"not a superuser, not a member, own org":     {false, own, false, "", false},
	} {
		role, allowed := roleInOrg(&policy.User{OrgID: own, Role: "owner", IsSuperuser: tc.superuser}, tc.org, "viewer", tc.member)
		if role != tc.role || allowed != tc.allowed {
			t.Errorf("%s: role %q allowed %v, want %q %v", name, role, allowed, tc.role, tc.allowed)
		}
	}
}

// TestQueryEdgeCarrierStatesTheMembershipRole: on /query too, the edge token's
// role is the role held in the token's org, never the role the token states.
func TestQueryEdgeCarrierStatesTheMembershipRole(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "viewer"},
	}
	handler, seen := iaDispatchWithEdge(t, nil, ecEdgeAuth(t, store), store)
	rec := ecPost(handler, ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "owner", tokenVersion: 5}))
	if rec.Code != http.StatusOK || len(*seen) != 1 || (*seen)[0].Role != "viewer" || (*seen)[0].OrgID != ecOrg.String() {
		t.Fatalf("got %d claims=%+v, want role viewer (the membership's) in the token's org", rec.Code, *seen)
	}
}
