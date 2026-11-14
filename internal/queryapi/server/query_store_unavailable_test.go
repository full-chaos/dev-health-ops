package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// A live identity read that cannot be made decided nothing about the credential. Every /query mount and /buildinfo
// answer it with go-api's answer for a session read it could not make (Scope.Impersonation:
// WriteAuthFailure(w, "", ErrUnavailable)): 503, never the 401 that tells the client its credential is bad. Every
// refusal of the credential itself keeps the bare 401.

type faultAnswer struct {
	status int
	body   string
	header http.Header
}

// faultRequest sends one request to path on listener: a POST carries document as its body, a GET none.
func faultRequest(s *behaviourStack, listener, method, path, bearer, document string) faultAnswer {
	var body io.Reader
	if method == http.MethodPost {
		raw, _ := json.Marshal(map[string]string{"query": document})
		body = bytes.NewReader(raw)
	}
	request := httptest.NewRequest(method, path, body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	return faultAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}
}

// goAPIUnavailable is go-api's own answer for an identity read it could not make (scope.go Scope.Impersonation).
func goAPIUnavailable() faultAnswer {
	recorder := httptest.NewRecorder()
	policy.WriteAuthFailure(recorder, "", policy.ErrUnavailable)
	return faultAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}
}

// unavailableProblem is "" when got is go-api's unavailable answer: its status, body and content type, with no
// credential challenge and no Retry-After.
func unavailableProblem(got faultAnswer) string {
	want := goAPIUnavailable()
	switch {
	case got.status != http.StatusServiceUnavailable || got.status != want.status:
		return fmt.Sprintf("status %d, want 503 (body %q)", got.status, got.body)
	case got.body != want.body || strings.TrimSpace(got.body) != restUnavailable:
		return fmt.Sprintf("body %q, want %q", got.body, want.body)
	case got.header.Get("Content-Type") != want.header.Get("Content-Type"):
		return fmt.Sprintf("Content-Type %q, want %q", got.header.Get("Content-Type"), want.header.Get("Content-Type"))
	case got.header.Get("Www-Authenticate") != "":
		return fmt.Sprintf("WWW-Authenticate %q on an unavailable answer", got.header.Get("Www-Authenticate"))
	case got.header.Get("Retry-After") != "":
		return fmt.Sprintf("Retry-After %q on an unavailable answer", got.header.Get("Retry-After"))
	}
	return ""
}

// laterMembershipReadFails fails the membership read numbered failAt (1 = the first) with err: /query reads the
// token org's row twice, once in the routed check and once in the carrier (identity_reads_test.go).
type laterMembershipReadFails struct {
	*accessStore
	failAt int
	err    error
	mu     *sync.Mutex
	reads  *int
}

func newLaterMembershipReadFails(base *accessStore, failAt int, err error) laterMembershipReadFails {
	return laterMembershipReadFails{accessStore: base, failAt: failAt, err: err, mu: &sync.Mutex{}, reads: new(int)}
}

func (s laterMembershipReadFails) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	s.mu.Lock()
	*s.reads++
	fail := *s.reads == s.failAt
	s.mu.Unlock()
	if fail {
		return "", false, s.err
	}
	return s.accessStore.Membership(ctx, userID, orgID)
}

// storeFault is one live read that fails, as the store (and key source) of a stack.
type storeFault struct {
	row      string
	store    func() queryAPIStoreBase
	keysDown bool
	refuse   bool
	document string
}

func withStore(shape func(*accessStore)) func() queryAPIStoreBase {
	return func() queryAPIStoreBase {
		store := newAccessStore()
		shape(store)
		return store
	}
}

var storeFaults = map[string]storeFault{
	"users row unavailable": {row: "8", store: withStore(func(s *accessStore) {
		s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	})},
	"auth session row unavailable": {row: "8", store: withStore(func(s *accessStore) {
		s.errSessionState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	})},
	"token org membership unavailable (routed read)": {row: "8", store: withStore(func(s *accessStore) {
		s.errIsMember = fmt.Errorf("down: %w", policy.ErrUnavailable)
	})},
	"token key source unreadable": {row: "8", keysDown: true, store: withStore(func(*accessStore) {})},
	"users row grant missing": {row: "9", store: withStore(func(s *accessStore) {
		s.errUserState = &policy.GrantError{Grant: "SELECT on users (id, is_active, is_superuser, token_version)", Err: errors.New("permission denied")}
	})},
	"users row other store error": {row: "10", store: withStore(func(s *accessStore) {
		s.errUserState = errors.New("syntax error")
	})},
	"holder session row read fails": {row: "12", store: func() queryAPIStoreBase {
		return legacyStore(nil, errors.New("session store timeout"))
	}},
	"carrier membership read unavailable": {row: "13", store: func() queryAPIStoreBase {
		return newLaterMembershipReadFails(newAccessStore(), 2, fmt.Errorf("down: %w", policy.ErrUnavailable))
	}},
	"carrier membership read other store error": {row: "13", store: func() queryAPIStoreBase {
		return newLaterMembershipReadFails(newAccessStore(), 2, errors.New("membership read failed"))
	}},
	"non-holder row read fails on a mutation under the switch": {row: "17", refuse: true, document: legacyMutationDocument, store: func() queryAPIStoreBase {
		return nonHolderStores(nil, errors.New("session store timeout"))["plain member"]
	}},
}

// faultStack is the listener stack over the fault's store, with a user access token for ecUser.
func faultStack(t *testing.T, fault storeFault) (*behaviourStack, string) {
	t.Helper()
	minter := newAccessMinter(t)
	var keys authverify.KeySource = minter.keys
	if fault.keysDown {
		keys = downKeys{}
	}
	stack := newAccessStackRefusing(t, queryAPIUserStore{queryAPIStoreBase: fault.store()}, testAccess(keys, nil, accessTestNow), fault.refuse)
	return stack, minter.mint(ecUser, nil, nil)
}

// TestQueryFamilyAnswersAFailedLiveReadAsUnavailable: on every /query mount, each live read of a user access token's
// identity that fails (rows 8, 9, 10, 12, 13 and 17 of the decision) answers go-api's 503 and runs nothing.
func TestQueryFamilyAnswersAFailedLiveReadAsUnavailable(t *testing.T) {
	quietStdLog(t)
	for name, fault := range storeFaults {
		for _, route := range legacyQueryRoutes {
			t.Run("row "+fault.row+" "+name+" "+route.listener+" "+route.path, func(t *testing.T) {
				stack, token := faultStack(t, fault)
				document := fault.document
				if document == "" {
					document = iaDocument
				}
				got := faultRequest(stack, route.listener, http.MethodPost, route.path, token, document)
				if problem := unavailableProblem(got); problem != "" || len(*stack.seen) != 0 {
					t.Fatalf("%s; resolver ran %d times, want 0", problem, len(*stack.seen))
				}
			})
		}
	}
}

// refusalCell is one refusal of the credential itself: the token, the store it is read against, and the reason the
// refusal is logged with ("" when it is refused before the carrier reads anything).
type refusalCell struct {
	shape  func(*accessStore)
	token  func(m *accessMinter, s *behaviourStack) string
	reason string
}

func mintWith(extra, header map[string]any) func(*accessMinter, *behaviourStack) string {
	return func(m *accessMinter, _ *behaviourStack) string { return m.mint(ecUser, extra, header) }
}

var refusalCells = map[string]refusalCell{
	"bad signature": {token: func(*accessMinter, *behaviourStack) string {
		return newAccessMinterForRefusal().mint(ecUser, nil, nil)
	}, reason: "reason=edge_rejected"},
	"unknown key id":           {token: mintWith(nil, map[string]any{"kid": "rotated-away"}), reason: "reason=edge_rejected"},
	"expired":                  {token: mintWith(map[string]any{"exp": accessTestNow.Unix() - 1}, nil), reason: "reason=edge_rejected"},
	"not yet valid":            {token: mintWith(map[string]any{"nbf": accessTestNow.Unix() + 600, "iat": accessTestNow.Unix() + 600}, nil), reason: "reason=edge_rejected"},
	"wrong audience":           {token: mintWith(map[string]any{"aud": "another-api"}, nil), reason: "reason=edge_rejected"},
	"wrong issuer":             {token: mintWith(map[string]any{"iss": "https://another.issuer.test"}, nil), reason: "reason=edge_rejected"},
	"wrong type header":        {token: mintWith(nil, map[string]any{"typ": "JWT"})},
	"retired HS256 edge token": {token: func(_ *accessMinter, s *behaviourStack) string { return s.edge }, reason: "reason=edge_rejected"},
	"unusable subject":         {token: mintWith(map[string]any{"sub": "prn_not-a-principal"}, nil), reason: "reason=edge_rejected"},
	"unusable actor":           {token: mintWith(map[string]any{"act": map[string]any{"sub": "prn_not-a-principal"}}, nil), reason: "reason=edge_rejected"},
	"actor is the subject":     {token: mintWith(map[string]any{"act": map[string]any{"sub": wirePrincipal(ecUser)}}, nil), reason: "reason=edge_rejected"},
	"no session claim":         {token: mintWith(map[string]any{"sid": nil}, nil), reason: "reason=edge_rejected"},
	"unknown user":             {shape: func(s *accessStore) { delete(s.found, ecUser); delete(s.states, ecUser) }, token: mintWith(nil, nil), reason: "reason=edge_rejected"},
	"inactive user":            {shape: func(s *accessStore) { s.states[ecUser] = policy.UserState{IsActive: false, TokenVersion: 5} }, token: mintWith(nil, nil), reason: "reason=edge_rejected"},
	"session revoked":          {shape: func(s *accessStore) { delete(s.liveSessions, accessTestSession) }, token: mintWith(nil, nil), reason: "reason=edge_rejected"},
	"session of another principal": {shape: func(s *accessStore) {
		s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecTarget, LegacyTokenVersion: i64(5)}
	}, token: mintWith(nil, nil), reason: "reason=edge_rejected"},
	"token version moved":            {shape: func(s *accessStore) { s.states[ecUser] = policy.UserState{IsActive: true, TokenVersion: 6} }, token: mintWith(nil, nil), reason: "reason=edge_rejected"},
	"not a member (row 14)":          {shape: func(s *accessStore) { delete(s.members, [2]uuid.UUID{ecUser, ecOrg}) }, token: mintWith(nil, nil), reason: "reason=edge_not_a_member"},
	"no organization (row 15)":       {token: mintWith(map[string]any{"org_id": nil}, nil), reason: "reason=edge_org_missing"},
	"no credential (row 5)":          {token: func(*accessMinter, *behaviourStack) string { return "" }, reason: "reason=no_carrier"},
	"unparseable bearer (row 6)":     {token: func(*accessMinter, *behaviourStack) string { return "not.a.token" }},
	"retired envelope class (row 6)": {token: func(_ *accessMinter, s *behaviourStack) string { return s.envelope }},
}

// newAccessMinterForRefusal is a minter whose key the stack does not hold, under the stack's key id: its tokens'
// signatures do not verify.
func newAccessMinterForRefusal() *accessMinter {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return &accessMinter{priv: priv}
}

// captureStdLog sends the standard logger to a buffer for the test.
func captureStdLog(t *testing.T) *lockedBuffer {
	t.Helper()
	buffer := &lockedBuffer{}
	previous := log.Writer()
	log.SetOutput(buffer)
	t.Cleanup(func() { log.SetOutput(previous) })
	return buffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// TestQueryFamilyKeepsEveryRefusalAt401: on every /query mount, each refusal of the credential itself, one subtype per
// cell, keeps the bare 401, runs nothing, and is logged as the refusal it is (not as a read that failed).
func TestQueryFamilyKeepsEveryRefusalAt401(t *testing.T) {
	logged := captureStdLog(t)
	for name, cell := range refusalCells {
		for _, route := range legacyQueryRoutes {
			t.Run(name+" "+route.listener+" "+route.path, func(t *testing.T) {
				minter := newAccessMinter(t)
				store := newAccessStore()
				if cell.shape != nil {
					cell.shape(store)
				}
				stack := newAccessStackRefusing(t, queryAPIUserStore{queryAPIStoreBase: store}, testAccess(minter.keys, nil, accessTestNow), false)
				logged.Reset()
				got := faultRequest(stack, route.listener, http.MethodPost, route.path, cell.token(minter, stack), iaDocument)
				if got.status != http.StatusUnauthorized || got.body != queryRefusal || len(*stack.seen) != 0 {
					t.Fatalf("got %d %q, resolver ran %d; want 401 %q and no resolver", got.status, got.body, len(*stack.seen), queryRefusal)
				}
				if cell.reason != "" && !strings.Contains(logged.String(), cell.reason) {
					t.Fatalf("logged %q, want %s", logged.String(), cell.reason)
				}
			})
		}
	}
}

// TestQueryGraphQLRESTAndGoAPIAnswerOneStoreFaultAlike: one store, one fault, every plane: /query, /graphql, a
// query-api REST route (authenticateRESTRequest) and go-api's own writer give the same status, body and content type.
func TestQueryGraphQLRESTAndGoAPIAnswerOneStoreFaultAlike(t *testing.T) {
	quietStdLog(t)
	for name, fault := range map[string]storeFault{
		"users row unavailable":         storeFaults["users row unavailable"],
		"holder session row read fails": storeFaults["holder session row read fails"],
	} {
		t.Run(name, func(t *testing.T) {
			stack, token := faultStack(t, fault)
			for _, plane := range []struct{ listener, path string }{{"internal", "/query"}, {"query", "/query"}, {"query", "/graphql"}} {
				if problem := unavailableProblem(faultRequest(stack, plane.listener, http.MethodPost, plane.path, token, iaDocument)); problem != "" {
					t.Errorf("%s %s: %s", plane.listener, plane.path, problem)
				}
			}
			if len(*stack.seen) != 0 {
				t.Errorf("resolver ran %d times, want 0", len(*stack.seen))
			}
			if name != "users row unavailable" {
				return
			}
			minter := newAccessMinter(t)
			rest := newRESTFixture(t, fault.store().(*accessStore), testAccess(minter.keys, nil, accessTestNow))
			want := goAPIUnavailable()
			if got := rest.do(http.MethodGet, minter.mint(ecUser, nil, nil)); got.Status != want.status || got.Body != strings.TrimSpace(want.body) {
				t.Errorf("REST /api/v1/home: %d %q, want %d %q", got.Status, got.Body, want.status, want.body)
			}
		})
	}
}

// TestGraphQLAnswersAnOtherStoreErrorAtTheUsersReadAsBefore: a users-row error that is not policy.ErrUnavailable.
// /graphql keeps the org scope's 500 (the read is answered by the middleware before the shared carrier check), the
// query-api REST route keeps WriteAuthFailure's 500, and /query answers 503: the named difference of the contract row.
func TestGraphQLAnswersAnOtherStoreErrorAtTheUsersReadAsBefore(t *testing.T) {
	quietStdLog(t)
	fault := storeFaults["users row other store error"]
	t.Run("graphql", func(t *testing.T) {
		stack, token := faultStack(t, fault)
		got := faultRequest(stack, "query", http.MethodPost, "/graphql", token, iaDocument)
		if got.status != http.StatusInternalServerError || got.body != `{"detail":"Internal Server Error"}` || len(*stack.seen) != 0 {
			t.Fatalf("/graphql: %d %q, ran %d; want the 500 and nothing run", got.status, got.body, len(*stack.seen))
		}
	})
	t.Run("rest", func(t *testing.T) {
		minter := newAccessMinter(t)
		rest := newRESTFixture(t, fault.store().(*accessStore), testAccess(minter.keys, nil, accessTestNow))
		if got := rest.do(http.MethodGet, minter.mint(ecUser, nil, nil)); got.Status != http.StatusInternalServerError {
			t.Fatalf("REST /api/v1/home: %+v, want 500", got)
		}
	})
	t.Run("query", func(t *testing.T) {
		stack, token := faultStack(t, fault)
		if problem := unavailableProblem(faultRequest(stack, "internal", http.MethodPost, "/query", token, iaDocument)); problem != "" {
			t.Fatal(problem)
		}
	})
}

// TestBuildInfoAnswersAFailedLiveReadAsUnavailable: /buildinfo checks a user access token through /query's carrier, so
// a platform admin whose live read fails gets the same 503; a refused token and a non-admin keep the bare 401.
func TestBuildInfoAnswersAFailedLiveReadAsUnavailable(t *testing.T) {
	quietStdLog(t)
	admin := func(shape func(*accessStore)) func() queryAPIStoreBase {
		return withStore(func(s *accessStore) { asSuperuser(s); shape(s) })
	}
	for _, listener := range []string{"query", "internal"} {
		for name, fault := range map[string]storeFault{
			"users row unavailable":         {store: admin(func(s *accessStore) { s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable) })},
			"users row other store error":   {store: admin(func(s *accessStore) { s.errUserState = errors.New("syntax error") })},
			"holder session row read fails": {store: admin(func(s *accessStore) { s.errSession = errors.New("session store timeout") })},
			"token key source unreadable":   {keysDown: true, store: admin(func(*accessStore) {})},
		} {
			t.Run(listener+" "+name, func(t *testing.T) {
				stack, token := faultStack(t, fault)
				if problem := unavailableProblem(faultRequest(stack, listener, http.MethodGet, "/buildinfo", token, "")); problem != "" {
					t.Fatal(problem)
				}
			})
		}
		t.Run(listener+" refusals stay 401", func(t *testing.T) {
			stack, token := faultStack(t, storeFault{store: admin(func(*accessStore) {})})
			if got := faultRequest(stack, listener, http.MethodGet, "/buildinfo", token, ""); got.status != http.StatusOK {
				t.Fatalf("platform admin: %d %q, want 200", got.status, got.body)
			}
			bad := newAccessMinterForRefusal().mint(ecUser, nil, nil)
			if got := faultRequest(stack, listener, http.MethodGet, "/buildinfo", bad, ""); got.status != http.StatusUnauthorized || got.body != queryRefusal {
				t.Fatalf("bad signature: %d %q, want 401", got.status, got.body)
			}
			member, memberToken := faultStack(t, storeFault{store: withStore(func(*accessStore) {})})
			if got := faultRequest(member, listener, http.MethodGet, "/buildinfo", memberToken, ""); got.status != http.StatusUnauthorized || got.body != queryRefusal {
				t.Fatalf("non-admin member: %d %q, want 401", got.status, got.body)
			}
		})
	}
}
