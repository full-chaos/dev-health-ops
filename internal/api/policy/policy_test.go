package policy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

var (
	testKey      = strings.Repeat("policy-fixture-", 3)
	userID       = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	ownOrg       = "22222222-2222-4222-8222-222222222222"
	memberOrg    = "33333333-3333-4333-8333-333333333333"
	strangerOrg  = "44444444-4444-4444-8444-444444444444"
	targetUserID = uuid.MustParse("55555555-5555-4555-8555-555555555555")
	targetOrgID  = uuid.MustParse("66666666-6666-4666-8666-666666666666")
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeStore is Store in memory. A non-nil err* field makes that call fail.
type fakeStore struct {
	users          map[uuid.UUID]UserState
	members        map[[2]uuid.UUID]bool
	sessions       map[uuid.UUID]*Impersonation
	errUser        error
	errMember      error
	errSession     error
	userCalls      int
	sessionCalls   int
	membershipSeen [][2]uuid.UUID
}

func (f *fakeStore) UserState(_ context.Context, id uuid.UUID) (UserState, bool, error) {
	f.userCalls++
	if f.errUser != nil {
		return UserState{}, false, f.errUser
	}
	state, found := f.users[id]
	return state, found, nil
}

func (f *fakeStore) IsMember(_ context.Context, user, org uuid.UUID) (bool, error) {
	f.membershipSeen = append(f.membershipSeen, [2]uuid.UUID{user, org})
	if f.errMember != nil {
		return false, f.errMember
	}
	return f.members[[2]uuid.UUID{user, org}], nil
}

func (f *fakeStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*Impersonation, error) {
	f.sessionCalls++
	if f.errSession != nil {
		return nil, f.errSession
	}
	return f.sessions[admin], nil
}

func newStore() *fakeStore {
	return &fakeStore{
		users:    map[uuid.UUID]UserState{userID: {IsActive: true}},
		members:  map[[2]uuid.UUID]bool{{userID, uuid.MustParse(memberOrg)}: true},
		sessions: map[uuid.UUID]*Impersonation{},
	}
}

func claims(mutate func(jwt.MapClaims)) jwt.MapClaims {
	now := time.Now()
	c := jwt.MapClaims{
		"sub": userID.String(), "email": "user@example.com", "org_id": ownOrg, "role": "member",
		"is_superuser": false, "type": "access", "iss": "dev-health-ops", "aud": "dev-health-api",
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "jti": "j", "tv": 0,
	}
	if mutate != nil {
		mutate(c)
	}
	return c
}

func sign(t *testing.T, c jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(testKey))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func authenticator(t *testing.T, store Store) *Authenticator {
	t.Helper()
	verifier, err := edgetoken.New(testKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewAuthenticator(verifier, store, quiet())
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestBearerTokenIsExtractTokenFromHeader(t *testing.T) {
	cases := []struct {
		header string
		token  string
		ok     bool
	}{
		{"", "", false},
		{"Bearer abc", "abc", true},
		{"bearer abc", "abc", true},
		{"BEARER abc", "abc", true},
		{"  Bearer   abc  ", "abc", true},
		{"Bearer\tabc", "abc", true},
		{"Bearer\xa0abc", "abc", true}, // latin-1 NBSP is str.isspace
		{"Bearer\x85abc", "abc", true},
		{"Bearer", "", false},
		{"Bearer a b", "", false},
		{"Basic abc", "", false},
		{"Bearerabc", "", false},
		{"Token abc", "", false},
	}
	for _, test := range cases {
		token, ok := BearerToken(test.header)
		if token != test.token || ok != test.ok {
			t.Errorf("%q: (%q, %v), want (%q, %v)", test.header, token, ok, test.token, test.ok)
		}
	}
}

func TestAuthenticateDecisionTable(t *testing.T) {
	unavailable := errors.Join(ErrUnavailable, errors.New("dial"))
	cases := []struct {
		name   string
		claims func(jwt.MapClaims)
		token  string
		store  func(*fakeStore)
		want   string // "ok", "refused", "unavailable", "internal"
		super  bool
	}{
		{name: "canonical", want: "ok"},
		{name: "bad signature", token: "tamper", want: "refused"},
		{name: "refresh token", claims: func(c jwt.MapClaims) { c["type"] = "refresh" }, want: "refused"},
		{name: "expired", claims: func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Minute).Unix() }, want: "refused"},
		{name: "iat in the future", claims: func(c jwt.MapClaims) { c["iat"] = time.Now().Add(time.Hour).Unix() }, want: "refused"},
		{name: "wrong audience", claims: func(c jwt.MapClaims) { c["aud"] = "other" }, want: "refused"},
		{name: "no audience or issuer", claims: func(c jwt.MapClaims) { delete(c, "aud"); delete(c, "iss") }, want: "ok"},
		{name: "missing sub", claims: func(c jwt.MapClaims) { delete(c, "sub") }, want: "refused"},
		{name: "non-string sub", claims: func(c jwt.MapClaims) { c["sub"] = 7 }, want: "internal"},
		{name: "sub not a uuid", claims: func(c jwt.MapClaims) { c["sub"] = "nope" }, want: "refused"},
		{name: "sub uuid without hyphens", claims: func(c jwt.MapClaims) { c["sub"] = strings.ReplaceAll(userID.String(), "-", "") }, want: "ok"},
		{name: "sub uuid in braces", claims: func(c jwt.MapClaims) { c["sub"] = "{" + userID.String() + "}" }, want: "ok"},
		{name: "user not found", store: func(s *fakeStore) { delete(s.users, userID) }, want: "refused"},
		{name: "user inactive", store: func(s *fakeStore) { s.users[userID] = UserState{} }, want: "refused"},
		{name: "tv absent", claims: func(c jwt.MapClaims) { delete(c, "tv") }, want: "ok"},
		{name: "tv null", claims: func(c jwt.MapClaims) { c["tv"] = nil }, want: "ok"},
		{name: "tv stale", claims: func(c jwt.MapClaims) { c["tv"] = 1 }, want: "refused"},
		{name: "tv newer than row", store: func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, TokenVersion: 2} }, want: "refused"},
		{name: "tv matches row", claims: func(c jwt.MapClaims) { c["tv"] = 2 }, store: func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, TokenVersion: 2} }, want: "ok"},
		{name: "tv string int", claims: func(c jwt.MapClaims) { c["tv"] = " 0 " }, want: "ok"},
		{name: "tv string garbage", claims: func(c jwt.MapClaims) { c["tv"] = "x" }, want: "refused"},
		{name: "tv float truncates", claims: func(c jwt.MapClaims) { c["tv"] = 0.9 }, want: "ok"},
		{name: "tv false", claims: func(c jwt.MapClaims) { c["tv"] = false }, want: "ok"},
		{name: "tv list", claims: func(c jwt.MapClaims) { c["tv"] = []int{0} }, want: "refused"},
		{name: "superuser from the row, not the claim", claims: func(c jwt.MapClaims) { c["is_superuser"] = true }, want: "ok"},
		{name: "row superuser", store: func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, IsSuperuser: true} }, want: "ok", super: true},
		{name: "store unavailable", store: func(s *fakeStore) { s.errUser = unavailable }, want: "unavailable"},
		{name: "store other failure", store: func(s *fakeStore) { s.errUser = errors.New("syntax") }, want: "internal"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := newStore()
			if test.store != nil {
				test.store(store)
			}
			token := test.token
			if token == "" {
				token = sign(t, claims(test.claims))
			} else {
				token = sign(t, claims(nil)) + test.token
			}
			user, err := authenticator(t, store).Authenticate(context.Background(), token)
			got := "ok"
			switch {
			case err == nil:
			case isRefusal(err):
				got = "refused"
			case isUnavailable(err):
				got = "unavailable"
			default:
				got = "internal"
			}
			if got != test.want {
				t.Fatalf("outcome %s (%v), want %s", got, err, test.want)
			}
			if got == "ok" && (user.IsSuperuser != test.super || user.ID != userID) {
				t.Fatalf("user %+v", user)
			}
			// A token whose sub does not parse never reaches the database.
			if test.name == "sub not a uuid" && store.userCalls != 0 {
				t.Fatalf("%d user lookups for an unparseable sub", store.userCalls)
			}
		})
	}
}

func TestPyTruthyAndIntAndUUID(t *testing.T) {
	truthy := map[string]struct {
		value any
		want  bool
	}{
		"nil": {nil, false}, "false": {false, false}, "true": {true, true}, "zero": {0.0, false},
		"one": {1.0, true}, "empty": {"", false}, "text": {"false", true}, "empty list": {[]any{}, false},
		"list": {[]any{1.0}, true}, "empty map": {map[string]any{}, false}, "map": {map[string]any{"a": 1.0}, true},
	}
	for name, test := range truthy {
		if pyTruthy(test.value) != test.want {
			t.Errorf("pyTruthy(%s)", name)
		}
	}
	ints := map[string]struct {
		value int64
		ok    bool
	}{
		"7": {7, true}, " -7 ": {-7, true}, "+7": {7, true}, "1_000": {1000, true}, "1__0": {0, false},
		"_1": {0, false}, "1_": {0, false}, "": {0, false}, "0x1": {0, false}, "1.0": {0, false},
		"\xa07": {0, false}, // raw byte, not a decoded NBSP: never a JSON string value
	}
	for text, test := range ints {
		value, ok := pyIntString(text)
		if value != test.value || ok != test.ok {
			t.Errorf("pyIntString(%q) = (%d, %v)", text, value, ok)
		}
	}
	id := "12345678-1234-4234-8234-123456789abc"
	for _, form := range []string{id, strings.ToUpper(id), strings.ReplaceAll(id, "-", ""), "{" + id + "}",
		"urn:uuid:" + id, "1-2345678-1234-4234-8234-123456789abc"} {
		if parsed, ok := ParsePyUUID(form); !ok || parsed.String() != id {
			t.Errorf("ParsePyUUID(%q) = %v %v", form, parsed, ok)
		}
	}
	for _, form := range []string{"", "nope", id + "0", id[:35], "g2345678-1234-4234-8234-123456789abc"} {
		if _, ok := ParsePyUUID(form); ok {
			t.Errorf("ParsePyUUID(%q) accepted", form)
		}
	}
}

type response struct {
	status int
	body   string
	header http.Header
}

func serve(handler http.Handler, method, path string, headers map[string]string) response {
	request := httptest.NewRequest(method, path, nil)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

// echo reports what the route saw.
var echo = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	user := UserFrom(r.Context())
	who := "-"
	if user != nil {
		who = user.UserID
	}
	imp := "-"
	if session := ImpersonationFrom(r.Context()); session != nil {
		imp = session.TargetOrgID.String()
	}
	_, _ = io.WriteString(w, "org="+OrgIDFrom(r.Context())+" user="+who+" imp="+imp)
})

func TestGuardDecisionTable(t *testing.T) {
	superStore := func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, IsSuperuser: true} }
	cases := []struct {
		name   string
		level  Authz
		auth   string // "" = no header; "valid"; raw header otherwise
		claims func(jwt.MapClaims)
		store  func(*fakeStore)
		status int
		body   string
		bearer bool
	}{
		{name: "public anonymous", level: Public, status: 200},
		{name: "no header", level: Authenticated, status: 401, body: `{"detail":{"message":"Not authenticated"}}`, bearer: true},
		{name: "empty header", level: Authenticated, auth: " ", status: 401, body: `{"detail":{"message":"Invalid authorization header"}}`, bearer: true},
		{name: "basic scheme", level: Authenticated, auth: "Basic x", status: 401, body: `{"detail":{"message":"Invalid authorization header"}}`, bearer: true},
		{name: "bad token", level: Authenticated, auth: "Bearer x.y.z", status: 401, body: `{"detail":{"message":"Invalid or expired token"}}`, bearer: true},
		{name: "valid", level: Authenticated, auth: "valid", status: 200},
		{name: "db unavailable", level: Authenticated, auth: "valid", store: func(s *fakeStore) { s.errUser = errors.Join(ErrUnavailable, errors.New("x")) }, status: 503, body: `{"detail":{"message":"Database temporarily unavailable"}}`},
		{name: "db other", level: Authenticated, auth: "valid", store: func(s *fakeStore) { s.errUser = errors.New("x") }, status: 500, body: `{"detail":"Internal Server Error"}`},
		{name: "admin as member", level: Admin, auth: "valid", status: 403, body: `{"detail":"Admin access required"}`},
		{name: "admin as admin", level: Admin, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = "admin" }, status: 200},
		{name: "admin as owner", level: Admin, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = "owner" }, status: 200},
		{name: "admin as superuser", level: Admin, auth: "valid", store: superStore, status: 200},
		{name: "admin null role", level: Admin, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = nil }, status: 403, body: `{"detail":"Admin access required"}`},
		{name: "superuser as admin", level: Superuser, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = "admin" }, status: 403, body: `{"detail":"Superuser access required"}`},
		{name: "superuser claim only", level: Superuser, auth: "valid", claims: func(c jwt.MapClaims) { c["is_superuser"] = true }, status: 403, body: `{"detail":"Superuser access required"}`},
		{name: "superuser", level: Superuser, auth: "valid", store: superStore, status: 200},
		{name: "admin org without org", level: AdminOrg, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = "admin"; delete(c, "org_id") }, status: 403, body: `{"detail":"Organization context required"}`},
		{name: "admin org", level: AdminOrg, auth: "valid", claims: func(c jwt.MapClaims) { c["role"] = "admin" }, status: 200},
		{name: "admin org as member", level: AdminOrg, auth: "valid", status: 403, body: `{"detail":"Admin access required"}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := newStore()
			if test.store != nil {
				test.store(store)
			}
			auth := authenticator(t, store)
			headers := map[string]string{}
			switch test.auth {
			case "":
			case "valid":
				headers["Authorization"] = "Bearer " + sign(t, claims(test.claims))
			default:
				headers["Authorization"] = test.auth
			}
			got := serve(NewGuard(auth, quiet()).Wrap(test.level, echo), http.MethodGet, "/x", headers)
			if got.status != test.status {
				t.Fatalf("status %d %s, want %d", got.status, got.body, test.status)
			}
			if test.body != "" && got.body != test.body {
				t.Fatalf("body %s, want %s", got.body, test.body)
			}
			if (got.header.Get("WWW-Authenticate") == "Bearer") != test.bearer {
				t.Fatalf("WWW-Authenticate %q", got.header.Get("WWW-Authenticate"))
			}
			if test.status == 200 && test.level != Public && !strings.Contains(got.body, "user="+userID.String()) {
				t.Fatalf("route did not see the user: %s", got.body)
			}
		})
	}
	for level := Public; level <= AdminOrg+1; level++ {
		if level.String() == "" {
			t.Errorf("level %d has no name", level)
		}
	}
}

func TestOrgScopeDecisionTable(t *testing.T) {
	superStore := func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, IsSuperuser: true} }
	cases := []struct {
		name   string
		path   string
		org    string
		auth   string // "" none, "valid", raw otherwise
		claims func(jwt.MapClaims)
		store  func(*fakeStore)
		status int
		body   string
	}{
		{name: "anonymous no header", status: 200, body: "org= "},
		{name: "anonymous with header", org: strangerOrg, status: 200, body: "org= "},
		{name: "refused token with header", org: strangerOrg, auth: "Bearer x.y.z", status: 200, body: "org= "},
		{name: "non-bearer with header", org: strangerOrg, auth: "Basic x", status: 200, body: "org= "},
		{name: "own org from token", auth: "valid", status: 200, body: "org=" + ownOrg + " "},
		{name: "no org claim", auth: "valid", claims: func(c jwt.MapClaims) { delete(c, "org_id") }, status: 200, body: "org= "},
		{name: "header own org", org: ownOrg, auth: "valid", status: 200, body: "org=" + ownOrg + " "},
		{name: "header member org", org: memberOrg, auth: "valid", status: 200, body: "org=" + memberOrg + " "},
		{name: "header member org padded", org: " " + memberOrg + "\t", auth: "valid", status: 200, body: "org=" + memberOrg + " "},
		{name: "header blank is no header", org: "   ", auth: "valid", status: 200, body: "org=" + ownOrg + " "},
		{name: "header stranger org", org: strangerOrg, auth: "valid", status: 403, body: `{"detail":"X-Org-Id not permitted for this user"}`},
		{name: "header not a uuid", org: "nope", auth: "valid", status: 403, body: `{"detail":"X-Org-Id not permitted for this user"}`},
		{name: "header stranger org superuser", org: strangerOrg, auth: "valid", store: superStore, status: 200, body: "org=" + strangerOrg + " "},
		{name: "membership lookup fails", org: strangerOrg, auth: "valid", store: func(s *fakeStore) { s.errMember = errors.New("x") }, status: 500, body: `{"detail":"Internal Server Error"}`},
		{name: "user lookup fails", auth: "valid", store: func(s *fakeStore) { s.errUser = errors.Join(ErrUnavailable, errors.New("x")) }, status: 500, body: `{"detail":"Internal Server Error"}`},
		{name: "acr internal path skips", path: "/api/v1/internal/acr/x", org: strangerOrg, auth: "valid", status: 200, body: "org= "},
		{name: "acr internal path exact prefix", path: "/api/v1/internal/acr", org: strangerOrg, auth: "valid", status: 403},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := newStore()
			if test.store != nil {
				test.store(store)
			}
			headers := map[string]string{}
			if test.org != "" {
				headers["X-Org-Id"] = test.org
			}
			switch test.auth {
			case "":
			case "valid":
				headers["Authorization"] = "Bearer " + sign(t, claims(test.claims))
			default:
				headers["Authorization"] = test.auth
			}
			path := test.path
			if path == "" {
				path = "/x"
			}
			got := serve(NewScope(authenticator(t, store), quiet()).OrgScope(echo), http.MethodGet, path, headers)
			if got.status != test.status || (test.body != "" && !strings.HasPrefix(got.body, test.body)) {
				t.Fatalf("%d %s, want %d %s", got.status, got.body, test.status, test.body)
			}
			// An X-Org-Id that does not parse never reaches the database.
			if test.name == "header not a uuid" && len(store.membershipSeen) != 0 {
				t.Fatalf("membership lookups for an unparseable org: %v", store.membershipSeen)
			}
			if test.status == 403 && got.header.Get("Content-Type") != "application/json" {
				t.Fatalf("403 content type %q", got.header.Get("Content-Type"))
			}
		})
	}
}

func TestImpersonationDecisionTable(t *testing.T) {
	active := &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID,
		TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
	superRow := func(s *fakeStore) { s.users[userID] = UserState{IsActive: true, IsSuperuser: true} }
	superClaim := func(c jwt.MapClaims) { c["is_superuser"] = true }
	cases := []struct {
		name         string
		auth         string
		claims       func(jwt.MapClaims)
		store        func(*fakeStore)
		status       int
		impersonated bool
		userLookups  int
	}{
		{name: "no header", status: 200},
		{name: "claim not superuser skips lookups", auth: "valid", store: superRow, status: 200, userLookups: 0},
		{name: "claim superuser, row not", auth: "valid", claims: superClaim, store: func(s *fakeStore) { s.sessions[userID] = active }, status: 200, userLookups: 1},
		{name: "superuser without session", auth: "valid", claims: superClaim, store: superRow, status: 200, userLookups: 1},
		{name: "superuser with session", auth: "valid", claims: superClaim, store: func(s *fakeStore) { superRow(s); s.sessions[userID] = active }, status: 200, impersonated: true, userLookups: 1},
		{name: "truthy string claim", auth: "valid", claims: func(c jwt.MapClaims) { c["is_superuser"] = "no" }, store: func(s *fakeStore) { superRow(s); s.sessions[userID] = active }, status: 200, impersonated: true, userLookups: 1},
		{name: "session lookup fails open", auth: "valid", claims: superClaim, store: func(s *fakeStore) { superRow(s); s.errSession = errors.New("x") }, status: 200, userLookups: 1},
		{name: "user lookup fails", auth: "valid", claims: superClaim, store: func(s *fakeStore) { s.errUser = errors.New("x") }, status: 500, userLookups: 1},
		{name: "refused token", auth: "Bearer x." + "eyJpc19zdXBlcnVzZXIiOnRydWV9" + ".z", status: 200},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := newStore()
			if test.store != nil {
				test.store(store)
			}
			headers := map[string]string{}
			switch test.auth {
			case "":
			case "valid":
				headers["Authorization"] = "Bearer " + sign(t, claims(test.claims))
			default:
				headers["Authorization"] = test.auth
			}
			scope := NewScope(authenticator(t, store), quiet())
			got := serve(scope.Impersonation(echo), http.MethodGet, "/x", headers)
			if got.status != test.status {
				t.Fatalf("status %d %s", got.status, got.body)
			}
			if store.userCalls != test.userLookups {
				t.Fatalf("%d user lookups, want %d", store.userCalls, test.userLookups)
			}
			hasHeaders := got.header.Get("X-Impersonating") == "true" &&
				got.header.Get("X-Impersonated-User-Id") == targetUserID.String()
			if test.impersonated != hasHeaders {
				t.Fatalf("impersonation headers %v", got.header)
			}
			if test.impersonated != strings.Contains(got.body, "org="+targetOrgID.String()+" user=- imp="+targetOrgID.String()) {
				t.Fatalf("route saw %s", got.body)
			}
		})
	}
}

// TestImpersonationWinsOverOrgScope pins the Python middleware order: the org
// scope resolves first, then an active impersonation replaces it.
func TestImpersonationWinsOverOrgScope(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	scope := NewScope(authenticator(t, store), quiet())
	handler := scope.OrgScope(scope.Impersonation(echo))
	token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true }))
	got := serve(handler, http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token, "X-Org-Id": strangerOrg})
	if got.status != 200 || !strings.HasPrefix(got.body, "org="+targetOrgID.String()) {
		t.Fatalf("%d %s", got.status, got.body)
	}
}

func TestWriteJSONMatchesStarlette(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteDetail(recorder, 418, "a<b>&é", http.Header{"X-Extra": {"1"}})
	if recorder.Body.String() != `{"detail":"a<b>&é"}` || recorder.Code != 418 ||
		recorder.Header().Get("Content-Length") != "20" || recorder.Header().Get("X-Extra") != "1" {
		t.Fatalf("%d %q %v", recorder.Code, recorder.Body.String(), recorder.Header())
	}
}

// TestStoreUnavailableWhenTheDatabaseCannotBeReached pins the 503 class: a
// refused connection is ErrUnavailable, as _db_temporarily_unavailable
// classifies a connection error.
func TestStoreUnavailableWhenTheDatabaseCannotBeReached(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgresql://nobody@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := PGStore{Pool: pool}
	_, _, err = store.UserState(context.Background(), userID)
	if !isUnavailable(err) {
		t.Fatalf("UserState: %v, want ErrUnavailable", err)
	}
	if _, err := store.IsMember(context.Background(), userID, targetOrgID); !isUnavailable(err) {
		t.Fatalf("IsMember: %v, want ErrUnavailable", err)
	}
	if _, err := store.ActiveImpersonation(context.Background(), userID); !isUnavailable(err) {
		t.Fatalf("ActiveImpersonation: %v, want ErrUnavailable", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyStoreError(ctx.Err()); isUnavailable(err) {
		t.Fatal("a cancelled request is not a database outage")
	}
	if err := classifyStoreError(context.DeadlineExceeded); !isUnavailable(err) {
		t.Fatal("a timeout is a database outage")
	}
	if !isUnavailable(classifyStoreError(retryable{})) {
		t.Fatal("an error the driver marks safe to retry is an outage")
	}
	for code, want := range map[string]bool{"08006": true, "08001": true, "57P01": true, "57P03": true, "42501": false, "42P01": false, "57014": false} {
		if got := isUnavailable(classifyStoreError(&pgconn.PgError{Code: code})); got != want {
			t.Errorf("SQLSTATE %s unavailable=%v, want %v", code, got, want)
		}
	}
}

// retryable is an error pgconn.SafeToRetry reports as never sent.
type retryable struct{}

func (retryable) Error() string     { return "not sent" }
func (retryable) SafeToRetry() bool { return true }
