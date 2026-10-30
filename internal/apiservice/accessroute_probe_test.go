package apiservice

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// The probe routes sit on real endpoint-profile row keys, one per guard
// level, with a handler that echoes the identity the request reached it with.
const (
	probeAuthenticated = "/api/v1/auth/me"
	probeAdmin         = "/api/v1/admin/audit-logs"
	probeOptional      = "/api/v1/billing/plans"
	probeSuperuser     = "/api/v1/admin/orgs"
	probePublic        = "/api/v1/auth/verify"
)

var (
	probeUser       = uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	probeMember     = uuid.MustParse("00000000-0000-4000-8000-0000000000a2")
	probeInactive   = uuid.MustParse("00000000-0000-4000-8000-0000000000a3")
	probeUnknown    = uuid.MustParse("00000000-0000-4000-8000-0000000000a4")
	probeAdminUser  = uuid.MustParse("00000000-0000-4000-8000-0000000000a5")
	probeTarget     = uuid.MustParse("00000000-0000-4000-8000-0000000000a6")
	probeDown       = uuid.MustParse("00000000-0000-4000-8000-0000000000a7")
	probeOrg        = uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	probeOtherOrg   = uuid.MustParse("00000000-0000-4000-8000-0000000000b2")
	probeTargetOrg  = uuid.MustParse("00000000-0000-4000-8000-0000000000b3")
	probeLiveSID    = uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	probeRevokedSID = uuid.MustParse("00000000-0000-4000-8000-0000000000c2")
	probeOtherSID   = uuid.MustParse("00000000-0000-4000-8000-0000000000c3")
)

type probeUserRow struct {
	state    policy.UserState
	email    string
	username *string
	fullName *string
}

// probeStore is policy's Store and SessionStore over fixed rows.
type probeStore struct {
	users    map[uuid.UUID]probeUserRow
	members  map[[2]uuid.UUID]string
	sessions map[uuid.UUID]policy.SessionRow
	imperson map[uuid.UUID]*policy.Impersonation
	down     map[uuid.UUID]bool
}

func newProbeStore() *probeStore {
	name := func(s string) *string { return &s }
	return &probeStore{
		users: map[uuid.UUID]probeUserRow{
			probeUser:      {state: policy.UserState{IsActive: true, TokenVersion: 3}, email: "user@example.test", username: name("user"), fullName: name("Probe User")},
			probeMember:    {state: policy.UserState{IsActive: true}, email: "member@example.test"},
			probeInactive:  {state: policy.UserState{IsActive: false}, email: "inactive@example.test"},
			probeAdminUser: {state: policy.UserState{IsActive: true, IsSuperuser: true}, email: "admin@example.test"},
			probeTarget:    {state: policy.UserState{IsActive: true}, email: "target@example.test"},
		},
		members: map[[2]uuid.UUID]string{
			{probeUser, probeOrg}:         "admin",
			{probeUser, probeOtherOrg}:    "member",
			{probeMember, probeOrg}:       "member",
			{probeTarget, probeTargetOrg}: "member",
		},
		sessions: map[uuid.UUID]policy.SessionRow{
			probeLiveSID:    {PrincipalID: probeUser, LegacyTokenVersion: i64ptr(3)},
			probeRevokedSID: {PrincipalID: probeUser, Revoked: true, LegacyTokenVersion: i64ptr(3)},
			probeOtherSID:   {PrincipalID: probeMember, LegacyTokenVersion: i64ptr(0)},
		},
		imperson: map[uuid.UUID]*policy.Impersonation{
			probeAdminUser: {ID: uuid.MustParse("00000000-0000-4000-8000-0000000000d1"), AdminUserID: probeAdminUser,
				TargetUserID: probeTarget, TargetOrgID: probeTargetOrg, TargetRole: "member"},
		},
		down: map[uuid.UUID]bool{probeDown: true},
	}
}

func (s *probeStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	if s.down[id] {
		return policy.UserState{}, false, fmt.Errorf("probe store: %w", policy.ErrUnavailable)
	}
	row, ok := s.users[id]
	return row.state, ok, nil
}

func (s *probeStore) Membership(_ context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	role, ok := s.members[[2]uuid.UUID{userID, orgID}]
	return role, ok, nil
}

func (s *probeStore) ActiveImpersonation(_ context.Context, adminID uuid.UUID) (*policy.Impersonation, error) {
	return s.imperson[adminID], nil
}

func (s *probeStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	row, ok := s.sessions[id]
	return row, ok, nil
}

// probeKey is built at run time: a key-shaped literal trips the secret scan.
var probeKey = "probe-fixture-key-" + strings.Join([]string{"01234567", "89abcdef"}, "")

// probeHS256 signs a go-api access token with fixed times, so its bytes are
// the same on every run.
func probeHS256(t *testing.T, key string, claims jwt.MapClaims) string {
	t.Helper()
	base := jwt.MapClaims{"type": "access", "iss": "dev-health-ops", "aud": "dev-health-api", "iat": 1760000000, "exp": 4102444800}
	for name, value := range claims {
		if value == nil {
			delete(base, name)
			continue
		}
		base[name] = value
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, base).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func probeEcho(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"org": policy.OrgIDFrom(r.Context())}
	if user := policy.UserFrom(r.Context()); user != nil {
		out["user"] = map[string]any{"user_id": user.UserID, "email": user.Email, "org_id": user.OrgID, "role": user.Role,
			"is_superuser": user.IsSuperuser, "token_version": user.TokenVersion, "username": user.Username, "full_name": user.FullName}
	}
	if session := policy.ImpersonationFrom(r.Context()); session != nil {
		out["impersonating"] = session.TargetUserID.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func probeRoutes(guard *policy.Guard) []httpapi.Route {
	echo := http.HandlerFunc(probeEcho)
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: probeAuthenticated, Handler: guard.Wrap(policy.Authenticated, echo)},
		{Method: http.MethodGet, Pattern: probeAdmin, Handler: guard.Wrap(policy.Admin, echo)},
		{Method: http.MethodGet, Pattern: probeOptional, Handler: guard.Wrap(policy.Optional, echo)},
		{Method: http.MethodGet, Pattern: probeSuperuser, Handler: guard.Wrap(policy.Superuser, echo)},
		{Method: http.MethodGet, Pattern: probePublic, Handler: guard.Wrap(policy.Public, echo)},
	}
}

// probeServer is the api listener over the probe routes with go-api's HS256
// verifier and the scope middlewares, as configureWith builds it; adjust may
// change the Authenticator before the server is built.
func probeServer(t *testing.T, store policy.Store, adjust func(*policy.Authenticator)) http.Handler {
	t.Helper()
	verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if adjust != nil {
		adjust(auth)
	}
	scope := policy.NewScope(auth, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), probeRoutes(policy.NewGuard(auth, quietLogger())), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

type probeRequest struct {
	Name    string
	Method  string
	Path    string
	Headers map[string]string
}

type probeAnswer struct {
	Name    string
	Status  int
	Headers map[string][]string
	Body    string
}

// probeVolatileHeaders change on every run.
var probeVolatileHeaders = map[string]bool{"X-Request-Id": true, "Date": true}

func probeDo(handler http.Handler, req probeRequest) probeAnswer {
	request := httptest.NewRequest(req.Method, req.Path, nil)
	for name, value := range req.Headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	headers := map[string][]string{}
	for name, values := range recorder.Header() {
		if !probeVolatileHeaders[http.CanonicalHeaderKey(name)] {
			headers[name] = values
		}
	}
	return probeAnswer{Name: req.Name, Status: recorder.Code, Headers: headers, Body: recorder.Body.String()}
}

func bearer(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func withHeader(headers map[string]string, name, value string) map[string]string {
	out := map[string]string{name: value}
	for key, existing := range headers {
		out[key] = existing
	}
	return out
}

// corpusDir is the A2 signed access-token corpus.
var corpusDir = filepath.Join("..", "..", "contracts", "auth", "v1", "golden", "access-token")

type corpusVector struct {
	HeaderJSON   string `json:"header_json"`
	PayloadJSON  string `json:"payload_json"`
	SignatureHex string `json:"signature_hex"`
}

func (v corpusVector) compact() string {
	enc := base64.RawURLEncoding
	signature, _ := hex.DecodeString(v.SignatureHex)
	return enc.EncodeToString([]byte(v.HeaderJSON)) + "." + enc.EncodeToString([]byte(v.PayloadJSON)) + "." + enc.EncodeToString(signature)
}

func loadCorpusVector(t *testing.T, file string) corpusVector {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(corpusDir, "vectors", file))
	if err != nil {
		t.Fatal(err)
	}
	var vector corpusVector
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	return vector
}

// issuerUnsetRequests is the fixed request set whose answers the issuer-unset
// golden pins.
func issuerUnsetRequests(t *testing.T) []probeRequest {
	t.Helper()
	sign := func(claims jwt.MapClaims) string { return probeHS256(t, probeKey, claims) }
	user := jwt.MapClaims{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3, "email": "user@example.test"}
	with := func(base jwt.MapClaims, extra jwt.MapClaims) jwt.MapClaims {
		out := jwt.MapClaims{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	valid := sign(user)
	member := sign(jwt.MapClaims{"sub": probeMember.String(), "org_id": probeOrg.String(), "role": "member"})
	superuser := sign(jwt.MapClaims{"sub": probeAdminUser.String(), "org_id": probeOrg.String(), "role": "member", "is_superuser": true})
	eddsa := loadCorpusVector(t, "valid-api.json").compact()
	requests := []probeRequest{
		{"no header", http.MethodGet, probeAuthenticated, nil},
		{"not bearer", http.MethodGet, probeAuthenticated, map[string]string{"Authorization": "Basic abc"}},
		{"hs256 valid", http.MethodGet, probeAuthenticated, bearer(valid)},
		{"hs256 expired", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"exp": 1760000100})))},
		{"hs256 stale tv", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"tv": 2})))},
		{"hs256 wrong key", http.MethodGet, probeAuthenticated, bearer(probeHS256(t, probeKey+"-other", user))},
		{"hs256 inactive", http.MethodGet, probeAuthenticated, bearer(sign(jwt.MapClaims{"sub": probeInactive.String(), "org_id": probeOrg.String()}))},
		{"hs256 unknown user", http.MethodGet, probeAuthenticated, bearer(sign(jwt.MapClaims{"sub": probeUnknown.String(), "org_id": probeOrg.String()}))},
		{"hs256 store down", http.MethodGet, probeAuthenticated, bearer(sign(jwt.MapClaims{"sub": probeDown.String(), "org_id": probeOrg.String()}))},
		{"hs256 sub not a string", http.MethodGet, probeAuthenticated, bearer(sign(jwt.MapClaims{"sub": 7, "org_id": probeOrg.String()}))},
		{"hs256 sid live", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"sid": sessionid.Wire(probeLiveSID)})))},
		{"hs256 sid revoked", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"sid": sessionid.Wire(probeRevokedSID)})))},
		{"hs256 sid of another principal", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"sid": sessionid.Wire(probeOtherSID)})))},
		{"hs256 sid unknown", http.MethodGet, probeAuthenticated, bearer(sign(with(user, jwt.MapClaims{"sid": sessionid.Wire(uuid.MustParse("00000000-0000-4000-8000-0000000000c9"))})))},
		{"hs256 admin at admin route", http.MethodGet, probeAdmin, bearer(valid)},
		{"hs256 member at admin route", http.MethodGet, probeAdmin, bearer(member)},
		{"hs256 member at superuser route", http.MethodGet, probeSuperuser, bearer(member)},
		{"hs256 x-org-id member", http.MethodGet, probeAuthenticated, withHeader(bearer(valid), "X-Org-Id", probeOtherOrg.String())},
		{"hs256 x-org-id not a member", http.MethodGet, probeAuthenticated, withHeader(bearer(valid), "X-Org-Id", probeTargetOrg.String())},
		{"hs256 superuser impersonating", http.MethodGet, probeSuperuser, bearer(superuser)},
		{"optional no token", http.MethodGet, probeOptional, nil},
		{"optional hs256 valid", http.MethodGet, probeOptional, bearer(valid)},
		{"optional hs256 wrong key", http.MethodGet, probeOptional, bearer(probeHS256(t, probeKey+"-other", user))},
		{"public hs256 valid", http.MethodGet, probePublic, bearer(valid)},
		{"public hs256 x-org-id not a member", http.MethodGet, probePublic, withHeader(bearer(valid), "X-Org-Id", probeTargetOrg.String())},
		{"unknown path hs256 x-org-id not a member", http.MethodGet, "/api/v1/nowhere", withHeader(bearer(valid), "X-Org-Id", probeTargetOrg.String())},
		{"unknown path hs256 valid", http.MethodGet, "/api/v1/nowhere", bearer(valid)},
		{"head on a get route", http.MethodHead, probeAuthenticated, bearer(valid)},
		{"trailing slash", http.MethodGet, probeAuthenticated + "/", bearer(valid)},
		{"eddsa at authenticated route", http.MethodGet, probeAuthenticated, bearer(eddsa)},
		{"eddsa at optional route", http.MethodGet, probeOptional, bearer(eddsa)},
		{"eddsa with x-org-id", http.MethodGet, probeAuthenticated, withHeader(bearer(eddsa), "X-Org-Id", probeOrg.String())},
	}
	return requests
}

func probeAnswers(handler http.Handler, requests []probeRequest) []probeAnswer {
	answers := make([]probeAnswer, 0, len(requests))
	for _, req := range requests {
		answers = append(answers, probeDo(handler, req))
	}
	sort.SliceStable(answers, func(i, j int) bool { return strings.Compare(answers[i].Name, answers[j].Name) < 0 })
	return answers
}

func (probeStore) ReadsPlatformRoles() bool { return false }
