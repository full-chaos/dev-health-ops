package authapi

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The cells of POST /internal/v1/principals/readable-repositories (CHAOS-13561, acp rulings Q1 and Q2): go-api's read
// listing of one user principal in one organization, keyed by ids, on go-api's own workload token. The route literal
// is written out so the cells build, and fail, on a tree without the route.
const readableRoute = "/internal/v1/principals/readable-repositories"

func readableRowsDoc(classes ...string) []byte {
	raw, err := json.Marshal(map[string]any{"rows": []map[string]any{{
		"service": AccessService, "method": "POST", "route": readableRoute, "accepted_credential_classes": classes,
	}}})
	if err != nil {
		panic(err)
	}
	return raw
}

type readableFixture struct {
	t         *testing.T
	state     *fakeDecisionState
	server    http.Handler
	logs      *bytes.Buffer
	principal uuid.UUID
	org       string
}

func newReadableFixture(t *testing.T) *readableFixture {
	t.Helper()
	f := &readableFixture{t: t, state: &fakeDecisionState{snapshots: map[snapKey]*fakeSnapshot{}}, logs: &bytes.Buffer{},
		principal: uuid.New(), org: uuid.NewString()}
	routes, err := ReadableRepositoriesRoutes(ReadableRepositoriesDeps{
		State: f.state, Evaluator: authorization.NewEvaluator(), Workload: testIssueWorkload(),
		ProfileRows: readableRowsDoc(workloadTokenClass), Logger: slog.New(slog.NewJSONHandler(f.logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("%d routes mounted, want 1", len(routes))
	}
	mux := http.NewServeMux()
	for _, route := range MarkResponseModels(routes) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	f.server = mux
	return f
}

func (f *readableFixture) body() string {
	return `{"principal_id":"` + f.principal.String() + `","organization_id":"` + f.org + `"}`
}

func (f *readableFixture) do(body string, header map[string]string) (int, map[string]any) {
	f.t.Helper()
	request := httptest.NewRequest(http.MethodPost, readableRoute, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	f.server.ServeHTTP(response, request)
	var answer map[string]any
	if strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
			f.t.Fatalf("answer %q: %v", response.Body, err)
		}
	}
	return response.Code, answer
}

func (f *readableFixture) plant(s *fakeSnapshot) {
	f.state.snapshots[snapKey{f.principal, f.org}] = s
}

func (f *readableFixture) member(role string, grants []authorization.Grant) *fakeSnapshot {
	return &fakeSnapshot{
		principal: authorization.StatusActive, org: f.org, orgStatus: authorization.StatusActive,
		membership: &authorization.Membership{OrgID: f.org, Role: role, Status: authorization.StatusActive},
		grants:     grants, now: authorizeNow,
	}
}

// signedBearer is a token signed with the default go-api workload key, of header typ, with claims; the subject and
// audience are whatever claims say, so another workload and a user token can be minted with the trusted key.
func signedBearer(typ string, claims any) map[string]string {
	testIssueSetup()
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "kid": testIssueKeyID, "typ": typ})
	if err != nil {
		panic(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		panic(err)
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	return map[string]string{"Authorization": "Bearer " + input + "." + enc.EncodeToString(ed25519.Sign(testIssue.private, []byte(input)))}
}

func workloadClaims(sub, aud string) contracts.WorkloadTokenClaims {
	now := time.Now()
	return contracts.WorkloadTokenClaims{Iss: goldenIssuer, Aud: aud, Sub: sub, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300}
}

func TestReadableRepositoriesRouteMountsOnlyWithItsRowAndTheGoAPIWorkload(t *testing.T) {
	for name, tc := range map[string]struct {
		workload bool
		rows     []byte
		mounted  bool
	}{
		"go-api workload, row of the workload token": {true, readableRowsDoc(workloadTokenClass), true},
		"no go-api workload path":                    {false, readableRowsDoc(workloadTokenClass), false},
		"no row":                                     {true, []byte(`{"rows":[]}`), false},
		"row lists another class too":                {true, readableRowsDoc(workloadTokenClass, "access_token"), false},
		"row lists another class only":               {true, readableRowsDoc("access_token"), false},
	} {
		t.Run(name, func(t *testing.T) {
			deps := ReadableRepositoriesDeps{State: &fakeDecisionState{}, Evaluator: authorization.NewEvaluator(), ProfileRows: tc.rows,
				Logger: slog.New(slog.DiscardHandler)}
			if tc.workload {
				deps.Workload = testIssueWorkload()
			}
			routes, err := ReadableRepositoriesRoutes(deps)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(routes) == 1 && routes[0].Pattern == readableRoute; got != tc.mounted {
				t.Fatalf("mounted %v (%d routes), want %v", got, len(routes), tc.mounted)
			}
		})
	}
}

// Only go-api's workload identity may call the route (acp ruling Q1): any other workload, any user token and every
// other bearer is the one 401, and nothing is read.
func TestReadableRepositoriesAdmitsOnlyTheGoAPIWorkloadToken(t *testing.T) {
	f := newReadableFixture(t)
	f.plant(f.member("member", nil))
	testIssueSetup()
	userClaims := map[string]any{"iss": goldenIssuer, "aud": "dev-health-api", "sub": session.WirePrincipalID(f.principal),
		"scope": "api:access", "iat": time.Now().Unix(), "exp": time.Now().Unix() + 300, "jti": uuid.NewString()}
	for name, tc := range map[string]struct {
		header map[string]string
		status int
	}{
		"the go-api workload token":                {issuerBearer(), http.StatusOK},
		"another workload, same key and audience":  {signedBearer(contracts.WorkloadTokenTyp, workloadClaims(session.WirePrincipalID(uuid.New()), "auth-service")), http.StatusUnauthorized},
		"the go-api workload for another audience": {signedBearer(contracts.WorkloadTokenTyp, workloadClaims(testIssue.subject, "dev-health-api")), http.StatusUnauthorized},
		"a user access token":                      {signedBearer("at+jwt", userClaims), http.StatusUnauthorized},
		"a user access token for auth-service":     {signedBearer("at+jwt", map[string]any{"iss": goldenIssuer, "aud": "auth-service", "sub": session.WirePrincipalID(f.principal)}), http.StatusUnauthorized},
		"a user token typed as a workload token":   {signedBearer(contracts.WorkloadTokenTyp, userClaims), http.StatusUnauthorized},
		"the retired session issuer credential":    {retiredSessionIssuerBearer, http.StatusUnauthorized},
		"an acr credential":                        {map[string]string{"Authorization": "Bearer fcacr_0123456789abcdef"}, http.StatusUnauthorized},
		"no bearer":                                {nil, http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			before := f.state.reads
			code, answer := f.do(f.body(), tc.header)
			if code != tc.status {
				t.Fatalf("%d %v, want %d", code, answer, tc.status)
			}
			if tc.status == http.StatusUnauthorized {
				if answer["reason_code"] != reasonInvalidCredentials {
					t.Fatalf("refusal %v, want %s", answer, reasonInvalidCredentials)
				}
				if f.state.reads != before {
					t.Fatalf("a refused bearer read the decision state")
				}
			}
		})
	}
}

func TestReadableRepositoriesRefusesABodyThatIsNotOnePair(t *testing.T) {
	f := newReadableFixture(t)
	p, o := f.principal.String(), f.org
	for name, body := range map[string]string{
		"not json":          `principal_id`,
		"empty":             `{}`,
		"no organization":   `{"principal_id":"` + p + `"}`,
		"no principal":      `{"organization_id":"` + o + `"}`,
		"another member":    `{"principal_id":"` + p + `","organization_id":"` + o + `","action":"acr.evidence.read"}`,
		"a member twice":    `{"principal_id":"` + p + `","principal_id":"` + p + `","organization_id":"` + o + `"}`,
		"upper case":        `{"principal_id":"` + strings.ToUpper(p) + `","organization_id":"` + o + `"}`,
		"a wire principal":  `{"principal_id":"` + session.WirePrincipalID(f.principal) + `","organization_id":"` + o + `"}`,
		"null":              `{"principal_id":null,"organization_id":"` + o + `"}`,
		"trailing data":     f.body() + `{}`,
		"an organization *": `{"principal_id":"` + p + `","organization_id":"*"}`,
	} {
		t.Run(name, func(t *testing.T) {
			code, answer := f.do(body, issuerBearer())
			if code != http.StatusBadRequest || answer["reason_code"] != reasonMalformed {
				t.Fatalf("%d %v, want 400 %s", code, answer, reasonMalformed)
			}
		})
	}
	if f.state.reads != 0 {
		t.Fatalf("a malformed body read the decision state %d time(s)", f.state.reads)
	}
}

func readableKeys(t *testing.T, answer map[string]any) []string {
	t.Helper()
	raw, ok := answer["repositories"].([]any)
	if !ok {
		t.Fatalf("no repositories array in %v", answer)
	}
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = v.(string)
	}
	return out
}

// The answer is Evaluator.ReadableRepositories for acr.context.read over the principal's own snapshot: the same rule as
// POST /v1/authorize:repositories (CHAOS-13542), keyed by ids, no catalog read.
func TestReadableRepositoriesAnswersThePrincipalsReadListing(t *testing.T) {
	f := newReadableFixture(t)
	named3, keys3 := ownGrants(f.org, 3)
	named256, keys256 := ownGrants(f.org, 256)
	named257, _ := ownGrants(f.org, 257)
	noLive := listedGrant(f.org, "acme/gone")
	noLive.Revoked = true
	platformAdmin := &fakeSnapshot{principal: authorization.StatusActive, platformRole: "platform_admin", org: f.org,
		orgStatus: authorization.StatusActive, now: authorizeNow}
	suspended := f.member("member", nil)
	suspended.orgStatus = authorization.StatusSuspended
	inactive := f.member("member", nil)
	inactive.principal = authorization.StatusSuspended
	for name, tc := range map[string]struct {
		snap   *fakeSnapshot
		scope  string
		keys   []string
		reason string
	}{
		"viewer by membership default":  {f.member("viewer", nil), ReadableScopeOrganization, nil, ""},
		"member by membership default":  {f.member("member", nil), ReadableScopeOrganization, nil, ""},
		"admin by membership default":   {f.member("admin", nil), ReadableScopeOrganization, nil, ""},
		"owner by membership default":   {f.member("owner", nil), ReadableScopeOrganization, nil, ""},
		"a live wildcard grant":         {f.member("member", []authorization.Grant{listedGrant(f.org, "*")}), ReadableScopeOrganization, nil, ""},
		"named, 3 grants":               {f.member("member", named3), ReadableScopeRepositories, keys3, ""},
		"named, 256 grants":             {f.member("member", named256), ReadableScopeRepositories, keys256, ""},
		"named, 257 grants":             {f.member("member", named257), ReadableScopeDenied, nil, "repository_allowlist_too_large"},
		"named, no live grant":          {f.member("member", []authorization.Grant{noLive}), ReadableScopeRepositories, []string{}, ""},
		"platform admin, no membership": {platformAdmin, ReadableScopeDenied, nil, "no_membership"},
		"suspended organization":        {suspended, ReadableScopeDenied, nil, "organization_inactive"},
		"inactive principal":            {inactive, ReadableScopeDenied, nil, "principal_inactive"},
	} {
		t.Run(name, func(t *testing.T) {
			f.plant(tc.snap)
			code, answer := f.do(f.body(), issuerBearer())
			if code != http.StatusOK || answer["schema_version"] != "readable_repositories.v1" || answer["scope"] != tc.scope {
				t.Fatalf("%d %v, want 200 scope %s", code, answer, tc.scope)
			}
			_, hasKeys := answer["repositories"]
			_, hasReason := answer["reason_code"]
			switch tc.scope {
			case ReadableScopeRepositories:
				if got := readableKeys(t, answer); !slices.Equal(got, tc.keys) || hasReason {
					t.Fatalf("keys %v (reason %v), want %v", got, answer["reason_code"], tc.keys)
				}
			case ReadableScopeDenied:
				if answer["reason_code"] != tc.reason || hasKeys {
					t.Fatalf("%v, want denied %s and no repositories", answer, tc.reason)
				}
			default:
				if hasKeys || hasReason {
					t.Fatalf("%v: an organization answer carries no repositories and no reason", answer)
				}
			}
		})
	}
}

// No decision cache (acp ruling Q1): every call reads the state again, so a revoke between two calls is in the second.
func TestReadableRepositoriesIsReadLiveOnEveryCall(t *testing.T) {
	f := newReadableFixture(t)
	grants, keys := ownGrants(f.org, 2)
	snap := f.member("member", grants)
	f.plant(snap)
	if _, answer := f.do(f.body(), issuerBearer()); !slices.Equal(readableKeys(t, answer), keys) {
		t.Fatalf("first call %v, want %v", answer, keys)
	}
	revoked := slices.Clone(grants)
	for i := range revoked {
		if revoked[i].ResourceKey == keys[0] {
			revoked[i].Revoked = true
		}
	}
	snap.grants = revoked
	if _, answer := f.do(f.body(), issuerBearer()); !slices.Equal(readableKeys(t, answer), keys[1:]) {
		t.Fatalf("after a revoke %v, want %v", answer, keys[1:])
	}
	if f.state.reads != 2 || f.state.snapReads != 2 {
		t.Fatalf("2 calls ran %d transaction(s) and %d snapshot read(s), want 2 and 2", f.state.reads, f.state.snapReads)
	}
}

// A state failure is the call's 503, never a deny and never a list.
func TestReadableRepositoriesFailsClosedOnItsState(t *testing.T) {
	for name, set := range map[string]func(f *readableFixture){
		"transaction": func(f *readableFixture) { f.state.beginErr = errors.New("connection refused") },
		"snapshot":    func(f *readableFixture) { f.state.snapErr = errors.New("connection refused") },
		"grant read": func(f *readableFixture) {
			s := f.member("member", nil)
			s.grantsErr = errors.New("timeout")
			f.plant(s)
		},
		"no clock": func(f *readableFixture) { s := f.member("member", nil); s.now = 0; f.plant(s) },
		"grant read, named": func(f *readableFixture) {
			s := f.member("member", []authorization.Grant{listedGrant(f.org, "acme/a")})
			s.grantsErr = fmt.Errorf("timeout")
			f.plant(s)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newReadableFixture(t)
			set(f)
			code, answer := f.do(f.body(), issuerBearer())
			if code != http.StatusServiceUnavailable || answer["reason_code"] != reasonStoreUnavailable {
				t.Fatalf("%d %v, want 503 %s", code, answer, reasonStoreUnavailable)
			}
			if _, listed := answer["repositories"]; listed {
				t.Fatalf("a failed read listed repositories: %v", answer)
			}
		})
	}
}
