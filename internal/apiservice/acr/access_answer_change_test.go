package acr

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// The answer-change table of CHAOS-13561 (acp ruling of 2026-11-11 on PLAN-DELTA-13561-1): the access read's
// repository_scope before (the org's repository inventory, for every reader) and after (the reader's own read
// listing), for every caller class in orgs of 3, 99 and 100 repositories (and 0 and 105 for the empty-org path and
// the bug). The new column runs end to end: auth-service's real readable-repositories route over the real
// Evaluator, go-api's real sessionissue client with a workload token, the real AccessStore and the real handler.
// The old column is the base rule (36432d71 access_store.go: repository_scope = the org inventory for every reader,
// 503 acr_access_repository_scope_over_limit above 100) through the same serializer. The acr column applies acr's
// live gate (3001af98 internal/api/principal_access.go requireLiveAccess, accessDenyReason, narrowLiveScope,
// keepOrganizationWideGrant) to both answers as a transcription (code-argued, not executed here).

const answerPlaneKeyID = "answer-change-test"

// answerPlane is auth-service's go-api workload path and go-api's workload token client over one in-memory key.
type answerPlane struct {
	issuer  string
	subject string
	private ed25519.PrivateKey
	set     token.KeySet
}

func newAnswerPlane(t *testing.T) *answerPlane {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: answerPlaneKeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	return &answerPlane{issuer: "https://auth.answer.example.test", subject: session.WirePrincipalID(uuid.New()), private: private, set: set}
}

func (p *answerPlane) KeySet(context.Context) (token.KeySet, error) { return p.set, nil }

func (p *answerPlane) mint() string {
	now := time.Now()
	claims, _ := json.Marshal(contracts.WorkloadTokenClaims{Iss: p.issuer, Aud: "auth-service", Sub: p.subject, PrincipalType: "workload",
		Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300})
	header, _ := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": answerPlaneKeyID, "typ": contracts.WorkloadTokenTyp})
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return input + "." + enc.EncodeToString(ed25519.Sign(p.private, []byte(input)))
}

func (p *answerPlane) client(t *testing.T, base *url.URL) *sessionissue.Client {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": p.mint(), "issued_token_type": workloadtoken.TokenTypeJWT,
			"token_type": "Bearer", "expires_in": 300})
	}))
	t.Cleanup(exchange.Close)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	workload, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: p.issuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := sessionissue.NewWorkload(base, workload, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// grantSnap is one principal's auth state in one organization (authorization.GrantSnapshot).
type grantSnap struct {
	principal    authorization.Status
	platformRole string
	org          string
	orgStatus    authorization.Status
	membership   *authorization.Membership
	grants       []authorization.Grant
}

func (s *grantSnap) PrincipalStatus() authorization.Status { return s.principal }
func (s *grantSnap) PlatformRole() string                  { return s.platformRole }
func (s *grantSnap) OrganizationStatus(org string) authorization.Status {
	if org != s.org {
		return authorization.StatusUnknown
	}
	return s.orgStatus
}
func (s *grantSnap) Membership(org string) (authorization.Membership, bool) {
	if org != s.org || s.membership == nil {
		return authorization.Membership{}, false
	}
	return *s.membership, true
}
func (s *grantSnap) Grants(string) ([]authorization.Grant, error) { return s.grants, nil }
func (s *grantSnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s *grantSnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s *grantSnap) NowUnixNano() int64 {
	return time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC).UnixNano()
}

// snapState is authapi.DecisionState over planted snapshots; it reads only Snapshot.
type snapState struct{ snaps map[uuid.UUID]*grantSnap }

func (s *snapState) Read(ctx context.Context, fn func(authapi.DecisionTx) error) error {
	return fn(snapTx{s})
}

type snapTx struct{ s *snapState }

func (snapTx) Session(context.Context, uuid.UUID) (authapi.DecisionSession, error) {
	return authapi.DecisionSession{}, fmt.Errorf("the listing read a session")
}
func (snapTx) Actor(context.Context, uuid.UUID) (authapi.ActorState, error) {
	return authapi.ActorState{}, fmt.Errorf("the listing read an actor")
}
func (snapTx) ServiceAccountKey(context.Context, uuid.UUID) (authapi.DecisionKey, error) {
	return authapi.DecisionKey{}, fmt.Errorf("the listing read a key")
}
func (t snapTx) Snapshot(_ context.Context, principal uuid.UUID, _ string) (authorization.GrantSnapshot, error) {
	if s, ok := t.s.snaps[principal]; ok {
		return s, nil
	}
	return &grantSnap{}, nil
}
func (snapTx) Unfed(context.Context, uuid.UUID, string) (authapi.UnfedState, error) {
	return authapi.UnfedState{}, fmt.Errorf("the listing read the cache state")
}
func (snapTx) StateVersion(context.Context, uuid.UUID, string) (string, error) {
	return "", fmt.Errorf("the listing read the cache state")
}

// answerRow is one caller class.
type answerRow struct {
	name string
	// member is go-api's membership read (users row active and a membership of an active org); impersonating its
	// admin-keyed impersonation read.
	member, impersonating bool
	snap                  func(org string, inventory []string) *grantSnap
	// credential is the repository scope of the caller's acr credential (issued earlier).
	credential func(inventory []string) []string
	// class is the declared difference: EQUAL, WIRE (the wire differs, acr's answer does not), WIDER_NOT_IN_CATALOG,
	// NARROWER_REVOKED or CAP.
	class map[int]string
}

func memberSnap(org, role string, grants []authorization.Grant) *grantSnap {
	return &grantSnap{principal: authorization.StatusActive, org: org, orgStatus: authorization.StatusActive,
		membership: &authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive}, grants: grants}
}

func repoGrant(org, key string, revoked bool) authorization.Grant {
	return authorization.Grant{OrgID: org, Role: "member", ResourceKind: "repository", ResourceKey: key, Revoked: revoked}
}

func inventoryOf(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("acme/repo-%03d", i)
	}
	return out
}

func every(class string) map[int]string {
	return map[int]string{0: class, 3: class, 99: class, 100: class, 105: class}
}

func classes(base string, overrides map[int]string) map[int]string {
	out := every(base)
	for n, c := range overrides {
		out[n] = c
	}
	return out
}

// answerRows are the caller classes of the ruling. Workload callers (acr workload, operator, legacy and web-assertion
// credentials) never reach the read: acr's requireLiveAccess serves them without it (principal_access.go:95-97), so
// no row can change for them.
func answerRows() []answerRow {
	star := func([]string) []string { return []string{"*"} }
	twoKeys := func(inv []string) []string {
		if len(inv) < 2 {
			return []string{"acme/repo-000", "acme/repo-001"}
		}
		return slices.Clone(inv[:2])
	}
	return []answerRow{
		{name: "org-wide admin (membership default)", member: true, credential: star,
			snap:  func(org string, _ []string) *grantSnap { return memberSnap(org, "admin", nil) },
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "org-wide owner (membership default)", member: true, credential: star,
			snap:  func(org string, _ []string) *grantSnap { return memberSnap(org, "owner", nil) },
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "org-wide viewer (membership default)", member: true, credential: star,
			snap:  func(org string, _ []string) *grantSnap { return memberSnap(org, "viewer", nil) },
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "live wildcard grant", member: true, credential: star,
			snap: func(org string, _ []string) *grantSnap {
				return memberSnap(org, "member", []authorization.Grant{repoGrant(org, "*", false)})
			},
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "named reader, grants in the catalog", member: true, credential: twoKeys,
			snap: func(org string, inv []string) *grantSnap {
				keys := twoKeys(inv)
				return memberSnap(org, "member", []authorization.Grant{repoGrant(org, keys[0], false), repoGrant(org, keys[1], false)})
			},
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "named reader, one grant not in the catalog", member: true,
			credential: func(inv []string) []string { return append(twoKeys(inv)[:1], "zeta/not-synced") },
			snap: func(org string, inv []string) *grantSnap {
				return memberSnap(org, "member", []authorization.Grant{repoGrant(org, twoKeys(inv)[0], false), repoGrant(org, "zeta/not-synced", false)})
			},
			class: classes("WIDER_NOT_IN_CATALOG", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "named reader, one grant revoked since issue", member: true, credential: twoKeys,
			snap: func(org string, inv []string) *grantSnap {
				keys := twoKeys(inv)
				return memberSnap(org, "member", []authorization.Grant{repoGrant(org, keys[0], false), repoGrant(org, keys[1], true)})
			},
			class: classes("NARROWER_REVOKED", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "named reader, every grant revoked since issue", member: true, credential: twoKeys,
			snap: func(org string, inv []string) *grantSnap {
				keys := twoKeys(inv)
				return memberSnap(org, "member", []authorization.Grant{repoGrant(org, keys[0], true), repoGrant(org, keys[1], true)})
			},
			class: classes("NARROWER_REVOKED", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "named reader, 257 live grants", member: true, credential: twoKeys,
			snap: func(org string, _ []string) *grantSnap {
				var grants []authorization.Grant
				for i := range 257 {
					grants = append(grants, repoGrant(org, fmt.Sprintf("acme/repo-%03d", i), false))
				}
				return memberSnap(org, "member", grants)
			},
			class: classes("CAP", map[int]string{0: "EQUAL", 105: "CAP"})},
		{name: "no membership, no grant (another org's user)", member: false, credential: star,
			snap: func(string, []string) *grantSnap {
				return &grantSnap{principal: authorization.StatusActive, org: uuid.NewString(), orgStatus: authorization.StatusActive}
			},
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "superadmin (platform_admin, no membership; product rules rule 1)", member: false, credential: star,
			snap: func(org string, _ []string) *grantSnap {
				return &grantSnap{principal: authorization.StatusActive, platformRole: "platform_admin", org: org, orgStatus: authorization.StatusActive}
			},
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "org-wide admin while impersonating", member: true, impersonating: true, credential: star,
			snap:  func(org string, _ []string) *grantSnap { return memberSnap(org, "admin", nil) },
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
		{name: "member of a suspended org", member: false, credential: star,
			snap: func(org string, _ []string) *grantSnap {
				s := memberSnap(org, "admin", nil)
				s.orgStatus = authorization.StatusSuspended
				return s
			},
			class: classes("WIRE", map[int]string{0: "EQUAL", 105: "FIX"})},
	}
}

// accessAnswer is one read: the status, the code of a 503, and repository_scope.
type accessAnswer struct {
	status int
	code   string
	kind   string
	repos  []string
}

func (a accessAnswer) String() string {
	if a.status != http.StatusOK {
		return fmt.Sprintf("%d %s", a.status, a.code)
	}
	if a.kind == RepositoryScopeRepos {
		return fmt.Sprintf("200 repos[%d]", len(a.repos))
	}
	return "200 " + a.kind
}

// oldAnswer is the base rule: repository_scope is the org inventory for every reader, refused above 100.
func oldAnswer(inventory []string) accessAnswer {
	if len(inventory) > 100 {
		return accessAnswer{status: http.StatusServiceUnavailable, code: "acr_access_repository_scope_over_limit"}
	}
	if len(inventory) == 0 {
		return accessAnswer{status: http.StatusOK, kind: RepositoryScopeNone}
	}
	return accessAnswer{status: http.StatusOK, kind: RepositoryScopeRepos, repos: inventory}
}

// acrEffective is acr's live gate on one answer for a credential with scope grant (transcribed, see the file
// comment): 503 for a refused read, 403 for a member false or impersonating read, then the scope step.
func acrEffective(a accessAnswer, member, impersonating bool, grant []string) string {
	switch {
	case a.status != http.StatusOK:
		return "503 " + a.code
	case !member, impersonating:
		return "403 refused before the scope"
	}
	universal := slices.Contains(grant, "*")
	switch a.kind {
	case RepositoryScopeOrganization:
		if universal {
			return "served *"
		}
		return "served " + strings.Join(grant, ",")
	case RepositoryScopeNone:
		return "409 no_repository"
	}
	var effective []string
	for _, repo := range a.repos {
		if universal || slices.Contains(grant, repo) {
			effective = append(effective, repo)
		}
	}
	if len(effective) == 0 {
		return "409 no_repository"
	}
	if universal {
		return "served *"
	}
	return "served " + strings.Join(effective, ",")
}

func TestAccessReadAnswerChangeTable(t *testing.T) {
	plane := newAnswerPlane(t)
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: plane.issuer, Keys: plane, Subject: plane.subject})
	if err != nil {
		t.Fatal(err)
	}
	state := &snapState{snaps: map[uuid.UUID]*grantSnap{}}
	rows, err := json.Marshal(map[string]any{"rows": []map[string]any{{"service": authapi.AccessService, "method": "POST",
		"route": sessionissue.PathReadableRepositories, "accepted_credential_classes": []string{"workload_token"}}}})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.ReadableRepositoriesRoutes(authapi.ReadableRepositoriesDeps{State: state, Evaluator: authorization.NewEvaluator(),
		Workload: workload, ProfileRows: rows})
	if err != nil || len(routes) != 1 {
		t.Fatalf("auth-service listing route: %d routes, %v", len(routes), err)
	}
	mux := http.NewServeMux()
	mux.Handle(routes[0].Method+" "+routes[0].Pattern, routes[0].Handler)
	auth := httptest.NewServer(mux)
	t.Cleanup(auth.Close)
	base, err := url.Parse(auth.URL)
	if err != nil {
		t.Fatal(err)
	}
	listing := plane.client(t, base)

	var table []string
	allowed := map[string]bool{"EQUAL": true, "WIRE": true, "WIDER_NOT_IN_CATALOG": true, "NARROWER_REVOKED": true, "CAP": true,
		"FIX": true, "EQUAL_REFUSED": true}
	for _, n := range []int{3, 99, 100, 0, 105} {
		inventory := inventoryOf(n)
		for _, row := range answerRows() {
			t.Run(fmt.Sprintf("%d repos/%s", n, row.name), func(t *testing.T) {
				f := newAccessFixture()
				f.inventory.repos = inventory
				if !row.member {
					delete(f.reads.members, [2]uuid.UUID{accessUser, accessOrg})
				}
				f.reads.impersonating[accessUser] = row.impersonating
				state.snaps[accessUser] = row.snap(accessOrg.String(), inventory)
				store := f.store()
				store.Listing = listing
				rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
				got := accessAnswer{status: rec.Code}
				var body struct {
					Code            string `json:"code"`
					Member          bool   `json:"member"`
					Impersonating   bool   `json:"impersonating"`
					RepositoryScope struct {
						Kind  string   `json:"kind"`
						Repos []string `json:"repos"`
					} `json:"repository_scope"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("body %s: %v", rec.Body, err)
				}
				got.code, got.kind, got.repos = body.Code, body.RepositoryScope.Kind, body.RepositoryScope.Repos
				if rec.Code == http.StatusOK && (body.Member != row.member || body.Impersonating != row.impersonating) {
					t.Fatalf("member %v impersonating %v, want %v %v", body.Member, body.Impersonating, row.member, row.impersonating)
				}
				old := oldAnswer(inventory)
				credential := row.credential(inventory)
				oldEff := acrEffective(old, row.member, row.impersonating, credential)
				newEff := acrEffective(got, row.member, row.impersonating, credential)
				class := "EQUAL"
				switch {
				case old.String() == got.String() && slices.Equal(old.repos, got.repos):
				case oldEff == newEff && strings.HasPrefix(oldEff, "503"):
					class = "EQUAL_REFUSED"
				case oldEff == newEff:
					class = "WIRE"
				case strings.HasPrefix(newEff, "503 acr_access_repository_allowlist_too_large"):
					class = "CAP"
				case strings.HasPrefix(oldEff, "503 acr_access_repository_scope_over_limit") && !strings.HasPrefix(newEff, "503"):
					class = "FIX"
				case strings.HasPrefix(oldEff, "503") && strings.HasPrefix(newEff, "503"):
					class = "EQUAL_REFUSED"
				case strings.Contains(newEff, "zeta/not-synced") && !strings.Contains(oldEff, "zeta/not-synced"):
					class = "WIDER_NOT_IN_CATALOG"
				case len(newEff) < len(oldEff) || strings.HasPrefix(newEff, "409"):
					class = "NARROWER_REVOKED"
				default:
					class = "UNEXPLAINED"
				}
				if !allowed[class] || class != row.class[n] {
					t.Errorf("class %s, want %s: old %s -> acr %q; new %s -> acr %q", class, row.class[n], old, oldEff, got, newEff)
				}
				table = append(table, fmt.Sprintf("| %d | %s | %s | %s | %s | %s | %s |", n, row.name, old, got, oldEff, newEff, class))
			})
		}
	}
	if out := os.Getenv("ANSWER_CHANGE_TABLE_OUT"); out != "" {
		header := "| repos | caller class | old read | new read | acr old | acr new | class |\n|---|---|---|---|---|---|---|\n"
		if err := os.WriteFile(out, []byte(header+strings.Join(table, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
