//go:build integration

package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authruntime"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/issuerstub"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// Probe (not for merge): ACR context-read scopes of one org, per subject, on the legacy path and the grants paths.
// L  = query-api POST /graphql ACRRepositoryScopes (web readCatalogCandidates), query-api built by Build with the LIVE
//      settings (QUERY_API_PDP_MODE=enforce, QUERY_API_AUTHZ_ENFORCE incl. analytics, AUTH_PLATFORM_ROLES_OWNED=true).
// G1 = auth-service POST /v1/authorize key "*" then POST /v1/authorize:batch per L name (web fb139e8b grantScopes).
// G2 = auth-service POST /v1/authorize:repositories (web b540a244 readRepositoryListing).
// The decision point is authruntime's production builder over the migrated auth schema; query-api's PDP client is
// its real workload client over AUTH_PROXY_URL. Every response body is logged (PARITY-BODY lines) for the web replay.

const (
	parityContextRead = "acr.context.read"
	parityWebService  = "dev-health-web"
)

var parityRepos = []string{"acme/alpha", "acme/beta", "acme/gamma"}

// parityEnforce is the LIVE query-api enforce set (deploy cd3fa8a2 render :4996-4997).
const parityEnforce = "analytics,metrics,git,work_items,integrations,reports,org"

type parityKey struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason_code"`
	Scope   string `json:"repository_scope,omitempty"`
}

type paritySeen struct {
	lStatus  int
	lNames   []string
	lOK      bool
	star     parityKey
	batch    map[string]parityKey
	g2Status int
	g2       parityKey
	g2List   []string
}

// parityWant is the predicted answer of each arm for one subject (from the code read at ops 3c44dbc6).
type parityWant struct {
	// lServed: L answers 200 with lNames; else L refuses (any status but 200).
	lServed bool
	// lStatus, when set, is the exact status of a refused L.
	lStatus int
	lNames  []string
	// batchAllowed is the set of L names the per-key reads allow; batchReason the reason of every other key.
	batchAllowed []string
	batchReason  string
	g2Allowed    bool
	g2Reason     string
	g2Scope      string
	g2List       []string
	// sameScopes is whether the live web's legacy scopes (L names) equal its grants scopes (G1).
	sameScopes bool
}

var parityWants = map[string]parityWant{
	// A: platform admin (auth platform_admin assignment, public.users.is_superuser false), no membership. Observed in run 4
	// (merge 5612ee36): L 401 edge_not_a_member (query-api roleInOrg admits admin.orgs only for an org other than the
	// token's), G1 and G2 no_membership: both paths refuse.
	"A_platform_admin_no_membership": {lServed: true, lStatus: 401, batchAllowed: nil, batchReason: "repository_not_granted",
		g2Allowed: true, g2Reason: "no_membership", sameScopes: true},
	// M: member with one named grant row (acme/beta): the control whose answers are known to differ.
	"M_member_named_grant": {lServed: true, lNames: parityRepos, batchAllowed: []string{"acme/beta"}, batchReason: "repository_not_granted",
		g2Allowed: true, g2List: []string{"acme/beta"}, sameScopes: true},
	// M0: member, no grant row: membership default, org-wide on the grants path.
	"M0_member_no_grant": {lServed: true, lNames: parityRepos, batchAllowed: parityRepos, g2Allowed: true, g2Scope: "organization",
		sameScopes: true},
	// N: no membership, no platform role: refused on both paths.
	"N_non_member": {lServed: false, lStatus: 401, batchReason: "no_membership", g2Allowed: false, g2Reason: "no_membership", sameScopes: true},
}

func TestAdminACRScopeParityLegacyVersusGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	env := platformroleseam.Start(ctx, t, true)
	ids, err := identity.New(identity.Config{Pool: env.Admin, Schema: env.Schema})
	if err != nil {
		t.Fatal(err)
	}
	ch, _, raw := migratedClickHouse(ctx, t)
	issuer := startParityIssuer(t)

	org := uuid.New()
	pgseed.Org(ctx, t, env.Admin, org.String(), "free")
	if err := ids.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	for _, repo := range parityRepos {
		if err := raw.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s', '%s', now64(3), now64(3), '%s', 'github')",
			uuid.New(), repo, org)); err != nil {
			t.Fatalf("seed repos: %v", err)
		}
	}

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := env.Admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	subjects := map[string]uuid.UUID{
		"A_platform_admin_no_membership": env.User(ctx, t, false, true),
		"M_member_named_grant":           env.User(ctx, t, false, false),
		"M0_member_no_grant":             env.User(ctx, t, false, false),
		"N_non_member":                   env.User(ctx, t, false, false),
	}
	pgseed.Membership(ctx, t, env.Admin, uuid.NewString(), subjects["M_member_named_grant"].String(), org.String(), "member")
	pgseed.Membership(ctx, t, env.Admin, uuid.NewString(), subjects["M0_member_no_grant"].String(), org.String(), "member")
	// resource_grants is written only through audit.Commit; the row is seeded with the triggers off, as the decisionstate
	// integration tests seed grants (decisionstate_integration_test.go armed).
	grantTx, err := env.Admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = grantTx.Rollback(ctx) }()
	if _, err := grantTx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := grantTx.Exec(ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'member', 'repository', 'acme/beta')`,
		subjects["M_member_named_grant"], org); err != nil {
		t.Fatalf("seed the named grant: %v", err)
	}
	if err := grantTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var assigned int
	if err := env.Admin.QueryRow(ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = 'platform_admin'`,
		subjects["A_platform_admin_no_membership"]).Scan(&assigned); err != nil || assigned != 1 {
		t.Fatalf("subject A platform_admin assignments = %d (%v), want 1", assigned, err)
	}
	tokens := map[string]string{}
	for name, user := range subjects {
		sid := uuid.New()
		hash := sha256.Sum256([]byte("adminscope-session:" + sid.String()))
		exec(`INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version) VALUES ($1, $2, $3, now() + interval '2 hours', 0)`,
			sid, user, hash[:])
		tokens[name] = issuer.tokens.MustMint(t, edtokens.Claims{UserID: user, OrgID: org.String(), SessionID: &sid})
	}

	// The decision point: auth-service's authorize routes (production builder), callers query-api and web.
	pool, err := pgxpool.New(ctx, env.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	signer := token.NewFileSigner(token.FileConfig{KeyPath: issuer.keyPath, KeyID: issuerstub.DefaultKeyID})
	queryAPICaller, webCaller := uuid.New(), uuid.New()
	for _, p := range []struct {
		id   uuid.UUID
		name string
	}{{queryAPICaller, "query-api-authorize"}, {webCaller, "web-authorize"}} {
		exec(`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', $2)`, p.id, p.name)
	}
	queryAPISubject, webSubject := session.WirePrincipalID(queryAPICaller), session.WirePrincipalID(webCaller)
	cfg := authconfig.Config{TokenIssuerURL: issuer.url, AuthorizeCallerSubjects: []string{queryAPISubject, webSubject},
		PlatformRolesOwned: true, UsersOwned: true}
	routes, err := authruntime.AuthorizeRoutesWithoutFeed(cfg, slog.New(slog.DiscardHandler), pool, env.Schema, signer)
	if err != nil {
		t.Fatal(err)
	}
	served, err := httpapi.NewServer(httpapi.ServerOptions{Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var pdpMu sync.Mutex
	var pdpSeen []string
	authorizeHandler := served.Handler()
	pdp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == workloadtoken.ExchangePath {
			parityMintWorkload(w, r, signer, issuer.url, queryAPISubject)
			return
		}
		rec := httptest.NewRecorder()
		authorizeHandler.ServeHTTP(rec, r)
		pdpMu.Lock()
		pdpSeen = append(pdpSeen, fmt.Sprintf("%s %s -> %d", r.Method, r.URL.Path, rec.Code))
		pdpMu.Unlock()
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(pdp.Close)
	web := parityWebClient(t, signer, issuer.url, webSubject, pdp.URL)

	// query-api: the real Build with the LIVE switch values; rig values are named in the probe record.
	envelopePub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_REGISTRY_POSTGRES_URI": env.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    writeTestJWKS(t, envelopePub),
		"GO_API_ENVELOPE_ISSUER":       itTestIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     itTestAudience,
		"DEV_HEALTH_ENV":               "ci",
		accessTokenIssuerEnvVar:        issuer.url,
		"AUTH_DATABASE_SCHEMA":         "auth",
		"AUTH_PROXY_SESSIONS":          "true",
		platformRolesOwnedEnv:          "true",
		"QUERY_API_PDP_MODE":           "enforce",
		"QUERY_API_AUTHZ_ENFORCE":      parityEnforce,
		"AUTH_PROXY_IMPERSONATION":     "true",
		"AUTH_PROXY_URL":               pdp.URL,
		"AUTH_WORKLOAD_BOOTSTRAP_FILE": parityBootstrapFile(t),
		"AUTH_WORKLOAD_ISSUER":         issuer.url,
	}
	for _, name := range switchNames(t) {
		settings[name] = "true"
	}
	plane, err := Build(func(key string) string { return settings[key] })
	if err != nil {
		t.Fatalf("build query-api: %v", err)
	}
	t.Cleanup(plane.Close)
	_, loopback, _ := net.ParseCIDR("127.0.0.1/32")
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, []*net.IPNet{loopback})
	for _, listener := range []*Listener{public, internal} {
		if err := listener.Start(ctx); err != nil {
			t.Fatal(err)
		}
		listener := listener
		t.Cleanup(func() { _ = listener.Shutdown(context.Background()) })
	}
	queryBase := "http://" + public.Address()

	seen := map[string]paritySeen{}
	names := make([]string, 0, len(subjects))
	for name := range subjects {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		bearer := tokens[name]
		var s paritySeen
		// L: the catalog read, as web sends it (ops.ts readCatalogCandidates).
		lBody, _ := json.Marshal(map[string]any{"query": registeredAcrRepositoryScopesDocument, "variables": map[string]any{"orgId": org.String()}})
		status, body := parityPost(t, http.DefaultClient, queryBase+"/graphql", bearer, lBody)
		parityLog(t, name, "L", status, body)
		s.lStatus = status
		var catalog struct {
			Data struct {
				Catalog struct {
					Values []struct {
						Value string `json:"value"`
					} `json:"values"`
				} `json:"catalog"`
			} `json:"data"`
		}
		if status == http.StatusOK && json.Unmarshal(body, &catalog) == nil {
			s.lOK = true
			for _, v := range catalog.Data.Catalog.Values {
				s.lNames = append(s.lNames, v.Value)
			}
		}
		candidates := s.lNames
		if !s.lOK {
			candidates = parityRepos
		}
		// G1: the live web grants path: key "*" first, then one batch of the candidates.
		status, body = parityPost(t, web, pdp.URL+"/v1/authorize", "", parityRequest(bearer, org.String(), "*"))
		parityLog(t, name, "G1.star", status, body)
		s.star = parityDecision(body)
		requests := make([]json.RawMessage, 0, len(candidates))
		for _, key := range candidates {
			requests = append(requests, parityRequest(bearer, org.String(), key))
		}
		batchBody, _ := json.Marshal(map[string]any{"schema_version": "authorization_batch_request.v1", "requests": requests})
		status, body = parityPost(t, web, pdp.URL+"/v1/authorize:batch", "", batchBody)
		parityLog(t, name, "G1.batch", status, body)
		s.batch = parityBatch(body, candidates)
		// G2: the listing route.
		status, body = parityPost(t, web, pdp.URL+"/v1/authorize:repositories", "", parityRequest(bearer, org.String(), "*"))
		parityLog(t, name, "G2", status, body)
		s.g2Status = status
		s.g2 = parityDecision(body)
		s.g2List = parityAllowlist(body)
		seen[name] = s
	}
	pdpMu.Lock()
	t.Logf("PARITY-PDP-CALLS %s", strings.Join(pdpSeen, "; "))
	pdpMu.Unlock()

	for _, name := range names {
		s, want := seen[name], parityWants[name]
		t.Run(name+"/L", func(t *testing.T) {
			if (s.lStatus == http.StatusOK) != want.lServed {
				t.Errorf("L status %d, want served=%v", s.lStatus, want.lServed)
			}
			if want.lStatus != 0 && s.lStatus != want.lStatus {
				t.Errorf("L status %d, want %d", s.lStatus, want.lStatus)
			}
			if want.lServed && !slices.Equal(s.lNames, want.lNames) {
				t.Errorf("L names %v, want %v", s.lNames, want.lNames)
			}
		})
		t.Run(name+"/G1", func(t *testing.T) {
			if want.lServed && s.star.Allowed && s.star.Scope != "organization" {
				t.Errorf("G1 key * allowed without repository_scope organization: %+v", s.star)
			}
			var allowed []string
			for _, key := range parityRepos {
				d, ok := s.batch[key]
				if !ok {
					t.Errorf("G1 batch has no decision for %s", key)
					continue
				}
				if d.Allowed {
					allowed = append(allowed, key)
				} else if d.Reason != want.batchReason {
					t.Errorf("G1 %s denied %q, want %q", key, d.Reason, want.batchReason)
				}
			}
			if !slices.Equal(allowed, want.batchAllowed) {
				t.Errorf("G1 allowed %v, want %v", allowed, want.batchAllowed)
			}
		})
		t.Run(name+"/G2", func(t *testing.T) {
			if s.g2Status != http.StatusOK {
				t.Errorf("G2 status %d, want 200", s.g2Status)
			}
			if s.g2.Allowed != want.g2Allowed {
				t.Errorf("G2 allowed %v (%q), want %v", s.g2.Allowed, s.g2.Reason, want.g2Allowed)
			}
			if !want.g2Allowed && s.g2.Reason != want.g2Reason {
				t.Errorf("G2 reason %q, want %q", s.g2.Reason, want.g2Reason)
			}
			if want.g2Allowed && (s.g2.Scope != want.g2Scope || !slices.Equal(s.g2List, want.g2List)) {
				t.Errorf("G2 scope %q list %v, want %q %v", s.g2.Scope, s.g2List, want.g2Scope, want.g2List)
			}
		})
		t.Run(name+"/verdict", func(t *testing.T) {
			legacy := "refused"
			if s.lOK {
				legacy = strings.Join(s.lNames, ",")
			}
			grants := "refused"
			if s.lOK {
				grants = strings.Join(parityG1Scopes(s, s.lNames), ",")
			}
			same := legacy == grants
			t.Logf("PARITY-VERDICT %s legacy=[%s] grants_g1=[%s] g2=%+v list=%v same=%v", name, legacy, grants, s.g2, s.g2List, same)
			if same != want.sameScopes {
				t.Errorf("legacy [%s] vs grants [%s]: same=%v, want same=%v", legacy, grants, same, want.sameScopes)
			}
		})
	}
}

// parityG1Scopes is web fb139e8b resolveRepositoryDecisions over the observed decisions (the authoritative mapping
// is the web replay; this mirror only feeds the verdict line).
func parityG1Scopes(s paritySeen, candidates []string) []string {
	if s.star.Allowed {
		return candidates
	}
	var out []string
	for _, key := range candidates {
		if s.batch[key].Allowed {
			out = append(out, key)
		}
	}
	return out
}

func parityRequest(bearer, org, key string) json.RawMessage {
	id := make([]byte, 20)
	_, _ = rand.Read(id)
	body, _ := json.Marshal(map[string]any{
		"schema_version": "authorization_request.v1", "principal_reference": bearer, "action": parityContextRead,
		"resource": map[string]any{"type": "repository", "key": key, "organization_id": org},
		"context":  map[string]any{"request_id": "req_" + base64.RawURLEncoding.EncodeToString(id)[:26], "client_service": parityWebService},
	})
	return body
}

func parityPost(t *testing.T, client *http.Client, url, bearer string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// parityLog writes one response as a PARITY-BODY line: subject, arm, status, sha256 and the body base64.
func parityLog(t *testing.T, subject, arm string, status int, body []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	t.Logf("PARITY-BODY %s %s status=%d sha256=%s b64=%s", subject, arm, status, hex.EncodeToString(sum[:]), base64.StdEncoding.EncodeToString(body))
}

func parityDecision(body []byte) parityKey {
	var d struct {
		Allowed     bool   `json:"allowed"`
		Reason      string `json:"reason_code"`
		Obligations struct {
			RepositoryScope string `json:"repository_scope"`
		} `json:"obligations"`
	}
	if json.Unmarshal(body, &d) != nil {
		return parityKey{Reason: "unparsed"}
	}
	return parityKey{Allowed: d.Allowed, Reason: d.Reason, Scope: d.Obligations.RepositoryScope}
}

func parityAllowlist(body []byte) []string {
	var d struct {
		Obligations struct {
			Allowlist []string `json:"repository_allowlist"`
		} `json:"obligations"`
	}
	_ = json.Unmarshal(body, &d)
	return d.Obligations.Allowlist
}

func parityBatch(body []byte, keys []string) map[string]parityKey {
	var out struct {
		Decisions []struct {
			Index    int             `json:"index"`
			Decision json.RawMessage `json:"decision"`
		} `json:"decisions"`
	}
	seen := map[string]parityKey{}
	if json.Unmarshal(body, &out) != nil {
		return seen
	}
	for _, d := range out.Decisions {
		if d.Index >= 0 && d.Index < len(keys) {
			seen[keys[d.Index]] = parityDecision(d.Decision)
		}
	}
	return seen
}

// parityMintWorkload answers a workload exchange with a workload token for subject, signed by the access key under
// issuer: the token the authorize caller gate verifies.
func parityMintWorkload(w http.ResponseWriter, r *http.Request, signer *token.Signer, issuer, subject string) {
	now := time.Now()
	claims, err := json.Marshal(contracts.WorkloadTokenClaims{
		Iss: issuer, Aud: authprovision.WorkloadAudience, Sub: subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	})
	if err != nil {
		http.Error(w, "claims", http.StatusInternalServerError)
		return
	}
	minted, _, err := signer.Sign(r.Context(), contracts.WorkloadTokenTyp, claims)
	if err != nil {
		http.Error(w, "sign", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": minted, "issued_token_type": workloadtoken.TokenTypeJWT,
		"token_type": "Bearer", "expires_in": 300})
}

// parityWebClient is web's caller: a workload token client for subject whose HTTP client presents it to target.
func parityWebClient(t *testing.T, signer *token.Signer, issuer, subject, target string) *http.Client {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parityMintWorkload(w, r, signer, issuer, subject)
	}))
	t.Cleanup(exchange.Close)
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: issuer,
		Subject: workloadtoken.BootstrapCredentialFile(parityBootstrapFile(t)), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	httpClient, err := client.HTTPClient(&http.Transport{}, target, authprovision.WorkloadAudience)
	if err != nil {
		t.Fatal(err)
	}
	return httpClient
}

func parityBootstrapFile(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// parityIssuer is one access token issuer for both planes: query-api reads its JWKS over https (as from
// AUTH_ACCESS_TOKEN_ISSUER in prod), the decision point signs and verifies with the same key under the same iss.
type parityIssuer struct {
	url     string
	keyPath string
	tokens  *edtokens.Issuer
}

func startParityIssuer(t *testing.T) parityIssuer {
	t.Helper()
	files, err := issuerstub.Prepare(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	priv, err := envelopemint.LoadPrivateKeyFromFile(files.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes, err := os.ReadFile(files.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "access.pem")
	if err := os.WriteFile(keyPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	jwks, err := os.ReadFile(files.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(files.CA)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("issuer stub: ca.pem holds no certificate")
	}
	cert, err := tls.LoadX509KeyPair(files.ServerCert, files.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	issuerURL := fmt.Sprintf("https://localhost:%d", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{
		Handler:           issuerstub.Handler(issuerURL, jwks),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close() })
	trustEdgeIssuerJWKS(t, roots)
	tokens, err := edtokens.FromKey(priv, issuerstub.DefaultKeyID, issuerURL)
	if err != nil {
		t.Fatal(err)
	}
	return parityIssuer{url: issuerURL, keyPath: keyPath, tokens: tokens}
}
