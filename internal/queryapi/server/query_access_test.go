package server

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const (
	queryRefusal   = "unauthorized\n"
	graphQLRefusal = `{"detail":"Authentication required"}`
)

// The /query family and /graphql with the issuer set: the user access token
// is served on every route whose row accepts it, through the real listeners.
func TestQueryAPIQueryAndGraphQLUserTokenPath(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	member := m.mint(ecUser, nil, nil)
	type target struct{ listener, method, path string }
	targets := []target{
		{"query", http.MethodPost, "/query"},
		{"query", http.MethodPost, "/query/proof"},
		{"internal", http.MethodPost, "/query/proof-write"},
		{"internal", http.MethodPost, "/query/run-operation"},
		{"query", http.MethodGet, "/graphql"},
		{"query", http.MethodPost, "/graphql"},
	}
	refusal := func(path string) string {
		if path == "/graphql" {
			return graphQLRefusal
		}
		return queryRefusal
	}
	for name, tc := range map[string]struct {
		store  func(*accessStore)
		token  string
		accept bool
	}{
		"member":        {nil, member, true},
		"not a member":  {func(s *accessStore) { s.members = nil }, member, false},
		"inactive user": {func(s *accessStore) { s.states[ecUser] = policy.UserState{TokenVersion: 5} }, member, false},
		"revoked session": {func(s *accessStore) {
			s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecUser, Revoked: true, LegacyTokenVersion: i64(5)}
		}, member, false},
		"session predates pin": {func(s *accessStore) {
			s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecUser, LegacyTokenVersion: i64(4)}
		}, member, false},
		"other audience":          {nil, m.mint(ecUser, map[string]any{"aud": "auth-service", "scope": "auth:session"}, nil), false},
		"no org, not a superuser": {nil, m.mint(ecUser, map[string]any{"org_id": nil}, nil), false},
	} {
		t.Run(name, func(t *testing.T) {
			store := newAccessStore()
			if tc.store != nil {
				tc.store(store)
			}
			stack := newAccessStack(t, store, testAccess(m.keys, nil, accessTestNow), true)
			for _, target := range targets {
				got := stack.send(target.listener, target.method, target.path, "", tc.token)
				switch {
				case tc.accept && got.Status != http.StatusOK:
					t.Errorf("%s %s: got %d %s, want served", target.method, target.path, got.Status, got.Body)
				case !tc.accept && (got.Status != http.StatusUnauthorized || got.Body != refusal(target.path)):
					t.Errorf("%s %s: got %d %q, want the path's 401", target.method, target.path, got.Status, got.Body)
				}
			}
		})
	}
}

// T5 on /query: the envelope keeps its verifier with the issuer set.
func TestQueryAPIQueryEnvelopeStillReachesTheEnvelopeVerifierWhenIssuerSet(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), true)
	if got := stack.send("query", http.MethodPost, "/query", "envelope", ""); got.Status != http.StatusOK {
		t.Fatalf("envelope: %d %s", got.Status, got.Body)
	}
	if got := stack.reader.counts(t); len(got) != 0 {
		t.Fatalf("the envelope reached the access-token validator: %v", got)
	}
	if got := stack.send("query", http.MethodPost, "/graphql", "envelope", ""); got.Status != http.StatusUnauthorized {
		t.Fatalf("/graphql takes no envelope: %d %s", got.Status, got.Body)
	}
}

// T7 on /query and /graphql: a JWKS outage decides nothing. /query answers its
// one bare 401 for every failure, as it does for a users-store outage; on
// /graphql the scope middleware answers 503 for a caller it could not read.
func TestQueryAPIQueryAndGraphQLJWKSOutage(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(downKeys{}, nil, accessTestNow), true)
	if got := stack.send("query", http.MethodPost, "/query", "", m.mint(ecUser, nil, nil)); got.Status != http.StatusUnauthorized {
		t.Fatalf("/query: %d %s", got.Status, got.Body)
	}
	if got := stack.send("query", http.MethodPost, "/graphql", "", m.mint(ecUser, nil, nil)); got.Status != http.StatusServiceUnavailable {
		t.Fatalf("/graphql: %d %s", got.Status, got.Body)
	}
	if got := stack.reader.counts(t)[accessverify.RefusedMetric+"|profile=POST /query,reason=unavailable,service=dev-health-query-api"]; got != 1 {
		t.Fatalf("the outage is not counted unavailable on POST /query: %d", got)
	}
}

// Q5 on /query and /graphql: with no edge secret the HS256 token is refused,
// the user token served.
func TestQueryAPIQueryAndGraphQLIssuerAloneServesOnlyTheUserToken(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), false)
	for _, path := range []string{"/query", "/graphql"} {
		if got := stack.send("query", http.MethodPost, path, "", m.mint(ecUser, nil, nil)); got.Status != http.StatusOK {
			t.Errorf("%s user token: %d %s", path, got.Status, got.Body)
		}
		if got := stack.send("query", http.MethodPost, path, "edge", ""); got.Status != http.StatusUnauthorized {
			t.Errorf("%s edge token with no edge secret: %d %s", path, got.Status, got.Body)
		}
	}
}

// Q8 on /query and /graphql: an edge token whose users read fails keeps its
// answer with the issuer set. /query answers its bare 401; /graphql answers as
// its scope middleware decides a caller it could not read.
func TestQueryAPIQueryAndGraphQLStoreFailureAnswersHoldWithTheIssuerSet(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	type want struct{ query, graphql int }
	for name, tc := range map[string]struct {
		store      func(*accessStore)
		unset, set want
	}{
		"users store error that is not unavailable": {
			func(s *accessStore) { s.errUserState = errors.New("syntax error") },
			want{http.StatusUnauthorized, http.StatusInternalServerError},
			want{http.StatusUnauthorized, http.StatusInternalServerError}},
		"users store unavailable": {
			func(s *accessStore) { s.errUserState = policy.ErrUnavailable },
			want{http.StatusUnauthorized, http.StatusInternalServerError},
			want{http.StatusUnauthorized, http.StatusInternalServerError}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, routed := range []bool{false, true} {
				store := newAccessStore()
				tc.store(store)
				var keys authverify.KeySource
				expect := tc.unset
				if routed {
					keys, expect = m.keys, tc.set
				}
				stack := newAccessStack(t, store, testAccess(keys, nil, accessTestNow), true)
				if got := stack.send("query", http.MethodPost, "/query", "edge", ""); got.Status != expect.query {
					t.Errorf("issuer set %v: /query %d, want %d", routed, got.Status, expect.query)
				}
				if got := stack.send("query", http.MethodPost, "/graphql", "edge", ""); got.Status != expect.graphql {
					t.Errorf("issuer set %v: /graphql %d, want %d", routed, got.Status, expect.graphql)
				}
			}
		})
	}
}

// corpusDir is the A2 signed access-token corpus.
var corpusDir = filepath.Join("..", "..", "..", "contracts", "auth", "v1", "golden", "access-token")

type corpusManifest struct {
	Issuer      string   `json:"issuer"`
	RemovedKids []string `json:"removed_kids"`
	RemovedKeys []struct {
		Kid string `json:"kid"`
		X   string `json:"x"`
	} `json:"removed_public_keys"`
	Vectors []struct {
		File     string `json:"file"`
		Now      int64  `json:"now"`
		Audience string `json:"verify_as_audience"`
		KeySet   string `json:"key_set"`
		Expect   string `json:"expect"`
		Reason   string `json:"reason"`
	} `json:"vectors"`
}

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

// corpusAtAuthService are the rows decided at auth-service, with query-api's
// decision of the same token at the same clock (query-api shares go-api's
// audience): the dev-health-api token in its window is good; expired, not yet
// valid, another issuer and an auth-service token are refused.
var corpusAtAuthService = map[string]string{
	"invalid-aud-api-token-at-auth-service.json":     "accept",
	"invalid-order-audience-over-expiry.json":        "reject",
	"invalid-order-audience-over-not-yet-valid.json": "reject",
	"invalid-order-issuer-over-audience.json":        "reject",
	"valid-act-auth-service.json":                    "reject",
	"valid-auth-service.json":                        "reject",
}

var (
	corpusSub = regexp.MustCompile(`"sub":"(prn_[A-Za-z0-9_-]+)"`)
	corpusSid = regexp.MustCompile(`"sid":"(ses_[A-Za-z0-9_-]+)"`)
	corpusOrg = regexp.MustCompile(`"org_id":"([0-9a-f-]{36})"`)
)

func decodeWireID(wire, prefix string) (uuid.UUID, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(wire, prefix))
	if err != nil || len(raw) != 16 || !strings.HasPrefix(wire, prefix) {
		return uuid.UUID{}, false
	}
	return uuid.UUID(raw), true
}

// TestA2CorpusThroughQueryAPI runs every vector of the signed access-token
// corpus through query-api at the vector's clock: REST (authenticateRESTRequest
// on the real mux pattern), POST /query and POST /graphql (the real listeners).
// The vector's principal is a live, active member of its organization with a
// live session; one with no organization is a live superuser, the one caller
// query-api serves with no organization.
func TestA2CorpusThroughQueryAPI(t *testing.T) {
	quietStdLog(t)
	raw, err := os.ReadFile(filepath.Join(corpusDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest corpusManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	published, err := authverify.NewEd25519JWKSVerifier(filepath.Join(corpusDir, "jwks.json")).Keys()
	if err != nil {
		t.Fatal(err)
	}
	accepted := map[string]int{}
	for _, row := range manifest.Vectors {
		t.Run(strings.TrimSuffix(row.File, ".json"), func(t *testing.T) {
			expect := row.Expect
			if row.Audience != accessTokenAudienceAPI {
				var known bool
				if expect, known = corpusAtAuthService[row.File]; !known {
					t.Fatalf("a corpus row decided at %s that this test does not place at query-api", row.Audience)
				}
			}
			keys := authverify.StaticKeys{}
			for kid, key := range published {
				keys[kid] = key
			}
			if row.KeySet == "published_and_removed" {
				for _, removed := range manifest.RemovedKeys {
					x, err := base64.RawURLEncoding.DecodeString(removed.X)
					if err != nil {
						t.Fatal(err)
					}
					keys[removed.Kid] = ed25519.PublicKey(x)
				}
			}
			body, err := os.ReadFile(filepath.Join(corpusDir, "vectors", row.File))
			if err != nil {
				t.Fatal(err)
			}
			var vector corpusVector
			if err := json.Unmarshal(body, &vector); err != nil {
				t.Fatal(err)
			}
			store := corpusStore(vector.PayloadJSON)
			now := time.Unix(row.Now, 0)
			token := vector.compact()

			setup := func() *accessSetup {
				s := testAccess(keys, manifest.RemovedKids, now)
				s.opts.issuer = manifest.Issuer
				return s
			}
			rest := newRESTFixture(t, store, setup(), true)
			stack := newAccessStack(t, store, setup(), true)
			answers := map[string]answer{
				"rest":    rest.do(http.MethodGet, token),
				"query":   stack.send("query", http.MethodPost, "/query", "", token),
				"graphql": stack.send("query", http.MethodPost, "/graphql", "", token),
			}
			refusals := map[string]answer{
				"rest":    {http.StatusUnauthorized, restRefusal},
				"query":   {http.StatusUnauthorized, queryRefusal},
				"graphql": {http.StatusUnauthorized, graphQLRefusal},
			}
			for path, got := range answers {
				if expect == "accept" {
					if got.Status != http.StatusOK {
						t.Errorf("%s: want accept, got %d %s", path, got.Status, got.Body)
						continue
					}
					accepted[path]++
					continue
				}
				if got != refusals[path] {
					t.Errorf("%s: want the generic 401 (%s), got %d %s", path, row.Reason, got.Status, got.Body)
				}
			}
		})
	}
	for _, path := range []string{"rest", "query", "graphql"} {
		if len(manifest.Vectors) < 40 || accepted[path] < 8 {
			t.Fatalf("%d vectors, %d accepted on %s: the corpus did not run", len(manifest.Vectors), accepted[path], path)
		}
	}
}

// corpusStore makes the vector's principal live: active, its session live and
// pinned at its token_version, a member of its organization, or a superuser
// when the token names no organization.
func corpusStore(payload string) *accessStore {
	store := newAccessStore()
	sub, sid := corpusSub.FindStringSubmatch(payload), corpusSid.FindStringSubmatch(payload)
	if sub == nil || sid == nil {
		return store
	}
	principalID, okPrincipal := decodeWireID(sub[1], "prn_")
	sessionID, okSession := decodeWireID(sid[1], "ses_")
	if !okPrincipal || !okSession {
		return store
	}
	state := policy.UserState{IsActive: true}
	if org := corpusOrg.FindStringSubmatch(payload); org != nil {
		store.members[[2]uuid.UUID{principalID, uuid.MustParse(org[1])}] = true
	} else {
		state.IsSuperuser = true
	}
	store.states[principalID] = state
	store.found[principalID] = true
	store.liveSessions[sessionID] = policy.SessionRow{PrincipalID: principalID, LegacyTokenVersion: i64(0)}
	return store
}
