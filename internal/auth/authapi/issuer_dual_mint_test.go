package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// newDualMintFixture is the access fixture with the issuer production builds under the eddsa class: the API audience
// configured, the session-route audience minted beside it.
func newDualMintFixture(t *testing.T) *accessFixture {
	t.Helper()
	signer, dir, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
	f := newFixtureWith(t, issuer)
	rows, err := AuthServiceAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	reader, provider := accessMeter()
	check, err := NewAccessCheck(AccessCheckConfig{IssuerURL: goldenIssuer, Keys: signer, Legacy: issuer, Rows: rows, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	issuer.WithAccessCheck(check)
	return &accessFixture{fixture: f, signer: signer, dir: dir, reader: reader, check: check}
}

func claimsOf(t *testing.T, compact string) contracts.AccessTokenClaims {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("not a compact JWS: %d parts", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var c contracts.AccessTokenClaims
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func kidOf(t *testing.T, compact string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	var h contracts.AccessTokenHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	return h.Kid
}

// Under eddsa one grant yields two tokens of one session: audience dev-health-api for go-api and query-api, audience
// auth-service for the session routes. Same sid, key, iat and exp; each its own jti.
func TestEdDSAIssuerMintsTheSessionRouteTokenBesideTheAPIToken(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
	now := time.Now().UTC().Truncate(time.Second)
	issuer.now = func() time.Time { return now }
	org, principal, sid := uuid.New(), uuid.New(), uuid.New()
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: principal, OrganizationID: &org, SessionID: sid, Role: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	set, err := signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	api := verifyEdDSA(t, tokens.Access, set, goldenIssuer, contracts.PlatformAPIAudience, now)
	route := verifyEdDSA(t, tokens.Session, set, goldenIssuer, contracts.SessionRouteAudience, now)
	for name, d := range map[string]decoded{"access": api, "session": route} {
		if _, err := contracts.ParseAccessToken(repoRoot(t), accessDocument(t, d)); err != nil {
			t.Fatalf("%s token is not access-token.v1: %v", name, err)
		}
	}
	a, s := claimsOf(t, tokens.Access), claimsOf(t, tokens.Session)
	if a.Scope != "api:access" || s.Scope != "auth:session" {
		t.Fatalf("scopes %q and %q", a.Scope, s.Scope)
	}
	if a.Sid != session.WireSessionID(sid) || s.Sid != a.Sid || s.Iat != a.Iat || s.Exp != a.Exp || s.Sub != a.Sub ||
		*s.OrgID != *a.OrgID || kidOf(t, tokens.Session) != kidOf(t, tokens.Access) {
		t.Fatalf("the two tokens differ beyond audience, scope and jti: %+v %+v", a, s)
	}
	if a.Jti == s.Jti {
		t.Fatalf("both tokens carry jti %q", a.Jti)
	}
	if got := issuer.ExpiresIn(); int64(got) != a.Exp-a.Iat {
		t.Fatalf("ExpiresIn %d, the minted lifetime is %d", got, a.Exp-a.Iat)
	}
}

// Under hs256_compat the HS256 token is accepted at the session routes itself (legacy_hs256), so no second token.
func TestHS256IssuerMintsNoSessionRouteToken(t *testing.T) {
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"})
	if err != nil {
		t.Fatal(err)
	}
	if tokens.Access == "" || tokens.Session != "" {
		t.Fatalf("hs256_compat tokens: access set %v, session %q", tokens.Access != "", tokens.Session)
	}
}

// The refresh route answers refresh-response.v2 under eddsa (both tokens) and v1 under hs256_compat; each parses with
// the contract reader, and the eddsa session token opens the session routes the API token cannot.
func TestRefreshRouteAnswersTheSurfaceOfItsClass(t *testing.T) {
	refresh := func(f *fixture) []byte {
		t.Helper()
		f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
			Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(7*24*time.Hour - time.Second),
		}}
		response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("refresh status %d: %s", response.Code, response.Body)
		}
		return response.Body.Bytes()
	}

	hs := newFixture(t)
	v1, err := contracts.ParseRefreshResponse(repoRoot(t), refresh(hs))
	if err != nil || v1.SchemaVersion != contracts.RefreshResponseSurface || v1.SessionAccessToken.Reveal() != "" {
		t.Fatalf("hs256_compat answer: %v", err)
	}

	a := newDualMintFixture(t)
	v2, err := contracts.ParseRefreshResponse(repoRoot(t), refresh(a.fixture))
	if err != nil {
		t.Fatalf("eddsa answer is not refresh-response.v2: %v", err)
	}
	if v2.SchemaVersion != contracts.RefreshResponseV2Surface {
		t.Fatalf("eddsa answer schema_version %q", v2.SchemaVersion)
	}
	bearer := func(token string) map[string]string { return map[string]string{"Authorization": "Bearer " + token} }
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", bearer(v2.SessionAccessToken.Reveal())); r.Code != http.StatusOK {
		t.Fatalf("the session token at GET /v1/sessions/current = %d %s", r.Code, r.Body)
	}
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", bearer(v2.AccessToken.Reveal())), http.StatusUnauthorized, "audience")
}

// The issue route (go-api's sign-ins ask it for every session) carries the session token under eddsa and no such
// member under hs256_compat.
func TestIssueRouteCarriesTheSessionRouteTokenUnderEdDSA(t *testing.T) {
	for class, want := range map[string]bool{"hs256_compat": false, "eddsa": true} {
		t.Run(class, func(t *testing.T) {
			var issuer *Issuer
			if want {
				signer, _, _ := fileSigner(t)
				issuer = newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
			} else {
				var err error
				if issuer, err = NewIssuer(testSecret, testIssuer, testAudience); err != nil {
					t.Fatal(err)
				}
			}
			body := issueAnswerFor(t, issuer)
			var answer map[string]any
			if err := json.Unmarshal(body, &answer); err != nil {
				t.Fatal(err)
			}
			token, has := answer["session_access_token"].(string)
			if has != want {
				t.Fatalf("session_access_token present %v, want %v: %s", has, want, body)
			}
			if want && claimsOf(t, token).Aud != contracts.SessionRouteAudience {
				t.Fatalf("session_access_token audience %q", claimsOf(t, token).Aud)
			}
		})
	}
}

func compatFixtureWith(t *testing.T, issuer *Issuer) *compatFixture {
	t.Helper()
	f := newCompatFixture(t)
	f.issuer = issuer
	f.buildCompat()
	return f
}

func issueAnswerFor(t *testing.T, issuer *Issuer) []byte {
	t.Helper()
	f := compatFixtureWith(t, issuer)
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer)
	if response.Code != http.StatusOK {
		t.Fatalf("issue status %d: %s", response.Code, response.Body)
	}
	return response.Body.Bytes()
}

// go-api's own wire (compat login and refresh, which web reads) carries the session token under eddsa as an added
// member, and its hs256_compat answers keep their exact member list.
func TestCompatAnswersCarryTheSessionRouteTokenUnderEdDSAOnly(t *testing.T) {
	login := `{"email":"` + compatEmail + `","password":"` + compatPassword + `"}`
	for class, want := range map[string]bool{"hs256_compat": false, "eddsa": true} {
		t.Run(class, func(t *testing.T) {
			var issuer *Issuer
			if want {
				signer, _, _ := fileSigner(t)
				issuer = newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
			} else {
				var err error
				if issuer, err = NewIssuer(testSecret, testIssuer, testAudience); err != nil {
					t.Fatal(err)
				}
			}
			f := compatFixtureWith(t, issuer)
			response := f.do(http.MethodPost, PatternCompatLogin, login, nil)
			if response.Code != http.StatusOK {
				t.Fatalf("login status %d: %s", response.Code, response.Body)
			}
			answer := decodeObject(t, response.Body.Bytes())
			token, has := answer["session_access_token"].(string)
			if has != want {
				t.Fatalf("login session_access_token present %v, want %v", has, want)
			}
			if want {
				s, a := claimsOf(t, token), claimsOf(t, answer["access_token"].(string))
				if s.Aud != contracts.SessionRouteAudience || a.Aud != contracts.PlatformAPIAudience || s.Sid != a.Sid {
					t.Fatalf("login tokens: session %+v api %+v", s, a)
				}
			}
		})
	}
}

// rotatingSigner answers each Sign with the next kid, as a key rotation between the two signs of one grant would.
type rotatingSigner struct {
	kids  []string
	calls int
}

func (r *rotatingSigner) Sign(_ context.Context, _ string, payload []byte) (string, string, error) {
	kid := r.kids[r.calls%len(r.kids)]
	r.calls++
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"at+jwt","kid":"` + kid + `"}`))
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".EXAMPLE", kid, nil
}

func (r *rotatingSigner) Check(context.Context) error { return nil }

// A rotation between the two signs gives the tokens different kids; both are published, and both are recorded on
// the session so a precise revoke reaches each.
func TestARotationBetweenTheTwoSignsRecordsBothKids(t *testing.T) {
	issuer := newEdDSAIssuer(t, &rotatingSigner{kids: []string{"k1", "k2"}}, contracts.PlatformAPIAudience)
	kids := &sidKids{}
	issuer.WithKidRecorder(kids)
	sid := uuid.New()
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: sid, Role: "member"})
	if err != nil {
		t.Fatal(err)
	}
	if kidOf(t, tokens.Access) != "k1" || kidOf(t, tokens.Session) != "k2" {
		t.Fatalf("kids %q and %q", kidOf(t, tokens.Access), kidOf(t, tokens.Session))
	}
	if got := kids.kids[sid]; len(got) != 2 || got[0] != "k1" || got[1] != "k2" {
		t.Fatalf("recorded kids %v, want [k1 k2]", got)
	}
}
