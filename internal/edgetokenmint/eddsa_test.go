package edgetokenmint

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const eddsaIssuer = "https://localhost:8443"

var eddsaSession = uuid.MustParse("cccccccc-1111-4222-8333-444444444444")

type sessionRow struct {
	owner            uuid.UUID
	revoked, expired bool
	pin              *int64
	err              error
}

func (r sessionRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*uuid.UUID), *dest[1].(*bool), *dest[2].(*bool), *dest[3].(**int64) = r.owner, r.revoked, r.expired, r.pin
	return nil
}

type sessionDB struct{ row sessionRow }

func (d sessionDB) QueryRow(context.Context, string, ...any) pgx.Row { return d.row }

func int64p(v int64) *int64 { return &v }

func eddsaFixture(t *testing.T) (EdDSASigner, authverify.StaticKeys) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return EdDSASigner{Key: priv, KeyID: "k1", Issuer: eddsaIssuer, SessionID: eddsaSession}, authverify.StaticKeys{"k1": pub}
}

func TestMintEdDSAVerifiesAsAnAccessTokenV1(t *testing.T) {
	signer, keys := eddsaFixture(t)
	token, err := MintEdDSA(signer, testPrincipal(), Options{Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: eddsaIssuer, Audience: DefaultAudience, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token, testNow.Add(time.Minute))
	if err != nil {
		t.Fatalf("token does not verify: %v", err)
	}
	if id, err := sessionid.Parse(claims.SessionID); err != nil || id != eddsaSession {
		t.Fatal("sid does not name the session of the signer")
	}
	if claims.OrgID == nil || *claims.OrgID != testOrg || claims.ExpiresAt-claims.IssuedAt != int64(DefaultTTL/time.Second) {
		t.Fatalf("claims = %+v", claims)
	}
	if _, err := verifier.Verify(token, testNow.Add(DefaultTTL+time.Minute)); authverify.ReasonOf(err) != authverify.ReasonExpired {
		t.Fatalf("the token must expire after its TTL, got %v", err)
	}
}

func TestMintEdDSARefusesWhatTheHS256ModeRefuses(t *testing.T) {
	signer, _ := eddsaFixture(t)
	admin := testPrincipal()
	admin.Role = "admin"
	if _, err := MintEdDSA(signer, admin, Options{}); !errors.Is(err, ErrRoleNotAllowed) {
		t.Fatalf("admin role for the proof principal: %v", err)
	}
	if _, err := MintEdDSA(signer, testPrincipal(), Options{TTL: MaxTTL + time.Second}); err == nil {
		t.Fatal("a TTL above the cap must be refused")
	}
	if _, err := MintEdDSA(signer, testPrincipal(), Options{Audience: "acr-api"}); err == nil {
		t.Fatal("an ACR audience must be refused")
	}
	signer.SessionID = uuid.Nil
	if _, err := MintEdDSA(signer, testPrincipal(), Options{}); err == nil {
		t.Fatal("no session must be refused")
	}
}

func TestCheckSessionRefusesWhatTheVerifierRefuses(t *testing.T) {
	principal := testPrincipal()
	owner := uuid.MustParse(principal.UserID)
	for name, tc := range map[string]struct {
		row  sessionRow
		want error
	}{
		"live":              {sessionRow{owner: owner, pin: int64p(3)}, nil},
		"no row":            {sessionRow{err: pgx.ErrNoRows}, ErrSessionNotFound},
		"another principal": {sessionRow{owner: uuid.New(), pin: int64p(3)}, ErrSessionPrincipal},
		"revoked":           {sessionRow{owner: owner, revoked: true, pin: int64p(3)}, ErrSessionNotLive},
		"expired":           {sessionRow{owner: owner, expired: true, pin: int64p(3)}, ErrSessionNotLive},
		"no pin":            {sessionRow{owner: owner}, ErrSessionPin},
		"stale pin":         {sessionRow{owner: owner, pin: int64p(2)}, ErrSessionPin},
	} {
		if err := CheckSession(context.Background(), sessionDB{tc.row}, eddsaSession, principal, ""); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", name, err, tc.want)
		}
	}
	if err := CheckSession(context.Background(), sessionDB{}, uuid.Nil, principal, ""); !errors.Is(err, ErrSessionMissing) {
		t.Errorf("no session id: %v", err)
	}
	if err := CheckSession(context.Background(), sessionDB{}, eddsaSession, principal, "bad;schema"); err == nil {
		t.Error("an unusable auth schema must be refused")
	}
}

func TestEdDSAModeSourceNamesNoHS256Secret(t *testing.T) {
	raw, err := os.ReadFile("eddsa.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, word := range []string{"JWT_SECRET_KEY", "SigningKeyEnvVar", "SigningMethodHS", "LoadSigningKey", "golang-jwt"} {
		if strings.Contains(string(raw), word) {
			t.Errorf("eddsa.go names %s", word)
		}
	}
}
