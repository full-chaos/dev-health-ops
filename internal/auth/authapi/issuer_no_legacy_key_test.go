package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

func TestNewEdDSAIssuerWithoutLegacyKeyHoldsNoLegacyVerifier(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer, err := NewEdDSAIssuer("", testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer)
	if err != nil {
		t.Fatalf("an empty legacy key is refused: %v", err)
	}
	if issuer.verifier != nil {
		t.Fatal("an empty legacy key built a legacy verifier")
	}
	compact, err := issuer.Access(Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil || compact == "" {
		t.Fatalf("the issuer without a legacy key mints no EdDSA token: %v", err)
	}
}

func TestNewEdDSAIssuerStillRefusesAShortLegacyKey(t *testing.T) {
	signer, _, _ := fileSigner(t)
	if _, err := NewEdDSAIssuer(strings.Repeat("k", edgetoken.MinSecretLength-1), testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer); err == nil {
		t.Fatal("a legacy key below the minimum length is accepted")
	}
}

func TestIssuerWithoutLegacyKeyRefusesAnHS256BearerWithoutReadingIt(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer, err := NewEdDSAIssuer("", testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer)
	if err != nil {
		t.Fatal(err)
	}
	hs, err := NewIssuer(testSecret, testIssuer, contracts.PlatformAPIAudience)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := hs.Access(Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	bearer, reason, err := issuer.Verify(compact)
	if !errors.Is(err, errLegacyKeyAbsent) {
		t.Fatalf("err = %v, want errLegacyKeyAbsent", err)
	}
	if reason != edgetoken.ReasonBadSignature || bearer != (Bearer{}) {
		t.Fatalf("reason %q bearer %+v, want bad_signature and no bearer", reason, bearer)
	}
}

// Product rules 1 and 3: without a legacy key the validate route mounts under eddsa (legacy served it; a missing key is
// no unmount); without the access-token verifier the issuer check refuses the mount and says so.
func TestValidateRouteMountsWithoutLegacyKeyUnderEdDSA(t *testing.T) {
	for _, eddsa := range []bool{false, true} {
		f := newValidateFixture(t, eddsa)
		f.issuer.verifier = nil
		f.logs.Reset()
		routes, err := CompatRoutes(f.deps())
		if err != nil {
			t.Fatalf("eddsa=%v: the compat routes do not build without a legacy key: %v", eddsa, err)
		}
		mounted := 0
		for _, route := range routes {
			if route.Method == http.MethodPost && route.Pattern == PatternCompatValidate {
				mounted++
			}
		}
		if eddsa {
			if mounted != 1 || strings.Contains(f.logs.String(), "auth validate route not mounted") {
				t.Fatalf("eddsa: validate mounted %d times, logs %s; want once and no not-mounted line", mounted, f.logs)
			}
			continue
		}
		line := "auth validate route not mounted: AUTH_TOKEN_ISSUER_URL is unset (no access-token verifier)"
		if mounted != 0 || !strings.Contains(f.logs.String(), line) {
			t.Fatalf("no access-token verifier: validate mounted %d times, logs %s; want 0 and %q", mounted, f.logs, line)
		}
	}
}

// Product rule 1: with no legacy key the validate route answers an EdDSA user token exactly as with the key set.
func TestValidateAnswersAnEdDSATokenWithoutTheLegacyKeyAsWithIt(t *testing.T) {
	f := newValidateFixture(t, true)
	token := f.eddsaToken(contracts.PlatformAPIAudience)
	withKey := f.post(f.server, tokenBody(token), nil)
	f.issuer.verifier = nil
	f.build()
	without := f.post(f.server, tokenBody(token), nil)
	expectAnswer(t, "with the legacy key", withKey, http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "admin"))
	if without.Code != withKey.Code || without.Body.String() != withKey.Body.String() {
		t.Errorf("without the legacy key %d %s, with it %d %s; want the same answer", without.Code, without.Body, withKey.Code, withKey.Body)
	}
}
