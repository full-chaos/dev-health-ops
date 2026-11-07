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

func TestValidateRouteIsNotMountedWithoutLegacyKey(t *testing.T) {
	for _, eddsa := range []bool{false, true} {
		f := newValidateFixture(t, eddsa)
		f.issuer.verifier = nil
		f.logs.Reset()
		routes, err := CompatRoutes(f.deps())
		if err != nil {
			t.Fatalf("eddsa=%v: the compat routes do not build without a legacy key: %v", eddsa, err)
		}
		for _, route := range routes {
			if route.Method == http.MethodPost && route.Pattern == PatternCompatValidate {
				t.Fatalf("eddsa=%v: the validate route is mounted without a legacy key", eddsa)
			}
		}
		// Without the access-token verifier the issuer check refuses the mount first; with it, the legacy key check.
		line := "auth validate route not mounted: no legacy key"
		if !eddsa {
			line = "auth validate route not mounted: AUTH_TOKEN_ISSUER_URL is unset (no access-token verifier)"
		}
		if !strings.Contains(f.logs.String(), line) {
			t.Fatalf("eddsa=%v: no not-mounted line %q: %s", eddsa, line, f.logs)
		}
	}
}
