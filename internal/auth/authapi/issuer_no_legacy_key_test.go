package authapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// The issuer takes no HS256 key and mints the EdDSA token.
func TestNewEdDSAIssuerMintsWithNoHS256Key(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer, err := NewEdDSAIssuer(contracts.PlatformAPIAudience, signer, goldenIssuer)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := issuer.Access(Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil || compact == "" {
		t.Fatalf("the issuer mints no EdDSA token: %v", err)
	}
}

// Product rules 1 and 3: without a legacy key the validate route mounts under eddsa (legacy served it; a missing key is
// no unmount); without the access-token verifier the issuer check refuses the mount and says so.
func TestValidateRouteMountsWithoutLegacyKeyUnderEdDSA(t *testing.T) {
	for _, eddsa := range []bool{false, true} {
		f := newValidateFixture(t, eddsa)
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

// Product rule 1: with no HS256 key the validate route answers an EdDSA user token.
func TestValidateAnswersAnEdDSATokenWithNoHS256Key(t *testing.T) {
	f := newValidateFixture(t, true)
	token := f.eddsaToken(contracts.PlatformAPIAudience)
	expectAnswer(t, "EdDSA token", f.post(f.server, tokenBody(token), nil), http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "admin"))
}
