package oidc

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
)

// jwksExtraKeys are keys an IdP may publish next to its signing key that go-jose v4.1.6 refuses to parse. Fixed bytes:
// the RSA modulus is 8192 bits (over go-jose's MaxRSAModulusBits 4096), the Ed25519 x is 31 bytes (not 32).
var jwksExtraKeys = map[string]map[string]any{
	"rsa 8192-bit extra key": {"kty": "RSA", "kid": "big", "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(append([]byte{0xC5}, bytes.Repeat([]byte{0xA7}, 1023)...)),
		"e": "AQAB"},
	"ed25519 extra key of wrong length": {"kty": "OKP", "crv": "Ed25519", "kid": "ed", "use": "sig", "alg": "EdDSA",
		"x": base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 31))},
}

// jwksCallback runs one OIDC callback through the route against a unit IdP whose /jwks serves the signing key k1 and
// extra; the account is linked and a member, so only the ID token decides the answer.
func jwksCallback(t *testing.T, extra map[string]any) (*unitEnv, *unitIdP, *oidcFailures, storedRow, *httptest.ResponseRecorder) {
	t.Helper()
	idp := startUnitIdP(t)
	if extra != nil {
		idp.extraJWKs = []map[string]any{extra}
	}
	c := routeCases[1]
	row := c.activeRow()
	row.Config = `{"client_id":"` + unitClient + `","issuer":"` + idp.server.URL + `","token_endpoint":"` + idp.server.URL +
		`/token","jwks_uri":"` + idp.server.URL + `/jwks"}`
	env := newUnitEnv(t, &fakeProviders{before: row})
	failures := &oidcFailures{}
	env.svc.cfg.HTTPClient, env.svc.oidcClient = idp.server.Client(), idp.server.Client()
	env.svc.cfg.Identities = &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
	env.svc.cfg.Memberships = resolveMembers{found: true, role: "admin"}
	env.svc.cfg.Sessions = &resolveSessions{}
	env.svc.cfg.ProviderFailure = failures
	claims := idp.claims(env.now)
	claims["nonce"], claims["email"] = "n-1", "p@example.com"
	idp.idToken = idp.sign(t, map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"}, claims)
	return env, idp, failures, row, env.serve(c, env.request(t, c, row, true))
}

// CHAOS-13723: since go-jose v4.1.6 a JWKS that holds one key go-jose cannot parse fails as a whole in go-oidc
// (only ErrUnsupportedKeyType is skipped), so a token signed by the valid key k1 is refused: our 400 authentication
// failure, reason bad_signature, go-api's id_token record, and one "request refused" line that carries the error class,
// never the go-jose text. Without the extra key the same callback signs in.
func TestJWKSWithAnUnparsableExtraKeyRefusesTheSignIn(t *testing.T) {
	t.Run("no extra key signs in", func(t *testing.T) {
		env, _, failures, _, rec := jwksCallback(t, nil)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"access_token":"a"`) {
			t.Fatalf("status %d body %s, want the 200 sign-in", rec.Code, rec.Body)
		}
		if got := outcomeCounts(t, env.reader); got["callback/success/accepted"] != 1 || len(got) != 1 {
			t.Fatalf("outcomes %v, want one callback/success/accepted", got)
		}
		if len(failures.rows) != 0 || !strings.Contains(env.logged.String(), `msg="auth oidc: request succeeded"`) {
			t.Fatalf("records %+v, log %q", failures.rows, env.logged.String())
		}
	})
	for name, extra := range jwksExtraKeys {
		t.Run(name, func(t *testing.T) {
			env, idp, failures, row, rec := jwksCallback(t, extra)
			if rec.Code != http.StatusBadRequest || detailOf(t, rec) != detailAuthFailed {
				t.Fatalf("status %d body %s log %q, want the 400 %q", rec.Code, rec.Body, env.logged.String(), detailAuthFailed)
			}
			if got := outcomeCounts(t, env.reader); got["callback/denied/"+string(ReasonBadSignature)] != 1 || len(got) != 1 {
				t.Fatalf("outcomes %v, want one callback/denied/%s", got, ReasonBadSignature)
			}
			if len(failures.rows) != 1 || failures.rows[0].ErrorMessage != msgIDTokenInvalid || failures.rows[0].ProviderID != row.ID {
				t.Fatalf("records %+v, want one %q for the provider row", failures.rows, msgIDTokenInvalid)
			}
			line := env.logged.String()
			for _, needle := range []string{`level=WARN msg="auth oidc: request refused" route=callback outcome=denied reason=bad_signature`,
				"provider_id=" + row.ID.String(), "status=400", "error.class=other"} {
				if !strings.Contains(line, needle) {
					t.Errorf("log %q lacks %q", line, needle)
				}
			}
			if strings.Count(line, "\n") != 1 || strings.Contains(line, "go-jose") {
				t.Errorf("log %q, want one refused line without the library text", line)
			}
			// The cause is the key set, not the signature: go-oidc reports the JWKS decode with go-jose's refusal.
			keys := gooidc.NewRemoteKeySet(gooidc.ClientContext(context.Background(), idp.server.Client()), idp.server.URL+"/jwks")
			_, err := verifyIDToken(context.Background(), keys, idp.idToken, claimRules{Issuer: idp.server.URL,
				ClientID: unitClient, Nonce: "n-1", Now: time.Unix(1_800_000_000, 0)})
			if err == nil || !strings.Contains(err.Error(), "failed to decode keys") || !strings.Contains(err.Error(), "go-jose/go-jose: invalid") {
				t.Fatalf("verifyIDToken = %v, want the JWKS decode refusal", err)
			}
		})
	}
}
