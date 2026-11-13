package sso

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e/ssoseed"
)

// The test-only seed stand-in (internal/cigates/livee2e/ssoseed, CHAOS-13636) mirrors the production row. These cells turn red when
// the production row, its column lists or the config and secret readers change under it. Deleted with the stand-in when
// CHAOS-13636 lands.

func columnNames(list string) []string {
	var out []string
	for _, c := range strings.Split(list, ",") {
		c = strings.TrimSpace(c)
		if i := strings.Index(c, "::"); i >= 0 {
			c = c[:i]
		}
		out = append(out, c)
	}
	return out
}

func TestSSOSeedColumnsAreTheProductionColumnListsInOrder(t *testing.T) {
	got := ssoseed.Columns()
	if want := columnNames(oidcProviderColumns); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("seed columns %v, production oidcProviderColumns %v", got, want)
	}
	plain := columnNames(providerColumns)
	if len(got) < len(plain) || strings.Join(got[:len(plain)], ",") != strings.Join(plain, ",") {
		t.Errorf("seed columns %v do not start with production providerColumns %v", got, plain)
	}
}

func TestSSOSeedProviderMirrorsTheProductionRowFieldForField(t *testing.T) {
	prod, seed := reflect.TypeOf(providerRow{}), reflect.TypeOf(ssoseed.Provider{})
	if prod.NumField() != seed.NumField() {
		t.Fatalf("providerRow has %d fields, ssoseed.Provider %d", prod.NumField(), seed.NumField())
	}
	for i := 0; i < prod.NumField(); i++ {
		p, s := prod.Field(i), seed.Field(i)
		if p.Name != s.Name || p.Type != s.Type {
			t.Errorf("field %d: providerRow %s %s, ssoseed.Provider %s %s", i, p.Name, p.Type, s.Name, s.Type)
		}
	}
}

func TestSSOSeedConfigsDecodeWithTheProductionDecoders(t *testing.T) {
	oidc, err := ssoseed.OIDCConfig("https://idp.test", "client-1", "https://idp.test/authorize", "https://idp.test/token", "https://idp.test/userinfo", "https://idp.test/jwks")
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeOIDCConfig(&oidc)
	if err != nil || got.ClientID != "client-1" || got.Issuer != "https://idp.test" || got.TokenEndpoint != "https://idp.test/token" ||
		got.AuthorizationEndpoint != "https://idp.test/authorize" || got.UserinfoEndpoint != "https://idp.test/userinfo" || got.JWKSURI != "https://idp.test/jwks" {
		t.Errorf("decodeOIDCConfig = %+v, %v", got, err)
	}
	providerID := uuid.NewString()
	saml, err := ssoseed.SAMLConfig(providerID, "https://idp.test/saml", "https://idp.test/saml/sso", "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n", "urn:example:sp", "http://127.0.0.1:3002")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeSAMLConfig(&saml, providerID)
	if err != nil || cfg.EntityID != "https://idp.test/saml" || cfg.SSOURL != "https://idp.test/saml/sso" || cfg.SPEntityID != "urn:example:sp" ||
		cfg.SPACSURL != "http://127.0.0.1:3002/api/v1/auth/saml/"+providerID+"/acs" || !strings.Contains(cfg.Certificate, "BEGIN CERTIFICATE") {
		t.Errorf("decodeSAMLConfig = %+v, %v", cfg, err)
	}
}

// The production reader gives back the secret the stand-in sealed: the fallback in decryptProviderSecret returns the stored text
// on any failure, so the cell also requires that the stored text is not the secret.
func TestSSOSeedSecretIsReadBackByTheProductionReader(t *testing.T) {
	const key, secret = "ssoseed-drift-key-0123456789", "drift-client-secret"
	stored, err := ssoseed.Seal(key, "", secret)
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := decryptProviderSecret(cipher, &stored, "client_secret"); got != secret {
		t.Errorf("decryptProviderSecret = %q, want the sealed secret", got)
	}
	object, err := decodeConfigObject(&stored)
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := object.Get("client_secret"); raw == secret {
		t.Error("the stored client_secret is the plain secret: the production fallback would hide it")
	}
}
