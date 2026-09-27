//go:build integration

// Real-producer coverage for CHAOS-6659 (samlMetadata / initiateSAMLAuth /
// samlACSCallback), the same posture as oidc_integration_test.go: no
// differential oracle against a live Python producer, because the one
// real reason the Python code path is unusable in production (signxml is
// an optional dependency the deployed image does not install, saml.go's
// own doc comment) makes it equally unreachable in the CI venv this
// repo's oracle tooling builds from (--extra dev does not pull
// enterprise-sso). Unlike OIDC, nothing here is dead BY DESIGN in
// Python -- only by a missing optional dependency -- so this is recorded
// as a narrower, more contingent gap than OIDC's, not assumed permanent.
//
// This file drives the full round trip against a real self-signed test
// IdP fixture -- a real signed SAML Response built with
// github.com/crewjam/saml's OWN IdentityProvider signing pipeline (the
// ADR CHAOS-4883-mandated dependency's exported API: IdpAuthnRequest,
// DefaultAssertionMaker, MakeAssertionEl/MakeResponse), the same library
// this package's production verifier (saml.go's processSAMLResponse) uses
// on the verify side via ServiceProvider.ParseXMLResponse -- against a
// real containerized Postgres and the real handler chain.
//
// D2737 (team-lead): an earlier version of this fixture hand-assembled
// goxmldsig SignEnveloped calls directly, and production code verified
// with hand-assembled goxmldsig ValidationContext.Validate calls; NEITHER
// half of that pairing is used any more (see saml.go's own doc comment
// for the reproduction that led to this). Both signing (here) and
// verifying (saml.go) now go through crewjam/saml's own exported,
// actively-tested API.
package sso_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/crewjam/saml"
)

// samlIdP is a self-signed test IdP fixture: an RSA key + a self-signed
// certificate, wired into a real saml.IdentityProvider so signing goes
// through crewjam/saml's own pipeline.
type samlIdP struct {
	key  *rsa.PrivateKey
	cert *x509.Certificate
	der  []byte
	idp  *saml.IdentityProvider
}

func newSAMLIdP(t *testing.T) *samlIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "saml-6659-test-idp"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	idpMetaURL, _ := url.Parse("https://idp.test/saml/metadata")
	idpSSOURL, _ := url.Parse("https://idp.test/saml/sso")
	idp := &saml.IdentityProvider{
		Key:         key,
		Certificate: cert,
		MetadataURL: *idpMetaURL,
		SSOURL:      *idpSSOURL,
	}
	return &samlIdP{key: key, cert: cert, der: der, idp: idp}
}

// certConfigValue is what this fixture's provider config stores for
// "certificate": bare base64 DER, the same shape parseSAMLCertificate's
// non-PEM branch reads.
func (idp *samlIdP) certConfigValue() string {
	return base64.StdEncoding.EncodeToString(idp.der)
}

type samlResponseOpts struct {
	spEntityID, acsURL, email, fullName, nameID string
	notOnOrAfter                                time.Time
	notBefore                                   time.Time
	skipSign                                    bool
	tamperAfterSign                             bool
}

type staticSPProvider struct{ sp *saml.EntityDescriptor }

func (p staticSPProvider) GetServiceProvider(_ *http.Request, _ string) (*saml.EntityDescriptor, error) {
	return p.sp, nil
}

type staticSessionProvider struct{ session *saml.Session }

func (p staticSessionProvider) GetSession(_ http.ResponseWriter, _ *http.Request, _ *saml.IdpAuthnRequest) *saml.Session {
	return p.session
}

// signedSAMLResponse builds a real SAML Response, with one genuinely
// signed Assertion, through crewjam/saml's own IdentityProvider pipeline:
// a minimal AuthnRequest -> Validate -> MakeAssertion -> (field overrides
// for the exact content this test wants) -> MakeAssertionEl (signs) ->
// MakeResponse, matching exactly what processSAMLResponse (saml.go) reads
// (Status/StatusCode, Issuer, Conditions, AudienceRestriction/Audience,
// Subject/NameID, SubjectConfirmationData, an AttributeStatement carrying
// "email"/"full_name"). skipSign strips the signature after the fact
// (still real, well-formed content, just unsigned); tamperAfterSign
// mutates the final signed text (breaks the signature, as any tamper
// must).
func (idp *samlIdP) signedSAMLResponse(t *testing.T, opts samlResponseOpts) string {
	t.Helper()
	sp := &saml.EntityDescriptor{
		EntityID: opts.spEntityID,
		SPSSODescriptors: []saml.SPSSODescriptor{{
			AssertionConsumerServices: []saml.IndexedEndpoint{{
				Binding: "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST", Location: opts.acsURL, Index: 0,
			}},
		}},
	}
	idp.idp.ServiceProviderProvider = staticSPProvider{sp: sp}
	idp.idp.SessionProvider = staticSessionProvider{session: &saml.Session{
		ID: "test-session", UserName: opts.email, UserEmail: opts.email, UserCommonName: opts.fullName,
		NameID: opts.nameID, CreateTime: time.Now(),
	}}

	now := time.Now().UTC()
	authnRequest := `<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
	xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
	ID="_authnreq` + fmt.Sprint(now.UnixNano()) + `" Version="2.0" IssueInstant="` + now.Format("2006-01-02T15:04:05Z") + `"
	Destination="` + idp.idp.SSOURL.String() + `"
	AssertionConsumerServiceURL="` + opts.acsURL + `"
	ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST">
	<saml:Issuer>` + opts.spEntityID + `</saml:Issuer>
</samlp:AuthnRequest>`

	req := saml.IdpAuthnRequest{
		IDP: idp.idp, Now: now, RequestBuffer: []byte(authnRequest), HTTPRequest: &http.Request{},
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("validate authn request: %v", err)
	}
	if err := (saml.DefaultAssertionMaker{}).MakeAssertion(&req, idp.idp.SessionProvider.(staticSessionProvider).session); err != nil {
		t.Fatalf("make assertion: %v", err)
	}

	// Override with exactly what this test asked for -- MakeAssertion's
	// own field derivations (audience/recipient/timestamps) are correct
	// defaults, but the expired/tampered-timestamp tests need explicit
	// control.
	if !opts.notBefore.IsZero() {
		req.Assertion.Conditions.NotBefore = opts.notBefore
	}
	if !opts.notOnOrAfter.IsZero() {
		req.Assertion.Conditions.NotOnOrAfter = opts.notOnOrAfter
		req.Assertion.Subject.SubjectConfirmations[0].SubjectConfirmationData.NotOnOrAfter = opts.notOnOrAfter
	}
	// A custom "email"/"full_name" attribute statement, matching what
	// this package's own attribute_mapping convention (config.
	// AttributeMapping, saml.go) and the default fallback both expect --
	// crewjam's own auto-added attributes use different (real-world,
	// URN-OID) names this package's own tests don't configure a mapping
	// for.
	req.Assertion.AttributeStatements = []saml.AttributeStatement{{
		Attributes: []saml.Attribute{
			{Name: "email", Values: []saml.AttributeValue{{Type: "xs:string", Value: opts.email}}},
			{Name: "full_name", Values: []saml.AttributeValue{{Type: "xs:string", Value: opts.fullName}}},
		},
	}}

	if err := req.MakeAssertionEl(); err != nil {
		t.Fatalf("make assertion el: %v", err)
	}
	if err := req.MakeResponse(); err != nil {
		t.Fatalf("make response: %v", err)
	}

	out, err := etree.NewDocumentWithRoot(req.ResponseEl).WriteToString()
	if err != nil {
		t.Fatalf("serialize response: %v", err)
	}
	if opts.skipSign {
		// MakeResponse signs BOTH the Response and the Assertion (a real,
		// commonly-seen IdP pattern); strip every ds:Signature block, not
		// just the first, or the Assertion's own signature alone still
		// verifies.
		for {
			start := strings.Index(out, "<ds:Signature")
			if start < 0 {
				break
			}
			end := strings.Index(out[start:], "</ds:Signature>")
			if end < 0 {
				break
			}
			out = out[:start] + out[start+end+len("</ds:Signature>"):]
		}
	}
	if opts.tamperAfterSign {
		out = strings.Replace(out, opts.email, "tampered-"+opts.email, 1)
	}
	return base64.StdEncoding.EncodeToString([]byte(out))
}

func TestSAMLMetadataAndInitiate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":"https://idp.test/entity","sso_url":"https://idp.test/sso","certificate":%q}`, idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	status, body := getJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/metadata")
	if status != http.StatusOK {
		t.Fatalf("metadata: status=%d body=%v", status, body)
	}
	metadataXML, _ := body["metadata_xml"].(string)
	if !strings.Contains(metadataXML, `entityID="`) || !strings.Contains(metadataXML, "AssertionConsumerService") {
		t.Fatalf("metadata_xml missing expected content: %s", metadataXML)
	}

	status, authResp := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("initiate: status=%d body=%v", status, authResp)
	}
	redirectURL, _ := authResp["redirect_url"].(string)
	if !strings.HasPrefix(redirectURL, "https://idp.test/sso?") || !strings.Contains(redirectURL, "SAMLRequest=") {
		t.Fatalf("redirect_url = %q, want the configured sso_url with a SAMLRequest param", redirectURL)
	}
}

func getJSON(t *testing.T, server *httptest.Server, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestSAMLACSFullRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	const (
		spEntityID = "https://sp.test/saml/metadata"
		acsURL     = "https://sp.test/saml/acs"
	)
	entityID := idp.idp.MetadataURL.String()
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		entityID, idp.certConfigValue(), spEntityID, acsURL)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	const testEmail = "saml.user@allowed.example"
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: spEntityID, acsURL: acsURL, email: testEmail, fullName: "SAML User", nameID: testEmail,
	})

	status, resp := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusOK {
		var lastError *string
		_ = st.pool.QueryRow(ctx, `SELECT last_error FROM sso_providers WHERE id = $1`, providerID).Scan(&lastError)
		last := "<nil>"
		if lastError != nil {
			last = *lastError
		}
		t.Fatalf("acs: status=%d body=%v last_error=%s", status, resp, last)
	}
	if resp["access_token"] == "" || resp["refresh_token"] == "" {
		t.Fatalf("acs: expected a token pair, got %v", resp)
	}
	if resp["email"] != testEmail {
		t.Fatalf("acs: email=%v, want %s", resp["email"], testEmail)
	}

	var lastLogin any
	if err := st.pool.QueryRow(ctx, `SELECT last_login_at FROM sso_providers WHERE id = $1`, providerID).Scan(&lastLogin); err != nil {
		t.Fatal(err)
	}
	if lastLogin == nil {
		t.Fatal("record_login: sso_providers.last_login_at was not set")
	}
	var auditCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'sso_login' AND status = 'success'`,
		orgID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("audit_logs: got %d success sso_login rows, want 1", auditCount)
	}
}

func TestSAMLACSRefusesATamperedAssertion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "victim@example.test", fullName: "Victim", nameID: "victim@example.test", tamperAfterSign: true,
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	// D2738 (mirrored from OIDC's r1-P1 fix on #3347): a tampered assertion
	// is unauthenticated -- the caller proved nothing, so the provider row
	// must stay untouched. The reason still lands in the audit trail.
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "signature_auth")
}

func TestSAMLACSRefusesAnUnsignedAssertion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "nobody@example.test", fullName: "Nobody", nameID: "nobody@example.test", skipSign: true,
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "signature_auth")
}

func TestSAMLACSRefusesAWrongIssuer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	// Deliberately mismatched: the provider is CONFIGURED to trust
	// "https://attacker.test/entity" as the IdP's entity_id, but the
	// fixture is signed by the real test IdP, whose actual entity_id is
	// idp.idp.MetadataURL.String() -- a genuine issuer mismatch, not a
	// simulated one.
	config := fmt.Sprintf(`{"entity_id":"https://attacker.test/entity","sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	// D2738, corrected: an earlier version of this test asserted the row
	// DOES flip here, reasoning an issuer mismatch is provider-config-only
	// and unforgeable. Team-lead's follow-up correctly challenged that:
	// crewjam/saml's response-level issuer check (saml.go's own comment on
	// the ParseXMLResponse call site) fires BEFORE the deferred signature
	// check is ever consulted, so an attacker can trigger this identical
	// error with a completely unsigned, forged Issuer value -- there is no
	// way to tell "genuine config problem" apart from "forged garbage"
	// from the error alone. So this stays in the safe, unauthenticated
	// bucket like every other ParseXMLResponse failure: no row mutation.
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "signature_auth")
}

func TestSAMLACSRefusesAnExpiredAssertion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "late@example.test", fullName: "Late", nameID: "late@example.test",
		notBefore: time.Now().Add(-2 * time.Hour), notOnOrAfter: time.Now().Add(-1 * time.Hour),
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	// An expired assertion mirrors OIDC's expired-state treatment: still
	// unauthenticated (D2738), even though it was genuinely signed by the
	// real IdP -- possession of a validity window that has passed proves
	// nothing about the CURRENT request being legitimate.
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "signature_auth")
}

func TestSAMLRefusesANonEntitledOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "team")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: "{}", autoProvision: true})

	status, body := getJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/metadata")
	if status != http.StatusPaymentRequired {
		t.Fatalf("status=%d body=%v, want 402", status, body)
	}
}
