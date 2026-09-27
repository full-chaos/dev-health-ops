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
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/crewjam/saml"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// noSubjectConfirmation strips every SubjectConfirmation element
	// BEFORE signing, so the returned assertion is genuinely, validly
	// signed with zero of them -- proving r1's P2 finding (Go accepted
	// this; Python's process_saml_response explicitly rejects it).
	noSubjectConfirmation bool
	// D2744 P3 split (an r1 finding that TestSAMLACSRefusesAnExpiredAssertion
	// set BOTH Conditions.NotOnOrAfter and SubjectConfirmationData.NotOnOrAfter
	// to the same past value, so deleting EITHER individual check in
	// production would still leave the test passing via the other one):
	// conditionsOnlyNotOnOrAfter sets ONLY Conditions.NotOnOrAfter (the
	// confirmation's own stays valid); confirmationOnlyNotOnOrAfter sets
	// ONLY SubjectConfirmationData.NotOnOrAfter (Conditions stays valid).
	conditionsOnlyNotOnOrAfter, confirmationOnlyNotOnOrAfter time.Time
	// D2744 P3 split (an r1 finding that TestSAMLACSRefusesAWrongIssuer's
	// single fixture mismatches BOTH the Response's and the Assertion's
	// Issuer at once, since both derive from the same idp.idp.MetadataURL
	// at generation time -- deleting EITHER individual crewjam check would
	// still leave the test passing via the other one): wrongIssuerAt
	// ("response" or "assertion") transiently swaps idp.idp.MetadataURL
	// for ONLY the one generation step that produces that element, so the
	// OTHER element is signed with the real, correct issuer.
	wrongIssuerAt string
	// authnRequestID overrides the fixture's own hand-built AuthnRequest's
	// ID attribute -- D2744's allow_idp_initiated=false tests need the
	// assertion's SubjectConfirmationData.InResponseTo (which crewjam
	// derives from this exact ID, identity_provider.go:811/1045) to match
	// the REAL requestID a genuine /initiate call embedded in its sealed
	// RelayState, not this fixture's own independently-generated one.
	authnRequestID string
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
	authnRequestID := opts.authnRequestID
	if authnRequestID == "" {
		authnRequestID = "_authnreq" + fmt.Sprint(now.UnixNano())
	}
	authnRequest := `<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
	xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
	ID="` + authnRequestID + `" Version="2.0" IssueInstant="` + now.Format("2006-01-02T15:04:05Z") + `"
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
	// D2744 P3 split: the assertion's own Issuer derives from
	// idp.idp.MetadataURL at THIS exact call (identity_provider.go's
	// MakeAssertion), so a transient swap here -- restored immediately
	// after -- signs the assertion with a wrong issuer while leaving the
	// (not-yet-generated) Response's own issuer untouched.
	realMetadataURL := idp.idp.MetadataURL
	if opts.wrongIssuerAt == "assertion" {
		wrong, _ := url.Parse("https://wrong-issuer.test/assertion")
		idp.idp.MetadataURL = *wrong
	}
	if err := (saml.DefaultAssertionMaker{}).MakeAssertion(&req, idp.idp.SessionProvider.(staticSessionProvider).session); err != nil {
		t.Fatalf("make assertion: %v", err)
	}
	idp.idp.MetadataURL = realMetadataURL

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
	if !opts.conditionsOnlyNotOnOrAfter.IsZero() {
		req.Assertion.Conditions.NotOnOrAfter = opts.conditionsOnlyNotOnOrAfter
	}
	if !opts.confirmationOnlyNotOnOrAfter.IsZero() {
		req.Assertion.Subject.SubjectConfirmations[0].SubjectConfirmationData.NotOnOrAfter = opts.confirmationOnlyNotOnOrAfter
	}
	if opts.noSubjectConfirmation {
		req.Assertion.Subject.SubjectConfirmations = nil
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
	// D2744 P3 split: the Response's own Issuer derives from
	// idp.idp.MetadataURL at THIS exact call (identity_provider.go's
	// MakeResponse) -- the assertion was already signed (with the real
	// issuer) above, so swapping only here signs the Response with a
	// wrong issuer while the Assertion's stays correct.
	if opts.wrongIssuerAt == "response" {
		wrong, _ := url.Parse("https://wrong-issuer.test/response")
		idp.idp.MetadataURL = *wrong
	}
	if err := req.MakeResponse(); err != nil {
		t.Fatalf("make response: %v", err)
	}
	idp.idp.MetadataURL = realMetadataURL

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

// TestSAMLACSRefusesAReplayedAssertion pins D2744's r1 P1 fix: a
// validly-signed, still-unexpired assertion carried NO replay protection
// at all before this ruling -- the same captured SAMLResponse could be
// presented to /acs repeatedly within its NotOnOrAfter window and mint a
// fresh token pair each time. The SAME raw samlResponse string, POSTed
// twice: the first succeeds for real (a genuine login), the second is
// refused as a replay -- authenticated (the assertion IS genuinely valid)
// but denied, not unauthenticated, and never a row mutation (D2742/D2744's
// third bucket, recordSSOAuthenticatedDenial).
func TestSAMLACSRefusesAReplayedAssertion(t *testing.T) {
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
		email: "replay@example.test", fullName: "Replay", nameID: "replay@example.test",
	})

	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusOK {
		t.Fatalf("first presentation: status=%d body=%v, want 200", status, body)
	}

	status, body = postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("second presentation (replay): status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "replay")

	var auditFailureCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'sso_login' AND status = 'failure'`,
		orgID).Scan(&auditFailureCount); err != nil {
		t.Fatal(err)
	}
	if auditFailureCount != 1 {
		t.Fatalf("audit_logs: got %d failure sso_login rows, want exactly 1 (the replay)", auditFailureCount)
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

// TestSAMLACSRefusesAnAssertionWithNoSubjectConfirmation pins r1's P2
// finding: crewjam/saml's own validateAssertion iterates
// assertion.Subject.SubjectConfirmations but never rejects an EMPTY list
// -- the per-confirmation Recipient/NotOnOrAfter checks simply never run,
// so a validly-signed assertion with zero of them sailed through
// ParseXMLResponse with no error at all before this fix. Python's
// process_saml_response explicitly requires at least one
// (sso.py:576-578). This is a genuine parity break (Go strictly more
// permissive), not a delta to preserve -- unlike the state_auth-stage
// tests above, this failure occurs AFTER signature verification
// succeeds, so it legitimately flips the row (Python's identical
// SAMLProcessingError does too, via record_error).
func TestSAMLACSRefusesAnAssertionWithNoSubjectConfirmation(t *testing.T) {
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
		email: "noconfirmation@example.test", fullName: "No Confirmation", nameID: "noconfirmation@example.test",
		noSubjectConfirmation: true,
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	var lastError *string
	var providerStatus string
	if err := st.pool.QueryRow(ctx, `SELECT last_error, status FROM sso_providers WHERE id = $1`, providerID).
		Scan(&lastError, &providerStatus); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || !strings.Contains(*lastError, "subject confirmation") {
		t.Fatalf("last_error = %v, want a recorded subject-confirmation reason", lastError)
	}
	if providerStatus != "error" {
		t.Fatalf("provider status = %q, want %q: a genuinely-authenticated but structurally-invalid assertion must still flip it", providerStatus, "error")
	}
}

// assertAuditErrorMessageContains is D2744's P3 split's own check: the
// audit row's error_message column (never shown to the caller, whose
// response is a fixed generic string either way) is the only place the
// SPECIFIC crewjam check that fired is recorded, so this is what proves
// each split test actually exercised the check it claims to.
func assertAuditErrorMessageContains(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, want string) {
	t.Helper()
	var errMsg *string
	if err := pool.QueryRow(ctx, `SELECT error_message FROM audit_logs
WHERE org_id = $1 AND action = 'sso_login' AND status = 'failure' ORDER BY created_at DESC LIMIT 1`, orgID).Scan(&errMsg); err != nil {
		t.Fatal(err)
	}
	if errMsg == nil || !strings.Contains(*errMsg, want) {
		t.Fatalf("audit error_message = %v, want it to contain %q", errMsg, want)
	}
}

// TestSAMLACSRefusesAWrongIssuerAtTheResponseLevel and
// TestSAMLACSRefusesAWrongIssuerAtTheAssertionLevel are D2744's split of
// an r1 P3 finding: the original single test mismatched BOTH the
// Response's and the Assertion's Issuer at once (both derive from the
// same idp.idp.MetadataURL at generation time), so deleting either
// individual crewjam check would still leave that one test passing via
// the other. These two independently sign ONLY one element with a wrong
// issuer (samlResponseOpts.wrongIssuerAt), proving each check fires on
// its own via the audit row's specific recorded reason.
func TestSAMLACSRefusesAWrongIssuerAtTheResponseLevel(t *testing.T) {
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
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
		wrongIssuerAt: "response",
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertAuditErrorMessageContains(t, ctx, st.pool, orgID, "does not match the IDP metadata")
}

func TestSAMLACSRefusesAWrongIssuerAtTheAssertionLevel(t *testing.T) {
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
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
		wrongIssuerAt: "assertion",
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertAuditErrorMessageContains(t, ctx, st.pool, orgID, "issuer is not")
}

// TestSAMLACSRefusesAnAssertionWithExpiredConditions and
// TestSAMLACSRefusesAnAssertionWithExpiredSubjectConfirmation are
// D2744's split of another r1 P3 finding: the original single test set
// BOTH Conditions.NotOnOrAfter and SubjectConfirmationData.NotOnOrAfter
// to the same past value, so deleting either individual timestamp check
// would still leave that one test passing via the other.
func TestSAMLACSRefusesAnAssertionWithExpiredConditions(t *testing.T) {
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
		conditionsOnlyNotOnOrAfter: time.Now().Add(-1 * time.Hour),
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertAuditErrorMessageContains(t, ctx, st.pool, orgID, "expired")
}

func TestSAMLACSRefusesAnAssertionWithExpiredSubjectConfirmation(t *testing.T) {
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
		confirmationOnlyNotOnOrAfter: time.Now().Add(-1 * time.Hour),
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertAuditErrorMessageContains(t, ctx, st.pool, orgID, "expired")
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

// TestSAMLConfigRefusesAnInsecureHTTPOverride pins D2748/D2745's
// class ruling (HTTPS-only enforcement extended from OAuth's base_url
// check to SAML): an admin-overridden sp_acs_url naming the insecure
// http:// scheme is refused at config-load time (decodeSAMLConfig ->
// validateSAMLConfigHTTPS, saml.go) -- before initiateSAMLAuth ever
// builds a redirect, not merely a property the login-nonce cookie's
// literal Secure:true (samlstate.go) happens to assume at runtime.
func TestSAMLConfigRefusesAnInsecureHTTPOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"http://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true,
	})

	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusInternalServerError {
		t.Fatalf("initiate: status=%d body=%v, want 500 (an insecure sp_acs_url override refused at config-load time)", status, body)
	}
	if _, ok := body["redirect_url"]; ok {
		t.Fatalf("initiate: got a redirect_url for a config this package should have refused: %v", body)
	}
}

// inflateAndExtractRequestID inverts initiateSAMLAuth's own deflateRaw
// (raw DEFLATE, no zlib header/trailer) to recover the real AuthnRequest
// XML a genuine /initiate call generated, and extracts its own ID
// attribute -- the value crewjam's possibleRequestIDs check (D2744)
// requires the assertion's SubjectConfirmationData.InResponseTo to
// match.
func inflateAndExtractRequestID(t *testing.T, samlRequestB64 string) string {
	t.Helper()
	deflated, err := base64.StdEncoding.DecodeString(samlRequestB64)
	if err != nil {
		t.Fatal(err)
	}
	reader := flate.NewReader(bytes.NewReader(deflated))
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	const marker = `ID="`
	idx := strings.Index(string(raw), marker)
	if idx < 0 {
		t.Fatalf("no ID attribute found in inflated AuthnRequest: %s", raw)
	}
	rest := string(raw)[idx+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("malformed ID attribute in inflated AuthnRequest: %s", raw)
	}
	return rest[:end]
}

// postJSONWithClient is postJSON but through a caller-supplied client, so
// tests that need /initiate's Set-Cookie (the D2745 browser-binding login
// nonce) to actually reach the following /acs POST -- or need to control
// whether it does -- can use a cookiejar-backed http.Client instead of
// postJSON's bare http.Post, which carries no cookie jar at all.
func postJSONWithClient(t *testing.T, client *http.Client, server *httptest.Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post(server.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestSAMLACSHonoursAllowIdpInitiatedFalse pins D2744's r1 P1 fix's
// happy path: a provider with allow_idp_initiated=false requires a real
// AEAD RelayState minted by a genuine /initiate call, carrying the exact
// AuthnRequest ID the IdP's assertion must echo back in
// SubjectConfirmationData.InResponseTo -- and a real end-to-end round
// trip through both routes succeeds when that's exactly what happens. It
// also exercises D2745's browser-binding login-nonce cookie's happy path:
// a cookiejar-backed client carries /initiate's Set-Cookie to /acs, the
// same way a genuine browser would.
func TestSAMLACSHonoursAllowIdpInitiatedFalse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}

	status, initResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("initiate: status=%d body=%v", status, initResp)
	}
	redirectURL, err := url.Parse(initResp["redirect_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	relayState := redirectURL.Query().Get("RelayState")
	if relayState == "" {
		t.Fatal("initiate: missing RelayState for an allow_idp_initiated=false provider")
	}
	requestID := inflateAndExtractRequestID(t, redirectURL.Query().Get("SAMLRequest"))

	const testEmail = "not-idp-initiated@example.test"
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: testEmail, fullName: "Not IdP Initiated", nameID: testEmail,
		authnRequestID: requestID,
	})
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs",
		map[string]any{"SAMLResponse": samlResponse, "RelayState": relayState})
	if status != http.StatusOK {
		t.Fatalf("acs: status=%d body=%v", status, body)
	}
	if body["access_token"] == "" || body["refresh_token"] == "" {
		t.Fatalf("acs: expected a token pair, got %v", body)
	}
	if body["email"] != testEmail {
		t.Fatalf("acs: email=%v, want %s", body["email"], testEmail)
	}
}

// TestSAMLACSRefusesAMissingLoginNonceCookie and
// TestSAMLACSRefusesATamperedLoginNonceCookie pin D2745's browser-binding
// class ruling applied to SAML (from CHAOS-6986's P1-3): a genuine,
// unexpired RelayState alone is not enough when the provider disallows
// IdP-initiated logins -- the login nonce cookie set at /initiate must
// also be present and match. A missing cookie is exactly what a
// cross-site (login-CSRF) submission looks like: SameSite=Lax withholds
// the cookie on a cross-site POST, so the attack surface this closes
// arrives at the handler indistinguishable from "no cookie sent".
func TestSAMLACSRefusesAMissingLoginNonceCookie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	// A real /initiate call, but WITHOUT a cookie jar: the Set-Cookie is
	// dropped on the floor, exactly as if the following /acs POST arrived
	// from a different browser (or a cross-site submission SameSite=Lax
	// already would have withheld the cookie from).
	status, initResp := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("initiate: status=%d body=%v", status, initResp)
	}
	redirectURL, err := url.Parse(initResp["redirect_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	relayState := redirectURL.Query().Get("RelayState")
	requestID := inflateAndExtractRequestID(t, redirectURL.Query().Get("SAMLRequest"))

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
		authnRequestID: requestID,
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs",
		map[string]any{"SAMLResponse": samlResponse, "RelayState": relayState})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

func TestSAMLACSRefusesATamperedLoginNonceCookie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}

	status, initResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("initiate: status=%d body=%v", status, initResp)
	}
	redirectURL, err := url.Parse(initResp["redirect_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	relayState := redirectURL.Query().Get("RelayState")
	requestID := inflateAndExtractRequestID(t, redirectURL.Query().Get("SAMLRequest"))

	// Tamper with the jar's own cookie value in place -- the RelayState
	// itself stays genuine and unexpired; only the browser-side half of
	// the binding is wrong, which is exactly the case this check exists
	// to catch (a genuine RelayState presented by the wrong browser).
	serverURL, err := url.Parse(st.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cookies := jar.Cookies(serverURL)
	found := false
	for _, c := range cookies {
		if c.Name == "dho_saml_login_nonce" {
			c.Value = flipMiddleByte(c.Value)
			found = true
		}
	}
	if !found {
		t.Fatal("initiate: expected a dho_saml_login_nonce cookie for an allow_idp_initiated=false provider")
	}
	jar.SetCookies(serverURL, cookies)

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
		authnRequestID: requestID,
	})
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs",
		map[string]any{"SAMLResponse": samlResponse, "RelayState": relayState})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

// TestSAMLACSRefusesMissingRelayStateWhenNotIdpInitiated and
// TestSAMLACSRefusesATamperedRelayStateWhenNotIdpInitiated pin D2744's
// r1 P1 fix's refusal paths: a provider with allow_idp_initiated=false
// requires a real RelayState, verified BEFORE ParseXMLResponse runs, so
// neither a missing one nor a tampered one ever reaches signature
// verification -- both are D2738's unauthenticated bucket (audit-only,
// no row mutation), since the caller has proven nothing about the
// initiate/callback correlation either way.
func TestSAMLACSRefusesMissingRelayStateWhenNotIdpInitiated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": samlResponse})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

func TestSAMLACSRefusesATamperedRelayStateWhenNotIdpInitiated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	status, initResp := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("initiate: status=%d body=%v", status, initResp)
	}
	redirectURL, err := url.Parse(initResp["redirect_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	relayState := flipMiddleByte(redirectURL.Query().Get("RelayState"))
	requestID := inflateAndExtractRequestID(t, redirectURL.Query().Get("SAMLRequest"))

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: "https://sp.test/acs",
		email: "someone@example.test", fullName: "Someone", nameID: "someone@example.test",
		authnRequestID: requestID,
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs",
		map[string]any{"SAMLResponse": samlResponse, "RelayState": relayState})
	if status != http.StatusBadRequest || body["detail"] != "SAML authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 SAML authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}
