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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// samlIdP is the shared idpstub SAML provider with the metadata and SSO URLs of the former crewjam-backed fixture.
type samlIdP struct{ *idpstub.SAML }

func newSAMLIdP(t *testing.T) *samlIdP {
	t.Helper()
	idp, err := idpstub.NewSAML(idpstub.SAMLOptions{EntityID: "https://idp.test/saml/metadata", SSOURL: "https://idp.test/saml/sso"})
	if err != nil {
		t.Fatal(err)
	}
	return &samlIdP{idp}
}

// certConfigValue is what this fixture's provider config stores for
// "certificate": bare base64 DER, the same shape parseSAMLCertificate's
// non-PEM branch reads.
func (idp *samlIdP) certConfigValue() string { return idp.Keys().CertBase64() }

type samlResponseOpts struct {
	spEntityID, acsURL, email, fullName, nameID string
	// nameIDFormat and spNameQualifier, when set, replace the NameID attributes the assertion maker wrote.
	nameIDFormat, spNameQualifier string
	notOnOrAfter                  time.Time
	notBefore                     time.Time
	skipSign                      bool
	tamperAfterSign               bool
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
	// Issuer at once, since both derive from the same idp.EntityID()
	// at generation time -- deleting EITHER individual crewjam check would
	// still leave the test passing via the other one): wrongIssuerAt
	// ("response" or "assertion") transiently swaps idp.EntityID()
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

// signedSAMLResponse builds a SAML Response signed on both the Assertion and the Response, as the former crewjam
// pipeline did, with the content this test asked for. skipSign strips every signature after the fact (still
// well-formed content, just unsigned); tamperAfterSign mutates the final signed text (breaks the signature).
func (idp *samlIdP) signedSAMLResponse(t *testing.T, opts samlResponseOpts) string {
	t.Helper()
	authnRequestID := opts.authnRequestID
	if authnRequestID == "" {
		authnRequestID = "_authnreq" + fmt.Sprint(time.Now().UnixNano())
	}
	nameID := idpstub.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:transient", SPNameQualifier: opts.spEntityID, Value: opts.nameID}
	if opts.nameIDFormat != "" {
		nameID.Format = opts.nameIDFormat
	}
	if opts.spNameQualifier != "" {
		nameID.SPNameQualifier = opts.spNameQualifier
	}
	response, err := idp.ResponseB64(idpstub.ResponseSpec{
		SPEntityID: opts.spEntityID, ACSURL: opts.acsURL, InResponseTo: authnRequestID, NameID: nameID, ExplicitNameID: true,
		Email: opts.email, FullName: opts.fullName, SignResponse: true,
		NotBefore: opts.notBefore, NotOnOrAfter: opts.notOnOrAfter,
		ConditionsOnlyNotOnOrAfter: opts.conditionsOnlyNotOnOrAfter, ConfirmationOnlyNotOnOrAfter: opts.confirmationOnlyNotOnOrAfter,
		NoConfirmation: opts.noSubjectConfirmation, WrongIssuerAt: opts.wrongIssuerAt,
		SkipSign: opts.skipSign, TamperAfterSign: opts.tamperAfterSign,
	})
	if err != nil {
		t.Fatalf("build saml response: %v", err)
	}
	return response
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

// A lone surrogate in relay_state is refused as string_unicode where the
// value goes into the redirect_url query (allow_idp_initiated); where the
// route replaces it with its own sealed token it is still ignored.
func TestSAMLInitiateRefusesALoneSurrogateRelayStateOnlyWhereItIsForwarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":"https://idp.test/entity","sso_url":"https://idp.test/sso","certificate":%q}`, idp.certConfigValue())
	forwarded := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})
	sealed := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true})

	post := func(id uuid.UUID, raw string) (int, string) {
		t.Helper()
		resp, err := http.Post(st.server.URL+"/api/v1/auth/saml/"+id.String()+"/initiate", "application/json", strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		text, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(text)
	}
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`} {
		status, text := post(forwarded, `{"relay_state":"a`+escape+`b"}`)
		if status != http.StatusUnprocessableEntity || !strings.Contains(text, `"string_unicode"`) || !strings.Contains(text, `"relay_state"`) {
			t.Errorf("forwarded %s: status=%d body=%s", escape, status, text)
		}
		if status, text = post(sealed, `{"relay_state":"a`+escape+`b"}`); status != http.StatusOK || !strings.Contains(text, "redirect_url") {
			t.Errorf("sealed %s: status=%d body=%s", escape, status, text)
		}
	}
	status, text := post(forwarded, `{"relay_state":"a\ud83d\ude00b"}`)
	if status != http.StatusOK || !strings.Contains(text, "RelayState=a") {
		t.Errorf("a surrogate pair: status=%d body=%s", status, text)
	}
}

// TestSSOSurrogateRefusalTableStackRows drives the saml_* and oauth_* rows of
// ci/sso_surrogate_refusals.tsv against a real provider row: the status and
// the error loc the table names must be what the route answers.
func TestSSOSurrogateRefusalTableStackRows(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":"https://idp.test/entity","sso_url":"https://idp.test/sso","certificate":%q}`, idp.certConfigValue())
	providers := map[string]uuid.UUID{
		"saml_forwarded": seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true}),
		"saml_sealed":    seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true}),
		"oauth_gitlab": seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oauth_gitlab", status: "active", config: gitlabConfig(startFakeGitLab(t).server.URL), autoProvision: true,
			clientSecretEncryptedFallback: "gitlab-secret",
		}),
	}
	driven := 0
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if line == "" || strings.HasPrefix(line, "#") || f[1] != "sso" || !(strings.HasPrefix(f[4], "saml_") || strings.HasPrefix(f[4], "oauth_")) {
			continue
		}
		id, request, body, setup, wantCell := f[0], f[2], f[3], f[4], strings.Fields(f[5])
		provider, known := providers[setup]
		if !known {
			t.Fatalf("%s: unknown setup %q", id, setup)
		}
		method, path, _ := strings.Cut(request, " ")
		if encoded, isHex := strings.CutPrefix(body, "hex:"); isHex {
			decoded, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatalf("%s: %v", id, err)
			}
			body = string(decoded)
		}
		req, err := http.NewRequest(method, st.server.URL+strings.Replace(path, "{provider}", provider.String(), 1), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		text, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if want, _ := strconv.Atoi(wantCell[0]); resp.StatusCode != want {
			t.Errorf("%s: status=%d want %s body=%s", id, resp.StatusCode, wantCell[0], text)
		}
		if len(wantCell) == 3 {
			var parsed struct {
				Detail []struct {
					Type string `json:"type"`
					Loc  []any  `json:"loc"`
				} `json:"detail"`
			}
			if err := json.Unmarshal(text, &parsed); err != nil || len(parsed.Detail) != 1 || parsed.Detail[0].Type != wantCell[1] ||
				fmt.Sprint(parsed.Detail[0].Loc) != "["+strings.ReplaceAll(wantCell[2], ".", " ")+"]" {
				t.Errorf("%s: body=%s want %s %s", id, text, wantCell[1], wantCell[2])
			}
		}
		driven++
	}
	if driven != 5 {
		t.Fatalf("drove %d saml and oauth rows, want 5", driven)
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
	)
	entityID := idp.EntityID()
	const acsURL = "https://sp.test/api/v1/auth/saml/{provider_id}/acs"
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		entityID, idp.certConfigValue(), spEntityID, acsURL)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	const testEmail = "saml.user@allowed.example"
	seedMember(t, ctx, st.pool, orgID, testEmail, "saml", testEmail)
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: spEntityID, acsURL: samlACSOverrideFor(providerID), email: testEmail, fullName: "SAML User", nameID: testEmail,
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	seedMember(t, ctx, st.pool, orgID, "replay@example.test", "saml", "replay@example.test")
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
// succeeds, so it is recorded on the row (last_error); the provider's
// status is left as it was.
func TestSAMLACSRefusesAnAssertionWithNoSubjectConfirmation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	if providerStatus != "active" {
		t.Fatalf("provider status = %q, want %q: a failure record leaves the status as it was", providerStatus, "active")
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
// same idp.EntityID() at generation time), so deleting either
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
		idp.EntityID(), idp.certConfigValue())
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
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
	seedMember(t, ctx, st.pool, orgID, testEmail, "saml", testEmail)
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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

// TestSAMLTwoConcurrentInitiateFlowsBothComplete pins D2759's P1-B fix
// directly: two /initiate calls in the SAME browser (one cookie jar --
// two tabs, or a retry) must not collide. Before the fix, both flows set
// the identical fixed cookie name, so the second silently overwrote the
// first in the jar and the first flow's later /acs failed with a false
// "state mismatch" even though its own RelayState was perfectly valid.
func TestSAMLTwoConcurrentInitiateFlowsBothComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}

	initiate := func() (relayState, requestID string) {
		status, initResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("initiate: status=%d body=%v", status, initResp)
		}
		redirectURL, err := url.Parse(initResp["redirect_url"].(string))
		if err != nil {
			t.Fatal(err)
		}
		return redirectURL.Query().Get("RelayState"), inflateAndExtractRequestID(t, redirectURL.Query().Get("SAMLRequest"))
	}

	// Both /initiate calls happen BEFORE either /acs -- exactly the two-tab
	// shape: flow A's cookie must still be there, unclobbered, when flow A
	// finally completes, even though flow B's /initiate ran in between.
	relayStateA, requestIDA := initiate()
	relayStateB, requestIDB := initiate()
	if relayStateA == relayStateB {
		t.Fatal("expected two distinct RelayState tokens for two distinct /initiate calls")
	}

	complete := func(email, relayState, requestID string) {
		t.Helper()
		seedMember(t, ctx, st.pool, orgID, email, "saml", email)
		samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
			spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
			email: email, fullName: email, nameID: email, authnRequestID: requestID,
		})
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs",
			map[string]any{"SAMLResponse": samlResponse, "RelayState": relayState})
		if status != http.StatusOK {
			t.Fatalf("acs (%s): status=%d body=%v -- the two flows' login-nonce cookies collided", email, status, body)
		}
		if body["email"] != email {
			t.Fatalf("acs (%s): email=%v, want %s", email, body["email"], email)
		}
	}

	// Flow A completes LAST, after flow B's /initiate already ran -- the
	// case the shared fixed cookie name used to break.
	complete("flow-b@example.test", relayStateB, requestIDB)
	complete("flow-a@example.test", relayStateA, requestIDA)
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
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
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
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
	// D2759: the cookie's Path is now scoped to this provider's own ACS
	// route (not "/"), so it must be queried at THAT path -- a query at
	// the bare server root would see no cookies at all, matching RFC
	// 6265's own path-matching direction (a request path must be the
	// cookie's path or an extension of it, never the other way). The
	// cookie's NAME is also per-flow now (suffixed by a random flow ID),
	// so match by prefix rather than the old fixed literal.
	acsURL, err := url.Parse(st.server.URL + "/api/v1/auth/saml/" + providerID.String() + "/acs")
	if err != nil {
		t.Fatal(err)
	}
	cookies := jar.Cookies(acsURL)
	found := false
	for _, c := range cookies {
		if strings.HasPrefix(c.Name, "dho_saml_login_nonce_") {
			c.Value = flipMiddleByte(c.Value)
			// jar.Cookies() does not report each cookie's own Path (it is
			// building a request Cookie: header, which never carries Path)
			// -- SetCookies-ing it back with Path="" does not overwrite
			// the original path-scoped entry, it ADDS a second cookie of
			// the same name, and the client then sends BOTH, the original
			// still-valid one included, silently defeating the tamper.
			// Restoring the real path here is what makes this a REPLACE.
			c.Path = acsURL.Path
			found = true
		}
	}
	if !found {
		t.Fatal("initiate: expected a dho_saml_login_nonce_<flowID> cookie for an allow_idp_initiated=false provider")
	}
	jar.SetCookies(acsURL, cookies)

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, disallowIdpInitiated: true,
	})

	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml/{provider_id}/acs"}`,
		idp.EntityID(), idp.certConfigValue())
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
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
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

// samlACSOverrideFor is the sp_acs_url override these tests store: the config
// literals carry {provider_id}, which seedProvider replaces with the row id.
func samlACSOverrideFor(providerID uuid.UUID) string {
	return "https://sp.test/api/v1/auth/saml/" + providerID.String() + "/acs"
}

func TestSAMLConfigRefusesAnACSOverrideWithAnotherPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/acs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true,
	})

	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/initiate", map[string]any{})
	if status != http.StatusInternalServerError {
		t.Fatalf("initiate: status=%d body=%v, want 500", status, body)
	}
	if _, ok := body["redirect_url"]; ok {
		t.Fatalf("initiate: got a redirect_url for a refused override: %v", body)
	}
}

func TestSAMLConfigRefusesAnACSOverrideWithAnotherPathOnEveryRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":"https://sp.test/m","sp_acs_url":"https://sp.test/api/v1/auth/saml%%2F{provider_id}%%2Facs"}`,
		idp.EntityID(), idp.certConfigValue())
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true,
	})

	base := "/api/v1/auth/saml/" + providerID.String()
	if status, body := postJSON(t, st.server, base+"/initiate", map[string]any{}); status != http.StatusInternalServerError {
		t.Fatalf("initiate: status=%d body=%v, want 500", status, body)
	}
	samlResponse := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: "https://sp.test/m", acsURL: samlACSOverrideFor(providerID),
		email: "route@example.test", fullName: "Route", nameID: "route@example.test",
	})
	if status, body := postJSON(t, st.server, base+"/acs", map[string]any{"SAMLResponse": samlResponse}); status != http.StatusInternalServerError {
		t.Fatalf("acs: status=%d body=%v, want 500", status, body)
	}
	resp, err := http.Get(st.server.URL + base + "/metadata")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("metadata: status=%d, want 500", resp.StatusCode)
	}
}
