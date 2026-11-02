package idpstub_test

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	dsig "github.com/russellhaering/goxmldsig"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

func newSAML(t *testing.T, opts idpstub.SAMLOptions) *idpstub.SAML {
	t.Helper()
	s, err := idpstub.NewSAML(opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func productSP(s *idpstub.SAML) saml.SP {
	return saml.SP{EntityID: idpstub.DefaultSPEntityID, ACSURL: idpstub.DefaultACSURL, IdPEntityID: s.EntityID(), IdPCert: s.Keys().Cert}
}

func verify(t *testing.T, s *idpstub.SAML, spec idpstub.ResponseSpec) (saml.Verified, error) {
	t.Helper()
	raw, err := s.Response(spec)
	if err != nil {
		t.Fatal(err)
	}
	return saml.Verify(raw, productSP(s), time.Now())
}

// This is the drift guard: the stub's default response must pass the product's own verifier.
func TestSAMLResponseDefaultsVerifyAgainstProductSP(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{})
	for name, spec := range map[string]idpstub.ResponseSpec{
		"default":         {},
		"response signed": {SignResponse: true, UnsignedAssertion: true},
		"both signed":     {SignResponse: true},
		"solicited":       {InResponseTo: "_id0123"},
	} {
		t.Run(name, func(t *testing.T) {
			v, err := verify(t, s, spec)
			if err != nil {
				t.Fatalf("product verifier refused the stub: %v", err)
			}
			if v.Issuer != s.EntityID() || v.NameID.Value != "alice" || v.Attributes["email"] != "alice@idp.example.test" {
				t.Fatalf("verified = %+v", v)
			}
			if v.InResponseTo != spec.InResponseTo {
				t.Fatalf("InResponseTo = %q", v.InResponseTo)
			}
		})
	}
}

func TestSAMLEveryFaultIsRefusedByTheVerifier(t *testing.T) {
	past, future := time.Now().Add(-2*time.Hour), time.Now().Add(2*time.Hour)
	wrong := "https://elsewhere.example.test/acs"
	for name, tc := range map[string]struct {
		spec   idpstub.ResponseSpec
		reason string
	}{
		"unsigned assertion":      {idpstub.ResponseSpec{UnsignedAssertion: true}, saml.ReasonSignatureMissing},
		"signatures stripped":     {idpstub.ResponseSpec{SkipSign: true}, saml.ReasonSignatureMissing},
		"sha1":                    {idpstub.ResponseSpec{SigMethod: dsig.RSASHA1SignatureMethod}, saml.ReasonSignatureAlgorithm},
		"tampered":                {idpstub.ResponseSpec{Email: "a@example.test", TamperAfterSign: true}, saml.ReasonSignatureInvalid},
		"other audience":          {idpstub.ResponseSpec{Audiences: [][]string{{"https://other.example.test/sp"}}}, saml.ReasonAudience},
		"no audience":             {idpstub.ResponseSpec{NoAudience: true}, saml.ReasonAudience},
		"wrong destination":       {idpstub.ResponseSpec{Destination: &wrong}, saml.ReasonDestination},
		"wrong recipient":         {idpstub.ResponseSpec{Recipient: &wrong}, saml.ReasonRecipient},
		"wrong issuer":            {idpstub.ResponseSpec{Issuer: "https://not-the-idp.example.test"}, saml.ReasonIssuer},
		"wrong issuer response":   {idpstub.ResponseSpec{WrongIssuerAt: "response"}, saml.ReasonIssuer},
		"wrong issuer assertion":  {idpstub.ResponseSpec{WrongIssuerAt: "assertion"}, saml.ReasonIssuer},
		"status failure":          {idpstub.ResponseSpec{Status: "urn:oasis:names:tc:SAML:2.0:status:Responder"}, saml.ReasonStatus},
		"expired":                 {idpstub.ResponseSpec{NotBefore: past.Add(-time.Hour), NotOnOrAfter: past}, saml.ReasonExpired},
		"conditions expired":      {idpstub.ResponseSpec{ConditionsOnlyNotOnOrAfter: past}, saml.ReasonExpired},
		"confirmation expired":    {idpstub.ResponseSpec{ConfirmationOnlyNotOnOrAfter: past}, saml.ReasonExpired},
		"not yet valid":           {idpstub.ResponseSpec{NotBefore: future, NotOnOrAfter: future.Add(time.Hour)}, saml.ReasonNotYetValid},
		"no subject confirmation": {idpstub.ResponseSpec{NoConfirmation: true}, saml.ReasonSubjectConfirmation},
		"sender vouches":          {idpstub.ResponseSpec{SenderVouches: true}, saml.ReasonSubjectConfirmation},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSAML(t, idpstub.SAMLOptions{})
			_, err := verify(t, s, tc.spec)
			if err == nil {
				t.Fatalf("product verifier accepted a %s response", name)
			}
			if got, ok := saml.IsRefused(err); !ok || got != tc.reason {
				t.Fatalf("refused with %q (%v), want %q", got, err, tc.reason)
			}
		})
	}
}

func TestSAMLWrongIssuerAtRefusesOtherValues(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{})
	if _, err := s.Response(idpstub.ResponseSpec{WrongIssuerAt: "both"}); err == nil {
		t.Fatal("an unknown WrongIssuerAt was accepted")
	}
}

func TestSAMLTwoUsersTwoNameIDs(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{})
	seen := map[string]bool{}
	for _, who := range []string{"alice", "bob"} {
		u, ok := s.User(who)
		if !ok {
			t.Fatalf("no user %s", who)
		}
		v, err := verify(t, s, idpstub.ResponseSpec{NameID: u.NameID, Email: u.Email, FullName: u.FullName})
		if err != nil {
			t.Fatal(err)
		}
		if v.NameID.Value != who || v.Attributes["full_name"] == "" {
			t.Fatalf("verified = %+v", v)
		}
		seen[v.NameID.Value] = true
	}
	if len(seen) != 2 {
		t.Fatalf("NameIDs = %v", seen)
	}
}

func TestSAMLMetadataCarriesCertAndSSOEndpoints(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{EntityID: "https://idp.kit.test/saml/metadata", SSOURL: "https://idp.kit.test/saml/sso"})
	md := string(s.Metadata())
	for _, want := range []string{`entityID="https://idp.kit.test/saml/metadata"`, s.Keys().CertBase64(),
		`Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.kit.test/saml/sso"`,
		`Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.kit.test/saml/sso"`} {
		if !strings.Contains(md, want) {
			t.Errorf("metadata lacks %s", want)
		}
	}
}

func TestKeysCertBase64IsBareDERProviderShape(t *testing.T) {
	k, err := idpstub.NewKeys("x")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := saml.ParseCertificate(k.CertBase64())
	if err != nil || !cert.Equal(k.Cert) {
		t.Fatalf("ParseCertificate(CertBase64) = %v, %v", cert, err)
	}
	if strings.Contains(k.CertBase64(), "BEGIN") {
		t.Fatal("CertBase64 is PEM")
	}
	if cert, err := saml.ParseCertificate(string(k.CertPEM())); err != nil || !cert.Equal(k.Cert) {
		t.Fatalf("ParseCertificate(CertPEM) = %v, %v", cert, err)
	}
}

func authnRequest(id, acs, sp string) string {
	return `<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="` + id +
		`" Version="2.0" IssueInstant="2026-11-01T12:00:00Z" AssertionConsumerServiceURL="` + acs + `"><saml:Issuer>` + sp + `</saml:Issuer></samlp:AuthnRequest>`
}

func deflate(t *testing.T, s string) string {
	t.Helper()
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestCompression)
	_, _ = w.Write([]byte(s))
	_ = w.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

var formValue = regexp.MustCompile(`name="(SAMLResponse|RelayState)" value="([^"]*)"`)

func TestSAMLSSOHandlerAnswersSignedPostFormForRedirectAndPostBinding(t *testing.T) {
	const acs = "https://sp.kit.test/api/v1/auth/saml/acs"
	s := newSAML(t, idpstub.SAMLOptions{AllowedACS: []string{acs}})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	req := authnRequest("_req-42", acs, idpstub.DefaultSPEntityID)
	for name, do := range map[string]func() (*http.Response, error){
		"redirect": func() (*http.Response, error) {
			return http.Get(srv.URL + "/saml/sso?" + url.Values{"SAMLRequest": {deflate(t, req)}, "RelayState": {"relay<1>"}, "user": {"bob"}}.Encode())
		},
		"post": func() (*http.Response, error) {
			return http.PostForm(srv.URL+"/saml/sso", url.Values{"SAMLRequest": {base64.StdEncoding.EncodeToString([]byte(req))}, "RelayState": {"relay<1>"}, "user": {"bob"}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			res, err := do()
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if res.StatusCode != http.StatusOK || !strings.Contains(string(body), `action="`+acs+`"`) {
				t.Fatalf("status %d body %s", res.StatusCode, body)
			}
			fields := map[string]string{}
			for _, m := range formValue.FindAllStringSubmatch(string(body), -1) {
				fields[m[1]] = html.UnescapeString(m[2])
			}
			if fields["RelayState"] != "relay<1>" {
				t.Fatalf("RelayState = %q", fields["RelayState"])
			}
			raw, err := saml.DecodeResponse(fields["SAMLResponse"])
			if err != nil {
				t.Fatal(err)
			}
			v, err := saml.Verify(raw, saml.SP{EntityID: idpstub.DefaultSPEntityID, ACSURL: acs, IdPEntityID: s.EntityID(), IdPCert: s.Keys().Cert}, time.Now())
			if err != nil {
				t.Fatalf("product verifier refused the handler's response: %v", err)
			}
			if v.InResponseTo != "_req-42" || v.NameID.Value != "bob" {
				t.Fatalf("verified = %+v", v)
			}
		})
	}
}

func TestSAMLSSOHandlerRefusesBadRequests(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{AllowedACS: []string{"https://sp.kit.test/acs"}})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	for name, req := range map[string]string{
		"acs not allowed": authnRequest("_1", "https://evil.example.test/acs", "sp"),
		"no id":           authnRequest("", "https://sp.kit.test/acs", "sp"),
		"not a request":   `<foo/>`,
	} {
		res, err := http.PostForm(srv.URL+"/saml/sso", url.Values{"SAMLRequest": {base64.StdEncoding.EncodeToString([]byte(req))}})
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, res.StatusCode)
		}
	}
	res, err := http.Get(srv.URL + "/saml/sso")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("no SAMLRequest: status %d", res.StatusCode)
	}
}

func TestSAMLCertEndpointServesPEM(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	res, err := http.Get(srv.URL + "/saml/cert")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if !bytes.Equal(body, s.Keys().CertPEM()) {
		t.Fatal("/saml/cert is not the signing certificate")
	}
}

func TestSAMLExplicitEmptyNameIDStaysEmpty(t *testing.T) {
	s := newSAML(t, idpstub.SAMLOptions{})
	spec := idpstub.ResponseSpec{ExplicitNameID: true, NameID: idpstub.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"}, Email: "a@example.test"}
	doc, _, assertion, err := s.Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	nameID := assertion.FindElement("./Subject/NameID")
	if nameID == nil || nameID.Text() != "" || nameID.SelectAttrValue("Format", "") != spec.NameID.Format {
		t.Fatalf("NameID element = %v, want an empty one with the given format", nameID)
	}
	if got := doc.Root().FindElement("//Attribute[@Name='email']/AttributeValue"); got == nil || got.Text() != "a@example.test" {
		t.Fatalf("email attribute = %v", got)
	}
	if _, err := verify(t, s, spec); err == nil {
		t.Fatal("the product verifier accepted an empty NameID")
	}
	other := spec
	other.ExplicitNameID, other.NameID = false, idpstub.NameID{}
	if doc2, _, a2, err := s.Build(other); err != nil || a2.FindElement("./Subject/NameID").Text() != "alice" || doc2 == nil {
		t.Fatalf("without ExplicitNameID the first user applies: %v", err)
	}
}
