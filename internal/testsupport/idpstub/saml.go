package idpstub

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beevik/etree"
	dsig "github.com/russellhaering/goxmldsig"
)

const (
	nsProtocol  = "urn:oasis:names:tc:SAML:2.0:protocol"
	nsAssertion = "urn:oasis:names:tc:SAML:2.0:assertion"
	nsMetadata  = "urn:oasis:names:tc:SAML:2.0:metadata"

	// DefaultIdPEntityID, DefaultSPEntityID and DefaultACSURL are what a zero SAMLOptions and a zero ResponseSpec use.
	DefaultIdPEntityID = "https://idp.example.test/metadata"
	DefaultSPEntityID  = "https://sp.example.test/saml/metadata"
	DefaultACSURL      = "https://sp.example.test/saml/acs"

	NameIDPersistent = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	statusSuccess    = "urn:oasis:names:tc:SAML:2.0:status:Success"
)

// NameID is a SAML NameID.
type NameID struct {
	Format, SPNameQualifier, Value string
}

// SAMLUser is one account the stub signs in.
type SAMLUser struct {
	NameID   NameID
	Email    string
	FullName string
}

// SAMLOptions configures NewSAML.
type SAMLOptions struct {
	Keys     *Keys
	Clock    func() time.Time
	EntityID string
	// SSOURL is the single sign-on URL the metadata names. Empty means EntityID with /sso in place of /metadata.
	SSOURL string
	// SPEntityID and ACSURL are the defaults of a ResponseSpec.
	SPEntityID, ACSURL string
	// AllowedACS, when set, lists the only ACS URLs the sign-on handler posts to.
	AllowedACS []string
	Users      []SAMLUser
}

// ResponseSpec describes one response. The zero value is a valid, assertion-signed response for the default SP,
// issued now, for the first user.
type ResponseSpec struct {
	ResponseID, AssertionID string
	SPEntityID, ACSURL      string
	Destination, Recipient  *string
	Audiences               [][]string
	NoAudience              bool
	InResponseTo            string
	NameID                  NameID
	// ExplicitNameID sends NameID exactly as given, even when its value is empty; without it an empty NameID value
	// means the first user.
	ExplicitNameID  bool
	Email, FullName string
	Issuer          string
	// WrongIssuerAt is "response" or "assertion": only that element carries a foreign issuer.
	WrongIssuerAt                                            string
	Status                                                   string
	IssueInstant, NotBefore, NotOnOrAfter                    time.Time
	ConditionsOnlyNotOnOrAfter, ConfirmationOnlyNotOnOrAfter time.Time
	NoConfirmation, SenderVouches                            bool
	SignResponse, UnsignedAssertion                          bool
	SigMethod                                                string
	NameIDComment                                            bool
	// SkipSign strips every signature block after signing; TamperAfterSign edits the email in the signed text.
	SkipSign, TamperAfterSign bool
}

// SAML is a SAML identity provider that signs responses.
type SAML struct {
	opts SAMLOptions
	keys *Keys
	now  func() time.Time
}

// NewSAML builds the provider; keys are generated unless opts.Keys is set.
func NewSAML(opts SAMLOptions) (*SAML, error) {
	keys := opts.Keys
	if keys == nil {
		var err error
		if keys, err = NewKeys("test idp"); err != nil {
			return nil, err
		}
	}
	now := opts.Clock
	if now == nil {
		now = time.Now
	}
	if opts.EntityID == "" {
		opts.EntityID = DefaultIdPEntityID
	}
	if opts.SSOURL == "" {
		opts.SSOURL = strings.TrimSuffix(opts.EntityID, "/metadata") + "/sso"
	}
	if opts.SPEntityID == "" {
		opts.SPEntityID = DefaultSPEntityID
	}
	if opts.ACSURL == "" {
		opts.ACSURL = DefaultACSURL
	}
	if len(opts.Users) == 0 {
		opts.Users = []SAMLUser{
			{NameID: NameID{Format: NameIDPersistent, Value: "alice"}, Email: "alice@idp.example.test", FullName: "Alice Example"},
			{NameID: NameID{Format: NameIDPersistent, Value: "bob"}, Email: "bob@idp.example.test", FullName: "Bob Example"},
		}
	}
	return &SAML{opts: opts, keys: keys, now: now}, nil
}

// Keys returns the signing key material.
func (s *SAML) Keys() *Keys { return s.keys }

// EntityID is the issuer of every response.
func (s *SAML) EntityID() string { return s.opts.EntityID }

// User returns the configured user with this NameID value or email.
func (s *SAML) User(nameIDOrEmail string) (SAMLUser, bool) {
	for _, u := range s.opts.Users {
		if u.NameID.Value == nameIDOrEmail || u.Email == nameIDOrEmail {
			return u, true
		}
	}
	return SAMLUser{}, false
}

func randomHex() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func strp(s string) *string { return &s }

// Build assembles and signs one response and returns the document, the Response element and the Assertion element.
func (s *SAML) Build(spec ResponseSpec) (*etree.Document, *etree.Element, *etree.Element, error) {
	now := s.now().UTC()
	if spec.SPEntityID == "" {
		spec.SPEntityID = s.opts.SPEntityID
	}
	if spec.ACSURL == "" {
		spec.ACSURL = s.opts.ACSURL
	}
	if spec.ResponseID == "" {
		spec.ResponseID = "_r" + randomHex()
	}
	if spec.AssertionID == "" {
		spec.AssertionID = "_a" + randomHex()
	}
	if spec.Destination == nil {
		spec.Destination = strp(spec.ACSURL)
	}
	if spec.Recipient == nil {
		spec.Recipient = strp(spec.ACSURL)
	}
	if spec.Audiences == nil && !spec.NoAudience {
		spec.Audiences = [][]string{{spec.SPEntityID}}
	}
	if spec.NameID.Value == "" && !spec.ExplicitNameID {
		spec.NameID = s.opts.Users[0].NameID
		if spec.Email == "" && spec.FullName == "" {
			spec.Email, spec.FullName = s.opts.Users[0].Email, s.opts.Users[0].FullName
		}
	}
	if spec.Issuer == "" {
		spec.Issuer = s.opts.EntityID
	}
	responseIssuer, assertionIssuer := spec.Issuer, spec.Issuer
	switch spec.WrongIssuerAt {
	case "":
	case "response":
		responseIssuer = "https://wrong-issuer.test/response"
	case "assertion":
		assertionIssuer = "https://wrong-issuer.test/assertion"
	default:
		return nil, nil, nil, fmt.Errorf("idpstub: WrongIssuerAt %q is not response or assertion", spec.WrongIssuerAt)
	}
	if spec.Status == "" {
		spec.Status = statusSuccess
	}
	if spec.IssueInstant.IsZero() {
		spec.IssueInstant = now
	}
	if spec.NotBefore.IsZero() {
		spec.NotBefore = now.Add(-time.Minute)
	}
	if spec.NotOnOrAfter.IsZero() {
		spec.NotOnOrAfter = now.Add(5 * time.Minute)
	}
	conditionsEnd, confirmationEnd := spec.NotOnOrAfter, spec.NotOnOrAfter
	if !spec.ConditionsOnlyNotOnOrAfter.IsZero() {
		conditionsEnd = spec.ConditionsOnlyNotOnOrAfter
	}
	if !spec.ConfirmationOnlyNotOnOrAfter.IsZero() {
		confirmationEnd = spec.ConfirmationOnlyNotOnOrAfter
	}
	ts := func(t time.Time) string { return t.UTC().Format(time.RFC3339) }

	doc := etree.NewDocument()
	r := doc.CreateElement("samlp:Response")
	r.CreateAttr("xmlns:samlp", nsProtocol)
	r.CreateAttr("xmlns:saml", nsAssertion)
	r.CreateAttr("ID", spec.ResponseID)
	r.CreateAttr("Version", "2.0")
	r.CreateAttr("IssueInstant", ts(spec.IssueInstant))
	if *spec.Destination != "" {
		r.CreateAttr("Destination", *spec.Destination)
	}
	if spec.InResponseTo != "" {
		r.CreateAttr("InResponseTo", spec.InResponseTo)
	}
	r.CreateElement("saml:Issuer").SetText(responseIssuer)
	r.CreateElement("samlp:Status").CreateElement("samlp:StatusCode").CreateAttr("Value", spec.Status)

	a := r.CreateElement("saml:Assertion")
	a.CreateAttr("xmlns:saml", nsAssertion)
	a.CreateAttr("ID", spec.AssertionID)
	a.CreateAttr("Version", "2.0")
	a.CreateAttr("IssueInstant", ts(spec.IssueInstant))
	a.CreateElement("saml:Issuer").SetText(assertionIssuer)
	subject := a.CreateElement("saml:Subject")
	nameID := subject.CreateElement("saml:NameID")
	if spec.NameID.Format != "" {
		nameID.CreateAttr("Format", spec.NameID.Format)
	}
	if spec.NameID.SPNameQualifier != "" {
		nameID.CreateAttr("SPNameQualifier", spec.NameID.SPNameQualifier)
	}
	if spec.NameIDComment {
		half := len(spec.NameID.Value) / 2
		nameID.SetText(spec.NameID.Value[:half])
		nameID.CreateComment("")
		nameID.CreateText(spec.NameID.Value[half:])
	} else {
		nameID.SetText(spec.NameID.Value)
	}
	if !spec.NoConfirmation {
		sc := subject.CreateElement("saml:SubjectConfirmation")
		method := "urn:oasis:names:tc:SAML:2.0:cm:bearer"
		if spec.SenderVouches {
			method = "urn:oasis:names:tc:SAML:2.0:cm:sender-vouches"
		}
		sc.CreateAttr("Method", method)
		data := sc.CreateElement("saml:SubjectConfirmationData")
		if spec.InResponseTo != "" {
			data.CreateAttr("InResponseTo", spec.InResponseTo)
		}
		data.CreateAttr("NotOnOrAfter", ts(confirmationEnd))
		if *spec.Recipient != "" {
			data.CreateAttr("Recipient", *spec.Recipient)
		}
	}
	cond := a.CreateElement("saml:Conditions")
	cond.CreateAttr("NotBefore", ts(spec.NotBefore))
	cond.CreateAttr("NotOnOrAfter", ts(conditionsEnd))
	if !spec.NoAudience {
		for _, group := range spec.Audiences {
			ar := cond.CreateElement("saml:AudienceRestriction")
			for _, aud := range group {
				ar.CreateElement("saml:Audience").SetText(aud)
			}
		}
	}
	authn := a.CreateElement("saml:AuthnStatement")
	authn.CreateAttr("AuthnInstant", ts(spec.IssueInstant))
	authn.CreateElement("saml:AuthnContext").CreateElement("saml:AuthnContextClassRef").SetText("urn:oasis:names:tc:SAML:2.0:ac:classes:Password")
	if spec.Email != "" || spec.FullName != "" {
		statement := a.CreateElement("saml:AttributeStatement")
		for _, kv := range [][2]string{{"email", spec.Email}, {"full_name", spec.FullName}} {
			if kv[1] == "" {
				continue
			}
			attr := statement.CreateElement("saml:Attribute")
			attr.CreateAttr("Name", kv[0])
			attr.CreateElement("saml:AttributeValue").SetText(kv[1])
		}
	}

	method := spec.SigMethod
	if method == "" {
		method = dsig.RSASHA256SignatureMethod
	}
	if !spec.UnsignedAssertion {
		signed, err := s.Sign(a, method)
		if err != nil {
			return nil, nil, nil, err
		}
		r.RemoveChild(a)
		r.AddChild(signed)
		a = signed
	}
	if spec.SignResponse {
		signed, err := s.Sign(r, method)
		if err != nil {
			return nil, nil, nil, err
		}
		doc.SetRoot(signed)
		r = signed
		a = r.FindElement("./Assertion")
	}
	return doc, r, a, nil
}

// Sign signs el with the stub key, as an enveloped signature with the given signature method.
func (s *SAML) Sign(el *etree.Element, method string) (*etree.Element, error) {
	ctx := dsig.NewDefaultSigningContext(dsig.TLSCertKeyStore(s.keys.TLS()))
	ctx.Canonicalizer = dsig.MakeC14N10ExclusiveCanonicalizerWithPrefixList("")
	if err := ctx.SetSignatureMethod(method); err != nil {
		return nil, err
	}
	return ctx.SignEnveloped(el)
}

// Response is the serialized response XML.
func (s *SAML) Response(spec ResponseSpec) ([]byte, error) {
	doc, _, _, err := s.Build(spec)
	if err != nil {
		return nil, err
	}
	out, err := doc.WriteToString()
	if err != nil {
		return nil, err
	}
	if spec.SkipSign {
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
	if spec.TamperAfterSign && spec.Email != "" {
		out = strings.Replace(out, spec.Email, "tampered-"+spec.Email, 1)
	}
	return []byte(out), nil
}

// ResponseB64 is the response base64-encoded, the form a browser posts as SAMLResponse.
func (s *SAML) ResponseB64(spec ResponseSpec) (string, error) {
	raw, err := s.Response(spec)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// Metadata is the IdP metadata document: entity id, signing certificate and the single sign-on endpoints.
func (s *SAML) Metadata() []byte {
	esc := html.EscapeString
	var b bytes.Buffer
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<md:EntityDescriptor xmlns:md=%q xmlns:ds="http://www.w3.org/2000/09/xmldsig#" entityID="%s">`+
		`<md:IDPSSODescriptor WantAuthnRequestsSigned="false" protocolSupportEnumeration=%q>`+
		`<md:KeyDescriptor use="signing"><ds:KeyInfo><ds:X509Data><ds:X509Certificate>%s</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>`+
		`<md:NameIDFormat>%s</md:NameIDFormat>`+
		`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="%s"/>`+
		`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="%s"/>`+
		`</md:IDPSSODescriptor></md:EntityDescriptor>`,
		nsMetadata, esc(s.opts.EntityID), nsProtocol, s.keys.CertBase64(), NameIDPersistent, esc(s.opts.SSOURL), esc(s.opts.SSOURL))
	return b.Bytes()
}

// Handler serves GET /saml/metadata, GET /saml/cert (PEM) and GET or POST /saml/sso.
func (s *SAML) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/saml/metadata", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		_, _ = w.Write(s.Metadata())
	})
	mux.HandleFunc("/saml/cert", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(s.keys.CertPEM())
	})
	mux.HandleFunc("/saml/sso", s.serveSSO)
	return mux
}

func (s *SAML) serveSSO(w http.ResponseWriter, r *http.Request) {
	if r.ParseForm() != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	request, err := decodeAuthnRequest(r.Form.Get("SAMLRequest"))
	if err != nil {
		http.Error(w, "bad SAMLRequest", http.StatusBadRequest)
		return
	}
	root := request.Root()
	if root == nil || root.Tag != "AuthnRequest" {
		http.Error(w, "not an AuthnRequest", http.StatusBadRequest)
		return
	}
	id, acs := root.SelectAttrValue("ID", ""), root.SelectAttrValue("AssertionConsumerServiceURL", "")
	spEntity := ""
	if issuer := root.FindElement("./Issuer"); issuer != nil {
		spEntity = strings.TrimSpace(issuer.Text())
	}
	if id == "" || acs == "" || spEntity == "" {
		http.Error(w, "AuthnRequest needs ID, Issuer and AssertionConsumerServiceURL", http.StatusBadRequest)
		return
	}
	if len(s.opts.AllowedACS) > 0 && !contains(s.opts.AllowedACS, acs) {
		http.Error(w, "ACS URL not allowed", http.StatusBadRequest)
		return
	}
	user := s.opts.Users[0]
	if u, ok := s.User(r.Form.Get("user")); ok && r.Form.Get("user") != "" {
		user = u
	}
	encoded, err := s.ResponseB64(ResponseSpec{SPEntityID: spEntity, ACSURL: acs, InResponseTo: id,
		NameID: user.NameID, Email: user.Email, FullName: user.FullName})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, `<!doctype html><html><body onload="document.forms[0].submit()"><form method="post" action="%s">`+
		`<input type="hidden" name="SAMLResponse" value="%s"><input type="hidden" name="RelayState" value="%s">`+
		`<noscript><button>Continue</button></noscript></form></body></html>`,
		html.EscapeString(acs), html.EscapeString(encoded), html.EscapeString(r.Form.Get("RelayState")))
}

func decodeAuthnRequest(posted string) (*etree.Document, error) {
	raw, err := base64.StdEncoding.DecodeString(posted)
	if err != nil {
		return nil, err
	}
	if inflated, err := io.ReadAll(io.LimitReader(flate.NewReader(bytes.NewReader(raw)), 1<<20)); err == nil && len(inflated) > 0 {
		raw = inflated
	}
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(raw); err != nil {
		return nil, err
	}
	return doc, nil
}
