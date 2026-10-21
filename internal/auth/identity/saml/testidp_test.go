package saml_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/beevik/etree"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

const (
	nsProtocol  = "urn:oasis:names:tc:SAML:2.0:protocol"
	nsAssertion = "urn:oasis:names:tc:SAML:2.0:assertion"
	idpEntityID = "https://idp.example.test/metadata"
	spEntityID  = "https://sp.example.test/saml/metadata"
	acsURL      = "https://sp.example.test/saml/acs"
)

type testIdP struct {
	key      *rsa.PrivateKey
	cert     *x509.Certificate
	tlsCert  tls.Certificate
	entityID string
}

func newTestIdP(t testing.TB) *testIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test idp"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testIdP{key: key, cert: cert, tlsCert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, entityID: idpEntityID}
}

func (idp *testIdP) sp() saml.SP {
	return saml.SP{EntityID: spEntityID, ACSURL: acsURL, IdPEntityID: idp.entityID, IdPCert: idp.cert}
}

// resp describes one response. Zero values give a valid, assertion-signed
// response for spEntityID/acsURL issued now.
type resp struct {
	responseID, assertionID string
	destination             *string
	recipient               *string
	audiences               [][]string
	noAudience              bool
	inResponseTo            string
	nameID                  saml.NameID
	email                   string
	issuer                  string
	status                  string
	issueInstant            time.Time
	notBefore, notOnOrAfter time.Time
	noConfirmation          bool
	signResponse            bool
	unsignedAssertion       bool
	sigMethod               string
	nameIDComment           bool
}

func strp(s string) *string { return &s }

func (idp *testIdP) build(t testing.TB, o resp) (*etree.Document, *etree.Element, *etree.Element) {
	t.Helper()
	now := time.Now().UTC()
	if o.responseID == "" {
		o.responseID = "_r" + randomHex(t)
	}
	if o.assertionID == "" {
		o.assertionID = "_a" + randomHex(t)
	}
	if o.destination == nil {
		o.destination = strp(acsURL)
	}
	if o.recipient == nil {
		o.recipient = strp(acsURL)
	}
	if o.audiences == nil && !o.noAudience {
		o.audiences = [][]string{{spEntityID}}
	}
	if o.nameID.Value == "" {
		o.nameID = saml.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", Value: "user-1"}
	}
	if o.issuer == "" {
		o.issuer = idp.entityID
	}
	if o.status == "" {
		o.status = "urn:oasis:names:tc:SAML:2.0:status:Success"
	}
	if o.issueInstant.IsZero() {
		o.issueInstant = now
	}
	if o.notBefore.IsZero() {
		o.notBefore = now.Add(-time.Minute)
	}
	if o.notOnOrAfter.IsZero() {
		o.notOnOrAfter = now.Add(5 * time.Minute)
	}
	ts := func(t time.Time) string { return t.UTC().Format(time.RFC3339) }

	doc := etree.NewDocument()
	r := doc.CreateElement("samlp:Response")
	r.CreateAttr("xmlns:samlp", nsProtocol)
	r.CreateAttr("xmlns:saml", nsAssertion)
	r.CreateAttr("ID", o.responseID)
	r.CreateAttr("Version", "2.0")
	r.CreateAttr("IssueInstant", ts(o.issueInstant))
	if *o.destination != "" {
		r.CreateAttr("Destination", *o.destination)
	}
	if o.inResponseTo != "" {
		r.CreateAttr("InResponseTo", o.inResponseTo)
	}
	r.CreateElement("saml:Issuer").SetText(o.issuer)
	r.CreateElement("samlp:Status").CreateElement("samlp:StatusCode").CreateAttr("Value", o.status)

	a := r.CreateElement("saml:Assertion")
	a.CreateAttr("xmlns:saml", nsAssertion)
	a.CreateAttr("ID", o.assertionID)
	a.CreateAttr("Version", "2.0")
	a.CreateAttr("IssueInstant", ts(o.issueInstant))
	a.CreateElement("saml:Issuer").SetText(o.issuer)
	subject := a.CreateElement("saml:Subject")
	nameID := subject.CreateElement("saml:NameID")
	if o.nameID.Format != "" {
		nameID.CreateAttr("Format", o.nameID.Format)
	}
	if o.nameID.SPNameQualifier != "" {
		nameID.CreateAttr("SPNameQualifier", o.nameID.SPNameQualifier)
	}
	if o.nameIDComment {
		half := len(o.nameID.Value) / 2
		nameID.SetText(o.nameID.Value[:half])
		nameID.CreateComment("")
		nameID.CreateText(o.nameID.Value[half:])
	} else {
		nameID.SetText(o.nameID.Value)
	}
	if !o.noConfirmation {
		sc := subject.CreateElement("saml:SubjectConfirmation")
		sc.CreateAttr("Method", "urn:oasis:names:tc:SAML:2.0:cm:bearer")
		data := sc.CreateElement("saml:SubjectConfirmationData")
		if o.inResponseTo != "" {
			data.CreateAttr("InResponseTo", o.inResponseTo)
		}
		data.CreateAttr("NotOnOrAfter", ts(o.notOnOrAfter))
		if *o.recipient != "" {
			data.CreateAttr("Recipient", *o.recipient)
		}
	}
	cond := a.CreateElement("saml:Conditions")
	cond.CreateAttr("NotBefore", ts(o.notBefore))
	cond.CreateAttr("NotOnOrAfter", ts(o.notOnOrAfter))
	if !o.noAudience {
		for _, group := range o.audiences {
			ar := cond.CreateElement("saml:AudienceRestriction")
			for _, aud := range group {
				ar.CreateElement("saml:Audience").SetText(aud)
			}
		}
	}
	authn := a.CreateElement("saml:AuthnStatement")
	authn.CreateAttr("AuthnInstant", ts(o.issueInstant))
	authn.CreateElement("saml:AuthnContext").CreateElement("saml:AuthnContextClassRef").SetText("urn:oasis:names:tc:SAML:2.0:ac:classes:Password")
	if o.email != "" {
		attr := a.CreateElement("saml:AttributeStatement").CreateElement("saml:Attribute")
		attr.CreateAttr("Name", "email")
		attr.CreateElement("saml:AttributeValue").SetText(o.email)
	}

	method := o.sigMethod
	if method == "" {
		method = dsig.RSASHA256SignatureMethod
	}
	if !o.unsignedAssertion {
		signed := idp.sign(t, a, method)
		r.RemoveChild(a)
		r.AddChild(signed)
		a = signed
	}
	if o.signResponse {
		signed := idp.sign(t, r, method)
		doc.SetRoot(signed)
		r = signed
		a = r.FindElement("./Assertion")
	}
	return doc, r, a
}

func (idp *testIdP) sign(t testing.TB, el *etree.Element, method string) *etree.Element {
	t.Helper()
	ctx := dsig.NewDefaultSigningContext(dsig.TLSCertKeyStore(idp.tlsCert))
	ctx.Canonicalizer = dsig.MakeC14N10ExclusiveCanonicalizerWithPrefixList("")
	if err := ctx.SetSignatureMethod(method); err != nil {
		t.Fatal(err)
	}
	signed, err := ctx.SignEnveloped(el)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func (idp *testIdP) response(t testing.TB, o resp) []byte {
	t.Helper()
	doc, _, _ := idp.build(t, o)
	return serialize(t, doc)
}

func serialize(t testing.TB, doc *etree.Document) []byte {
	t.Helper()
	out, err := doc.WriteToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func randomHex(t testing.TB) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 24)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0xf])
	}
	return string(out)
}
