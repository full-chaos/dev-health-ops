package saml_test

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"testing"
	"time"

	"github.com/beevik/etree"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

const (
	nsAssertion = "urn:oasis:names:tc:SAML:2.0:assertion"
	idpEntityID = idpstub.DefaultIdPEntityID
	spEntityID  = idpstub.DefaultSPEntityID
	acsURL      = idpstub.DefaultACSURL
)

type testIdP struct {
	idp      *idpstub.SAML
	cert     *x509.Certificate
	entityID string
}

func newTestIdP(t testing.TB) *testIdP {
	t.Helper()
	idp, err := idpstub.NewSAML(idpstub.SAMLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return &testIdP{idp: idp, cert: idp.Keys().Cert, entityID: idp.EntityID()}
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
	senderVouches           bool
	signResponse            bool
	unsignedAssertion       bool
	sigMethod               string
	nameIDComment           bool
}

func strp(s string) *string { return &s }

func (o resp) spec() idpstub.ResponseSpec {
	nameID := idpstub.NameID{Format: o.nameID.Format, SPNameQualifier: o.nameID.SPNameQualifier, Value: o.nameID.Value}
	if nameID.Value == "" {
		nameID = idpstub.NameID{Format: idpstub.NameIDPersistent, Value: "user-1"}
	}
	return idpstub.ResponseSpec{ResponseID: o.responseID, AssertionID: o.assertionID, Destination: o.destination, Recipient: o.recipient,
		Audiences: o.audiences, NoAudience: o.noAudience, InResponseTo: o.inResponseTo, NameID: nameID, Email: o.email, Issuer: o.issuer,
		Status: o.status, IssueInstant: o.issueInstant, NotBefore: o.notBefore, NotOnOrAfter: o.notOnOrAfter,
		NoConfirmation: o.noConfirmation, SenderVouches: o.senderVouches, SignResponse: o.signResponse,
		UnsignedAssertion: o.unsignedAssertion, SigMethod: o.sigMethod, NameIDComment: o.nameIDComment}
}

func (idp *testIdP) build(t testing.TB, o resp) (*etree.Document, *etree.Element, *etree.Element) {
	t.Helper()
	doc, r, a, err := idp.idp.Build(o.spec())
	if err != nil {
		t.Fatal(err)
	}
	return doc, r, a
}

func (idp *testIdP) sign(t testing.TB, el *etree.Element, method string) *etree.Element {
	t.Helper()
	signed, err := idp.idp.Sign(el, method)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func (idp *testIdP) response(t testing.TB, o resp) []byte {
	t.Helper()
	raw, err := idp.idp.Response(o.spec())
	if err != nil {
		t.Fatal(err)
	}
	return raw
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
	return hex.EncodeToString(b)
}
