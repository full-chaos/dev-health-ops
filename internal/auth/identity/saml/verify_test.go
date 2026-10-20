package saml_test

import (
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

func mustVerify(t *testing.T, idp *testIdP, raw []byte) saml.Verified {
	t.Helper()
	v, err := saml.Verify(raw, idp.sp(), time.Now())
	if err != nil {
		t.Fatalf("Verify refused a valid response: %v (%+v)", err, err)
	}
	return v
}

func wantRefused(t *testing.T, idp *testIdP, raw []byte, reasons ...string) {
	t.Helper()
	_, err := saml.Verify(raw, idp.sp(), time.Now())
	if err == nil {
		t.Fatalf("Verify accepted a response that must be refused (want reason %v)", reasons)
	}
	got, ok := saml.IsRefused(err)
	if !ok {
		t.Fatalf("Verify error %v is not a RefusedError", err)
	}
	for _, r := range reasons {
		if got == r {
			return
		}
	}
	t.Fatalf("Verify refused with reason %q, want one of %v (%+v)", got, reasons, err)
}

func TestSAMLValidResponseIsAccepted(t *testing.T) {
	idp := newTestIdP(t)
	for name, o := range map[string]resp{
		"assertion signed":      {},
		"response signed":       {signResponse: true, unsignedAssertion: true},
		"both signed":           {signResponse: true},
		"solicited":             {inResponseTo: "_id0123"},
		"sp name qualifier set": {nameID: saml.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", SPNameQualifier: spEntityID, Value: "abc"}},
	} {
		t.Run(name, func(t *testing.T) {
			v := mustVerify(t, idp, idp.response(t, o))
			if v.Issuer != idpEntityID || v.AssertionID == "" || v.NameID.Value == "" {
				t.Fatalf("verified claims incomplete: %+v", v)
			}
			if v.InResponseTo != o.inResponseTo {
				t.Fatalf("InResponseTo = %q, want %q", v.InResponseTo, o.inResponseTo)
			}
		})
	}
}

func TestSAMLUnsignedAssertionIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{unsignedAssertion: true}), saml.ReasonSignatureMissing)
}

func TestSAMLSignatureByAnotherKeyIsRefused(t *testing.T) {
	idp, other := newTestIdP(t), newTestIdP(t)
	wantRefused(t, idp, other.response(t, resp{}), saml.ReasonSignatureInvalid)
}

func TestSAMLSHA1SignatureIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{sigMethod: dsig.RSASHA1SignatureMethod}), saml.ReasonSignatureAlgorithm)
	wantRefused(t, idp, idp.response(t, resp{signResponse: true, unsignedAssertion: true, sigMethod: dsig.RSASHA1SignatureMethod}), saml.ReasonSignatureAlgorithm)
}

func TestSAMLTamperedAssertionIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	raw := idp.response(t, resp{nameID: saml.NameID{Value: "user-1"}})
	tampered := strings.Replace(string(raw), ">user-1<", ">user-2<", 1)
	wantRefused(t, idp, []byte(tampered), saml.ReasonSignatureInvalid)
}

func TestSAMLDoctypeIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	raw := string(idp.response(t, resp{}))
	for name, doc := range map[string]string{
		"doctype":          `<!DOCTYPE r [<!ENTITY x "y">]>` + raw,
		"external entity":  `<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>` + raw,
		"processing instr": strings.Replace(raw, "<samlp:Response", `<?xml-stylesheet href="x"?><samlp:Response`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			wantRefused(t, idp, []byte(doc), saml.ReasonXMLDirective)
		})
	}
}

func TestSAMLAudienceMismatchIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	other := "https://other-sp.example.test"
	wantRefused(t, idp, idp.response(t, resp{audiences: [][]string{{other}}}), saml.ReasonAudience)
	wantRefused(t, idp, idp.response(t, resp{audiences: [][]string{{spEntityID}, {other}}}), saml.ReasonAudience)
	wantRefused(t, idp, idp.response(t, resp{audiences: [][]string{{other}, {spEntityID}}}), saml.ReasonAudience)
	mustVerify(t, idp, idp.response(t, resp{audiences: [][]string{{other, spEntityID}}}))
}

func TestSAMLMissingAudienceIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{noAudience: true}), saml.ReasonAudience)
}

func TestSAMLRecipientMismatchIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{recipient: strp("https://other-sp.example.test/acs")}), saml.ReasonRecipient)
	wantRefused(t, idp, idp.response(t, resp{recipient: strp("")}), saml.ReasonRecipient)
}

func TestSAMLDestinationMismatchIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{destination: strp("https://other-sp.example.test/acs")}), saml.ReasonDestination)
	wantRefused(t, idp, idp.response(t, resp{destination: strp("")}), saml.ReasonDestination)
	wantRefused(t, idp, idp.response(t, resp{destination: strp(""), signResponse: true}), saml.ReasonDestination)
}

func TestSAMLWrongIssuerIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{issuer: "https://other-idp.example.test"}), saml.ReasonIssuer)
}

func TestSAMLFailedStatusIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{status: "urn:oasis:names:tc:SAML:2.0:status:Requester"}), saml.ReasonStatus)
}

func TestSAMLExpiredAssertionIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	past := time.Now().Add(-2 * time.Hour)
	wantRefused(t, idp, idp.response(t, resp{issueInstant: past, notBefore: past, notOnOrAfter: past.Add(10 * time.Minute)}),
		saml.ReasonExpired, saml.ReasonIssueInstant)
	wantRefused(t, idp, idp.response(t, resp{notOnOrAfter: time.Now().Add(-6 * time.Minute)}), saml.ReasonExpired)
	wantRefused(t, idp, idp.response(t, resp{issueInstant: time.Now().Add(-6 * time.Minute)}), saml.ReasonIssueInstant)
}

func TestSAMLNotYetValidAssertionIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{notBefore: time.Now().Add(10 * time.Minute), notOnOrAfter: time.Now().Add(20 * time.Minute)}), saml.ReasonNotYetValid)
}

func TestSAMLMissingSubjectConfirmationIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{noConfirmation: true}), saml.ReasonSubjectConfirmation)
}

func TestSAMLTransientNameIDIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, idp.response(t, resp{nameID: saml.NameID{Format: saml.NameIDFormatTransient, Value: "_t1"}}), saml.ReasonNameID)
}

func TestSAMLNameIDCommentDoesNotTruncate(t *testing.T) {
	idp := newTestIdP(t)
	v := mustVerify(t, idp, idp.response(t, resp{nameID: saml.NameID{Value: "first@example.test.second.test"}, nameIDComment: true}))
	if v.NameID.Value != "first@example.test.second.test" {
		t.Fatalf("NameID = %q, want the full value", v.NameID.Value)
	}
}

func TestSAMLInResponseToMustAgree(t *testing.T) {
	idp := newTestIdP(t)
	doc, r, _ := idp.build(t, resp{inResponseTo: "_id1", unsignedAssertion: false})
	r.RemoveAttr("InResponseTo")
	r.CreateAttr("InResponseTo", "_id2")
	wantRefused(t, idp, serialize(t, doc), saml.ReasonInResponseTo)
}

// TestSAMLSignatureMustCoverTheUsedAssertion: the claims come only from the
// element the signature covers.
func TestSAMLSignatureMustCoverTheUsedAssertion(t *testing.T) {
	idp := newTestIdP(t)

	t.Run("second unsigned assertion before the signed one", func(t *testing.T) {
		doc, r, signed := idp.build(t, resp{nameID: saml.NameID{Value: "user-1"}})
		extra := unsignedAssertion(t, idp, "other")
		r.InsertChildAt(signed.Index(), extra)
		wantRefused(t, idp, serialize(t, doc), saml.ReasonAssertionCount)
	})
	t.Run("second unsigned assertion after a signed response", func(t *testing.T) {
		doc, r, _ := idp.build(t, resp{signResponse: true, unsignedAssertion: true})
		r.AddChild(unsignedAssertion(t, idp, "other"))
		wantRefused(t, idp, serialize(t, doc), saml.ReasonAssertionCount, saml.ReasonSignatureInvalid)
	})
	t.Run("signed assertion wrapped inside an unsigned one", func(t *testing.T) {
		doc, r, signed := idp.build(t, resp{})
		r.RemoveChild(signed)
		extra := unsignedAssertion(t, idp, "other")
		extra.CreateElement("saml:Advice").AddChild(signed)
		r.AddChild(extra)
		wantRefused(t, idp, serialize(t, doc), saml.ReasonSignaturePlacement, saml.ReasonSignatureMissing, saml.ReasonDuplicateID)
	})
	t.Run("unsigned assertion reuses the signed assertion ID", func(t *testing.T) {
		doc, r, signed := idp.build(t, resp{assertionID: "_same"})
		extra := unsignedAssertion(t, idp, "other")
		extra.RemoveAttr("ID")
		extra.CreateAttr("ID", "_same")
		ext := r.CreateElement("samlp:Extensions")
		ext.AddChild(signed.Copy())
		r.RemoveChild(signed)
		r.AddChild(extra)
		wantRefused(t, idp, serialize(t, doc), saml.ReasonDuplicateID, saml.ReasonSignaturePlacement, saml.ReasonSignatureMissing)
	})
	t.Run("signature moved from another element", func(t *testing.T) {
		doc, r, signed := idp.build(t, resp{unsignedAssertion: true})
		other := etree.NewElement("saml:Assertion")
		other.CreateAttr("xmlns:saml", nsAssertion)
		other.CreateAttr("ID", "_other")
		other.CreateElement("saml:Issuer").SetText(idpEntityID)
		otherSigned := idp.sign(t, other, dsig.RSASHA256SignatureMethod)
		sig := otherSigned.FindElement("./Signature")
		otherSigned.RemoveChild(sig)
		signed.AddChild(sig)
		_ = r
		wantRefused(t, idp, serialize(t, doc), saml.ReasonSignatureReference)
	})
	t.Run("encrypted assertion beside the signed one", func(t *testing.T) {
		doc, r, _ := idp.build(t, resp{})
		r.CreateElement("saml:EncryptedAssertion")
		wantRefused(t, idp, serialize(t, doc), saml.ReasonEncryptedAssertion)
	})
}

func unsignedAssertion(t *testing.T, idp *testIdP, subject string) *etree.Element {
	t.Helper()
	_, _, a := idp.build(t, resp{nameID: saml.NameID{Value: subject}, unsignedAssertion: true})
	a.Parent().RemoveChild(a)
	return a
}

func TestSAMLOversizedResponseIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	wantRefused(t, idp, make([]byte, saml.MaxResponseBytes+1), saml.ReasonPayload)
	if _, err := saml.DecodeResponse(strings.Repeat("A", 4*saml.MaxResponseBytes)); err == nil {
		t.Fatal("DecodeResponse accepted an oversized payload")
	}
}

func TestSAMLRequestIDComesOnlyFromSignedXML(t *testing.T) {
	idp := newTestIdP(t)
	doc, r, _ := idp.build(t, resp{})
	r.CreateAttr("InResponseTo", "_id-of-another-browser")
	wantRefused(t, idp, serialize(t, doc), saml.ReasonInResponseTo)
}
