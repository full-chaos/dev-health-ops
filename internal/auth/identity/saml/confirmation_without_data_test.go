package saml_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

const (
	senderVouches = "urn:oasis:names:tc:SAML:2.0:cm:sender-vouches"
	holderOfKey   = "urn:oasis:names:tc:SAML:2.0:cm:holder-of-key"
)

// extraConfirmation is one more SubjectConfirmation the IdP signs into the assertion, next to its bearer one.
type extraConfirmation struct {
	method    string
	withData  bool
	recipient string
}

// withConfirmation builds o's response with the assertion unsigned, adds the extra confirmation to its Subject, then
// signs the assertion: a validly signed assertion from the provider's trusted IdP.
func withConfirmation(t *testing.T, idp *testIdP, o resp, extra extraConfirmation) []byte {
	t.Helper()
	o.unsignedAssertion = true
	doc, r, a := idp.build(t, o)
	subject := a.FindElement("./Subject")
	if subject == nil {
		t.Fatal("the built assertion has no Subject")
	}
	sc := subject.CreateElement("saml:SubjectConfirmation")
	sc.CreateAttr("Method", extra.method)
	if extra.withData {
		data := sc.CreateElement("saml:SubjectConfirmationData")
		data.CreateAttr("NotOnOrAfter", time.Now().Add(5*time.Minute).UTC().Format("2006-01-02T15:04:05Z"))
		data.CreateAttr("Recipient", extra.recipient)
	}
	signed := idp.sign(t, a, dsig.RSASHA256SignatureMethod)
	r.RemoveChild(a)
	r.AddChild(signed)
	return serialize(t, doc)
}

func verifyNoPanic(t *testing.T, idp *testIdP, raw []byte) (panicked any, err error) {
	t.Helper()
	defer func() { panicked = recover() }()
	_, err = saml.Verify(raw, idp.sp(), time.Now())
	return nil, err
}

// A signed assertion with a bearer confirmation and a confirmation of any other method that carries no
// SubjectConfirmationData is refused as a subject-confirmation failure before the library check, which reads the data of
// every confirmation and would dereference nil (GO-2026-6909 class). A confirmation of another method WITH valid data is
// accepted as before, and a bearer confirmation without data stays refused.
func TestSAMLVerifyRefusesAConfirmationWithoutDataOfAnyMethod(t *testing.T) {
	idp := newTestIdP(t)
	acs := idp.sp().ACSURL
	for name, c := range map[string]struct {
		extra  extraConfirmation
		refuse bool
	}{
		"sender-vouches without data": {extraConfirmation{method: senderVouches}, true},
		"holder-of-key without data":  {extraConfirmation{method: holderOfKey}, true},
		"unknown method without data": {extraConfirmation{method: "urn:example:cm:other"}, true},
		"bearer without data":         {extraConfirmation{method: "urn:oasis:names:tc:SAML:2.0:cm:bearer"}, true},
		"sender-vouches with data":    {extraConfirmation{method: senderVouches, withData: true, recipient: acs}, false},
	} {
		t.Run(name, func(t *testing.T) {
			panicked, err := verifyNoPanic(t, idp, withConfirmation(t, idp, resp{}, c.extra))
			if panicked != nil {
				t.Fatalf("Verify panicked: %v", panicked)
			}
			if !c.refuse {
				if err != nil {
					t.Fatalf("Verify refused %v, want accepted", err)
				}
				return
			}
			if reason, ok := saml.IsRefused(err); !ok || reason != saml.ReasonSubjectConfirmation {
				t.Fatalf("Verify = %v (reason %q), want a %s refusal", err, reason, saml.ReasonSubjectConfirmation)
			}
		})
	}
}

// Through the ACS route as auth-service serves it (the production recover middleware around the real handler), the same
// assertion answers the SAML failure class (400 "SAML authentication failed"), never a recovered 500.
func TestSAMLACSRouteAnswersTheSAMLFailureForAConfirmationWithoutData(t *testing.T) {
	for _, method := range []string{senderVouches, holderOfKey} {
		idp := newTestIdP(t)
		p := saml.Provider{
			ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
			AllowIdPInitiated: true, AutoProvision: true, AllowedDomains: []string{"example.com"},
			Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)},
		}
		store, members, sessions := &lookupStore{}, &memberSet{}, &sessionsSpy{}
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		svc := &saml.Service{
			Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: openFlows{}, Uses: freshAssertions{},
			Store: store, Memberships: members, BaseURL: func() string { return refusalBase }, Logger: logger,
		}
		h := saml.Handlers{Service: svc, Sessions: sessions, Logger: logger}
		const pattern = "POST /saml/{provider_id}/acs"
		mux := http.NewServeMux()
		mux.Handle(pattern, httpapi.RecoverWith(logger, pattern, httpapi.WriteError)(http.HandlerFunc(h.ACS)))

		acs := p.ACSURL(refusalBase)
		raw := withConfirmation(t, idp, resp{
			nameID: saml.NameID{Value: "user-" + uuid.NewString()}, email: "free-" + uuid.NewString() + "@example.com",
			destination: strp(acs), recipient: strp(acs), audiences: [][]string{{p.SPEntityID(refusalBase)}},
		}, extraConfirmation{method: method})
		req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs",
			strings.NewReader(url.Values{"SAMLResponse": {b64(raw)}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		text := logs.String()
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "SAML authentication failed") ||
			strings.Contains(text, "handler panicked") || !strings.Contains(text, `"reason_code":"`+saml.ReasonSubjectConfirmation+`"`) ||
			len(sessions.got) != 0 || store.provisioned != 0 {
			t.Errorf("%s: ACS = %d %s, sessions %d, provisions %d, logs %s; want 400 SAML authentication failed, reason %s, no panic",
				method, rec.Code, rec.Body, len(sessions.got), store.provisioned, text, saml.ReasonSubjectConfirmation)
		}
	}
}
