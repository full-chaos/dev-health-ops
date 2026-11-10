package sso

import (
	"context"
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

// On a provider without IdP-initiated sign-in the ACS refuses a missing and an
// unequal RelayState before the response is parsed, so a bad signature does
// not change the refusal. The route answers every such refusal 400 "SAML
// authentication failed" (samlACSCallback; route cell
// TestSAMLACSBadSignatureAndBadRelayStateAnswer, integration).
func TestSAMLACSRelayStateRefusalsOnASealedProvider(t *testing.T) {
	idp, err := idpstub.NewSAML(idpstub.SAMLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := idpstub.NewSAML(idpstub.SAMLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	row := &providerRow{ID: uuid.NewString(), OrgID: uuid.NewString(), Protocol: "saml", Status: "active", AllowIdpInitiated: false}
	config := samlConfigValues{EntityID: idp.EntityID(), Certificate: base64.StdEncoding.EncodeToString(idp.Keys().Cert.Raw)}
	h := handlers{Deps{StateSecret: "relay-state-cell-secret-0123456789abcdef"}}
	for _, signer := range []*idpstub.SAML{idp, other} {
		raw, err := signer.Response(idpstub.ResponseSpec{InResponseTo: "_id-request-1", NameID: idpstub.NameID{Format: idpstub.NameIDPersistent, Value: "user-1"}, Email: "member@example.com"})
		if err != nil {
			t.Fatal(err)
		}
		posted := base64.StdEncoding.EncodeToString(raw)
		for relay, want := range map[string]string{"": "Missing SAML RelayState", "mismatched-relay-state": "SAML RelayState mismatch"} {
			r := httptest.NewRequest("POST", "/api/v1/auth/saml/"+row.ID+"/acs", strings.NewReader(""))
			_, err := h.processSAMLResponse(context.Background(), httptest.NewRecorder(), r, row, config, "https://sp.example.test", posted, relay, time.Now())
			var unauth ssoUnauthenticated
			if !asSSOUnauthenticated(err, &unauth) || unauth.msg != want {
				t.Errorf("own signer %v, relay %q: err=%v, want unauthenticated %q", signer == idp, relay, err, want)
			}
		}
	}
}
