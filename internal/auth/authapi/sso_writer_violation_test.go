package authapi

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

const samlWriterBase = "https://sp.example.test/api/v1/auth"

type samlWriterProviders struct{ p saml.Provider }

func (s samlWriterProviders) Provider(context.Context, uuid.UUID) (saml.Provider, error) {
	return s.p, nil
}

type samlWriterOpen struct{}

func (samlWriterOpen) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return true, nil }
func (samlWriterOpen) SaveFlow(context.Context, saml.Flow) error          { return nil }
func (samlWriterOpen) ConsumeFlow(context.Context, uuid.UUID, string, [32]byte, time.Time) error {
	return nil
}
func (samlWriterOpen) UseAssertion(context.Context, uuid.UUID, string, string, time.Time) error {
	return nil
}
func (samlWriterOpen) Member(context.Context, uuid.UUID, uuid.UUID) (bool, error) { return true, nil }

type samlWriterStore struct{ principal uuid.UUID }

func (s samlWriterStore) IdentityBySubject(context.Context, saml.Identity, *saml.LegacyMatch) (saml.Principal, bool, error) {
	return saml.Principal{ID: s.principal}, true, nil
}
func (samlWriterStore) NormalizeEmail(raw string) (string, error) { return strings.ToLower(raw), nil }
func (samlWriterStore) PrincipalByEmail(context.Context, string) (saml.EmailOwner, bool, error) {
	return saml.EmailOwner{}, false, nil
}

// The SAML ACS success is the SSO path that ends in writeCompatModel (the oidc
// and oauth handlers answer through their own encoding/json writer, so they
// never reach the policy writer check). It runs here through the production
// route flags, and a flag flipped to false must raise a writer violation.
func TestSAMLACSSuccessRaisesNoWriterViolation(t *testing.T) {
	t.Parallel()
	f := newCompatFixture(t)
	idp, err := idpstub.NewSAML(idpstub.SAMLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := saml.Provider{
		ID: uuid.New(), OrgID: f.org, Protocol: "saml", Status: "active", AllowIdPInitiated: true,
		AllowedDomains: []string{"example.com"},
		Config:         saml.Config{EntityID: idp.EntityID(), Certificate: encodeSAMLCert(idp)},
	}
	open := samlWriterOpen{}
	logger := slog.New(slog.NewJSONHandler(f.logs, nil))
	svc := &saml.Service{
		Providers: samlWriterProviders{p}, Entitlements: open, Flows: open, Uses: open,
		Store: samlWriterStore{principal: f.principal}, Memberships: open,
		BaseURL: func() string { return samlWriterBase }, Logger: logger,
	}
	handlers := saml.Handlers{Service: svc, Sessions: f.ssoSessions(t).SAML(), Logger: logger}
	mounted := MarkResponseModels([]httpapi.Route{
		{Method: http.MethodPost, Pattern: "/api/v1/auth/saml/{provider_id}/acs", Handler: http.HandlerFunc(handlers.ACS)},
	})
	mux := http.NewServeMux()
	for _, route := range mounted {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}

	acs := p.ACSURL(samlWriterBase)
	posted, err := idp.ResponseB64(idpstub.ResponseSpec{
		NameID: idpstub.NameID{Format: idpstub.NameIDPersistent, Value: "user-" + uuid.NewString()},
		Email:  "member@example.com", Destination: &acs, Recipient: &acs,
		Audiences: [][]string{{p.SPEntityID(samlWriterBase)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/"+p.ID.String()+"/acs",
		strings.NewReader(url.Values{"SAMLResponse": {posted}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	before := policy.WriterViolations()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "access_token") {
		t.Fatalf("ACS status %d: %s", rec.Code, rec.Body)
	}
	if delta := policy.WriterViolations() - before; delta != 0 {
		t.Errorf("SAML ACS: %d writer violations on a success body", delta)
	}
}

func encodeSAMLCert(idp *idpstub.SAML) string {
	return base64.StdEncoding.EncodeToString(idp.Keys().Cert.Raw)
}
