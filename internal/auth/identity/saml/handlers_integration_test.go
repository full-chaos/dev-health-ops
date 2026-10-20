//go:build integration

package saml_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

type recordingIssuer struct{ got []saml.SignIn }

func (r *recordingIssuer) IssueFederatedSession(w http.ResponseWriter, _ *http.Request, s saml.SignIn) {
	r.got = append(r.got, s)
	w.WriteHeader(http.StatusOK)
}

func (h *harness) mux(issuer saml.SessionIssuer) *http.ServeMux {
	handlers := saml.Handlers{Service: h.svc, Sessions: issuer}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/auth/saml/{provider_id}/metadata", handlers.Metadata)
	mux.HandleFunc("POST /api/v1/auth/saml/{provider_id}/initiate", handlers.Initiate)
	mux.HandleFunc("POST /api/v1/auth/saml/{provider_id}/acs", handlers.ACS)
	return mux
}

func serve(mux http.Handler, method, path string, body any, cookies []*http.Cookie) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestSAMLRoutesEndToEnd(t *testing.T) {
	h := newHarness(t, false)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	issuer := &recordingIssuer{}
	mux := h.mux(issuer)
	base := "/api/v1/auth/saml/" + h.provider.ID.String()

	md := serve(mux, http.MethodGet, base+"/metadata", nil, nil)
	var mdBody map[string]string
	if md.Code != http.StatusOK || json.Unmarshal(md.Body.Bytes(), &mdBody) != nil || mdBody["acs_url"] != h.provider.ACSURL(baseURL) ||
		!strings.Contains(mdBody["metadata_xml"], `WantAssertionsSigned="true"`) {
		t.Fatalf("metadata %d %s", md.Code, md.Body)
	}

	init := serve(mux, http.MethodPost, base+"/initiate", map[string]string{"relay_state": "/after"}, nil)
	var initBody map[string]string
	if init.Code != http.StatusOK || json.Unmarshal(init.Body.Bytes(), &initBody) != nil {
		t.Fatalf("initiate %d %s", init.Code, init.Body)
	}
	cookies := init.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].Path != saml.ACSPath(h.provider) {
		t.Fatalf("binding cookie %+v", cookies)
	}
	requestID := requestIDFromRedirect(t, initBody["redirect_url"])

	posted := h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"})
	acs := serve(mux, http.MethodPost, base+"/acs", map[string]string{"SAMLResponse": posted}, cookies)
	if acs.Code != http.StatusOK || len(issuer.got) != 1 || issuer.got[0].PrincipalID != user {
		t.Fatalf("acs %d %s sessions %+v", acs.Code, acs.Body, issuer.got)
	}
	cleared := false
	for _, c := range acs.Result().Cookies() {
		if c.Name == cookies[0].Name && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("the binding cookie was not cleared")
	}

	replay := serve(mux, http.MethodPost, base+"/acs", map[string]string{"saml_response": posted}, cookies)
	if replay.Code != http.StatusBadRequest || !strings.Contains(replay.Body.String(), "SAML authentication failed") || len(issuer.got) != 1 {
		t.Fatalf("replay %d %s", replay.Code, replay.Body)
	}
}

func TestSAMLRoutesAnswerConflictAndGate(t *testing.T) {
	h := newHarness(t, true)
	email := uniqueEmail("existing")
	h.linkedUser(email, "home-subject")
	issuer := &recordingIssuer{}
	mux := h.mux(issuer)
	base := "/api/v1/auth/saml/" + h.provider.ID.String()

	rec := serve(mux, http.MethodPost, base+"/acs", map[string]string{"SAMLResponse": h.respond(resp{nameID: nid("other"), email: email})}, nil)
	var conflict struct {
		Detail struct {
			Message          string `json:"message"`
			ExistingProvider string `json:"existing_provider"`
		} `json:"detail"`
	}
	if rec.Code != http.StatusConflict || len(issuer.got) != 0 || json.Unmarshal(rec.Body.Bytes(), &conflict) != nil ||
		conflict.Detail.ExistingProvider != "local" || conflict.Detail.Message != "An account with this email already exists. Please sign in with local." {
		t.Fatalf("collision %d %s", rec.Code, rec.Body)
	}

	h.svc.Entitlements = allowSSO{deny: true}
	if rec := serve(mux, http.MethodGet, base+"/metadata", nil, nil); rec.Code != http.StatusPaymentRequired {
		t.Fatalf("not entitled: %d", rec.Code)
	}
	if rec := serve(mux, http.MethodGet, "/api/v1/auth/saml/not-a-uuid/metadata", nil, nil); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad id: %d", rec.Code)
	}
}

func TestSAMLClientRequestIDNeverReachesTheAuditActor(t *testing.T) {
	h := newHarness(t, true)
	h.ownUsers()
	issuer := &recordingIssuer{}
	mux := h.mux(issuer)
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]string{"SAMLResponse": h.respond(resp{nameID: nid("new-subject"), email: uniqueEmail("new")})})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/"+h.provider.ID.String()+"/acs", &buf)
	req.Header.Set("X-Request-ID", "client-chosen-id")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || len(issuer.got) != 1 || !issuer.got[0].Created {
		t.Fatalf("sign-up %d %s", rec.Code, rec.Body)
	}
	var rows, fromClient int
	h.assertProvisionAudit(t, issuer.got[0].PrincipalID, &rows, &fromClient)
	if rows != 1 || fromClient != 0 {
		t.Fatalf("provisioning audit rows %d, %d of them carry the client's X-Request-ID", rows, fromClient)
	}

	t.Run("with the request id middleware", func(t *testing.T) {
		issuer := &recordingIssuer{}
		mux := requestid.Middleware(h.mux(issuer))
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(map[string]string{"SAMLResponse": h.respond(resp{nameID: nid("new-subject-2"), email: uniqueEmail("new")})})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/"+h.provider.ID.String()+"/acs", &buf)
		req.Header.Set("X-Request-ID", "client-chosen-id")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || len(issuer.got) != 1 {
			t.Fatalf("sign-up %d %s", rec.Code, rec.Body)
		}
		var audited *string
		if err := h.db.admin.QueryRow(context.Background(), `SELECT request_id FROM auth.security_audit_events
WHERE event_type = 'auth.principal.provisioned' AND subject_principal_id = $1`, issuer.got[0].PrincipalID).Scan(&audited); err != nil {
			t.Fatal(err)
		}
		if audited == nil || *audited == "client-chosen-id" || uuid.Validate(*audited) != nil {
			t.Fatalf("audit request id %v, want the server-minted UUID", audited)
		}
		if wire := rec.Header().Get(requestid.Header); wire == "" || !strings.HasPrefix(wire, requestid.WirePrefix) {
			t.Fatalf("response carries no server request id: %q", wire)
		}
	})
}

func (h *harness) assertProvisionAudit(t *testing.T, principal uuid.UUID, rows, fromClient *int) {
	t.Helper()
	if err := h.db.admin.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE request_id = 'client-chosen-id')
FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned' AND subject_principal_id = $1`, principal).Scan(rows, fromClient); err != nil {
		t.Fatal(err)
	}
}

// TestSAMLEntitlementRefusesANonEntitledOrg: the license gate runs on every
// route, through the real license tables, before any request or response is
// processed.
func TestSAMLEntitlementRefusesANonEntitledOrg(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, true)
	h.linkedUser(uniqueEmail("u"), "user-1")
	h.svc.Entitlements = saml.LicensingEntitlements{DB: h.db.runtime}
	issuer := &recordingIssuer{}
	mux := h.mux(issuer)
	base := "/api/v1/auth/saml/" + h.provider.ID.String()
	posted := h.respond(resp{nameID: nid("user-1"), email: "a@example.com"})

	refused := func(name string) {
		t.Helper()
		for route, rec := range map[string]*httptest.ResponseRecorder{
			"metadata": serve(mux, http.MethodGet, base+"/metadata", nil, nil),
			"initiate": serve(mux, http.MethodPost, base+"/initiate", map[string]string{}, nil),
			"acs":      serve(mux, http.MethodPost, base+"/acs", map[string]string{"SAMLResponse": posted}, nil),
		} {
			var raw map[string]map[string]any
			if rec.Code != http.StatusPaymentRequired || json.Unmarshal(rec.Body.Bytes(), &raw) != nil ||
				raw["detail"]["error"] != "feature_not_licensed" || raw["detail"]["feature"] != saml.SSOFeature || raw["detail"]["required_tier"] != "enterprise" {
				t.Fatalf("%s: %s answered %d %s, want the 402 feature_not_licensed body", name, route, rec.Code, rec.Body)
			}
		}
		if len(issuer.got) != 0 {
			t.Fatalf("%s: a session was issued", name)
		}
	}
	refused("default tier organization, no license")

	if _, err := h.db.admin.Exec(ctx, `UPDATE public.organizations SET tier = 'team' WHERE id = $1`, h.provider.OrgID); err != nil {
		t.Fatal(err)
	}
	refused("team tier organization")
	if _, err := h.db.admin.Exec(ctx, `INSERT INTO public.org_licenses (id, org_id, tier, created_at, updated_at) VALUES ($1, $2, 'team', now(), now())`, uuid.New(), h.provider.OrgID); err != nil {
		t.Fatal(err)
	}
	refused("team tier license")

	if _, err := h.db.admin.Exec(ctx, `UPDATE public.org_licenses SET features_override = '{"sso_saml": true}'::json WHERE org_id = $1`, h.provider.OrgID); err != nil {
		t.Fatal(err)
	}
	if rec := serve(mux, http.MethodGet, base+"/metadata", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("feature override: metadata %d %s", rec.Code, rec.Body)
	}
	if _, err := h.db.admin.Exec(ctx, `UPDATE public.org_licenses SET tier = 'enterprise', features_override = '{}'::json WHERE org_id = $1`, h.provider.OrgID); err != nil {
		t.Fatal(err)
	}
	if rec := serve(mux, http.MethodPost, base+"/acs", map[string]string{"SAMLResponse": posted}, nil); rec.Code != http.StatusOK || len(issuer.got) != 1 {
		t.Fatalf("enterprise license: acs %d %s", rec.Code, rec.Body)
	}
}
