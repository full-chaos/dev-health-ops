//go:build integration

package saml_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
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
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].Path != saml.ACSPath(h.provider, baseURL) {
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

// TestSAMLRoutesRefuseANewAccountWithoutWrites: through the routes and the
// request id middleware, a sign-in that would need a new account answers 403
// with the membership detail, issues no session and writes no account,
// identity, membership or audit row.
func TestSAMLRoutesRefuseANewAccountWithoutWrites(t *testing.T) {
	h := newHarness(t, true)
	h.ownUsers()
	issuer := &recordingIssuer{}
	mux := requestid.Middleware(h.mux(issuer))
	before := h.writeCounts()
	audits := h.count(`SELECT count(*) FROM auth.security_audit_events`)
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]string{"SAMLResponse": h.respond(resp{nameID: nid("new-subject"), email: uniqueEmail("new")})})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/"+h.provider.ID.String()+"/acs", &buf)
	req.Header.Set("X-Request-ID", "client-chosen-id")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var body struct {
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != http.StatusForbidden ||
		body.Detail != "This account is not a member of the provider's organization." || len(issuer.got) != 0 {
		t.Fatalf("ACS = %d %s sessions %d, want 403 with the membership detail and no session", rec.Code, rec.Body, len(issuer.got))
	}
	if after := h.writeCounts(); after != before {
		t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
	}
	if n := h.count(`SELECT count(*) FROM auth.security_audit_events`); n != audits {
		t.Fatalf("audit rows %d -> %d, want no write", audits, n)
	}
	if wire := rec.Header().Get(requestid.Header); wire == "" || !strings.HasPrefix(wire, requestid.WirePrefix) {
		t.Fatalf("response carries no server request id: %q", wire)
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

// TestSAMLACSAcceptsTheAdvertisedHTTPPostBinding: the metadata advertises the
// HTTP-POST binding at the ACS URL, so a browser that the IdP auto-posts there
// as a form completes the sign-in, carrying the cross-site binding cookie.
func TestSAMLACSAcceptsTheAdvertisedHTTPPostBinding(t *testing.T) {
	h := newHarness(t, false)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	issuer := &recordingIssuer{}
	mux := h.mux(issuer)
	base := "/api/v1/auth/saml/" + h.provider.ID.String()

	md := serve(mux, http.MethodGet, base+"/metadata", nil, nil)
	var mdBody map[string]string
	if md.Code != http.StatusOK || json.Unmarshal(md.Body.Bytes(), &mdBody) != nil {
		t.Fatalf("metadata %d %s", md.Code, md.Body)
	}
	advertised := `Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"`
	if !strings.Contains(mdBody["metadata_xml"], advertised) || !strings.Contains(mdBody["metadata_xml"], `Location="`+mdBody["acs_url"]+`"`) {
		t.Fatalf("metadata does not advertise HTTP-POST at the ACS URL: %s", mdBody["metadata_xml"])
	}

	init := serve(mux, http.MethodPost, base+"/initiate", map[string]string{}, nil)
	var initBody map[string]string
	if init.Code != http.StatusOK || json.Unmarshal(init.Body.Bytes(), &initBody) != nil {
		t.Fatalf("initiate %d %s", init.Code, init.Body)
	}
	cookies := init.Result().Cookies()
	if len(cookies) != 1 || cookies[0].SameSite != http.SameSiteNoneMode || !cookies[0].Secure || !cookies[0].HttpOnly ||
		cookies[0].Path != saml.ACSPath(h.provider, baseURL) || cookies[0].MaxAge != int(saml.FlowTTL.Seconds()) {
		t.Fatalf("binding cookie must be SameSite=None; Secure; HttpOnly; Path=ACS; Max-Age=flow TTL: %+v", cookies)
	}
	requestID := requestIDFromRedirect(t, initBody["redirect_url"])

	form := url.Values{"SAMLResponse": {h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"})}, "RelayState": {"/after"}}
	req := httptest.NewRequest(http.MethodPost, base+"/acs", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookies[0])
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || len(issuer.got) != 1 || issuer.got[0].PrincipalID != user {
		t.Fatalf("HTTP-POST binding at the ACS: %d %s sessions %+v", rec.Code, rec.Body, issuer.got)
	}
	cleared := false
	for _, c := range rec.Result().Cookies() {
		if c.Name == cookies[0].Name && c.MaxAge < 0 && c.SameSite == http.SameSiteNoneMode {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("the binding cookie was not cleared on consume")
	}

	for name, ct := range map[string]string{"text/plain": "text/plain", "XML": "application/xml"} {
		other := httptest.NewRequest(http.MethodPost, base+"/acs", strings.NewReader(form.Encode()))
		other.Header.Set("Content-Type", ct)
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, other)
		if rec.Code != http.StatusUnprocessableEntity || len(issuer.got) != 1 {
			t.Fatalf("%s body: %d %s", name, rec.Code, rec.Body)
		}
	}

	missing := httptest.NewRequest(http.MethodPost, base+"/acs", strings.NewReader("RelayState=x"))
	missing.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, missing)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("form without SAMLResponse: %d %s", rec.Code, rec.Body)
	}
}

// TestSAMLBrowserCompletesSignInAtTheAdvertisedACSURL: a browser with a cookie
// jar starts the flow, reads the ACS URL from the metadata and posts the form
// there; the binding cookie must reach that URL. The routes are mounted under
// the configured base, whatever its path.
func TestSAMLBrowserCompletesSignInAtTheAdvertisedACSURL(t *testing.T) {
	h := newHarness(t, false)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	issuer := &recordingIssuer{}
	handlers := saml.Handlers{Service: h.svc, Sessions: issuer}
	mux := http.NewServeMux()
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	const prefix = "/sso-under-test"
	h.svc.BaseURL = func() string { return srv.URL + prefix }
	mux.HandleFunc("GET "+prefix+"/saml/{provider_id}/metadata", handlers.Metadata)
	mux.HandleFunc("POST "+prefix+"/saml/{provider_id}/initiate", handlers.Initiate)
	mux.HandleFunc("POST "+prefix+"/saml/{provider_id}/acs", handlers.ACS)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := srv.Client()
	client.Jar = jar
	base := srv.URL + prefix + "/saml/" + h.provider.ID.String()

	resp0, err := client.Post(base+"/initiate", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var initBody map[string]string
	if err := json.NewDecoder(resp0.Body).Decode(&initBody); err != nil || resp0.StatusCode != http.StatusOK {
		t.Fatalf("initiate %d %v", resp0.StatusCode, err)
	}
	resp0.Body.Close()
	requestID := requestIDFromRedirect(t, initBody["redirect_url"])

	md, err := client.Get(base + "/metadata")
	if err != nil {
		t.Fatal(err)
	}
	var mdBody map[string]string
	if err := json.NewDecoder(md.Body).Decode(&mdBody); err != nil {
		t.Fatal(err)
	}
	md.Body.Close()
	location := regexp.MustCompile(`Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"\s+Location="([^"]+)"`).FindStringSubmatch(mdBody["metadata_xml"])
	if location == nil || location[1] != mdBody["acs_url"] {
		t.Fatalf("metadata advertises no HTTP-POST ACS location: %s", mdBody["metadata_xml"])
	}
	acs, err := url.Parse(location[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(jar.Cookies(acs)) != 1 {
		t.Fatalf("the browser holds no binding cookie for the advertised ACS URL %s", acs)
	}

	posted := b64(h.idp.response(t, resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com",
		destination: strp(location[1]), recipient: strp(location[1]), audiences: [][]string{{mdBody["entity_id"]}}}))
	res, err := client.PostForm(location[1], url.Values{"SAMLResponse": {posted}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK || len(issuer.got) != 1 || issuer.got[0].PrincipalID != user {
		t.Fatalf("form post to the advertised ACS URL: %d sessions %+v", res.StatusCode, issuer.got)
	}
	if len(jar.Cookies(acs)) != 0 {
		t.Fatal("the binding cookie outlived the sign-in")
	}
}

// TestSAMLEntitlementIsCheckedBeforeTheBody: a non-entitled organization gets
// 402 on every route whatever the body or content type.
func TestSAMLEntitlementIsCheckedBeforeTheBody(t *testing.T) {
	h := newHarness(t, true)
	h.svc.Entitlements = saml.LicensingEntitlements{DB: h.db.runtime}
	mux := h.mux(&recordingIssuer{})
	base := "/api/v1/auth/saml/" + h.provider.ID.String()
	for name, tc := range map[string]struct{ path, contentType, body string }{
		"initiate, malformed JSON":   {"/initiate", "application/json", "{"},
		"initiate, text/plain":       {"/initiate", "text/plain", "x"},
		"acs, malformed JSON":        {"/acs", "application/json", "{"},
		"acs, unsupported type":      {"/acs", "application/xml", "<x/>"},
		"acs, form without response": {"/acs", "application/x-www-form-urlencoded", "RelayState=x"},
		"acs, oversized form":        {"/acs", "application/x-www-form-urlencoded", "SAMLResponse=" + strings.Repeat("A", 2<<20)},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, base+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusPaymentRequired || !strings.Contains(rec.Body.String(), "feature_not_licensed") {
				t.Fatalf("%d %s, want 402 feature_not_licensed", rec.Code, rec.Body)
			}
		})
	}
}
