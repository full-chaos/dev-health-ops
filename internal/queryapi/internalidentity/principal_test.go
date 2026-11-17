package internalidentity

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// CHAOS-13945: X-DH-Internal-Principal-Id is the fifth carrier header: optional, at most once, a principal wire id.

const testPrincipal = "prn_AAECAwQFBgcICQoLDA0ODw"

func fourHeaders() http.Header {
	h := http.Header{}
	h.Set(HeaderOrgID, "org-1")
	h.Set(HeaderRole, "member")
	h.Set(HeaderSuperuser, "false")
	h.Set(HeaderImpersonationActive, "false")
	return h
}

// RC7: the public listener deletes the principal header too.
func TestPublicDeletesThePrincipalHeader(t *testing.T) {
	var seen http.Header
	handler := Public(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = r.Header.Clone() }))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", nil)
	req.Header.Set(HeaderPrincipalID, testPrincipal)
	req.Header["x-dh-internal-principal-id"] = []string{testPrincipal}
	handler.ServeHTTP(httptest.NewRecorder(), req)
	for name := range seen {
		if len(name) >= 12 && (name[:12] == "X-Dh-Interna" || name[:12] == "x-dh-interna") {
			t.Fatalf("spelling %q survived: %v", name, seen)
		}
	}
}

func TestFromHeaderReadsTheOptionalPrincipal(t *testing.T) {
	claims, err := FromHeader(fourHeaders())
	if err != nil || claims.HeaderPrincipal != "" {
		t.Fatalf("four headers: %+v %v, want no principal and no error", claims, err)
	}
	h := fourHeaders()
	h.Set(HeaderPrincipalID, testPrincipal)
	if claims, err = FromHeader(h); err != nil || claims.HeaderPrincipal != testPrincipal {
		t.Fatalf("with the principal: %+v %v, want %s", claims, err, testPrincipal)
	}
	for name, values := range map[string][]string{
		"two values":        {testPrincipal, testPrincipal},
		"empty":             {""},
		"another prefix":    {"usr_AAECAwQFBgcICQoLDA0ODw"},
		"too long":          {testPrincipal + "AA"},
		"padded":            {testPrincipal + " "},
		"not base64url":     {"prn_AAECAwQFBgcICQoLDA0OD+"},
		"the nil principal": {"prn_AAAAAAAAAAAAAAAAAAAAAA"},
	} {
		h := fourHeaders()
		h[HeaderPrincipalID] = values
		if _, err := FromHeader(h); err == nil {
			t.Errorf("%s: accepted, want a rejection", name)
		}
	}
}

// RC5b: the principal header alone is a partial carrier: present, and refused as a missing header.
func TestPrincipalHeaderAloneIsPresentAndRefused(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderPrincipalID, testPrincipal)
	if !Present(h) {
		t.Fatal("the principal header alone is not present")
	}
	if _, err := FromHeader(h); ReasonOf(err) != ReasonMissing {
		t.Fatalf("the principal header alone: %v, want %s", err, ReasonMissing)
	}
}
