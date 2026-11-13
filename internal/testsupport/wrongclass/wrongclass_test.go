package wrongclass

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

func fakeMint(alg, typ string) string { return "h." + alg + typ + ".s" }

func classesFrom(t *testing.T, classes ...map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"classes": classes})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Every class of the real registry loads, and the not-driven set is exactly the classes with no carrier kind.
func TestTheRealRegistryLoadsAndEveryClassHasACarrierOrNoCarrierKind(t *testing.T) {
	classes, err := LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	driven := 0
	for _, class := range classes {
		carrier, ok, err := CarrierFor(class, Options{Mint: fakeMint})
		if err != nil {
			t.Fatalf("%s: %v", class.ID, err)
		}
		hasCarrierKind := slices.ContainsFunc(class.Markers, func(m Marker) bool { return slices.Contains(carrierKinds, m.Kind) })
		if ok != hasCarrierKind {
			t.Errorf("%s: driven %v, has a carrier kind %v", class.ID, ok, hasCarrierKind)
		}
		if ok {
			driven++
			if carrier.Header == "" && carrier.BodyField == "" {
				t.Errorf("%s: a carrier with no header and no body field", class.ID)
			}
		}
	}
	if driven == 0 || driven == len(classes) {
		t.Fatalf("driven %d of %d classes", driven, len(classes))
	}
}

func TestLoadClassesRefusesWhatHasNoRule(t *testing.T) {
	for name, class := range map[string]map[string]any{
		"unknown marker kind":     {"class_id": "c", "markers": []map[string]any{{"kind": "cookie", "name": "sid"}}},
		"no marker and no reason": {"class_id": "c", "markers": []map[string]any{}},
		"no marker, blank reason": {"class_id": "c", "markers": []map[string]any{}, "markers_none_reason": "  "},
		"no class id":             {"markers": []map[string]any{}, "markers_none_reason": "r"},
	} {
		if _, err := LoadClasses(classesFrom(t, class)); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
	if _, err := LoadClasses(classesFrom(t)); err == nil {
		t.Error("an empty registry loaded")
	}
	if _, err := LoadClasses(classesFrom(t, map[string]any{"class_id": "c", "markers": []map[string]any{}, "markers_none_reason": "outbound"})); err != nil {
		t.Errorf("a class with a reason and no marker: %v", err)
	}
}

func TestCarrierForEachKind(t *testing.T) {
	mint := func(alg, typ string) string { return "a." + alg + "-" + typ + ".b" }
	cases := []struct {
		markers []Marker
		header  string
		value   string
		body    string
	}{
		{[]Marker{{Kind: "credential_prefix", Name: "fcx_"}, {Kind: "authorization_scheme", Name: "Bearer"}}, Authorization, "Bearer fcx_" + Filler, ""},
		{[]Marker{{Kind: "credential_prefix", Name: "fcx_"}}, Authorization, "Bearer fcx_" + Filler, ""},
		{[]Marker{{Kind: "authorization_scheme", Name: "Bearer"}, {Kind: "jwt_header_alg", Name: "EdDSA"}, {Kind: "jwt_header_typ", Name: "at+jwt"}}, Authorization, "Bearer a.EdDSA-at+jwt.b", ""},
		{[]Marker{{Kind: "authorization_scheme", Name: "Bearer"}, {Kind: "jwt_header_alg", Name: "HS256"}}, Authorization, "Bearer a.HS256-.b", ""},
		{[]Marker{{Kind: "dedicated_header", Name: "X-Org-Id"}}, "X-Org-Id", "org", ""},
		{[]Marker{{Kind: "dedicated_header", Name: "X-Other"}}, "X-Other", Filler, ""},
		{[]Marker{{Kind: "signature_header", Name: "X-Sig"}}, "X-Sig", "sha256=" + Filler, ""},
		{[]Marker{{Kind: "body_field", Name: "refresh", Shape: "prefix:dhr1_"}}, "", "dhr1_" + Filler, "refresh"},
	}
	for _, c := range cases {
		carrier, ok, err := CarrierFor(Class{ID: "c", Markers: c.markers}, Options{Mint: mint, HeaderValue: func(name string) string {
			if name == "X-Org-Id" {
				return "org"
			}
			return ""
		}})
		if err != nil || !ok || carrier.Header != c.header || carrier.Value != c.value || carrier.BodyField != c.body {
			t.Errorf("%v: carrier %+v ok %v err %v", c.markers, carrier, ok, err)
		}
		baseline := carrier.Baseline()
		if baseline.Value == carrier.Value && baseline.Header == carrier.Header {
			t.Errorf("%v: the baseline is the carrier", c.markers)
		}
	}
	for name, markers := range map[string][]Marker{
		"bearer with no alg":         {{Kind: "authorization_scheme", Name: "Bearer"}},
		"bearer with two algs":       {{Kind: "authorization_scheme", Name: "Bearer"}, {Kind: "jwt_header_alg", Name: "EdDSA"}, {Kind: "jwt_header_alg", Name: "HS256"}},
		"a scheme other than Bearer": {{Kind: "authorization_scheme", Name: "Basic"}, {Kind: "jwt_header_alg", Name: "EdDSA"}},
		"unknown body shape":         {{Kind: "body_field", Name: "f", Shape: "uuid"}},
	} {
		if _, _, err := CarrierFor(Class{ID: "c", Markers: markers}, Options{Mint: mint}); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, _, err := CarrierFor(Class{ID: "c", Markers: []Marker{{Kind: "authorization_scheme", Name: "Bearer"}, {Kind: "jwt_header_alg", Name: "EdDSA"}}}, Options{Mint: func(string, string) string { return "" }}); err == nil {
		t.Error("a minter that made no JWT was accepted")
	}
	if _, ok, err := CarrierFor(Class{ID: "c", Markers: []Marker{{Kind: "content_type", Name: "application/json"}}}, Options{}); ok || err != nil {
		t.Errorf("a qualifier-only class: ok %v err %v", ok, err)
	}
}

func TestBaselines(t *testing.T) {
	if got := (Carrier{Header: Authorization, Value: "Bearer fcx_" + Filler}).Baseline(); got.Header != Authorization || got.Value != "Bearer "+Filler {
		t.Errorf("authorization baseline %+v", got)
	}
	if got := (Carrier{Header: "X-Org-Id", Value: "o"}).Baseline(); got.Header != "" || got.BodyField != "" {
		t.Errorf("header baseline %+v", got)
	}
	got := (Carrier{BodyField: "refresh_token", Value: "x.y.z"}).Baseline()
	request := got.Request(http.MethodPost, "/p")
	raw, _ := io.ReadAll(request.Body)
	if string(raw) != `{"refresh_token":"`+Filler+`"}` || request.Header.Get("Content-Type") != "application/json" {
		t.Errorf("body baseline %s %q", raw, request.Header.Get("Content-Type"))
	}
}

func TestLoadRowsReadsTheListenerTag(t *testing.T) {
	rows, err := LoadRows(authv1.GoAPIEndpointProfiles)
	if err != nil || len(rows) == 0 {
		t.Fatalf("rows %d err %v", len(rows), err)
	}
	for _, row := range rows {
		if len(row.Listeners) == 0 || row.Listeners[0] == "" {
			t.Errorf("%s: no listener", row.ID)
		}
	}
	bad, _ := json.Marshal(map[string]any{"rows": []map[string]any{{"id": "GET /x", "service": "s", "method": "GET", "route": "/x"}}})
	if _, err := LoadRows(bad); err == nil {
		t.Error("a row with no listener tag loaded")
	}
	other, _ := json.Marshal(map[string]any{"rows": []map[string]any{{"id": "GET /x [t @api]", "service": "s", "method": "GET", "route": "/x"}}})
	if _, err := LoadRows(other); err == nil {
		t.Error("a row whose tag names another service loaded")
	}
	if (Row{Route: "/a/{id}/b/{c}"}).Path() != "/a/x/b/x" {
		t.Error("path params")
	}
}

func TestCaptureMasksTheRequestIDAndKeepsTheComparedHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("X-Request-Id", "rid-1")
	recorder.Header().Set("Www-Authenticate", "Bearer")
	recorder.Header().Set("X-Other", "dropped")
	recorder.WriteHeader(http.StatusUnauthorized)
	_, _ = recorder.WriteString(`{"request_id":"rid-1"}`)
	got := Capture(recorder)
	if got.Status != 401 || got.Body != `{"request_id":"<request-id>"}` || got.Header != "Www-Authenticate: Bearer" {
		t.Fatalf("answer %+v", got)
	}
	if !strings.Contains((Census{Service: "s", NotDriven: []string{"b", "a"}}).Line(), "not_driven=a,b") {
		t.Error("census line")
	}
}

// A class with two dedicated headers gives one carrier per header, each alone; a class with one gives one.
func TestCarriersForGivesEveryDedicatedHeaderItsOwnCarrier(t *testing.T) {
	two := Class{ID: "c", Markers: []Marker{{Kind: "dedicated_header", Name: "X-A"}, {Kind: "dedicated_header", Name: "X-B"}}}
	carriers, ok, err := CarriersFor(two, Options{Mint: fakeMint, HeaderValue: func(name string) string {
		if name == "X-B" {
			return "b"
		}
		return ""
	}})
	if err != nil || !ok {
		t.Fatalf("two headers: ok %v, err %v", ok, err)
	}
	want := []Carrier{{Class: "c", Header: "X-A", Value: Filler}, {Class: "c", Header: "X-B", Value: "b"}}
	if !slices.Equal(carriers, want) {
		t.Fatalf("two headers: carriers %+v, want %+v", carriers, want)
	}
	for _, carrier := range carriers {
		request := carrier.Request(http.MethodGet, "/x")
		if got := len(request.Header); got != 1 {
			t.Errorf("%s: the request carries %d headers, want only its own", carrier.Header, got)
		}
	}
	one, ok, err := CarriersFor(Class{ID: "d", Markers: []Marker{{Kind: "dedicated_header", Name: "X-A"}}}, Options{Mint: fakeMint})
	if err != nil || !ok || len(one) != 1 {
		t.Fatalf("one header: %d carriers, ok %v, err %v", len(one), ok, err)
	}
}

// The real registry presents acr_web_assertion in both of its headers: the web assertion and the web approver
// reference (the second presentation of the same class, signed with the same key). Removing either marker fails here.
func TestTheRealRegistryPresentsTheWebAssertionInBothHeaders(t *testing.T) {
	classes, err := LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	var carriers []Carrier
	for _, class := range classes {
		if class.ID != "acr_web_assertion" {
			continue
		}
		found, ok, err := CarriersFor(class, Options{Mint: fakeMint})
		if err != nil || !ok {
			t.Fatalf("acr_web_assertion: ok %v, err %v", ok, err)
		}
		carriers = found
	}
	for _, header := range []string{"X-ACR-Web-Assertion", "X-ACR-Approver-Reference"} {
		if !HasHeaderCarrier(carriers, header) {
			t.Errorf("acr_web_assertion has no %s carrier: %+v", header, carriers)
		}
	}
}
