package internalidentity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

func full() http.Header {
	h := http.Header{}
	h.Set(HeaderOrgID, "org-1")
	h.Set(HeaderRole, "admin")
	h.Set(HeaderSuperuser, "false")
	h.Set(HeaderImpersonationActive, "false")
	return h
}

func TestFromHeaderReadsTheFourFields(t *testing.T) {
	h := full()
	h.Set(HeaderSuperuser, "true")
	h.Set(HeaderImpersonationActive, "true")
	got, err := FromHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	want := authctx.Claims{OrgID: "org-1", Role: "admin", IsSuperuser: true, ImpersonationActive: true}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestFromHeaderRefusesAnyPartialOrMalformedSet(t *testing.T) {
	for _, name := range Headers {
		h := full()
		h.Del(name)
		if _, err := FromHeader(h); ReasonOf(err) != ReasonMissing {
			t.Errorf("missing %s: reason %q err %v", name, ReasonOf(err), err)
		}
		if !Present(h) {
			t.Errorf("a set missing %s is still present and must be refused, not ignored", name)
		}
		d := full()
		d.Add(name, "second")
		if _, err := FromHeader(d); ReasonOf(err) != ReasonDuplicate {
			t.Errorf("duplicate %s: reason %q err %v", name, ReasonOf(err), err)
		}
	}
	for _, name := range []string{HeaderSuperuser, HeaderImpersonationActive} {
		for _, value := range []string{"", "True", "1", "yes", " true", "TRUE"} {
			h := full()
			h.Set(name, value)
			if _, err := FromHeader(h); ReasonOf(err) != ReasonBoolean {
				t.Errorf("%s=%q: reason %q err %v", name, value, ReasonOf(err), err)
			}
		}
	}
}

func TestPresentIsFalseOnlyWithNoInternalHeader(t *testing.T) {
	if Present(http.Header{"Authorization": {"Bearer x"}}) {
		t.Fatal("an unrelated header is not an internal identity")
	}
	for _, name := range Headers {
		h := http.Header{}
		h.Set(name, "x")
		if !Present(h) {
			t.Errorf("%s alone must count as present", name)
		}
	}
}

func TestHeaderNamesAreTheDeployContract(t *testing.T) {
	// The Ingress strips exactly these names (deploy PR). Changing one
	// without that PR leaves it client-settable.
	want := []string{"X-DH-Internal-Org-Id", "X-DH-Internal-Role", "X-DH-Internal-Superuser", "X-DH-Internal-Impersonation-Active"}
	if len(Headers) != len(want) {
		t.Fatalf("headers %v", Headers)
	}
	for i := range want {
		if Headers[i] != want[i] {
			t.Fatalf("header %d = %q, want %q", i, Headers[i], want[i])
		}
	}
}

// pythonEdgeGolden is testdata/python_edge_identity_headers.json: produced by the
// REAL Python edge function (go_api_dispatcher._internal_identity_headers)
// together with the identity the signed envelope carries for the same principal
// (tests/api/graphql/test_go_api_internal_identity_headers.py regenerates and
// compares it by execution).
type pythonEdgeGolden struct {
	Cases []struct {
		Name     string            `json:"name"`
		Headers  map[string]string `json:"headers"`
		Expected authctx.Claims    `json:"expected"`
	} `json:"cases"`
	Refused []struct {
		Name  string `json:"name"`
		OrgID string `json:"org_id"`
		Role  string `json:"role"`
	} `json:"refused"`
}

func loadPythonEdgeGolden(t *testing.T) pythonEdgeGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "python_edge_identity_headers.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden pythonEdgeGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) < 10 || len(golden.Refused) < 5 {
		t.Fatalf("the golden holds %d cases and %d refusals: a shrunken oracle proves nothing", len(golden.Cases), len(golden.Refused))
	}
	return golden
}

// TestFromHeaderReadsWhatThePythonEdgeSends is the cross-language oracle for the
// carrier: every header set the Python edge produces travels through a REAL
// net/http server (so its parsing, trimming and canonicalisation apply, not a
// hand-built http.Header) and FromHeader must read exactly the identity the
// envelope carries, byte for byte: non-ASCII, four-byte unicode, combining
// marks, empty, very long, interior spaces and header-looking text included.
func TestFromHeaderReadsWhatThePythonEdgeSends(t *testing.T) {
	golden := loadPythonEdgeGolden(t)
	var (
		got    authctx.Claims
		gotErr error
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, gotErr = FromHeader(r.Header)
	}))
	defer server.Close()
	for _, c := range golden.Cases {
		if len(c.Headers) != len(Headers) {
			t.Errorf("%s: the Python edge sent %d headers, the reader wants %d", c.Name, len(c.Headers), len(Headers))
		}
		request, err := http.NewRequest(http.MethodPost, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range c.Headers {
			request.Header[http.CanonicalHeaderKey(name)] = []string{value}
		}
		got, gotErr = authctx.Claims{}, nil
		response, err := server.Client().Do(request)
		if err != nil {
			t.Errorf("%s: the request could not be sent: %v", c.Name, err)
			continue
		}
		_ = response.Body.Close()
		if gotErr != nil {
			t.Errorf("%s: FromHeader refused what the Python edge sends: %v", c.Name, gotErr)
			continue
		}
		if got != c.Expected {
			t.Errorf("%s: read %+v, the envelope carries %+v", c.Name, got, c.Expected)
		}
	}
}

// TestValuesThePythonEdgeRefusesCannotArriveUnaltered proves the refusal is
// necessary: each value the edge will not send is either rejected by a Go HTTP
// client or arrives as a DIFFERENT string than the envelope would have carried.
func TestValuesThePythonEdgeRefusesCannotArriveUnaltered(t *testing.T) {
	golden := loadPythonEdgeGolden(t)
	var received string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Header.Get(HeaderOrgID) + "|" + r.Header.Get(HeaderRole) + "|" + r.Header.Get(HeaderSuperuser)
	}))
	defer server.Close()
	for _, c := range golden.Refused {
		request, err := http.NewRequest(http.MethodPost, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set(HeaderOrgID, c.OrgID)
		request.Header.Set(HeaderRole, c.Role)
		received = ""
		response, err := server.Client().Do(request)
		if err != nil {
			continue // a Go client refuses it outright
		}
		_ = response.Body.Close()
		if received == c.OrgID+"|"+c.Role+"|" {
			t.Errorf("%s: a value the Python edge refuses arrived unaltered, the refusal is not needed", c.Name)
		}
	}
}
