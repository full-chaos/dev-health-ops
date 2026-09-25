package internalidentity

import (
	"encoding/json"
	"net/http"
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

// TestFromHeaderReadsWhatThePythonEdgeSends is the cross-language oracle for the
// carrier: testdata/python_edge_identity_headers.json is produced by the REAL
// Python edge function (go_api_dispatcher._internal_identity_headers) together
// with the identity the signed envelope carries for the same principal
// (tests/api/graphql/test_go_api_internal_identity_headers.py regenerates and
// compares it by execution). Every header set the Python edge produces must be
// read by FromHeader as exactly that identity, byte for byte (non-ASCII values
// included: Go reads the raw bytes the edge sends).
func TestFromHeaderReadsWhatThePythonEdgeSends(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "python_edge_identity_headers.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Cases []struct {
			Name     string            `json:"name"`
			Headers  map[string]string `json:"headers"`
			Expected authctx.Claims    `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) < 5 {
		t.Fatalf("the golden holds %d cases: a shrunken oracle proves nothing", len(golden.Cases))
	}
	for _, c := range golden.Cases {
		h := http.Header{}
		for name, value := range c.Headers {
			h[http.CanonicalHeaderKey(name)] = []string{value}
		}
		if len(c.Headers) != len(Headers) {
			t.Errorf("%s: the Python edge sent %d headers, the reader wants %d", c.Name, len(c.Headers), len(Headers))
		}
		got, err := FromHeader(h)
		if err != nil {
			t.Errorf("%s: FromHeader refused what the Python edge sends: %v", c.Name, err)
			continue
		}
		if got != c.Expected {
			t.Errorf("%s: read %+v, the envelope carries %+v", c.Name, got, c.Expected)
		}
	}
}
