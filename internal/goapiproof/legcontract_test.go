package goapiproof

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

// claimsJWT builds an unsigned three-segment token whose payload is
// exactly claims (any JSON value per claim), so a claim's TYPE can be
// varied as well as its value.
func claimsJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"EdDSA"}`)) + "." + enc(payload) + "." + enc([]byte("sig"))
}

func TestNewLegClientNeverConsultsAProxyAndRefusesRedirects(t *testing.T) {
	client := NewLegClient(0)
	transport, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("leg client transport is %T, want *http.Transport", client.http.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("the leg client consults a proxy: a proxy is a different server than the one named, and one answering a leg itself admitted a match")
	}

	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, redirector.URL, nil)
	if _, err := client.Do(request); !errors.Is(err, errRedirectRefused) {
		t.Fatalf("Do across a redirect: err = %v, want errRedirectRefused", err)
	}
	if reached.Load() != 0 {
		t.Fatalf("the redirect target was reached %d time(s)", reached.Load())
	}
}

// TestLegClientCountsATransparentResend: the transport resends an
// idempotent request on its own when a reused connection fails before
// the response; the count records it.
func TestLegClientCountsATransparentResend(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 2 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := NewLegClient(0)
	var got []int
	for i := 0; i < 2; i++ {
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
		// Drained before Close so the connection returns to the pool:
		// the resend under test happens only on a REUSED connection.
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		got = append(got, response.WireAttempts)
	}
	if got[0] != 1 || got[1] != 2 || hits.Load() != 3 {
		t.Fatalf("wire attempts = %v with %d server hits, want [1 2] with 3 (the second call resent once)", got, hits.Load())
	}
}

func TestValidateBaseURLInputDomain(t *testing.T) {
	cells := []struct {
		raw    string
		accept bool
	}{
		{"http://api:8000", true},
		{"http://api:8000/", true},
		{"https://api.internal/prefix", true},
		{"http://api:8000/#frag", false},
		{"http://api:8000/#", false},
		{"http://api:8000/?x=1", false},
		{"http://api:8000/?", false},
		{"http://u:p@api:8000", false},
		{"http://u@api:8000", false},
		{"api:8000", false},
		{"ftp://api:8000", false},
		{"http:api", false},
		{"http://", false},
		{"", false},
		{"http://api:8000/%zz", false},
	}
	for _, cell := range cells {
		err := ValidateBaseURL("-base", cell.raw)
		if (err == nil) != cell.accept {
			t.Errorf("ValidateBaseURL(%q) err = %v, want accept=%v", cell.raw, err, cell.accept)
		}
		if err != nil && cell.raw != "" && strings.Contains(err.Error(), cell.raw) {
			t.Errorf("ValidateBaseURL(%q) printed the value: %v", cell.raw, err)
		}
	}
}

func TestIsPathLiteralIDInputDomain(t *testing.T) {
	cells := map[string]bool{
		"0272b61335293d17125e3062779c023d638e6e2334ecad70e9ac6199e57ad167": true,
		"11111111-1111-4111-8111-111111111111":                             true,
		"ABC-123":                                                          true,
		"a:b":                                                              true,
		"a@b":                                                              true,
		"":                                                                 false,
		".":                                                                false,
		"..":                                                               false,
		"%61bc":                                                            false,
		"a/b":                                                              false,
		"a b":                                                              false,
		"a?b":                                                              false,
		"a#b":                                                              false,
		"a;b":                                                              false,
		"a,b":                                                              false,
		"é":                                                                false,
	}
	for id, want := range cells {
		if got := IsPathLiteralID(id); got != want {
			t.Errorf("IsPathLiteralID(%q) = %v, want %v", id, got, want)
		}
	}
}

// TestBindOrgInputDomain drives every value shape through both the
// static and the minted path; a refused value is never sent.
func TestBindOrgInputDomain(t *testing.T) {
	const org = "11111111-1111-4111-8111-111111111111"
	cells := []struct {
		name   string
		value  string
		accept bool
	}{
		{"named org", claimsJWT(t, map[string]any{"org_id": org}), true},
		{"named org, impersonation false", claimsJWT(t, map[string]any{"org_id": org, "impersonation_active": false}), true},
		{"named org, impersonation null", claimsJWT(t, map[string]any{"org_id": org, "impersonation_active": nil}), true},
		{"other org", claimsJWT(t, map[string]any{"org_id": "22222222-2222-4222-8222-222222222222"}), false},
		{"empty org", claimsJWT(t, map[string]any{"org_id": ""}), false},
		{"org absent", claimsJWT(t, map[string]any{"sub": "x"}), false},
		{"org null", claimsJWT(t, map[string]any{"org_id": nil}), false},
		{"org a number", claimsJWT(t, map[string]any{"org_id": 7}), false},
		{"org a list", claimsJWT(t, map[string]any{"org_id": []string{org}}), false},
		{"impersonation true", claimsJWT(t, map[string]any{"org_id": org, "impersonation_active": true}), false},
		{"impersonation a string", claimsJWT(t, map[string]any{"org_id": org, "impersonation_active": "true"}), false},
		{"two segments", "aGVhZA.cGF5bG9hZA", false},
		{"payload not base64url", "aGVhZA.!!!.c2ln", false},
		{"payload not an object", "aGVhZA." + base64.RawURLEncoding.EncodeToString([]byte(`[1]`)) + ".c2ln", false},
		{"payload trailing bytes", "aGVhZA." + base64.RawURLEncoding.EncodeToString([]byte(`{"org_id":"`+org+`"}x`)) + ".c2ln", false},
	}
	for _, cell := range cells {
		for _, minted := range []bool{false, true} {
			var credential *Credential
			if minted {
				value := cell.value
				credential = MintedCredential("Authorization", "test token", 0, func(context.Context) (string, error) { return value, nil })
			} else {
				credential = StaticCredential("Authorization", "test token", cell.value)
			}
			credential.BindOrg(org)
			request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://api/x", nil)
			err := credential.Apply(context.Background(), request)
			if (err == nil) != cell.accept {
				t.Errorf("%s (minted=%v): Apply err = %v, want accept=%v", cell.name, minted, err, cell.accept)
			}
			if err != nil {
				if !errors.Is(err, ErrCredentialNamesAnotherOrg) {
					t.Errorf("%s (minted=%v): err = %v, want ErrCredentialNamesAnotherOrg", cell.name, minted, err)
				}
				if request.Header.Get("Authorization") != "" {
					t.Errorf("%s (minted=%v): a refused value was set on the request", cell.name, minted)
				}
				if strings.Contains(err.Error(), cell.value) {
					t.Errorf("%s (minted=%v): the value was printed", cell.name, minted)
				}
			}
		}
	}
	unbound := StaticCredential("Authorization", "test token", "opaque-token")
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://api/x", nil)
	if err := unbound.Apply(context.Background(), request); err != nil {
		t.Fatalf("an unbound credential is checked: %v", err)
	}
}

func TestVerifyReferencePrincipalInputDomain(t *testing.T) {
	const org = "11111111-1111-4111-8111-111111111111"
	type reply struct {
		status        int
		server        string
		build         string
		impersonating bool
		body          string
	}
	good := reply{status: 200, server: ReferencePlaneServer, body: `{"org_id":"` + org + `"}`}
	cells := []struct {
		name   string
		reply  reply
		accept bool
	}{
		{"python answers the named org", good, true},
		{"server header upper case", reply{status: 200, server: "Uvicorn", body: good.body}, true},
		{"401 naming the org", reply{status: 401, server: ReferencePlaneServer, body: good.body}, false},
		{"500 naming the org", reply{status: 500, server: ReferencePlaneServer, body: good.body}, false},
		{"impersonation stamp", reply{status: 200, server: ReferencePlaneServer, impersonating: true, body: good.body}, false},
		{"no server header", reply{status: 200, body: good.body}, false},
		{"another server", reply{status: 200, server: "nginx", body: good.body}, false},
		{"query-api build header", reply{status: 200, server: ReferencePlaneServer, build: "abc", body: good.body}, false},
		{"other org", reply{status: 200, server: ReferencePlaneServer, body: `{"org_id":"22222222-2222-4222-8222-222222222222"}`}, false},
		{"org absent", reply{status: 200, server: ReferencePlaneServer, body: `{"id":"u"}`}, false},
		{"org null", reply{status: 200, server: ReferencePlaneServer, body: `{"org_id":null}`}, false},
		{"org a number", reply{status: 200, server: ReferencePlaneServer, body: `{"org_id":7}`}, false},
		{"body not JSON", reply{status: 200, server: ReferencePlaneServer, body: `<html>`}, false},
		{"body empty", reply{status: 200, server: ReferencePlaneServer, body: ``}, false},
	}
	for _, cell := range cells {
		var paths []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			if cell.reply.server != "" {
				w.Header().Set("Server", cell.reply.server)
			}
			if cell.reply.build != "" {
				w.Header().Set(buildHeader, cell.reply.build)
			}
			if cell.reply.impersonating {
				w.Header().Set(impersonationHeader, "true")
			}
			w.WriteHeader(cell.reply.status)
			_, _ = w.Write([]byte(cell.reply.body))
		}))
		credential := StaticCredential("Authorization", "edge token", claimsJWT(t, map[string]any{"org_id": org})).BindOrg(org)
		err := VerifyReferencePrincipal(context.Background(), NewLegClient(0), server.URL+"/", credential, org)
		server.Close()
		if (err == nil) != cell.accept {
			t.Errorf("%s: err = %v, want accept=%v", cell.name, err, cell.accept)
		}
		if err != nil && !errors.Is(err, ErrReferencePrincipal) {
			t.Errorf("%s: err = %v, want ErrReferencePrincipal", cell.name, err)
		}
		if len(paths) != 1 || paths[0] != ReferencePrincipalPath {
			t.Errorf("%s: requested %v, want exactly [%s]", cell.name, paths, ReferencePrincipalPath)
		}
	}

	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	credential := StaticCredential("Authorization", "edge token", claimsJWT(t, map[string]any{"org_id": org})).BindOrg(org)
	if err := VerifyReferencePrincipal(context.Background(), NewLegClient(0), deadURL, credential, org); !errors.Is(err, ErrReferencePrincipal) {
		t.Fatalf("transport failure: err = %v, want ErrReferencePrincipal", err)
	}
	wrongOrgCredential := StaticCredential("Authorization", "edge token", claimsJWT(t, map[string]any{"org_id": "other"})).BindOrg(org)
	if err := VerifyReferencePrincipal(context.Background(), NewLegClient(0), deadURL, wrongOrgCredential, org); !errors.Is(err, ErrReferencePrincipal) || !errors.Is(err, ErrCredentialNamesAnotherOrg) {
		t.Fatalf("credential naming another org: err = %v, want both ErrReferencePrincipal and ErrCredentialNamesAnotherOrg", err)
	}
}

// TestRESTAdmitLegIdentityInputDomain enumerates every identity input of
// the two legs and pins that an admission happens exactly when the
// baseline is positively the Python app, no leg was served under an
// impersonation, and a forwarder endpoint's baseline is not a 200.
func TestRESTAdmitLegIdentityInputDomain(t *testing.T) {
	const build = "abc123"
	servers := []string{"uvicorn", "UVICORN", " uvicorn ", "", "nginx", "uvicorn/0.30"}
	statuses := []int{200, 422}
	count := 0
	for _, server := range servers {
		for _, baselineBuild := range []string{"", build} {
			for _, candidateImp := range []bool{false, true} {
				for _, baselineImp := range []bool{false, true} {
					for _, forwarder := range []bool{false, true} {
						for _, status := range statuses {
							count++
							in := RESTAdmissionInput{
								NamedBuild:          build,
								WantCandidateStatus: status, WantBaselineStatus: status,
								PythonForwarder: forwarder,
								Candidate:       RESTLeg{StatusCode: status, Body: []byte(`{}`), Build: build, Impersonating: candidateImp},
								Baseline:        RESTLeg{StatusCode: status, Body: []byte(`{}`), Build: baselineBuild, Server: server, Impersonating: baselineImp},
							}
							got := RESTAdmit(in, true)
							pythonIdentity := strings.EqualFold(strings.TrimSpace(server), "uvicorn") && baselineBuild == ""
							want := pythonIdentity && !candidateImp && !baselineImp && !(forwarder && status == 200)
							if got.Admitted != want {
								t.Errorf("server=%q baselineBuild=%q imp=%v/%v forwarder=%v status=%d: admitted=%v (%s), want %v",
									server, baselineBuild, candidateImp, baselineImp, forwarder, status, got.Admitted, got.Reason, want)
							}
							var wantReason string
							switch {
							case candidateImp || baselineImp:
								wantReason = RESTRefusalServedUnderImpersonation
							case !pythonIdentity:
								wantReason = RESTRefusalBaselineNotReferencePlane
							case forwarder && status == 200:
								wantReason = RESTRefusalBaselineMayBeRelayed
							}
							if got.Reason != wantReason {
								t.Errorf("server=%q baselineBuild=%q imp=%v/%v forwarder=%v status=%d: reason %q, want %q",
									server, baselineBuild, candidateImp, baselineImp, forwarder, status, got.Reason, wantReason)
							}
						}
					}
				}
			}
		}
	}
	if count != 6*2*2*2*2*2 {
		t.Fatalf("enumerated %d cells", count)
	}
}

func TestAdmitRefusesALegServedUnderImpersonation(t *testing.T) {
	for _, legs := range [][2]bool{{true, false}, {false, true}, {true, true}} {
		in := AdmissionInput{
			Candidate: Observation{Plane: "go", Impersonating: legs[0]},
			Baseline:  Observation{Plane: "python", Impersonating: legs[1]},
		}
		if got := admitPlanes(in); got.Admitted || got.Reason != RefusalServedUnderImpersonation {
			t.Errorf("impersonating candidate=%v baseline=%v: %+v, want %q", legs[0], legs[1], got, RefusalServedUnderImpersonation)
		}
	}
	if got := admitPlanes(AdmissionInput{Candidate: Observation{Plane: "go"}, Baseline: Observation{Plane: "python"}}); !got.Admitted {
		t.Fatalf("control: %+v", got)
	}
}

func TestServedUnderImpersonationReadsThePresenceOfTheStamp(t *testing.T) {
	for value, want := range map[string]bool{"true": true, "false": true, "": true} {
		header := http.Header{}
		header["X-Impersonating"] = []string{value}
		if got := ServedUnderImpersonation(header); got != want {
			t.Errorf("x-impersonating %q: %v, want %v", value, got, want)
		}
	}
	if ServedUnderImpersonation(http.Header{}) {
		t.Fatal("an absent stamp read as present")
	}
}

// TestPythonForwarderEndpointsAreExactlyTheForwardedRoutes pins the set
// of corpus endpoints the Python app can forward to query-api. The Python
// source has one REST forwarder, investment_explain_dispatcher.py, called
// from the POST /api/v1/investment/explain handler in api/main.py.
func TestPythonForwarderEndpointsAreExactlyTheForwardedRoutes(t *testing.T) {
	var got []string
	for key, spec := range restEndpointSpecs {
		if spec.PythonForwarder {
			got = append(got, key)
		}
	}
	sort.Strings(got)
	want := []string{"REST:POST:/api/v1/investment/explain"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("PythonForwarder endpoints = %v, want %v", got, want)
	}
}

// TestRunnerRefusesALegTheEdgeServedUnderImpersonation drives the stamp
// from a real response through Runner.post to the outcome, on each leg.
func TestRunnerRefusesALegTheEdgeServedUnderImpersonation(t *testing.T) {
	for _, leg := range []string{"baseline", "candidate"} {
		edge := &fakeEdge{goBody: `{"data":{"featureFlags":[]}}`, pythonBody: `{"data":{"featureFlags":[]}}`}
		stamp := map[string]string{"x-impersonating": "true"}
		if leg == "baseline" {
			edge.pyHeaders = stamp
		} else {
			edge.goHeaders = stamp
		}
		runner := newRunner(t, edge, "canary")
		// Run also reports that nothing was executed; the outcome is what
		// names why.
		outcomes, _, _ := runner.Run(context.Background())
		if len(outcomes) == 0 {
			t.Fatalf("%s: no outcome", leg)
		}
		if outcomes[0].Executed || outcomes[0].RefusalReason != RefusalServedUnderImpersonation {
			t.Errorf("%s stamped: executed=%v reason=%q, want a %q refusal", leg, outcomes[0].Executed, outcomes[0].RefusalReason, RefusalServedUnderImpersonation)
		}
	}
	control := newRunner(t, &fakeEdge{goBody: `{"data":{"featureFlags":[]}}`, pythonBody: `{"data":{"featureFlags":[]}}`}, "canary")
	outcomes, _, err := control.Run(context.Background())
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if outcomes[0].RefusalReason == RefusalServedUnderImpersonation {
		t.Fatalf("control refused as impersonated: %+v", outcomes[0])
	}
	if outcomes[0].Baseline == nil || outcomes[0].Baseline.WireAttempts != 1 || outcomes[0].Candidate == nil || outcomes[0].Candidate.WireAttempts != 1 {
		t.Fatalf("control: wire attempts not recorded on both observations: %+v", outcomes[0])
	}
}
