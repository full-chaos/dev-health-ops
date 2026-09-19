package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

const legContractOrg = "11111111-1111-4111-8111-111111111111"

// orgToken is an unsigned three-segment token naming org.
func orgToken(org string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"EdDSA"}`)) + "." + enc([]byte(`{"org_id":"`+org+`"}`)) + "." + enc([]byte("sig"))
}

// TestLegReadInvariantByEnumeration proves, through proveOneRESTRequest
// and the real LegClient, the invariant the leg-read contract exists for:
// a request is admitted -- and a receipt written -- only when the baseline
// leg is positively the Python app (`server: uvicorn`, no query-api build
// header), no leg was served under an impersonation, a forwarder
// endpoint's baseline is not a 200, every path-bound id is a path literal
// and both credentials name -org. Every combination of those inputs is
// run; a refused id or credential reaches neither server.
func TestLegReadInvariantByEnumeration(t *testing.T) {
	const build = "abc123def456"
	type cell struct {
		server, baselineBuild, stamp, boundID, credentialOrg string
		forwarder, attested                                  bool
		status                                               int
	}
	var cells []cell
	for _, server := range []string{goapiproof.ReferencePlaneServer, "", "nginx"} {
		for _, baselineBuild := range []string{"", build} {
			for _, stamp := range []string{"", "baseline", "candidate"} {
				for _, forwarder := range []bool{false, true} {
					for _, attested := range []bool{false, true} {
						for _, status := range []int{200, 422} {
							for _, boundID := range []string{"", "abcdef0123", "%61bcdef0123"} {
								for _, credentialOrg := range []string{legContractOrg, "22222222-2222-4222-8222-222222222222"} {
									cells = append(cells, cell{server, baselineBuild, stamp, boundID, credentialOrg, forwarder, attested, status})
								}
							}
						}
					}
				}
			}
		}
	}
	if len(cells) != 3*2*3*2*2*2*3*2 {
		t.Fatalf("enumerated %d cells", len(cells))
	}
	admitted := 0
	for _, c := range cells {
		var candidateHits, baselineHits atomic.Int32
		candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			candidateHits.Add(1)
			w.Header().Set("x-dev-health-build", build)
			if c.stamp == "candidate" {
				w.Header().Set("x-impersonating", "true")
			}
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(`{"teams":["a"]}`))
		}))
		baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			baselineHits.Add(1)
			if c.server != "" {
				w.Header().Set("Server", c.server)
			}
			if c.baselineBuild != "" {
				w.Header().Set("x-dev-health-build", c.baselineBuild)
			}
			if c.stamp == "baseline" {
				w.Header().Set("x-impersonating", "true")
			}
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(`{"teams":["a"]}`))
		}))
		f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: legContractOrg, recordedBy: "r", reviewEvidence: "e", timeout: 5 * time.Second, pythonForwarderOff: c.attested}
		spec := goapiproof.RESTEndpointSpec{Method: http.MethodPost, Path: "/api/v1/things", PythonForwarder: c.forwarder}
		request := goapiproof.RESTRequest{Name: "cell", WantCandidateStatus: c.status, WantBaselineStatus: c.status, BodyMode: goapiproof.RESTBodyModeJSON}
		var boundIDs map[string]string
		if c.boundID != "" {
			request.IDBindings = []goapiproof.RESTIDBinding{{Producer: "thing_id", PathParam: "thing_id"}}
			spec.Path = "/api/v1/things/" + c.boundID
			boundIDs = map[string]string{"thing_id": c.boundID}
		}
		credential := func() *goapiproof.Credential {
			return goapiproof.StaticCredential("Authorization", "token", orgToken(c.credentialOrg)).BindOrg(legContractOrg)
		}
		writer := &fakeReceiptWriter{}
		out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:POST:/api/v1/things", spec, request,
			credential(), credential(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, boundIDs)
		candidate.Close()
		baseline.Close()

		// The oracle names the one escaped id literally; it never asks
		// the function under test.
		idLiteral := c.boundID != "%61bcdef0123"
		orgNamed := c.credentialOrg == legContractOrg
		pythonIdentity := c.server == goapiproof.ReferencePlaneServer && c.baselineBuild == ""
		want := idLiteral && orgNamed && pythonIdentity && c.stamp == "" && !(c.forwarder && !c.attested && c.status == 200)

		got := err == nil && out.Admitted
		if got != want {
			t.Errorf("%+v: admitted=%v (err=%v refusal=%q), want %v", c, got, err, out.Refusal, want)
		}
		if got != (len(writer.receipts) == 1) || len(writer.receipts) > 1 {
			t.Errorf("%+v: admitted=%v but wrote %d receipt(s)", c, got, len(writer.receipts))
		}
		switch {
		case !idLiteral:
			if err != nil || out.Refusal != goapiproof.RESTRefusalBoundIDNotAPathLiteral {
				t.Errorf("%+v: err=%v refusal=%q, want %q", c, err, out.Refusal, goapiproof.RESTRefusalBoundIDNotAPathLiteral)
			}
		case !orgNamed:
			if !errors.Is(err, goapiproof.ErrCredentialNamesAnotherOrg) {
				t.Errorf("%+v: err=%v, want ErrCredentialNamesAnotherOrg", c, err)
			}
		}
		if (!idLiteral || !orgNamed) && candidateHits.Load()+baselineHits.Load() != 0 {
			t.Errorf("%+v: a refused id or credential reached a server (%d candidate, %d baseline requests)", c, candidateHits.Load(), baselineHits.Load())
		}
		if got {
			var evidence restReviewEvidence
			if err := json.Unmarshal([]byte(writer.receipts[0].ReviewEvidence), &evidence); err != nil {
				t.Errorf("%+v: review evidence: %v", c, err)
			}
			if evidence.PythonForwarderOffAttested != (c.forwarder && c.attested) {
				t.Errorf("%+v: receipt python_forwarder_off_attested=%v, want %v", c, evidence.PythonForwarderOffAttested, c.forwarder && c.attested)
			}
			admitted++
			if out.BaselineWireAttempts != 1 || out.CandidateWireAttempts != 1 {
				t.Errorf("%+v: wire attempts baseline=%d candidate=%d, want 1 each", c, out.BaselineWireAttempts, out.CandidateWireAttempts)
			}
		}
	}
	if admitted == 0 {
		t.Fatal("no cell was admitted: the enumeration proves nothing about the admitting path")
	}
	t.Logf("%d cells, %d admitted", len(cells), admitted)
}

// TestParseFlags_RefusesABaseURLThatWouldChangeTheRequest runs every
// base URL flag through the components that make the request sent
// differ from the one named.
func TestParseFlags_RefusesABaseURLThatWouldChangeTheRequest(t *testing.T) {
	good := map[string]string{
		"-query-api-url":  "http://query-api:8090",
		"-python-api-url": "http://api:8000",
		"-buildinfo-url":  "http://query-api:8090/buildinfo",
	}
	for _, name := range []string{"-query-api-url", "-python-api-url", "-buildinfo-url"} {
		for _, suffix := range []string{"/#frag", "/#", "/?x=1", "/?"} {
			args := []string{"prove", "-org", legContractOrg, "-recorded-by", "r", "-review-evidence", "e", "-dry-run",
				"-candidate-bearer-exec", `["true"]`, "-baseline-bearer-exec", `["true"]`, "-artifact-dir", t.TempDir()}
			for flagName, value := range good {
				if flagName == name {
					value += suffix
				}
				args = append(args, flagName, value)
			}
			resetFlagsForTest(t)
			restore := setOSArgs(t, args)
			_, err := parseFlags()
			restore()
			if !errors.Is(err, goapiproof.ErrBaseURLComponents) {
				t.Errorf("%s with %q: err = %v, want ErrBaseURLComponents", name, suffix, err)
			}
			if err != nil && strings.Contains(err.Error(), good[name]) {
				t.Errorf("%s: the value was printed: %v", name, err)
			}
		}
	}
	resetFlagsForTest(t)
	args := []string{"prove", "-org", legContractOrg, "-recorded-by", "r", "-review-evidence", "e", "-dry-run",
		"-candidate-bearer-exec", `["true"]`, "-baseline-bearer-exec", `["true"]`, "-artifact-dir", t.TempDir()}
	for flagName, value := range good {
		args = append(args, flagName, value)
	}
	restore := setOSArgs(t, args)
	defer restore()
	if _, err := parseFlags(); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// TestPrepareLegs_RefusesEveryPrincipalThatIsNotTheNamedOrg drives the
// checks run() makes before its first corpus request: each cell names a
// way the candidate or baseline principal differs from -org, and each
// must stop the run before any corpus request.
func TestPrepareLegs_RefusesEveryPrincipalThatIsNotTheNamedOrg(t *testing.T) {
	const build = "abc123def456"
	const other = "22222222-2222-4222-8222-222222222222"
	cells := []struct {
		name                      string
		candidateOrg, baselineOrg string
		meOrg                     string
		meImpersonating, meNotPy  bool
		want                      error
	}{
		{"candidate credential names another org", other, legContractOrg, legContractOrg, false, false, goapiproof.ErrCredentialNamesAnotherOrg},
		{"baseline credential names another org", legContractOrg, other, legContractOrg, false, false, goapiproof.ErrCredentialNamesAnotherOrg},
		{"python resolves the baseline credential to another org", legContractOrg, legContractOrg, other, false, false, goapiproof.ErrReferencePrincipal},
		{"python answers under an impersonation session", legContractOrg, legContractOrg, legContractOrg, true, false, goapiproof.ErrReferencePrincipal},
		{"the baseline URL is not the python app", legContractOrg, legContractOrg, legContractOrg, false, true, goapiproof.ErrReferencePrincipal},
		{"control", legContractOrg, legContractOrg, legContractOrg, false, false, nil},
	}
	for _, cell := range cells {
		query := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"service":"query-api","version":"v","commit":"` + build + `","build_time":"t","modified":false}`))
		}))
		python := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cell.meNotPy {
				w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			}
			if cell.meImpersonating {
				w.Header().Set("x-impersonating", "true")
			}
			_, _ = w.Write([]byte(`{"org_id":"` + cell.meOrg + `"}`))
		}))
		f := flags{queryAPIURL: query.URL, pythonAPIURL: python.URL, buildInfoURL: query.URL + "/buildinfo", org: legContractOrg, timeout: 5 * time.Second}
		candidate := goapiproof.StaticCredential("Authorization", "candidate", orgToken(cell.candidateOrg))
		baseline := goapiproof.StaticCredential("Authorization", "baseline", orgToken(cell.baselineOrg))
		var builds *goapiproof.ProverBuild
		var err error
		_ = captureStdout(t, func() {
			builds, err = prepareLegs(context.Background(), goapiproof.NewLegClient(0), f, candidate, baseline, version.Info{Commit: build})
		})
		query.Close()
		python.Close()
		if cell.want == nil {
			if err != nil || builds == nil || builds.Candidate != build {
				t.Errorf("%s: builds=%+v err=%v, want %q and no error", cell.name, builds, err, build)
			}
			continue
		}
		if !errors.Is(err, cell.want) {
			t.Errorf("%s: err = %v, want %v", cell.name, err, cell.want)
		}
	}
}
