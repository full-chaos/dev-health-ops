package restprove

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func serviceArgs(extra ...string) []string {
	return append([]string{
		"-python-api-url", "http://api:8000",
		"-candidate-bearer-exec", `["mint-edge-token"]`,
		"-baseline-bearer-exec", `["mint-edge-token"]`,
		"-org", "org-1",
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-artifact-dir", os.TempDir() + "/rest-prove-service-test-unused",
		"-dry-run",
	}, extra...)
}

func TestParseFlags_DefaultServiceIsQueryAPI(t *testing.T) {
	f, err := parseFlags(serviceArgs("-query-api-url", "http://query-api:8090"))
	if err != nil {
		t.Fatal(err)
	}
	if f.service != goapiproof.RESTServiceQueryAPI || f.candidateBase() != "http://query-api:8090" || !f.sendsEdgeCredentialLeg() {
		t.Fatalf("default run must behave as before: service=%q base=%q edgeLeg=%v", f.service, f.candidateBase(), f.sendsEdgeCredentialLeg())
	}
}

func TestParseFlags_DHOAPIServiceMeasuresTheDHOAPI(t *testing.T) {
	f, err := parseFlags(serviceArgs("-service", "dho-api", "-dho-api-url", "http://go-api:8000", "-query-api-url", "http://query-api:8090"))
	if err != nil {
		t.Fatal(err)
	}
	if f.candidateBase() != "http://go-api:8000" {
		t.Fatalf("candidate base = %q, want the dho api", f.candidateBase())
	}
	if f.buildInfoURL != "http://go-api:8000/buildinfo" {
		t.Fatalf("build identity must come from the service measured: %q", f.buildInfoURL)
	}
	if f.sendsEdgeCredentialLeg() {
		t.Fatal("the dho api authenticates one edge token on both legs; the edge-credential leg must not run")
	}
}

func TestParseFlags_DHOAPIServiceRequiresItsURLAndRefusesUnknownServices(t *testing.T) {
	if _, err := parseFlags(serviceArgs("-service", "dho-api")); err == nil || !strings.Contains(err.Error(), "-dho-api-url") {
		t.Fatalf("want a missing -dho-api-url refusal, got %v", err)
	}
	if _, err := parseFlags(serviceArgs("-service", "api")); err == nil {
		t.Fatal("an unknown -service must be refused")
	}
}

func TestPlanRESTRequestsForNeverMixesServices(t *testing.T) {
	queryPlan, err := planRESTRequestsFor(goapiproof.RESTServiceQueryAPI)
	if err != nil || len(queryPlan) == 0 {
		t.Fatalf("query-api plan: %d entries, %v", len(queryPlan), err)
	}
	for _, p := range queryPlan {
		if p.spec.EffectiveService() != goapiproof.RESTServiceQueryAPI {
			t.Fatalf("%s is not a query-api entry", p.operation)
		}
	}
	if legacy, _ := planRESTRequests(); len(legacy) != len(queryPlan) {
		t.Fatalf("planRESTRequests must stay the query-api plan: %d vs %d", len(legacy), len(queryPlan))
	}
}

func TestNotRunKeysNamesNoEdgeLegForADHOAPIEntry(t *testing.T) {
	plan := []plannedRequest{{
		operation: "REST:GET:/x",
		spec:      goapiproof.RESTEndpointSpec{Service: goapiproof.RESTServiceDHOAPI},
		request:   goapiproof.RESTRequest{Name: "r"},
	}}
	got := notRunKeys(plan, map[string]bool{})
	if len(got) != 1 || got[0] != "REST:GET:/x/r" {
		t.Fatalf("notRunKeys = %v; a dho-api entry never gets an edge-credential leg", got)
	}
}

func TestRunRefusesAServiceWithNoCorpusEntriesBeforeSendingAnything(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer server.Close()
	args := serviceArgs("-service", "dho-api", "-dho-api-url", server.URL)
	args = append(args, "-artifact-dir", t.TempDir())
	f, err := parseFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	f.service = "a-service-no-corpus-entry-targets"
	err = run(f)
	if err == nil || !strings.Contains(err.Error(), "would send nothing") {
		t.Fatalf("a run that plans nothing must fail loudly before any request, got %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("a run that plans nothing sent %d request(s)", hits.Load())
	}
}

func TestParseFlags_DHOAPIRefusesABuildInfoURLOnAnotherService(t *testing.T) {
	base := serviceArgs("-service", "dho-api", "-dho-api-url", "http://go-api:8000")
	if _, err := parseFlags(append(base, "-buildinfo-url", "http://query-api:8090/buildinfo")); err == nil || !strings.Contains(err.Error(), "-buildinfo-url") {
		t.Fatalf("a build identity read from another service must be refused, got %v", err)
	}
	if _, err := parseFlags(append(base, "-buildinfo-url", "https://go-api:8000/buildinfo")); err == nil {
		t.Fatal("a build identity read over another scheme than the measured service must be refused")
	}
	f, err := parseFlags(append(base, "-buildinfo-url", "http://go-api:8000/buildinfo"))
	if err != nil || f.buildInfoURL != "http://go-api:8000/buildinfo" {
		t.Fatalf("an explicit -buildinfo-url on the dho api host must be accepted: %v", err)
	}
	// query-api runs keep an explicit -buildinfo-url exactly as before.
	if _, err := parseFlags(serviceArgs("-buildinfo-url", "http://elsewhere:1/buildinfo")); err != nil {
		t.Fatalf("query-api run: %v", err)
	}
}

func TestParseFlags_DHOAPIRequiresEdgeTokenBearers(t *testing.T) {
	args := serviceArgs("-service", "dho-api", "-dho-api-url", "http://go-api:8000")
	for _, flagName := range []string{"-candidate-bearer-exec", "-baseline-bearer-exec"} {
		bad := append(append([]string(nil), args...), flagName, `["mint-envelope"]`)
		if _, err := parseFlags(bad); err == nil || !strings.Contains(err.Error(), "mint-edge-token") {
			t.Fatalf("%s running mint-envelope must be refused under dho-api, got %v", flagName, err)
		}
	}
}

func TestFinalRunReportFailsADHOAPIRunThatAdmittedNothing(t *testing.T) {
	refused := []outcome{{Operation: "REST:GET:/x", Request: "r", Refusal: goapiproof.RESTRefusalIDBindingUnresolved}}
	admitted := []outcome{{Operation: "REST:GET:/x", Request: "r", Admitted: true}}

	dho := flags{service: goapiproof.RESTServiceDHOAPI}
	if report, err := finalRunReport(dho, refused, nil, nil, nil, nil, nil); err == nil || report.ExitCause != exitCompletedWithNothingMeasured {
		t.Fatalf("all-refused dho-api run: exit_cause=%q err=%v, want a failure naming nothing measured", report.ExitCause, err)
	}
	if report, err := finalRunReport(dho, admitted, nil, nil, nil, nil, nil); err != nil || report.ExitCause != exitCompleted {
		t.Fatalf("a dho-api run with an admitted request must complete: %q %v", report.ExitCause, err)
	}
	// query-api keeps its per-case refusal semantics.
	if report, err := finalRunReport(flags{}, refused, nil, nil, nil, nil, nil); err != nil || report.ExitCause != exitCompleted {
		t.Fatalf("query-api run changed behaviour: %q %v", report.ExitCause, err)
	}
}

func TestCredentialsForSendsThePushTokenOnBothLegsOnlyForIngestEntries(t *testing.T) {
	push := goapiproof.StaticCredential("Authorization", "push bearer", "fcpush_x")
	runCand := goapiproof.StaticCredential("Authorization", "candidate bearer", "a.b.c")
	runBase := goapiproof.StaticCredential("Authorization", "baseline bearer", "d.e.f")

	c, b := credentialsFor(goapiproof.RESTEndpointSpec{Credential: goapiproof.RESTCredentialPushToken}, push, runCand, runBase)
	if c != push || b != push {
		t.Fatal("a push-token entry must send the push token on BOTH legs and never the run's bearers")
	}
	c, b = credentialsFor(goapiproof.RESTEndpointSpec{}, push, runCand, runBase)
	if c != runCand || b != runBase {
		t.Fatal("every other entry must keep the run's own bearers")
	}
}

func TestRunRefusesAPushTokenCorpusWithoutATokenFile(t *testing.T) {
	f, err := parseFlags(serviceArgs("-service", "dho-api", "-dho-api-url", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	f.pushTokenFile = ""
	err = run(f)
	if err == nil || !strings.Contains(err.Error(), "-push-token-file") {
		t.Fatalf("a dho-api run planning ingest entries needs -push-token-file, got %v", err)
	}
}

// TestResolveSingleShotRequestFillsPathLiteralsWithoutIDBindings drives the
// prover's own request path for an entry that declares PathLiterals and no
// IDBindings: the placeholder must be filled before either leg is sent (a
// literal "{placeholder}" on the wire is a 404/422, not a measurement).
func TestResolveSingleShotRequestFillsPathLiteralsWithoutIDBindings(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, paths := iteratingFixtureServers(t, build, nil, map[string]string{"/things/known/x": `[{"a":1}]`})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things/{name}/x"}
	request := goapiproof.RESTRequest{
		Name: "literal", PathLiterals: map[string]string{"name": "known"},
		WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeStatusOnly,
	}
	attempt, err := resolveSingleShotRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things/{name}/x",
		spec, request, map[string]string{},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.spec.Path != "/things/known/x" || !attempt.legsSent {
		t.Fatalf("resolved path = %q (legsSent %v), want /things/known/x with legs sent", attempt.spec.Path, attempt.legsSent)
	}
	for _, sent := range *paths {
		if strings.Contains(sent, "{") || strings.Contains(sent, "%7B") {
			t.Fatalf("a placeholder reached the wire: %q", sent)
		}
	}
}
