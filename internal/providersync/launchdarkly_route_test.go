package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestLaunchDarklyNormalizationMatchesLivePythonFunctions(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_launchdarkly_normalization_oracle.py"),
		filepath.Join(
			packageDir, "..", "..", "src", "dev_health_ops",
			"processors", "launchdarkly.py",
		),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python LaunchDarkly oracle: %v: %s", err, output)
	}
	var want struct {
		Flags  []launchDarklyFlagRow  `json:"flags"`
		Events []launchDarklyEventRow `json:"events"`
	}
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python LaunchDarkly oracle: %v: %s", err, output)
	}
	normalizedAt := time.Date(
		2026, 7, 23, 12, 34, 56, 789000000, time.UTC,
	)
	flags, err := normalizeLaunchDarklyFlags(
		[]json.RawMessage{
			json.RawMessage(`{"key":"checkout","_projectKey":"payments","kind":"multivariate","creationDate":1725000000123}`),
			json.RawMessage(`{"key":"search"}`),
		},
		"org-acme", "production", normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	events, err := normalizeLaunchDarklyEvents(
		[]json.RawMessage{
			json.RawMessage(`{"_id":"event-1","kind":"toggleFlag","date":1725000001234,"member":{"email":"operator@example.test"},"target":{"resources":["proj/payments:env/prod:flag/checkout"]}}`),
			json.RawMessage(`{"_id":"event-2","kind":"customKind","name":"search","date":"2026-07-22T01:02:03Z","member":{"_id":"member-2"}}`),
		},
		"org-acme", "production", normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(flags, want.Flags) {
		t.Fatalf("flags=%+v want=%+v", flags, want.Flags)
	}
	if !reflect.DeepEqual(events, want.Events) {
		t.Fatalf("events=%+v want=%+v", events, want.Events)
	}
}

func TestLaunchDarklyRouteFetchesCompleteUnitAndUsesEventReadbackPolicy(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 23, 12, 34, 56, 789000000, time.UTC)
	doer := &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
		{status: http.StatusOK, body: `{"items":[{"key":"checkout","_projectKey":"payments","creationDate":1725000000123}],"totalCount":1}`},
		{status: http.StatusOK, body: `{"items":[{"_id":"event-1","kind":"toggleFlag","date":1725000001234,"target":{"resources":["proj/payments:env/prod:flag/checkout"]}}]}`},
		{status: http.StatusOK, body: `{"items":[]}`},
	}}
	client, err := providerfoundation.NewHTTPClient(
		"launchdarkly",
		"https://app.launchdarkly.com",
		doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	claim.DatasetOptions = map[string]any{
		"project_key": "payments", "environment": "production",
	}
	handler := LaunchDarklyRouteHandler{
		CodeReferences: staticLaunchDarklyReferenceResolver{},
	}
	batch, err := handler.Collect(
		context.Background(),
		claim,
		providerfoundation.Credential{
			Provider: "launchdarkly", Config: map[string]string{},
		},
		client,
		normalizedAt,
	)
	if err != nil {
		t.Fatalf("collect error=%v requests=%v", err, requestURLs(doer.requests))
	}
	descriptor, _ := (CompleteRouteSwitches{
		LaunchDarklyFeatureFlags: true,
	}).Descriptor("launchdarkly", "feature-flags")
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Requests != 3 || batch.Evidence.Pages != 2 ||
		batch.Evidence.Records != 6 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	policies := map[string]EffectRecoveryPolicy{}
	for _, effect := range batch.Effects {
		policies[effect.Destination] = effect.Recovery
	}
	if policies["feature_flag_event"] != EffectReadbackRequired ||
		policies["feature_flag"] != EffectReplaySafe ||
		policies["feature_flag_link"] != EffectReplaySafe ||
		policies["work_graph_edges"] != EffectReplaySafe {
		t.Fatalf("policies=%v", policies)
	}
	if doer.requests[0].URL.Path != "/api/v2/flags/payments" ||
		doer.requests[0].URL.Query().Get("limit") != "50" ||
		doer.requests[1].URL.Path != "/api/v2/auditlog" ||
		doer.requests[2].URL.Path != "/api/v2/code-refs/repositories" {
		t.Fatalf("requests=%v", requestURLs(doer.requests))
	}
}

func TestLaunchDarklyRouteKeepsCodeReferencesBestEffort(t *testing.T) {
	t.Parallel()
	doer := &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
		{status: http.StatusOK, body: `{"items":[],"totalCount":0}`},
		{status: http.StatusOK, body: `{"items":[]}`},
		{status: http.StatusForbidden, body: `{"message":"forbidden"}`},
	}}
	client, err := providerfoundation.NewHTTPClient(
		"launchdarkly",
		"https://app.launchdarkly.com",
		doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	claim.DatasetOptions = map[string]any{"project_key": "payments"}
	batch, err := (LaunchDarklyRouteHandler{
		CodeReferences: staticLaunchDarklyReferenceResolver{},
	}).Collect(
		context.Background(),
		claim,
		providerfoundation.Credential{Provider: "launchdarkly"},
		client,
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["code_references_error"] != "provider_request_failed" {
		t.Fatalf("result=%v", batch.Result)
	}
	if batch.Result["code_references_synced"] != 0 {
		t.Fatalf("result=%v", batch.Result)
	}
}

type staticLaunchDarklyReferenceResolver struct{}

func (staticLaunchDarklyReferenceResolver) ResolveLaunchDarklyCodeReferences(
	_ context.Context,
	claim Claim,
	projectKey string,
	_ json.RawMessage,
	normalizedAt time.Time,
) ([]launchDarklyLinkRow, []launchDarklyEdgeRow, error) {
	flagID := launchDarklyFeatureFlagID(
		claim.OrgID, "launchdarkly", projectKey, "checkout",
	)
	return []launchDarklyLinkRow{launchDarklyReferenceLink(
			claim.OrgID, "checkout", "file", "repo:file.go", normalizedAt,
		)}, []launchDarklyEdgeRow{newLaunchDarklyEdge(
			claim.OrgID, flagID, "feature_flag", "repo:file.go", "file",
			"guards", "11111111-1111-4111-8111-111111111111",
			"launchdarkly", launchDarklyCodeReferenceConfidence,
			"ld_code_ref:repo:main:file.go:L1", normalizedAt, normalizedAt,
		)}, nil
}

type launchDarklyRouteResponse struct {
	status int
	body   string
}

type launchDarklyRouteDoer struct {
	responses []launchDarklyRouteResponse
	requests  []*http.Request
}

func (doer *launchDarklyRouteDoer) Do(request *http.Request) (*http.Response, error) {
	if len(doer.requests) >= len(doer.responses) {
		return nil, errors.New("unexpected request")
	}
	doer.requests = append(doer.requests, request.Clone(request.Context()))
	response := doer.responses[len(doer.requests)-1]
	return &http.Response{
		StatusCode: response.status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func requestURLs(requests []*http.Request) []string {
	values := make([]string, len(requests))
	for index, request := range requests {
		values[index] = request.URL.String()
	}
	return values
}

var _ LaunchDarklyCodeReferenceResolver = staticLaunchDarklyReferenceResolver{}
