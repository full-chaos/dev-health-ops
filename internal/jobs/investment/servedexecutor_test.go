package investment

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
)

// Default OFF. With the list empty -- and also with the key present, and with
// the shadow switch on -- the executor builds no served backend: no client
// exists and no request is possible.
func TestWithTheServedSwitchOffNoServedBackendIsBuilt(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		org  string
	}{
		{"nothing set", nil, "org-a"},
		{"only the key", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue}, "org-a"},
		{"the shadow switch is not the served switch", map[string]string{
			"TYPESAFE_API_KEY": shadowTestKeyValue, EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*"}, "org-a"},
		{"another org on the list", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, EnvServedDecisionOrgIDs: "org-b"}, "org-a"},
		{"all orgs, run with no org", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, EnvServedDecisionOrgIDs: "*"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearShadowEnv(t)
			t.Setenv(EnvServedDecisionOrgIDs, "")
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			logs := &syncBuffer{}
			executor, recorder := shadowTestExecutor(t, logs)
			served, err := executor.newServed(tc.org)
			if served != nil || err != nil {
				t.Fatalf("a served backend was built, or an error returned: %v %v", served, err)
			}
			if recorder.count() != 0 || logs.String() != "" {
				t.Fatalf("an off switch must build nothing and say nothing:\n%s", logs.String())
			}
		})
	}
}

// With only the served switch on (the org list and the key) the executor
// builds the backend over a client for api.typesafe.ai and a categorization
// reaches a send.
func TestWithOnlyTheServedSwitchOnTheExecutorBuildsTheBackendAndReachesASend(t *testing.T) {
	clearShadowEnv(t)
	t.Setenv(EnvServedDecisionOrgIDs, "org-a")
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	logs := &syncBuffer{}
	executor, recorder := shadowTestExecutor(t, logs)
	served, err := executor.newServed("org-a")
	if err != nil || served == nil {
		t.Fatalf("no served backend was built: %v\n%s", err, logs.String())
	}
	defer func() { _ = served.Close() }()
	if served.stamp != decision.IdentityFor("").Stamp() {
		t.Fatalf("stamp = %q", served.stamp)
	}
	outcome, err := served.categorize(context.Background(), shadowTestConfig(), shadowTestEntries(t, "u1")[0])
	if err != nil || outcome.Status != categorize.StatusOK {
		t.Fatalf("categorize: %v status %q", err, outcome.Status)
	}
	if recorder.count() != 1 {
		t.Fatalf("sends = %d, want 1", recorder.count())
	}
	request := recorder.requests[0]
	if request.URL.String() != "https://api.typesafe.ai/v1/systemone" || request.Method != http.MethodPost {
		t.Fatalf("the send went to %s %s", request.Method, request.URL)
	}
	if request.Header.Get("Authorization") != "Bearer "+shadowTestKeyValue {
		t.Fatal("the send did not carry the configured key")
	}
}

// An org on the list whose backend cannot be built is an ERROR, never a silent
// run on the generative provider. The error names no key, URL or model value.
func TestAServedBackendThatCannotBeBuiltIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"no key", nil},
		{"a model alias", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_MODEL": "jev-latest"}},
		{"another host", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_BASE_URL": "https://example.invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearShadowEnv(t)
			t.Setenv(EnvServedDecisionOrgIDs, "*")
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			logs := &syncBuffer{}
			executor, recorder := shadowTestExecutor(t, logs)
			served, err := executor.newServed("org-a")
			if served != nil || err == nil {
				t.Fatalf("served %v err %v, want an error", served, err)
			}
			// (The alias rule names "jev-latest" itself, so it is not in this list.)
			for _, secret := range []string{shadowTestKeyValue, "example.invalid"} {
				if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
					t.Errorf("the error or a log line holds a configured value")
				}
			}
			if recorder.count() != 0 {
				t.Fatal("a request was sent")
			}
		})
	}
}
