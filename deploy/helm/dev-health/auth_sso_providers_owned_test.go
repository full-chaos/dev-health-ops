package devhealth_test

import (
	"encoding/json"
	"strings"
	"testing"
)

const ssoProvidersEnv = "AUTH_SSO_PROVIDERS_OWNED"

var ssoProvidersJobs = []struct{ kind, name string }{
	{"Job", "a-dev-health-auth-migrate"},
	{"Job", "a-dev-health-auth-grants"},
	{"Deployment", "a-dev-health-auth-service"},
}

// Default and false render the switch nowhere: the grants Jobs carry no new variable, so the prod grants stay as they are.
func TestSSOProvidersOwnedFalseRendersNothing(t *testing.T) {
	t.Parallel()

	for _, extra := range [][]string{nil, {"--set", "authService.ssoProvidersOwned=false"}} {
		docs, out, err := renderAuth(t, phaseArgs(4, extra...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.400s", err, out)
		}
		for _, target := range ssoProvidersJobs {
			doc := authDoc(docs, target.kind, target.name)
			if doc == nil {
				t.Fatalf("no %s %s in the render", target.kind, target.name)
			}
			if value, set := containerEnvValue(t, doc, ssoProvidersEnv); set {
				t.Errorf("%s sets %s=%q; want it unset", target.name, ssoProvidersEnv, value)
			}
		}
		for _, doc := range docs {
			raw, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), ssoProvidersEnv) {
				t.Errorf("a rendered document names %s with the switch off", ssoProvidersEnv)
			}
		}
	}
}

// True renders AUTH_SSO_PROVIDERS_OWNED=true on the auth-migrate and auth-grants Jobs and on auth-service (the routes mount from it), and on no other workload.
func TestSSOProvidersOwnedTrueRendersOnTheGrantJobsAndAuthService(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.ssoProvidersOwned=true")...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, target := range ssoProvidersJobs {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			t.Fatalf("no %s %s in the render", target.kind, target.name)
		}
		if value, set := containerEnvValue(t, doc, ssoProvidersEnv); !set || value != "true" {
			t.Errorf("%s %s=%q set=%v; want true", target.name, ssoProvidersEnv, value, set)
		}
	}
	for _, other := range []struct{ kind, name string }{
		{"Deployment", "a-dev-health-go-api"}, {"Deployment", "a-dev-health-query-api"},
	} {
		if doc := authDoc(docs, other.kind, other.name); doc != nil {
			if _, set := containerEnvValue(t, doc, ssoProvidersEnv); set {
				t.Errorf("%s sets %s; only the grant Jobs and auth-service do", other.name, ssoProvidersEnv)
			}
		}
	}
}

func TestSSOProvidersOwnedMustBeABoolean(t *testing.T) {
	t.Parallel()

	_, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.ssoProvidersOwned=yes")...)
	if err == nil || !strings.Contains(out, "ssoProvidersOwned") {
		t.Fatalf("not a boolean rendered: err=%v out=%.300s", err, out)
	}
}
