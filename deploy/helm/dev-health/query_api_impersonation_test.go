package devhealth_test

import (
	"strings"
	"testing"
)

// query-api reads go-api's AUTH_PROXY_IMPERSONATION from the same value, goApi.authProxy.impersonation: on, /graphql
// refuses the admin's own bearer beside a live impersonation row. Off, the query-api Deployment renders nothing new.

func queryAPIImpersonationArgs(phase string, sets ...string) []string {
	head := append([]string{"--set", "queryApi.enabled=true"}, roleEnv("devhealth_query")...)
	return impersonationArgs(phase, append(head, sets...)...)
}

func queryAPIEnvValue(t *testing.T, docs []map[string]any, name string) (string, bool) {
	t.Helper()
	return containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-query-api"), name)
}

func TestQueryAPIImpersonationSwitchFollowsTheGoAPIValue(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"2", "3", "4"} {
		docs, out, err := renderAuth(t, queryAPIImpersonationArgs(phase)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, out)
		}
		if value, ok := queryAPIEnvValue(t, docs, impersonationEnv); ok {
			t.Errorf("phase %s: the default render sets query-api %s=%q", phase, impersonationEnv, value)
		}
		_, offOut, err := renderAuth(t, queryAPIImpersonationArgs(phase, "--set", "goApi.authProxy.impersonation=false")...)
		if err != nil || offOut != out {
			t.Errorf("phase %s: impersonation=false differs from the default render (err %v)", phase, err)
		}
		onDocs, onOut, err := renderAuth(t, queryAPIImpersonationArgs(phase, "--set", "goApi.authProxy.impersonation=true")...)
		if err != nil {
			t.Fatalf("phase %s, on: render failed: %v\n%.400s", phase, err, onOut)
		}
		if value, ok := queryAPIEnvValue(t, onDocs, impersonationEnv); !ok || value != "true" {
			t.Errorf("phase %s, on: query-api %s=%q (set %v), want \"true\"", phase, impersonationEnv, value, ok)
		}
		if value := proxyEnv(t, onDocs)[impersonationEnv]; value != "true" {
			t.Errorf("phase %s, on: go-api %s=%q, want \"true\"", phase, impersonationEnv, value)
		}
		if extra := strings.Count(onOut, "name: "+impersonationEnv) - strings.Count(out, "name: "+impersonationEnv); extra != 2 {
			t.Errorf("phase %s, on: %d %s entries added, want 2 (go-api and query-api)", phase, extra, impersonationEnv)
		}
	}
}

// Below phase 2 the switch never renders for query-api (on, the render is refused); from phase 2 query-api's entry is
// go-api's, for every value: absent, false and true.
func TestQueryAPIImpersonationEqualsGoAPIByPhase(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"0", "1"} {
		below := func(sets ...string) []string {
			head := append(append([]string{}, authBase...), "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true", "--set", "authService.phase="+phase)
			return append(head, sets...)
		}
		docs, out, err := renderAuth(t, below()...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, out)
		}
		if value, ok := queryAPIEnvValue(t, docs, impersonationEnv); ok {
			t.Errorf("phase %s: query-api %s=%q, want absent", phase, impersonationEnv, value)
		}
		if _, out, err := renderAuth(t, below("--set", "goApi.authProxy.impersonation=true")...); err == nil {
			t.Errorf("phase %s: impersonation=true rendered; it must refuse\n%.400s", phase, out)
		}
	}
	for _, phase := range []string{"2", "3", "4"} {
		for _, sets := range [][]string{nil, {"--set", "goApi.authProxy.impersonation=false"}, {"--set", "goApi.authProxy.impersonation=true"}} {
			docs, out, err := renderAuth(t, queryAPIImpersonationArgs(phase, sets...)...)
			if err != nil {
				t.Fatalf("phase %s %v: render failed: %v\n%.400s", phase, sets, err, out)
			}
			queryValue, querySet := queryAPIEnvValue(t, docs, impersonationEnv)
			goValue, goSet := proxyEnv(t, docs)[impersonationEnv]
			if queryValue != goValue || querySet != goSet {
				t.Errorf("phase %s %v: query-api %s=%q (set %v), go-api %q (set %v); want equal", phase, sets, impersonationEnv, queryValue, querySet, goValue, goSet)
			}
		}
	}
}

func TestQueryAPIExtraEnvRefusesTheImpersonationSwitch(t *testing.T) {
	t.Parallel()

	for _, name := range []string{impersonationEnv, strings.ToLower(impersonationEnv), " " + impersonationEnv + " "} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, out, err := renderAuth(t, "--set", "queryApi.enabled=true", "--set-json", `queryApi.extraEnv=[{"name":"`+name+`","value":"true"}]`)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if want := "queryApi.extraEnv must not set AUTH_PROXY_IMPERSONATION"; !strings.Contains(out, want) {
				t.Fatalf("the refusal does not say %q: %.400s", want, out)
			}
		})
	}
}
