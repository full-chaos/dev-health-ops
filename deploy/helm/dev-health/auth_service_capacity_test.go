package devhealth_test

import (
	"strings"
	"testing"
)

func authWith(extra ...string) []string {
	return append(append([]string{}, authBase...), extra...)
}

func TestAuthServiceCapacityDefaults(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if deployment == nil {
		t.Fatal("no auth-service Deployment")
	}
	if got := at(deployment, "spec", "replicas"); got != 2 {
		t.Errorf("replicas = %v, want 2", got)
	}
	container := at(deployment, "spec", "template", "spec", "containers", 0)
	for _, want := range []struct {
		path []any
		val  string
	}{
		{[]any{"resources", "requests", "cpu"}, "250m"},
		{[]any{"resources", "limits", "cpu"}, "1"},
		{[]any{"resources", "requests", "memory"}, "64Mi"},
		{[]any{"resources", "limits", "memory"}, "256Mi"},
	} {
		got := at(container, want.path...)
		if want.path[2] == "cpu" && want.path[1] == "limits" {
			if got != "1" && got != "1000m" && got != 1 {
				t.Errorf("%v = %v, want 1 CPU", want.path, got)
			}
			continue
		}
		if got != want.val {
			t.Errorf("%v = %v, want %s", want.path, got, want.val)
		}
	}
	if value, set := containerEnvValue(t, deployment, "AUTH_DATABASE_MAX_CONNS"); !set || value != "8" {
		t.Errorf("AUTH_DATABASE_MAX_CONNS = %q (set=%v), want 8", value, set)
	}
}

// Steady pool is replicas x maxConns; a rollout adds one surge pod
// (maxSurge 1), so the ceiling is (replicas + 1) x maxConns. The chart pins no
// auth database budget; this test fixes only the figures the chart itself owns.
func TestAuthServiceConnectionCeiling(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	value, _ := containerEnvValue(t, deployment, "AUTH_DATABASE_MAX_CONNS")
	if value != "8" {
		t.Fatalf("maxConns %q", value)
	}
	if at(deployment, "spec", "strategy", "rollingUpdate", "maxSurge") != 1 {
		t.Fatal("maxSurge is not 1: the surge ceiling in this test no longer holds")
	}
	if steady, surge := 2*8, 3*8; steady != 16 || surge != 24 {
		t.Fatalf("steady %d surge %d", steady, surge)
	}
}

func TestAuthServiceMaxConnsStrict(t *testing.T) {
	for name, set := range map[string][]string{
		"zero":       {"--set", "authService.database.maxConns=0"},
		"too large":  {"--set", "authService.database.maxConns=33"},
		"negative":   {"--set", "authService.database.maxConns=-1"},
		"fraction":   {"--set-json", "authService.database.maxConns=8.5"},
		"string":     {"--set-string", "authService.database.maxConns=8"},
		"bool":       {"--set", "authService.database.maxConns=true"},
		"null":       {"--set-json", "authService.database.maxConns=null"},
		"empty list": {"--set-json", "authService.database.maxConns=[]"},
	} {
		_, out, err := renderAuth(t, authWith(set...)...)
		if err == nil || !strings.Contains(out, "authService.database.maxConns must be a whole number from 1 to 32") {
			t.Errorf("%s: want the named refusal, err=%v out=%.300s", name, err, out)
		}
	}
	for _, ok := range [][]string{
		{"--set", "authService.database.maxConns=1"},
		{"--set", "authService.database.maxConns=32"},
		{"--set-json", "authService.database.maxConns=16"},
	} {
		docs, out, err := renderAuth(t, authWith(ok...)...)
		if err != nil {
			t.Errorf("%v: %v\n%.300s", ok, err, out)
			continue
		}
		want := strings.SplitN(ok[1], "=", 2)[1]
		if got, _ := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), "AUTH_DATABASE_MAX_CONNS"); got != want {
			t.Errorf("%v: env %q, want %s", ok, got, want)
		}
	}
}

func TestAuthServiceMaxConnsCannotBeSetThroughExtraEnv(t *testing.T) {
	for _, name := range []string{"AUTH_DATABASE_MAX_CONNS", " auth_database_max_conns "} {
		_, out, err := renderAuth(t, authWith("--set-json",
			`authService.extraEnv=[{"name":"`+name+`","value":"4"}]`)...)
		if err == nil || !strings.Contains(out, "authService.extraEnv must not set AUTH_DATABASE_MAX_CONNS") {
			t.Errorf("extraEnv %q rendered: err=%v out=%.300s", name, err, out)
		}
	}
}

func TestAuthServiceDisruptionBudgetFollowsReplicas(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	pdb := authDoc(docs, "PodDisruptionBudget", "a-dev-health-auth-service")
	if pdb == nil {
		t.Fatal("two replicas render no PodDisruptionBudget")
	}
	if at(pdb, "spec", "minAvailable") != 1 {
		t.Errorf("minAvailable = %v, want 1", at(pdb, "spec", "minAvailable"))
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	want := at(deployment, "spec", "selector", "matchLabels")
	got := at(pdb, "spec", "selector", "matchLabels")
	if len(want.(map[string]any)) == 0 || strings.Join(keysVals(want), ",") != strings.Join(keysVals(got), ",") {
		t.Errorf("PDB selector %v does not match the Deployment selector %v", got, want)
	}

	docs, out, err = renderAuth(t, authWith("--set", "authService.replicas=1")...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if authDoc(docs, "PodDisruptionBudget", "a-dev-health-auth-service") != nil {
		t.Error("one replica renders a PodDisruptionBudget that would block every node drain")
	}

	docs, _, _ = renderAuth(t)
	if authDoc(docs, "PodDisruptionBudget", "a-dev-health-auth-service") != nil || authDoc(docs, "Deployment", "a-dev-health-auth-service") != nil {
		t.Error("the dormant default renders auth-service objects")
	}
}

func keysVals(v any) []string {
	m, _ := v.(map[string]any)
	var out []string
	for k, val := range m {
		out = append(out, k+"="+toStr(val))
	}
	sortStrings(out)
	return out
}

func toStr(v any) string { s, _ := v.(string); return s }

func sortStrings(s []string) {
	for i := range s {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}
