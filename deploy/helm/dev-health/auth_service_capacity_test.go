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
	if got := maxConnsArg(t, deployment); got != "8" {
		t.Errorf("--database-max-conns = %q, want 8", got)
	}
	if _, set := containerEnvValue(t, deployment, "AUTH_DATABASE_MAX_CONNS"); set {
		t.Error("the pool bound is also set as an environment variable: the service warns for settings that arrive only through the environment")
	}
}

// Steady pool is replicas x maxConns; a rollout adds one surge pod (planning figure, terminating pods can overlap it)
// (maxSurge 1), so the ceiling is (replicas + 1) x maxConns. The budget check
// is in auth_service_connection_budget_test.go; this test fixes the chart figures.
func TestAuthServiceConnectionCeiling(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	value := maxConnsArg(t, deployment)
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
		if got := maxConnsArg(t, authDoc(docs, "Deployment", "a-dev-health-auth-service")); got != want {
			t.Errorf("%v: --database-max-conns %q, want %s", ok, got, want)
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

func maxConnsArg(t *testing.T, deployment map[string]any) string {
	t.Helper()
	args, _ := at(deployment, "spec", "template", "spec", "containers", 0, "args").([]any)
	var found []string
	for _, arg := range args {
		if text, _ := arg.(string); strings.HasPrefix(text, "--database-max-conns=") {
			found = append(found, strings.TrimPrefix(text, "--database-max-conns="))
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one --database-max-conns argument, got %v", found)
	}
	return found[0]
}

func TestAuthServiceMaxConnsCannotBeSetThroughExtraArgs(t *testing.T) {
	for _, arg := range []string{"--database-max-conns=4", "database-max-conns=4", "-database-max-conns=4", "--database-max-conns =4", " --database-max-conns\\t=4", "--database-max-conns 4", "--database-max-conns\\t4"} {
		_, out, err := renderAuth(t, authWith("--set-json", `authService.extraArgs=["`+arg+`"]`)...)
		if err == nil || !strings.Contains(out, "authService.extraArgs must not set") || !strings.Contains(out, "authService.database.maxConns") {
			t.Errorf("extraArgs %q rendered: err=%v out=%.300s", arg, err, out)
		}
	}
}

func TestAuthServiceSigningKeyFlagsCannotBeSetAsOneSpaceSeparatedArg(t *testing.T) {
	for _, arg := range []string{"--signing-key-file /x", "--signing-key-id\\tx", "--signing-keyset-file /x"} {
		_, out, err := renderAuth(t, authWith("--set-json", `authService.extraArgs=["`+arg+`"]`)...)
		if err == nil || !strings.Contains(out, "authService.extraArgs must not set") {
			t.Errorf("extraArgs %q rendered: err=%v out=%.300s", arg, err, out)
		}
	}
}
