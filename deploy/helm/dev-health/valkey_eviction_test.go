package devhealth_test

import (
	"strings"
	"testing"
)

func valkeyArg(t *testing.T, deployment map[string]any, flag string) string {
	t.Helper()
	args := valkeyArgs(deployment)
	for i, a := range args {
		if a == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	t.Fatalf("valkey args %v carry no %s", args, flag)
	return ""
}

func TestValkeyMaxmemoryPolicyFollowsTheValue(t *testing.T) {
	requireHelmUnderCI(t)
	deployment, _ := valkeyDeployment(t)
	if got := valkeyArg(t, deployment, "--maxmemory-policy"); got != "allkeys-lru" {
		t.Errorf("default --maxmemory-policy = %q, want the unchanged allkeys-lru", got)
	}
	for _, policy := range []string{
		"noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random",
		"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl",
	} {
		deployment, _ := valkeyDeployment(t, "--set", "valkey.maxmemoryPolicy="+policy, "--set", "valkey.maxmemory=192mb")
		if got := valkeyArg(t, deployment, "--maxmemory-policy"); got != policy {
			t.Errorf("--maxmemory-policy = %q, want %q", got, policy)
		}
		if got := valkeyArg(t, deployment, "--maxmemory"); got != "192mb" {
			t.Errorf("--maxmemory = %q, want 192mb", got)
		}
	}
}

func TestValkeyMaxmemoryPolicyRefusesUnknownValues(t *testing.T) {
	requireHelmUnderCI(t)
	for name, value := range map[string]string{
		"unknown":       "bogus",
		"empty":         "",
		"extra flag":    "allkeys-lru --save x",
		"case":          "Volatile-TTL",
		"trailing line": "volatile-ttl\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, "--set-string", "valkey.maxmemoryPolicy="+value)
			if err == nil {
				t.Fatalf("the render accepted maxmemoryPolicy %q; it must refuse", value)
			}
			if !strings.Contains(out, "valkey.maxmemoryPolicy must be one of") {
				t.Fatalf("the refusal does not say so: %.400s", out)
			}
		})
	}
}
