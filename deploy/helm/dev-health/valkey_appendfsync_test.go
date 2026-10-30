package devhealth_test

import (
	"strings"
	"testing"
)

func TestValkeyAppendfsyncIsPinnedAfterAppendonly(t *testing.T) {
	requireHelmUnderCI(t)
	deployment, _ := valkeyDeployment(t)
	want := "--appendonly yes --appendfsync everysec --maxmemory 256mb --maxmemory-policy allkeys-lru"
	if got := strings.Join(valkeyArgs(deployment), " "); got != want {
		t.Errorf("valkey args = %q, want %q", got, want)
	}
}

func TestValkeyAppendfsyncFollowsTheValue(t *testing.T) {
	requireHelmUnderCI(t)
	deployment, _ := valkeyDeployment(t, "--set", "valkey.appendfsync=always")
	if got := valkeyArg(t, deployment, "--appendfsync"); got != "always" {
		t.Errorf("--appendfsync = %q, want always", got)
	}
}

func TestValkeyAppendfsyncRefusesOtherValues(t *testing.T) {
	requireHelmUnderCI(t)
	for name, value := range map[string]string{
		"no":             "no",
		"empty":          "",
		"case":           "EVERYSEC",
		"trailing space": "everysec ",
		"number":         "1",
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, "--set-string", "valkey.appendfsync="+value)
			if err == nil {
				t.Fatalf("the render accepted appendfsync %q; it must refuse", value)
			}
			if !strings.Contains(out, "valkey.appendfsync must be one of everysec, always") {
				t.Fatalf("the refusal does not say so: %.400s", out)
			}
		})
	}
}

func TestValkeyAppendfsyncStaysWithTheAclFile(t *testing.T) {
	requireHelmUnderCI(t)
	deployment, _ := valkeyDeployment(t, append([]string{
		"--set", "valkey.acl.enabled=true", "--set", "valkey.acl.secretName=valkey-acl-secret",
		"--set", "networkPolicy.enabled=true",
	}, authNetworkPolicy...)...)
	if got := valkeyArg(t, deployment, "--appendfsync"); got != "everysec" {
		t.Errorf("--appendfsync = %q with the ACL file on, want everysec", got)
	}
}
