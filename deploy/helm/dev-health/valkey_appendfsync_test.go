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
			if !strings.Contains(out, "/valkey/appendfsync") {
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

func TestValkeyAppendfsyncIsCheckedWhenValkeyIsDisabled(t *testing.T) {
	requireHelmUnderCI(t)
	for name, args := range map[string][]string{
		"no":             {"--set-string", "valkey.appendfsync=no"},
		"no plain":       {"--set", "valkey.appendfsync=no"},
		"empty":          {"--set-string", "valkey.appendfsync="},
		"EVERYSEC":       {"--set-string", "valkey.appendfsync=EVERYSEC"},
		"ALWAYS":         {"--set-string", "valkey.appendfsync=ALWAYS"},
		"Everysec":       {"--set-string", "valkey.appendfsync=Everysec"},
		"trailing space": {"--set-string", "valkey.appendfsync=everysec "},
		"integer 1":      {"--set", "valkey.appendfsync=1"},
		"integer 0":      {"--set", "valkey.appendfsync=0"},
		"float":          {"--set", "valkey.appendfsync=1.5"},
		"numeric string": {"--set-string", "valkey.appendfsync=1"},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, append([]string{"--set", "valkey.enabled=false"}, args...)...)
			if err == nil {
				t.Fatalf("the render accepted %v with valkey.enabled=false; it must refuse", args)
			}
			if !strings.Contains(out, "appendfsync") {
				t.Fatalf("the refusal does not name the key: %.400s", out)
			}
		})
	}
}

func TestValkeyAppendfsyncValidValuesRenderWhenValkeyIsDisabled(t *testing.T) {
	requireHelmUnderCI(t)
	for _, value := range []string{"everysec", "always"} {
		if _, out, err := renderAuth(t, "--set", "valkey.enabled=false", "--set", "valkey.appendfsync="+value); err != nil {
			t.Errorf("appendfsync=%s with valkey.enabled=false was refused: %.400s", value, out)
		}
	}
}
