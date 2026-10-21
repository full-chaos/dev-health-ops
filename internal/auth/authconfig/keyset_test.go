package authconfig

import (
	"fmt"
	"strings"
	"testing"
)

func keySetEnv() map[string]string {
	env := baseEnv()
	delete(env, EnvSigningKeyFile)
	delete(env, EnvSigningKeyID)
	env[EnvSigningKeySetFile] = "/run/auth/signing/keyset.json"
	return env
}

func TestKeySetManifestReplacesTheSingleKey(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(keySetEnv())})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SigningKeySetPath != "/run/auth/signing/keyset.json" || cfg.SigningKeyPath != "" || cfg.SigningKeyID != "" {
		t.Fatalf("resolved keyset %q path %q kid %q", cfg.SigningKeySetPath, cfg.SigningKeyPath, cfg.SigningKeyID)
	}
	for _, other := range []string{EnvSigningKeyFile, EnvSigningKeyID} {
		env := keySetEnv()
		env[other] = "x"
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvSigningKeySetFile) {
			t.Errorf("%s beside %s: %v, want a refusal naming the key set", other, EnvSigningKeySetFile, err)
		}
	}
	env := keySetEnv()
	env[EnvSigningKeyDirect] = "-----BEGIN PRIVATE KEY-----"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvSigningKeyDirect) {
		t.Errorf("a direct-value key beside the key set: %v", err)
	}
}

func TestSafeAttrsReportTheKeySetWithoutItsPath(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(keySetEnv())})
	if err != nil {
		t.Fatal(err)
	}
	var rendered []string
	configured := false
	for _, attr := range cfg.SafeAttrs() {
		rendered = append(rendered, fmt.Sprint(attr))
		if attr.Key == "signing_keyset_configured" && attr.Value.Bool() {
			configured = true
		}
	}
	line := strings.Join(rendered, " ")
	if !configured || strings.Contains(line, "keyset.json") || strings.Contains(line, "/run/auth") {
		t.Fatalf("summary %s", line)
	}
}
