package authconfig

import (
	"strings"
	"testing"
)

func TestTheAcrEventsWorkloadSubjectLoadsAndNeedsTheTokenIssuer(t *testing.T) {
	env := baseEnv()
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AcrEventsWorkloadSubject != "" {
		t.Fatalf("subject %q when unset", cfg.AcrEventsWorkloadSubject)
	}
	env[EnvAcrEventsWorkloadSubject] = issueWorkloadSubject
	if cfg, err = Load(Spec{LookupEnv: lookupFrom(env)}); err != nil || cfg.AcrEventsWorkloadSubject != issueWorkloadSubject {
		t.Fatalf("subject %q, %v", cfg.AcrEventsWorkloadSubject, err)
	}
	env[EnvAcrEventsWorkloadSubject] = "not-a-principal"
	if _, err = Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvAcrEventsWorkloadSubject) || strings.Contains(err.Error(), "not-a-principal") {
		t.Fatalf("a bad subject: %v", err)
	}
	env[EnvAcrEventsWorkloadSubject] = issueWorkloadSubject
	delete(env, EnvTokenIssuerURL)
	if _, err = Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvTokenIssuerURL) {
		t.Fatalf("a subject with no issuer: %v", err)
	}
}
