package authconfig

import (
	"log/slog"
	"strings"
	"testing"
)

const issueWorkloadSubject = "prn_AAAAAAAAAAAAAAAAAAAAAA"

func TestTheSessionIssueWorkloadPathIsOffByDefault(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SessionIssueWorkloadSubject != "" {
		t.Fatalf("subject %q without %s", cfg.SessionIssueWorkloadSubject, EnvSessionIssueWorkloadSubject)
	}
}

func TestTheSessionIssueWorkloadSubjectLoadsAndNeedsTheTokenIssuer(t *testing.T) {
	env := baseEnv()
	env[EnvSessionIssueWorkloadSubject] = issueWorkloadSubject
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SessionIssueWorkloadSubject != issueWorkloadSubject {
		t.Fatalf("subject %q", cfg.SessionIssueWorkloadSubject)
	}
	delete(env, EnvTokenIssuerURL)
	_, err = Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvSessionIssueWorkloadSubject) || !strings.Contains(err.Error(), EnvTokenIssuerURL) {
		t.Fatalf("a subject without an issuer was not refused by name: %v", err)
	}
	if strings.Contains(err.Error(), issueWorkloadSubject) {
		t.Fatalf("the error names the value: %v", err)
	}
}

func TestTheSessionIssueWorkloadSubjectIsStrictAndTheErrorNamesNoValue(t *testing.T) {
	for name, value := range map[string]string{
		"user uuid":        "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
		"no prefix":        "AAAAAAAAAAAAAAAAAAAAAA",
		"too short":        "prn_AAAA",
		"too long":         "prn_AAAAAAAAAAAAAAAAAAAAAAA",
		"padded":           "prn_AAAAAAAAAAAAAAAAAAAAAA==",
		"whitespace":       " " + issueWorkloadSubject,
		"trailing newline": issueWorkloadSubject + "\n",
		"non base64url":    "prn_AAAAAAAAAAAAAAAAAA+/AA",
		"upper prefix":     "PRN_AAAAAAAAAAAAAAAAAAAAAA",
		"two subjects":     issueWorkloadSubject + "," + issueWorkloadSubject,
		"fcwlb credential": "fcwlb_" + strings.Repeat("A", 43),
	} {
		env := baseEnv()
		env[EnvTokenIssuerURL] = "https://auth.example.com"
		env[EnvSessionIssueWorkloadSubject] = value
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), EnvSessionIssueWorkloadSubject) {
			t.Errorf("%s: not refused by name: %v", name, err)
			continue
		}
		if strings.Contains(err.Error(), strings.TrimSpace(value)) {
			t.Errorf("%s: the error names the value: %v", name, err)
		}
	}
}

func TestTheSessionIssueWorkloadSwitchIsLoggedAsABoolNeverTheSubject(t *testing.T) {
	env := baseEnv()
	env[EnvSessionIssueWorkloadSubject] = issueWorkloadSubject
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, attr := range cfg.SafeAttrs() {
		if strings.Contains(attr.Value.String(), issueWorkloadSubject) {
			t.Fatalf("log attribute %s carries the subject", attr.Key)
		}
		if attr.Key == "session_issue_workload_enabled" {
			seen = attr.Value.Kind() == slog.KindBool && attr.Value.Bool()
		}
	}
	if !seen {
		t.Fatal("session_issue_workload_enabled is not logged as true")
	}
}

// The variable is a declared option: --help names it, and it is reported as an
// environment-only setting when set.
func TestTheSessionIssueWorkloadSubjectIsADeclaredEnvOnlyOption(t *testing.T) {
	var declared bool
	for _, option := range Options() {
		if option.Env == EnvSessionIssueWorkloadSubject {
			declared = true
			if option.Secret || option.Flag != "" {
				t.Errorf("option %+v: want a plain env-only setting", option)
			}
		}
	}
	if !declared {
		t.Fatalf("%s is not in the option registry", EnvSessionIssueWorkloadSubject)
	}
	if !strings.Contains(HelpText(), EnvSessionIssueWorkloadSubject) {
		t.Fatalf("--help does not name %s", EnvSessionIssueWorkloadSubject)
	}
	env := baseEnv()
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	env[EnvSessionIssueWorkloadSubject] = issueWorkloadSubject
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	var listed bool
	for _, name := range cfg.EnvOnlySettings {
		listed = listed || name == EnvSessionIssueWorkloadSubject
	}
	if !listed {
		t.Fatalf("EnvOnlySettings %v does not list %s", cfg.EnvOnlySettings, EnvSessionIssueWorkloadSubject)
	}
}
