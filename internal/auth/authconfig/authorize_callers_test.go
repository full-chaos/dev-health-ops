package authconfig

import (
	"slices"
	"strings"
	"testing"
)

const (
	authorizeCallerA = "prn_AAAAAAAAAAAAAAAAAAAAAA"
	authorizeCallerB = "prn_BBBBBBBBBBBBBBBBBBBBBB"
)

func TestTheAuthorizeCallerListIsEmptyByDefault(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.AuthorizeCallerSubjects) != 0 {
		t.Fatalf("callers %v without %s", cfg.AuthorizeCallerSubjects, EnvAuthorizeCallerSubjects)
	}
}

func TestTheAuthorizeCallerListLoadsAndNeedsTheTokenIssuer(t *testing.T) {
	env := baseEnv()
	env[EnvAuthorizeCallerSubjects] = authorizeCallerA + "," + authorizeCallerB
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.AuthorizeCallerSubjects, []string{authorizeCallerA, authorizeCallerB}) {
		t.Fatalf("callers %v", cfg.AuthorizeCallerSubjects)
	}
	delete(env, EnvTokenIssuerURL)
	_, err = Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvAuthorizeCallerSubjects) || !strings.Contains(err.Error(), EnvTokenIssuerURL) {
		t.Fatalf("callers without an issuer were not refused by name: %v", err)
	}
}

func TestTheAuthorizeCallerListIsStrictAndTheErrorNamesNoValue(t *testing.T) {
	for name, value := range map[string]string{
		"user uuid":      "6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e",
		"too short":      "prn_AAAA",
		"space":          authorizeCallerA + ", " + authorizeCallerB,
		"trailing comma": authorizeCallerA + ",",
		"leading comma":  "," + authorizeCallerA,
		"repeated":       authorizeCallerA + "," + authorizeCallerA,
		"upper prefix":   "PRN_AAAAAAAAAAAAAAAAAAAAAA",
	} {
		env := baseEnv()
		env[EnvTokenIssuerURL] = "https://auth.example.com"
		env[EnvAuthorizeCallerSubjects] = value
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), EnvAuthorizeCallerSubjects) {
			t.Errorf("%s: %v, want a refusal naming the variable", name, err)
			continue
		}
		if strings.Contains(err.Error(), "AAAAAAAAAAAAAAAAAAAAAA") || strings.Contains(err.Error(), "6f1c2d3e") {
			t.Errorf("%s: the error names a value: %v", name, err)
		}
	}
}
