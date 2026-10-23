package authconfig

import (
	"strings"
	"testing"
)

func TestAccessTokenClassDefaultsToHS256Compat(t *testing.T) {
	for name, set := range map[string]func(map[string]string){
		"unset":    func(map[string]string) {},
		"empty":    func(env map[string]string) { env[EnvAccessTokenClass] = "" },
		"explicit": func(env map[string]string) { env[EnvAccessTokenClass] = "hs256_compat" },
	} {
		env := sessionEnv(testAccessSecret, testSuccessorKey)
		set(env)
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil || cfg.Session.AccessClass != AccessClassHS256Compat {
			t.Errorf("%s: class %q, err %v; want hs256_compat", name, cfg.Session.AccessClass, err)
		}
	}
}

func eddsaEnv() map[string]string {
	env := sessionEnv(testAccessSecret, testSuccessorKey)
	env[EnvAccessTokenClass] = "eddsa"
	env[EnvTokenIssuerURL] = "https://auth.example.test"
	return env
}

func TestAccessTokenClassEdDSALoadsWithItsIssuerURLAndEitherUserAudience(t *testing.T) {
	for _, audience := range []string{"", "dev-health-api", "auth-service"} {
		env := eddsaEnv()
		if audience != "" {
			env[EnvAccessTokenAudience] = audience
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil || cfg.Session.AccessClass != AccessClassEdDSA || cfg.TokenIssuerURL != "https://auth.example.test" {
			t.Errorf("audience %q: class %q, issuer %q, err %v", audience, cfg.Session.AccessClass, cfg.TokenIssuerURL, err)
		}
	}
}

// badAccessClassValues is the refusal table of AUTH_ACCESS_TOKEN_CLASS; its
// row count is pinned so a value added or dropped is a decision.
var badAccessClassValues = []string{
	"hs256", "HS256_COMPAT", "EdDSA", "EDDSA", " eddsa", "eddsa ", "eddsa\n", "ed25519", "both", "hs256_compat,eddsa",
	"eddsa|hs256_compat", "true", "1", "none", "off",
}

func TestAccessTokenClassRefusesEveryOtherValue(t *testing.T) {
	if len(badAccessClassValues) != 15 {
		t.Fatalf("%d refusal rows, want 15", len(badAccessClassValues))
	}
	for _, value := range badAccessClassValues {
		env := eddsaEnv()
		env[EnvAccessTokenClass] = value
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil {
			t.Errorf("%q: Load accepted it", value)
			continue
		}
		if err.Error() != "AUTH_ACCESS_TOKEN_CLASS must be hs256_compat or eddsa" {
			t.Errorf("%q: error %q is not the fixed message (it must not echo the value)", value, err)
		}
	}
}

func TestAccessTokenClassEdDSARefusesWhatCannotIssue(t *testing.T) {
	cases := map[string]func(map[string]string){
		"no issuer URL":         func(env map[string]string) { delete(env, EnvTokenIssuerURL) },
		"blank issuer URL":      func(env map[string]string) { env[EnvTokenIssuerURL] = "  " },
		"query-api audience":    func(env map[string]string) { env[EnvAccessTokenAudience] = "query-api" },
		"unknown audience":      func(env map[string]string) { env[EnvAccessTokenAudience] = "dev-health-ops" },
		"audience wrong case":   func(env map[string]string) { env[EnvAccessTokenAudience] = "Dev-Health-API" },
		"trailing slash issuer": func(env map[string]string) { env[EnvTokenIssuerURL] = "https://auth.example.test/" },
	}
	for name, mutate := range cases {
		env := eddsaEnv()
		mutate(env)
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
			t.Errorf("%s: Load accepted it", name)
		}
	}
}

func TestAccessTokenClassEdDSARefusesWhitespaceAroundIssAndAud(t *testing.T) {
	for _, name := range []string{EnvTokenIssuerURL, EnvAccessTokenAudience} {
		base := map[string]string{EnvTokenIssuerURL: "https://auth.example.test", EnvAccessTokenAudience: "dev-health-api"}
		for _, pad := range []string{" %s", "%s ", "\t%s", "%s\t", "\n%s", "%s\n", " %s\n", "\r\n%s"} {
			env := eddsaEnv()
			env[name] = strings.ReplaceAll(pad, "%s", base[name])
			if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
				t.Errorf("%s=%q: Load accepted it", name, env[name])
			}
		}
	}
	env := eddsaEnv()
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || cfg.TokenIssuerURL != "https://auth.example.test" {
		t.Fatalf("an exact issuer was refused or changed: %q, %v", cfg.TokenIssuerURL, err)
	}
}

func TestHS256CompatKeepsAnyAudienceAsBefore(t *testing.T) {
	env := sessionEnv(testAccessSecret, testSuccessorKey)
	env[EnvAccessTokenAudience] = "go-api-legacy-audience"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || cfg.Session.AccessAudience != "go-api-legacy-audience" {
		t.Fatalf("audience %q, err %v", cfg.Session.AccessAudience, err)
	}
}
