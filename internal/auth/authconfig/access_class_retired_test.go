package authconfig

import (
	"strings"
	"testing"
)

// The class default is eddsa; hs256_compat is a retired value, refused naming eddsa (CHAOS-13155).
func TestAccessTokenClassDefaultsToEdDSAAndRefusesTheRetiredValue(t *testing.T) {
	for name, set := range map[string]func(map[string]string){
		"unset": func(env map[string]string) { delete(env, EnvAccessTokenClass) },
		"empty": func(env map[string]string) { env[EnvAccessTokenClass] = "" },
	} {
		env := eddsaEnv()
		set(env)
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil || cfg.Session.AccessClass != AccessClassEdDSA {
			t.Errorf("%s: class %q, err %v; want eddsa", name, cfg.Session.AccessClass, err)
		}
	}
	env := eddsaEnv()
	env[EnvAccessTokenClass] = "hs256_compat"
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), "eddsa") || !strings.Contains(err.Error(), EnvAccessTokenClass) {
		t.Errorf("hs256_compat: err %v; want a refusal naming %s and eddsa", err, EnvAccessTokenClass)
	}
	env[EnvAccessTokenClass] = "junk"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Error("junk: Load accepted it")
	}
}

func TestAccessTokenClassUnsetNeedsTheIssuerURLLikeEdDSA(t *testing.T) {
	env := eddsaEnv()
	delete(env, EnvAccessTokenClass)
	delete(env, EnvTokenIssuerURL)
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvTokenIssuerURL) {
		t.Errorf("err %v; want the issuer URL refusal", err)
	}
}
